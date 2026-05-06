package sing_vless

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"

	"github.com/metacubex/mihomo/adapter/inbound"
	"github.com/metacubex/mihomo/component/ca"
	"github.com/metacubex/mihomo/component/ech"
	C "github.com/metacubex/mihomo/constant"
	LC "github.com/metacubex/mihomo/listener/config"
	"github.com/metacubex/mihomo/listener/reality"
	"github.com/metacubex/mihomo/listener/sing"
	"github.com/metacubex/mihomo/ntp"
	"github.com/metacubex/mihomo/transport/gun"
	"github.com/metacubex/mihomo/transport/splithttp"
	"github.com/metacubex/mihomo/transport/vless/encryption"
	mihomoVMess "github.com/metacubex/mihomo/transport/vmess"

	"github.com/metacubex/http"
	"github.com/metacubex/http/h2c"
	"github.com/metacubex/sing/common"
	"github.com/metacubex/sing/common/metadata"
	"github.com/metacubex/tls"
	"golang.org/x/exp/slices"
)

func parseSplitHTTPRangeString(value string) (*splithttp.RangeConfig, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}

	parts := strings.Split(value, "-")
	switch len(parts) {
	case 1:
		n, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, err
		}
		return &splithttp.RangeConfig{From: n, To: n}, nil
	case 2:
		from, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, err
		}
		to, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			return nil, err
		}
		return &splithttp.RangeConfig{From: from, To: to}, nil
	default:
		return nil, errors.New("invalid splithttp range")
	}
}

type Listener struct {
	closed     bool
	config     LC.VlessServer
	listeners  []net.Listener
	service    *Service[string]
	decryption *encryption.ServerInstance
}

func New(config LC.VlessServer, tunnel C.Tunnel, additions ...inbound.Addition) (sl *Listener, err error) {
	if len(additions) == 0 {
		additions = []inbound.Addition{
			inbound.WithInName("DEFAULT-VLESS"),
			inbound.WithSpecialRules(""),
		}
	}
	h, err := sing.NewListenerHandler(sing.ListenerConfig{
		Tunnel:    tunnel,
		Type:      C.VLESS,
		Additions: additions,
		MuxOption: config.MuxOption,
	})
	if err != nil {
		return nil, err
	}

	service := NewService[string](h)
	service.UpdateUsers(
		common.Map(config.Users, func(it LC.VlessUser) string {
			return it.Username
		}),
		common.Map(config.Users, func(it LC.VlessUser) string {
			return it.UUID
		}),
		common.Map(config.Users, func(it LC.VlessUser) string {
			return it.Flow
		}))

	sl = &Listener{config: config, service: service}

	sl.decryption, err = encryption.NewServer(config.Decryption)
	if err != nil {
		return nil, err
	}
	if sl.decryption != nil {
		listener := sl
		defer func() { // decryption must be closed to avoid the goroutine leak
			if err != nil {
				_ = listener.decryption.Close()
				listener.decryption = nil
			}
		}()
	}

	tlsConfig := &tls.Config{Time: ntp.Now}
	var realityBuilder *reality.Builder
	var httpServer http.Server

	if config.Certificate != "" && config.PrivateKey != "" {
		certLoader, err := ca.NewTLSKeyPairLoader(config.Certificate, config.PrivateKey)
		if err != nil {
			return nil, err
		}
		tlsConfig.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return certLoader()
		}

		if config.EchKey != "" {
			err = ech.LoadECHKey(config.EchKey, tlsConfig)
			if err != nil {
				return nil, err
			}
		}
	}
	tlsConfig.ClientAuth = ca.ClientAuthTypeFromString(config.ClientAuthType)
	if len(config.ClientAuthCert) > 0 {
		if tlsConfig.ClientAuth == tls.NoClientCert {
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}
	if tlsConfig.ClientAuth == tls.VerifyClientCertIfGiven || tlsConfig.ClientAuth == tls.RequireAndVerifyClientCert {
		pool, err := ca.LoadCertificates(config.ClientAuthCert)
		if err != nil {
			return nil, err
		}
		tlsConfig.ClientCAs = pool
	}
	if config.RealityConfig.PrivateKey != "" {
		if tlsConfig.GetCertificate != nil {
			return nil, errors.New("certificate is unavailable in reality")
		}
		if tlsConfig.ClientAuth != tls.NoClientCert {
			return nil, errors.New("client-auth is unavailable in reality")
		}
		realityBuilder, err = config.RealityConfig.Build(tunnel)
		if err != nil {
			return nil, err
		}
	}
	if config.WsPath != "" {
		httpMux := http.NewServeMux()
		httpMux.HandleFunc(config.WsPath, func(w http.ResponseWriter, r *http.Request) {
			conn, err := mihomoVMess.StreamUpgradedWebsocketConn(w, r)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			sl.HandleConn(conn, tunnel, additions...)
		})
		httpServer.Handler = httpMux
		tlsConfig.NextProtos = append(tlsConfig.NextProtos, "http/1.1")
	}
	if config.GrpcServiceName != "" {
		httpServer.Handler = gun.NewServerHandler(gun.ServerOption{
			ServiceName: config.GrpcServiceName,
			ConnHandler: func(conn net.Conn) {
				sl.HandleConn(conn, tunnel, additions...)
			},
			HttpHandler: httpServer.Handler,
		})
		tlsConfig.NextProtos = append([]string{"h2"}, tlsConfig.NextProtos...) // h2 must before http/1.1
	}
	if config.XHTTPConfig.Mode != "" {
		switch config.XHTTPConfig.Mode {
		case "auto", "stream-up", "stream-one", "packet-up":
		default:
			return nil, errors.New("unsupported xhttp mode")
		}
	}
	if config.XHTTPConfig.Path != "" || config.XHTTPConfig.Host != "" || config.XHTTPConfig.Mode != "" {
		scStreamUpServerSecs, err := parseSplitHTTPRangeString(config.XHTTPConfig.ScStreamUpServerSecs)
		if err != nil {
			return nil, errors.New("invalid xhttp sc-stream-up-server-secs")
		}
		scMaxEachPostBytes, err := parseSplitHTTPRangeString(config.XHTTPConfig.ScMaxEachPostBytes)
		if err != nil {
			return nil, errors.New("invalid xhttp sc-max-each-post-bytes")
		}
		xPaddingBytes, err := parseSplitHTTPRangeString(config.XHTTPConfig.XPaddingBytes)
		if err != nil {
			return nil, errors.New("invalid xhttp x-padding-bytes")
		}
		uplinkChunkSize, err := parseSplitHTTPRangeString(config.XHTTPConfig.UplinkChunkSize)
		if err != nil {
			return nil, errors.New("invalid xhttp uplink-chunk-size")
		}
		scMaxBufferedPosts := 0
		if config.XHTTPConfig.ScMaxBufferedPosts != "" {
			scMaxBufferedPosts, err = strconv.Atoi(config.XHTTPConfig.ScMaxBufferedPosts)
			if err != nil {
				return nil, errors.New("invalid xhttp sc-max-buffered-posts")
			}
		}
		importSplithttpConfig := &splithttp.SplitHTTPConfig{
			Path:                 config.XHTTPConfig.Path,
			Host:                 config.XHTTPConfig.Host,
			Mode:                 config.XHTTPConfig.Mode,
			MaxConcurrentPosts:   100,
			XPaddingBytes:        xPaddingBytes,
			XPaddingObfsMode:     config.XHTTPConfig.XPaddingObfsMode,
			XPaddingKey:          config.XHTTPConfig.XPaddingKey,
			XPaddingHeader:       config.XHTTPConfig.XPaddingHeader,
			XPaddingPlacement:    config.XHTTPConfig.XPaddingPlacement,
			XPaddingMethod:       config.XHTTPConfig.XPaddingMethod,
			UplinkHTTPMethod:     config.XHTTPConfig.UplinkHTTPMethod,
			NoSSEHeader:          config.XHTTPConfig.NoSSEHeader,
			SessionPlacement:     config.XHTTPConfig.SessionPlacement,
			SessionKey:           config.XHTTPConfig.SessionKey,
			SeqPlacement:         config.XHTTPConfig.SeqPlacement,
			SeqKey:               config.XHTTPConfig.SeqKey,
			UplinkDataPlacement:  config.XHTTPConfig.UplinkDataPlacement,
			UplinkDataKey:        config.XHTTPConfig.UplinkDataKey,
			UplinkChunkSize:      uplinkChunkSize,
			ScStreamUpServerSec:  scStreamUpServerSecs,
			ScMaxBufferedPosts:   scMaxBufferedPosts,
			ScMaxEachPostBytes:   scMaxEachPostBytes,
			ServerMaxHeaderBytes: config.XHTTPConfig.ServerMaxHeaderBytes,
		}
		xhttpPath := importSplithttpConfig.GetNormalizedPath()
		splithttpServer := splithttp.NewSplitHTTPServer(importSplithttpConfig, func(conn net.Conn) {
			sl.HandleConn(conn, tunnel, additions...)
		})

		httpMux := http.NewServeMux()
		httpMux.Handle(xhttpPath, splithttpServer)
		trimmedPath := strings.TrimSuffix(xhttpPath, "/")
		if trimmedPath != "" {
			httpMux.Handle(trimmedPath, splithttpServer)
		}

		if httpServer.Handler != nil && xhttpPath != "/" {
			httpMux.Handle("/", httpServer.Handler)
		}
		httpServer.Handler = httpMux
		if !slices.Contains(tlsConfig.NextProtos, "h2") {
			tlsConfig.NextProtos = append([]string{"h2"}, tlsConfig.NextProtos...)
		}
		httpServer.MaxHeaderBytes = importSplithttpConfig.GetNormalizedServerMaxHeaderBytes()
	}
	if httpServer.Handler != nil {
		h2Server := &http.Http2Server{}
		if realityBuilder != nil || tlsConfig.GetCertificate == nil {
			// XHTTP needs h2c for plain HTTP/2 and for non-standard TLS wrappers like Reality.
			httpServer.Handler = h2c.NewHandler(httpServer.Handler, h2Server)
		}
		if err := http.Http2ConfigureServer(&httpServer, h2Server); err != nil {
			return nil, err
		}
	}
	for _, addr := range strings.Split(config.Listen, ",") {
		addr := addr

		//TCP
		l, err := inbound.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}
		if realityBuilder != nil {
			l = realityBuilder.NewListener(l)
		} else if tlsConfig.GetCertificate != nil {
			l = tls.NewListener(l, tlsConfig)
		} else if sl.decryption == nil {
			return nil, errors.New("disallow using Vless without any certificates/reality/decryption config")
		}
		sl.listeners = append(sl.listeners, l)

		go func() {
			if httpServer.Handler != nil {
				_ = httpServer.Serve(l)
				return
			}
			for {
				c, err := l.Accept()
				if err != nil {
					if sl.closed {
						break
					}
					continue
				}

				go sl.HandleConn(c, tunnel)
			}
		}()
	}

	return sl, nil
}

func (l *Listener) Close() error {
	l.closed = true
	var retErr error
	for _, lis := range l.listeners {
		err := lis.Close()
		if err != nil {
			retErr = err
		}
	}
	if l.decryption != nil {
		_ = l.decryption.Close()
	}
	return retErr
}

func (l *Listener) Config() string {
	return l.config.String()
}

func (l *Listener) AddrList() (addrList []net.Addr) {
	for _, lis := range l.listeners {
		addrList = append(addrList, lis.Addr())
	}
	return
}

func (l *Listener) HandleConn(conn net.Conn, tunnel C.Tunnel, additions ...inbound.Addition) {
	ctx := sing.WithAdditions(context.TODO(), additions...)
	if l.decryption != nil {
		var err error
		conn, err = l.decryption.Handshake(conn, nil)
		if err != nil {
			return
		}
	}
	err := l.service.NewConnection(ctx, conn, metadata.Metadata{
		Protocol: "vless",
		Source:   metadata.SocksaddrFromNet(conn.RemoteAddr()),
	})
	if err != nil {
		_ = conn.Close()
		return
	}
}
