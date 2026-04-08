package splithttp

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/metacubex/http"
	"github.com/metacubex/mihomo/component/resolver"
	"github.com/metacubex/mihomo/log"
	quic "github.com/metacubex/quic-go"
	"github.com/metacubex/quic-go/http3"
	"github.com/metacubex/tls"
)

func appendToPath(path string, sessionId string) string {
	if path == "" {
		return "/" + sessionId
	}
	if path[len(path)-1] == '/' {
		return path + sessionId
	}
	return path + "/" + sessionId
}

func getBaseRequest(ctx context.Context, method, urlStr string, body io.Reader, config *SplitHTTPConfig) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, urlStr, body)
	if err != nil {
		return nil, err
	}
	return req, nil
}

type WaitReadCloser struct {
	Wait chan struct{}
	io.ReadCloser
}

func (w *WaitReadCloser) Set(rc io.ReadCloser) {
	w.ReadCloser = rc
	defer func() {
		if recover() != nil {
			rc.Close()
		}
	}()
	close(w.Wait)
}

func (w *WaitReadCloser) Read(b []byte) (int, error) {
	if w.ReadCloser == nil {
		if <-w.Wait; w.ReadCloser == nil {
			return 0, io.ErrClosedPipe
		}
	}
	return w.ReadCloser.Read(b)
}

func (w *WaitReadCloser) Close() error {
	if w.ReadCloser != nil {
		return w.ReadCloser.Close()
	}
	defer func() {
		if recover() != nil && w.ReadCloser != nil {
			w.ReadCloser.Close()
		}
	}()
	close(w.Wait)
	return nil
}

type connTelemetry struct {
	localAddr  string
	remoteAddr string
}

func getConnTelemetry(c net.Conn) connTelemetry {
	baseConn := c
	meta := connTelemetry{}
	if baseConn != nil {
		if l := baseConn.LocalAddr(); l != nil {
			meta.localAddr = l.String()
		}
		if r := baseConn.RemoteAddr(); r != nil {
			meta.remoteAddr = r.String()
		}
	}
	return meta
}

func logRequest(config *SplitHTTPConfig, stage string, req *http.Request, sessionID string, seqStr string, meta connTelemetry) {
	if !config.RequestLog {
		return
	}
	log.Infoln("splithttp[%s] %s %s host=%s session=%s seq=%s meta={%s} local=%s remote=%s headers={%s}",
		stage,
		req.Method,
		req.URL.String(),
		req.Host,
		sessionID,
		seqStr,
		config.MetaSummary(req),
		meta.localAddr,
		meta.remoteAddr,
		config.HeaderSummary(req),
	)
}

func logResponse(config *SplitHTTPConfig, stage string, resp *http.Response, sessionID string, seqStr string, meta connTelemetry) {
	if !config.RequestLog || resp == nil {
		return
	}
	server := resp.Header.Get("Server")
	if server == "" {
		server = "Unknown"
	}
	cfRayPart := ""
	if cfRay := resp.Header.Get("Cf-Ray"); cfRay != "" {
		cfRayPart = " cf-ray=" + cfRay
	}
	log.Infoln("splithttp[%s-resp] status=%d session=%s seq=%s actual_http=%s local=%s remote=%s server=%s%s content-length=%s",
		stage,
		resp.StatusCode,
		sessionID,
		seqStr,
		resp.Proto,
		meta.localAddr,
		meta.remoteAddr,
		server,
		cfRayPart,
		resp.Header.Get("Content-Length"),
	)
}

func ensureHTTPProtocolAllowed(config *SplitHTTPConfig, resp *http.Response) error {
	if resp == nil {
		return nil
	}
	if config.IsHTTPProtoAllowed(resp.Proto) {
		return nil
	}
	return fmt.Errorf("splithttp protocol mismatch: actual=%s not allowed by alpn=%v", resp.Proto, config.ALPN)
}

func buildHTTP3Client(config *SplitHTTPConfig) *http.Client {
	tlsServerName := config.TLSServerName
	if tlsServerName == "" {
		tlsServerName = config.Host
	}
	tlsConf := &tls.Config{
		ServerName: tlsServerName,
		NextProtos: []string{"h3"},
	}
	quicConf := &quic.Config{
		KeepAlivePeriod: 15 * time.Second,
	}
	rt := &http3.Transport{
		TLSClientConfig: tlsConf,
		QUICConfig:      quicConf,
		Dial: func(ctx context.Context, _ string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
			udpAddrs, err := resolveH3DialUDPAddrs(ctx, config.DialAddr)
			if err != nil {
				return nil, err
			}

			if tlsCfg == nil {
				tlsCfg = tlsConf.Clone()
			} else {
				tlsCfg = tlsCfg.Clone()
			}
			if tlsCfg.ServerName == "" {
				tlsCfg.ServerName = tlsServerName
			}
			if len(tlsCfg.NextProtos) == 0 {
				tlsCfg.NextProtos = []string{"h3"}
			}

			var lastErr error
			for _, udpAddr := range udpAddrs {
				var conn net.PacketConn
				if config.H3PacketDial != nil {
					conn, err = config.H3PacketDial(ctx, udpAddr)
				} else {
					conn, err = net.ListenPacket("udp", ":0")
				}
				if err != nil {
					lastErr = err
					continue
				}

				quicConn, dialErr := quic.DialEarly(ctx, conn, udpAddr, tlsCfg, cfg)
				if dialErr == nil {
					return quicConn, nil
				}
				_ = conn.Close()
				lastErr = dialErr
			}

			if lastErr == nil {
				lastErr = fmt.Errorf("no reachable h3 endpoint for %s", config.DialAddr)
			}
			return nil, lastErr
		},
	}
	return &http.Client{Transport: rt}
}

func resolveH3DialUDPAddrs(ctx context.Context, dialAddr string) ([]*net.UDPAddr, error) {
	host, port, err := net.SplitHostPort(dialAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid splithttp dial addr %q: %w", dialAddr, err)
	}
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("invalid splithttp dial addr port %q: %w", port, err)
	}

	if ip := net.ParseIP(host); ip != nil {
		return []*net.UDPAddr{{IP: ip, Port: portNum}}, nil
	}

	ips, err := resolver.LookupIPWithResolver(ctx, host, resolver.ProxyServerHostResolver)
	if err != nil {
		return nil, fmt.Errorf("resolve splithttp h3 host %q failed: %w", host, err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("resolve splithttp h3 host %q got empty result", host)
	}

	ipv4s, ipv6s := resolver.SortationAddr(ips)
	addrs := make([]*net.UDPAddr, 0, len(ips))
	for _, ip := range ipv6s {
		addrs = append(addrs, &net.UDPAddr{IP: net.IP(ip.AsSlice()), Port: portNum})
	}
	for _, ip := range ipv4s {
		addrs = append(addrs, &net.UDPAddr{IP: net.IP(ip.AsSlice()), Port: portNum})
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("resolve splithttp h3 host %q produced no dialable ip", host)
	}
	return addrs, nil
}
