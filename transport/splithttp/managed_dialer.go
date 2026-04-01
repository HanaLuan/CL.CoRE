package splithttp

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"

	"github.com/metacubex/mihomo/common/utils"
)

type managedPacketWriter struct {
	ctx       context.Context
	url       string
	config    *SplitHTTPConfig
	sessionID string
	shared    *sharedClient
	seq       int64
	closed    bool
}

func (w *managedPacketWriter) Write(b []byte) (int, error) {
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	seqStr := strconv.FormatInt(w.seq, 10)
	w.seq++
	if err := w.shared.client.PostPacket(w.ctx, w.url, w.sessionID, seqStr, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (w *managedPacketWriter) Close() error {
	w.closed = true
	return nil
}

func candidateHTTPVersions(config *SplitHTTPConfig) []string {
	seen := map[string]bool{}
	out := make([]string, 0, 3)
	appendVersion := func(v string) {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}

	if config.TryQUIC && config.HasALPN("h3") {
		appendVersion("3")
	}
	if len(config.ALPN) == 0 || config.HasALPN("h2") || config.HasALPN("http/1.1") {
		appendVersion("2")
	}
	if config.HasALPN("http/1.1") || config.HasALPN("http/1.0") {
		appendVersion("1.1")
	}
	if len(out) == 0 {
		appendVersion("2")
	}
	return out
}

func sharedClientKey(config *SplitHTTPConfig, httpVersion string) string {
	return config.ClientKey + "|" + httpVersion
}

func openSharedStream(ctx context.Context, config *SplitHTTPConfig, httpVersion, url, sessionID string, body io.Reader, uploadOnly bool) (io.ReadCloser, net.Addr, net.Addr, *sharedClient, error) {
	key := sharedClientKey(config, httpVersion)
	shared := globalClientManager.acquire(key, func() DialerClient {
		return createHTTPClient(config, httpVersion)
	})

	reader, remoteAddr, localAddr, err := shared.client.OpenStream(ctx, url, sessionID, body, uploadOnly)
	if err != nil {
		shared.release()
		return nil, nil, nil, nil, err
	}
	return reader, remoteAddr, localAddr, shared, nil
}

func dialWithVersion(ctx context.Context, config *SplitHTTPConfig, httpVersion string) (net.Conn, error) {
	mode := parseMode(config)

	sessionID := ""
	if mode != "stream-one" {
		sessionID = utils.NewUUIDV4().String()
	}

	url := fmt.Sprintf("https://%s%s", config.Host, config.GetNormalizedPath())
	reader, writer := io.Pipe()

	var sharedUpload *sharedClient
	var sharedDownload *sharedClient
	var remoteAddr net.Addr
	var localAddr net.Addr
	var err error

	releaseAll := func() {
		if sharedUpload != nil {
			sharedUpload.release()
		}
		if sharedDownload != nil && sharedDownload != sharedUpload {
			sharedDownload.release()
		}
	}

	if mode == "stream-one" {
		var body io.ReadCloser
		body, remoteAddr, localAddr, sharedUpload, err = openSharedStream(ctx, config, httpVersion, url, sessionID, reader, false)
		if err != nil {
			_ = reader.Close()
			_ = writer.Close()
			return nil, err
		}
		return &managedConn{
			writer:     writer,
			reader:     body,
			remoteAddr: remoteAddr,
			localAddr:  localAddr,
			onClose:    releaseAll,
		}, nil
	}

	var downBody io.ReadCloser
	downBody, remoteAddr, localAddr, sharedDownload, err = openSharedStream(ctx, config, httpVersion, url, sessionID, nil, false)
	if err != nil {
		_ = reader.Close()
		_ = writer.Close()
		return nil, err
	}

	if mode == "stream-up" {
		sharedUpload = sharedDownload
		_, _, _, err = sharedUpload.client.OpenStream(ctx, url, sessionID, reader, true)
		if err != nil {
			_ = downBody.Close()
			_ = reader.Close()
			_ = writer.Close()
			releaseAll()
			return nil, err
		}
		return &managedConn{
			writer:     writer,
			reader:     downBody,
			remoteAddr: remoteAddr,
			localAddr:  localAddr,
			onClose:    releaseAll,
		}, nil
	}

	packetWriter := &managedPacketWriter{
		ctx:       ctx,
		url:       url,
		config:    config,
		sessionID: sessionID,
		shared:    sharedDownload,
	}
	return &managedConn{
		writer:     packetWriter,
		reader:     downBody,
		remoteAddr: remoteAddr,
		localAddr:  localAddr,
		onClose:    releaseAll,
	}, nil
}

func DialContext(ctx context.Context, config *SplitHTTPConfig) (net.Conn, error) {
	if config.DialTransport == nil {
		return nil, fmt.Errorf("splithttp transport dial is not configured")
	}
	var lastErr error
	for _, httpVersion := range candidateHTTPVersions(config) {
		conn, err := dialWithVersion(ctx, config, httpVersion)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("splithttp failed to select a transport")
	}
	return nil, lastErr
}
