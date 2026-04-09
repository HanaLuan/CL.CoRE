package splithttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/http"
	"github.com/metacubex/mihomo/log"
	gotls "github.com/metacubex/tls"
)

type DialerClient interface {
	IsClosed() bool
	Close() error
	OpenStream(context.Context, string, string, io.Reader, bool) (io.ReadCloser, net.Addr, net.Addr, error)
	PostPacket(context.Context, string, string, string, []byte) error
}

type DefaultDialerClient struct {
	transportConfig *SplitHTTPConfig
	client          *http.Client
	closed          atomic.Bool
	httpVersion     string
	uploadRawPool   *sync.Pool
	uploadMu        sync.Mutex
	uploadConns     map[*H1Conn]struct{}
	dialUploadConn  func(ctx context.Context) (net.Conn, error)
}

func createHTTPClient(config *SplitHTTPConfig, httpVersion string) DialerClient {
	if httpVersion == "3" {
		client := &DefaultDialerClient{
			transportConfig: config,
			client:          buildHTTP3Client(config),
			httpVersion:     httpVersion,
		}
		active := splitHTTPDiagActiveClients.Load()
		splitHTTPDiagLog(config, "dialer-client init http=%s host=%s active=%d "+splitHTTPDiagSnapshot(), httpVersion, config.Host, active, splitHTTPDiagActiveClients.Load(), splitHTTPDiagActiveConns.Load(), splitHTTPDiagActiveWriters.Load(), splitHTTPDiagActiveH1Conns.Load(), splitHTTPDiagInFlightOpen.Load(), splitHTTPDiagInFlightPost.Load())
		return client
	}

	var transport http.RoundTripper
	if httpVersion == "2" {
		transport = &http.Http2Transport{
			DialTLSContext: func(ctx context.Context, network, addr string, cfg *gotls.Config) (net.Conn, error) {
				return config.DialTransport(ctx, httpVersion)
			},
			ReadIdleTimeout: 30 * time.Second,
		}
	} else {
		httpDialContext := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return config.DialTransport(ctx, "1.1")
		}
		transport = &http.Transport{
			DialTLSContext:    httpDialContext,
			DialContext:       httpDialContext,
			IdleConnTimeout:   30 * time.Second,
			DisableKeepAlives: true,
			ForceAttemptHTTP2: false,
		}
	}

	client := &DefaultDialerClient{
		transportConfig: config,
		client: &http.Client{
			Transport: transport,
		},
		httpVersion:   httpVersion,
		uploadRawPool: &sync.Pool{},
		uploadConns:   map[*H1Conn]struct{}{},
	}
	if config.DialTransport != nil {
		client.dialUploadConn = func(ctx context.Context) (net.Conn, error) {
			return config.DialTransport(ctx, "1.1")
		}
	}
	active := splitHTTPDiagActiveClients.Load()
	splitHTTPDiagLog(config, "dialer-client init http=%s host=%s active=%d "+splitHTTPDiagSnapshot(), httpVersion, config.Host, active, splitHTTPDiagActiveClients.Load(), splitHTTPDiagActiveConns.Load(), splitHTTPDiagActiveWriters.Load(), splitHTTPDiagActiveH1Conns.Load(), splitHTTPDiagInFlightOpen.Load(), splitHTTPDiagInFlightPost.Load())
	return client
}

func (c *DefaultDialerClient) IsClosed() bool {
	return c.closed.Load()
}

func (c *DefaultDialerClient) retire() {
	if c.closed.Swap(true) {
		return
	}
	splitHTTPDiagLog(c.transportConfig, "dialer-client retire http=%s host=%s "+splitHTTPDiagSnapshot(), c.httpVersion, c.transportConfig.Host, splitHTTPDiagActiveClients.Load(), splitHTTPDiagActiveConns.Load(), splitHTTPDiagActiveWriters.Load(), splitHTTPDiagActiveH1Conns.Load(), splitHTTPDiagInFlightOpen.Load(), splitHTTPDiagInFlightPost.Load())
}

func (c *DefaultDialerClient) markClosed() {
	alreadyClosed := c.closed.Swap(true)
	if tr, ok := c.client.Transport.(interface{ CloseIdleConnections() }); ok {
		tr.CloseIdleConnections()
	}
	c.closeUploadConns()
	if !alreadyClosed {
		splitHTTPDiagLog(c.transportConfig, "dialer-client close http=%s host=%s "+splitHTTPDiagSnapshot(), c.httpVersion, c.transportConfig.Host, splitHTTPDiagActiveClients.Load(), splitHTTPDiagActiveConns.Load(), splitHTTPDiagActiveWriters.Load(), splitHTTPDiagActiveH1Conns.Load(), splitHTTPDiagInFlightOpen.Load(), splitHTTPDiagInFlightPost.Load())
	}
}

func (c *DefaultDialerClient) Close() error {
	c.markClosed()
	return nil
}

func (c *DefaultDialerClient) trackUploadConn(conn *H1Conn) {
	c.uploadMu.Lock()
	c.uploadConns[conn] = struct{}{}
	c.uploadMu.Unlock()
	active := splitHTTPDiagActiveH1Conns.Add(1)
	splitHTTPDiagLog(c.transportConfig, "h1-upload track http=%s host=%s active_h1=%d", c.httpVersion, c.transportConfig.Host, active)
}

func (c *DefaultDialerClient) untrackUploadConn(conn *H1Conn) {
	c.uploadMu.Lock()
	_, existed := c.uploadConns[conn]
	delete(c.uploadConns, conn)
	c.uploadMu.Unlock()
	if existed {
		active := splitHTTPDiagActiveH1Conns.Add(-1)
		splitHTTPDiagLog(c.transportConfig, "h1-upload untrack http=%s host=%s active_h1=%d", c.httpVersion, c.transportConfig.Host, active)
	}
}

func (c *DefaultDialerClient) closeUploadConns() {
	c.uploadMu.Lock()
	conns := make([]*H1Conn, 0, len(c.uploadConns))
	for conn := range c.uploadConns {
		conns = append(conns, conn)
	}
	c.uploadConns = map[*H1Conn]struct{}{}
	c.uploadMu.Unlock()
	if len(conns) > 0 {
		active := splitHTTPDiagActiveH1Conns.Add(int64(-len(conns)))
		splitHTTPDiagLog(c.transportConfig, "h1-upload bulk-close http=%s host=%s closed=%d active_h1=%d", c.httpVersion, c.transportConfig.Host, len(conns), active)
	}

	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (c *DefaultDialerClient) OpenStream(ctx context.Context, url string, sessionID string, body io.Reader, uploadOnly bool) (io.ReadCloser, net.Addr, net.Addr, error) {
	var remoteAddr net.Addr
	var localAddr net.Addr
	meta := connTelemetry{}
	reqID := splitHTTPDiagReqIDs.Add(1)
	inFlight := splitHTTPDiagInFlightOpen.Add(1)
	splitHTTPDiagLog(c.transportConfig, "open-stream start req=%d session=%s upload_only=%t http=%s host=%s in_flight=%d", reqID, sessionID, uploadOnly, c.httpVersion, c.transportConfig.Host, inFlight)
	defer func() {
		inFlight := splitHTTPDiagInFlightOpen.Add(-1)
		splitHTTPDiagLog(c.transportConfig, "open-stream return req=%d session=%s upload_only=%t http=%s host=%s in_flight=%d", reqID, sessionID, uploadOnly, c.httpVersion, c.transportConfig.Host, inFlight)
	}()
	var gotConn sync.Once
	gotConnCh := make(chan struct{})
	closeGotConn := func() { gotConn.Do(func() { close(gotConnCh) }) }

	traceCtx := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(connInfo httptrace.GotConnInfo) {
			remoteAddr = connInfo.Conn.RemoteAddr()
			localAddr = connInfo.Conn.LocalAddr()
			if localAddr != nil {
				meta.localAddr = localAddr.String()
			}
			if remoteAddr != nil {
				meta.remoteAddr = remoteAddr.String()
			}
			closeGotConn()
		},
	})

	method := "GET"
	if body != nil {
		method = c.transportConfig.GetNormalizedUplinkHTTPMethod()
	}

	req, err := http.NewRequestWithContext(context.WithoutCancel(traceCtx), method, url, body)
	if err != nil {
		return nil, nil, nil, err
	}
	c.transportConfig.FillStreamRequest(req, sessionID)
	logRequest(c.transportConfig, "stream", req, sessionID, "", meta)

	reader := &WaitReadCloser{Wait: make(chan struct{})}
	errCh := make(chan error, 1)

	go func() {
		resp, reqErr := c.client.Do(req)
		if reqErr != nil {
			if !uploadOnly {
				c.retire()
				log.Debugln("splithttp shared client open stream failed: %v", reqErr)
			}
			closeGotConn()
			errCh <- reqErr
			_ = reader.Close()
			return
		}

		closeGotConn()
		logResponse(c.transportConfig, "stream", resp, sessionID, "", meta)
		if ensureErr := ensureHTTPProtocolAllowed(c.transportConfig, resp); ensureErr != nil {
			resp.Body.Close()
			errCh <- ensureErr
			_ = reader.Close()
			return
		}
		if resp.StatusCode != http.StatusOK || uploadOnly {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK && !uploadOnly {
				errCh <- fmt.Errorf("splithttp bad status: %s", resp.Status)
			}
			_ = reader.Close()
			return
		}
		reader.Set(resp.Body)
	}()

	<-gotConnCh
	select {
	case reqErr := <-errCh:
		return nil, remoteAddr, localAddr, reqErr
	default:
		return reader, remoteAddr, localAddr, nil
	}
}

func (c *DefaultDialerClient) PostPacket(ctx context.Context, url string, sessionID string, seqStr string, payload []byte) error {
	method := c.transportConfig.GetNormalizedUplinkHTTPMethod()
	reqID := splitHTTPDiagReqIDs.Add(1)
	inFlight := splitHTTPDiagInFlightPost.Add(1)
	splitHTTPDiagLog(c.transportConfig, "post-packet start req=%d session=%s seq=%s http=%s host=%s bytes=%d in_flight=%d", reqID, sessionID, seqStr, c.httpVersion, c.transportConfig.Host, len(payload), inFlight)
	defer func() {
		inFlight := splitHTTPDiagInFlightPost.Add(-1)
		splitHTTPDiagLog(c.transportConfig, "post-packet return req=%d session=%s seq=%s http=%s host=%s in_flight=%d", reqID, sessionID, seqStr, c.httpVersion, c.transportConfig.Host, inFlight)
	}()
	var remoteAddr net.Addr
	var localAddr net.Addr
	meta := connTelemetry{}

	traceCtx := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(connInfo httptrace.GotConnInfo) {
			remoteAddr = connInfo.Conn.RemoteAddr()
			localAddr = connInfo.Conn.LocalAddr()
			if localAddr != nil {
				meta.localAddr = localAddr.String()
			}
			if remoteAddr != nil {
				meta.remoteAddr = remoteAddr.String()
			}
		},
	})

	req, err := http.NewRequestWithContext(context.WithoutCancel(traceCtx), method, url, nil)
	if err != nil {
		return err
	}
	c.transportConfig.FillPacketRequest(req, sessionID, seqStr, payload)
	logRequest(c.transportConfig, "packet-up", req, sessionID, seqStr, meta)

	if c.httpVersion != "1.1" {
		resp, err := c.client.Do(req)
		if err != nil {
			c.retire()
			return err
		}
		logResponse(c.transportConfig, "packet-up", resp, sessionID, seqStr, meta)
		if ensureErr := ensureHTTPProtocolAllowed(c.transportConfig, resp); ensureErr != nil {
			resp.Body.Close()
			return ensureErr
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("bad status code: %s", resp.Status)
		}
		return nil
	}

	requestBuff := new(bytes.Buffer)
	requestBuff.Grow(512 + int(req.ContentLength))
	if err := req.Write(requestBuff); err != nil {
		return err
	}

	var uploadConn any
	var h1UploadConn *H1Conn

	for {
		uploadConn = c.uploadRawPool.Get()
		newConnection := uploadConn == nil
		if newConnection {
			if c.dialUploadConn == nil {
				return fmt.Errorf("h1 upload dial is not configured")
			}
			newConn, err := c.dialUploadConn(context.WithoutCancel(ctx))
			if err != nil {
				return err
			}
			h1UploadConn = NewH1Conn(newConn)
			c.trackUploadConn(h1UploadConn)
			uploadConn = h1UploadConn
		} else {
			h1UploadConn = uploadConn.(*H1Conn)
			if h1UploadConn.UnreadedResponsesCount > 0 {
				resp, err := http.ReadResponse(h1UploadConn.RespBufReader, req)
				if err != nil {
					c.untrackUploadConn(h1UploadConn)
					_ = h1UploadConn.Close()
					c.retire()
					return fmt.Errorf("error while reading response: %w", err)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				h1UploadConn.UnreadedResponsesCount--
				if resp.StatusCode != http.StatusOK {
					return fmt.Errorf("got non-200 error response code: %d", resp.StatusCode)
				}
			}
		}

		_, err = h1UploadConn.Write(requestBuff.Bytes())
		if err == nil {
			break
		} else if newConnection {
			c.untrackUploadConn(h1UploadConn)
			_ = h1UploadConn.Close()
			return err
		} else {
			c.untrackUploadConn(h1UploadConn)
			_ = h1UploadConn.Close()
		}
	}

	h1UploadConn.UnreadedResponsesCount++
	c.uploadRawPool.Put(uploadConn)
	return nil
}
