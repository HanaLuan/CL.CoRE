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
	OpenStream(context.Context, string, string, io.Reader, bool) (io.ReadCloser, net.Addr, net.Addr, error)
	PostPacket(context.Context, string, string, string, []byte) error
}

type DefaultDialerClient struct {
	transportConfig *SplitHTTPConfig
	client          *http.Client
	closed          atomic.Bool
	httpVersion     string
	uploadRawPool   *sync.Pool
	dialUploadConn  func(ctx context.Context) (net.Conn, error)
}

func createHTTPClient(config *SplitHTTPConfig, httpVersion string) DialerClient {
	if httpVersion == "3" {
		return &DefaultDialerClient{
			transportConfig: config,
			client:          buildHTTP3Client(config),
			httpVersion:     httpVersion,
		}
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
	}
	if config.DialTransport != nil {
		client.dialUploadConn = func(ctx context.Context) (net.Conn, error) {
			return config.DialTransport(ctx, "1.1")
		}
	}
	return client
}

func (c *DefaultDialerClient) IsClosed() bool {
	return c.closed.Load()
}

func (c *DefaultDialerClient) markClosed() {
	c.closed.Store(true)
	if tr, ok := c.client.Transport.(interface{ CloseIdleConnections() }); ok {
		tr.CloseIdleConnections()
	}
}

func (c *DefaultDialerClient) OpenStream(ctx context.Context, url string, sessionID string, body io.Reader, uploadOnly bool) (io.ReadCloser, net.Addr, net.Addr, error) {
	var remoteAddr net.Addr
	var localAddr net.Addr
	meta := connTelemetry{}
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
				c.markClosed()
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
			c.markClosed()
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
			uploadConn = h1UploadConn
		} else {
			h1UploadConn = uploadConn.(*H1Conn)
			if h1UploadConn.UnreadedResponsesCount > 0 {
				resp, err := http.ReadResponse(h1UploadConn.RespBufReader, req)
				if err != nil {
					c.markClosed()
					return fmt.Errorf("error while reading response: %w", err)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					return fmt.Errorf("got non-200 error response code: %d", resp.StatusCode)
				}
			}
		}

		_, err = h1UploadConn.Write(requestBuff.Bytes())
		if err == nil {
			break
		} else if newConnection {
			return err
		}
	}

	c.uploadRawPool.Put(uploadConn)
	return nil
}
