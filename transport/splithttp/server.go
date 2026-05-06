package splithttp

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/metacubex/http"
)

var errPayloadTooLarge = errors.New("packet-up payload too large")

func applyStreamingResponseHeaders(header http.Header, noSSEHeader bool) {
	header.Set("X-Accel-Buffering", "no")
	header.Set("Cache-Control", "no-store")
	if noSSEHeader {
		header.Set("Content-Type", "application/grpc")
		return
	}
	header.Set("Content-Type", "text/event-stream")
}

func startStreamUpKeepalive(config *SplitHTTPConfig, request *http.Request, writer io.Writer, done <-chan struct{}) {
	if request == nil || writer == nil || request.Header.Get("Referer") == "" {
		return
	}

	interval := config.GetNormalizedScStreamUpServerSecs()
	if interval.To <= 0 {
		return
	}

	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			padding := generatePadding(PaddingMethodRepeatX, randInRange(config.GetNormalizedXPaddingBytes()))
			if padding == "" {
				return
			}
			if _, err := writer.Write([]byte(padding)); err != nil {
				return
			}
			timer := time.NewTimer(time.Duration(randInRange(interval)) * time.Second)
			select {
			case <-done:
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

type SplitHTTPServer struct {
	config    *SplitHTTPConfig
	sessionMu sync.Mutex
	sessions  sync.Map
	addConn   func(net.Conn)
}

func NewSplitHTTPServer(config *SplitHTTPConfig, addConn func(net.Conn)) *SplitHTTPServer {
	return &SplitHTTPServer{
		config:  config,
		addConn: addConn,
	}
}

type httpSession struct {
	uploadQueue      *uploadQueue
	isFullyConnected chan struct{}
	once             sync.Once
}

func (s *httpSession) fullyConnected() {
	s.once.Do(func() {
		close(s.isFullyConnected)
	})
}

func (h *SplitHTTPServer) upsertSession(sessionId string) *httpSession {
	if currentSessionAny, ok := h.sessions.Load(sessionId); ok {
		return currentSessionAny.(*httpSession)
	}

	h.sessionMu.Lock()
	defer h.sessionMu.Unlock()

	if currentSessionAny, ok := h.sessions.Load(sessionId); ok {
		return currentSessionAny.(*httpSession)
	}

	s := &httpSession{
		uploadQueue:      NewUploadQueue(h.config.GetNormalizedScMaxBufferedPosts()),
		isFullyConnected: make(chan struct{}),
	}

	h.sessions.Store(sessionId, s)

	go func() {
		timer := time.NewTimer(30 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
			h.sessions.Delete(sessionId)
			s.uploadQueue.Close()
		case <-s.isFullyConnected:
		}
	}()

	return s
}

func (h *SplitHTTPServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	config := h.config
	path := config.GetNormalizedPath()

	if !strings.HasPrefix(request.URL.Path, path) {
		writer.WriteHeader(http.StatusNotFound)
		return
	}

	config.WriteResponseHeader(writer, request.Method, request.Header)
	config.ApplyXPaddingToResponse(writer, config.BuildResponseXPadding())
	if request.Method == http.MethodOptions {
		writer.WriteHeader(http.StatusOK)
		return
	}

	validRange := config.GetNormalizedXPaddingBytes()
	paddingValue, _ := config.ExtractXPaddingFromRequest(request, config.XPaddingObfsMode)
	if !config.IsPaddingValid(paddingValue, validRange.From, validRange.To, PaddingMethod(config.XPaddingMethod)) {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	sessionId, seqStr := config.ExtractMetaFromRequest(request, path)

	if request.Method == config.GetNormalizedUplinkHTTPMethod() && sessionId != "" && seqStr == "" {
		// stream-up upload: POST /path/{session}
		if config.Mode != "" && config.Mode != "auto" && config.Mode != "stream-up" {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		session := h.upsertSession(sessionId)
		httpSC := &httpServerConn{
			waitCh:         make(chan struct{}),
			Reader:         request.Body,
			ResponseWriter: writer,
		}
		if err := session.uploadQueue.Push(Packet{Reader: httpSC}); err != nil {
			writer.WriteHeader(http.StatusConflict)
			return
		}

		applyStreamingResponseHeaders(writer.Header(), config.NoSSEHeader)
		writer.WriteHeader(http.StatusOK)
		rc := http.NewResponseController(writer)
		_ = rc.EnableFullDuplex()
		_ = rc.Flush()

		startStreamUpKeepalive(config, request, httpSC, httpSC.waitCh)

		select {
		case <-request.Context().Done():
		case <-httpSC.waitCh:
		}
		httpSC.Close()
		return
	}

	if request.Method == config.GetNormalizedUplinkHTTPMethod() && sessionId != "" && seqStr != "" {
		// packet-up
		if config.Mode != "" && config.Mode != "auto" && config.Mode != "packet-up" {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}

		payload, bodyPayloadLen, err := h.readPacketPayload(request)
		if err != nil {
			if errors.Is(err, errPayloadTooLarge) {
				writer.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(payload) > config.GetNormalizedScMaxEachPostBytes().To {
			writer.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}

		session := h.upsertSession(sessionId)
		if err := session.uploadQueue.Push(Packet{Payload: payload, Seq: seq}); err != nil {
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}

		if bodyPayloadLen == 0 {
			writer.Header().Set("Cache-Control", "no-store")
		}
		writer.Header().Set("Content-Type", "application/grpc")
		writer.WriteHeader(http.StatusOK)
		return
	}

	if request.Method == "GET" || sessionId == "" {
		// stream-down or stream-one
		if sessionId == "" && config.Mode != "" && config.Mode != "auto" && config.Mode != "stream-one" && config.Mode != "stream-up" {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		var currentSession *httpSession
		if sessionId != "" {
			if sessionId == "" {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			currentSession = h.upsertSession(sessionId)
			currentSession.fullyConnected()
			defer h.sessions.Delete(sessionId)
		}

		if sessionId == "" {
			writer.Header().Set("X-Accel-Buffering", "no")
			writer.Header().Set("Cache-Control", "no-store")
			writer.Header().Set("Content-Type", "application/grpc")
		} else {
			applyStreamingResponseHeaders(writer.Header(), config.NoSSEHeader)
		}
		writer.WriteHeader(http.StatusOK)
		rc := http.NewResponseController(writer)
		_ = rc.EnableFullDuplex()
		_ = rc.Flush()

		httpSC := &httpServerConn{
			waitCh:         make(chan struct{}),
			Reader:         request.Body,
			ResponseWriter: writer,
		}
		conn := &splitConn{
			writer:     httpSC,
			reader:     httpSC,
			remoteAddr: request.RemoteAddr,
			localAddr:  request.Host,
		}
		if currentSession != nil { // if not stream-one
			conn.reader = currentSession.uploadQueue
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			h.addConn(conn)
		}()

		select {
		case <-request.Context().Done():
			conn.Close()
		case <-httpSC.waitCh:
			conn.Close()
		case <-done:
		}
		<-done
	} else {
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *SplitHTTPServer) readPacketPayload(request *http.Request) ([]byte, int, error) {
	config := h.config
	placement := config.GetNormalizedUplinkDataPlacement()
	limit := config.GetNormalizedScMaxEachPostBytes().To
	key := config.GetNormalizedUplinkDataKey()

	var headerPayload []byte
	if placement == PlacementHeader || placement == PlacementAuto {
		payload, err := readHeaderPayload(request, key)
		if err != nil {
			return nil, 0, err
		}
		headerPayload = payload
	}

	var cookiePayload []byte
	if placement == PlacementCookie || placement == PlacementAuto {
		payload, err := readCookiePayload(request, key)
		if err != nil {
			return nil, 0, err
		}
		cookiePayload = payload
	}

	var bodyPayload []byte
	if placement == PlacementBody || placement == PlacementAuto {
		payload, err := readLimitedBody(request, limit)
		if err != nil {
			return nil, 0, err
		}
		bodyPayload = payload
	}

	switch placement {
	case PlacementHeader:
		return headerPayload, 0, nil
	case PlacementCookie:
		return cookiePayload, 0, nil
	case PlacementAuto:
		payload := make([]byte, 0, len(headerPayload)+len(cookiePayload)+len(bodyPayload))
		payload = append(payload, headerPayload...)
		payload = append(payload, cookiePayload...)
		payload = append(payload, bodyPayload...)
		return payload, len(bodyPayload), nil
	default:
		return bodyPayload, len(bodyPayload), nil
	}
}

func readHeaderPayload(request *http.Request, key string) ([]byte, error) {
	var chunks []string
	for i := 0; ; i++ {
		chunk := request.Header.Get(fmt.Sprintf("%s-%d", key, i))
		if chunk == "" {
			break
		}
		chunks = append(chunks, chunk)
	}
	return base64.RawURLEncoding.DecodeString(strings.Join(chunks, ""))
}

func readCookiePayload(request *http.Request, key string) ([]byte, error) {
	var chunks []string
	for i := 0; ; i++ {
		cookie, err := request.Cookie(fmt.Sprintf("%s_%d", key, i))
		if err != nil || cookie == nil {
			break
		}
		chunks = append(chunks, cookie.Value)
	}
	return base64.RawURLEncoding.DecodeString(strings.Join(chunks, ""))
}

func readLimitedBody(request *http.Request, limit int) ([]byte, error) {
	if request.ContentLength > int64(limit) {
		return nil, errPayloadTooLarge
	}
	payload, err := io.ReadAll(io.LimitReader(request.Body, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(payload) > limit {
		return nil, errPayloadTooLarge
	}
	return payload, nil
}

type httpServerConn struct {
	sync.Mutex
	waitCh   chan struct{}
	waitOnce sync.Once
	closed   bool
	io.Reader
	http.ResponseWriter
}

func (c *httpServerConn) Close() error {
	c.waitOnce.Do(func() {
		c.Lock()
		c.closeLocked()
		c.Unlock()
	})
	return nil
}

func (c *httpServerConn) closeLocked() {
	c.closed = true
	close(c.waitCh)
}

func (c *httpServerConn) Write(b []byte) (n int, err error) {
	c.Lock()
	defer c.Unlock()
	if c.closed {
		return 0, io.ErrClosedPipe
	}
	defer func() {
		if recover() != nil {
			n = 0
			err = io.ErrClosedPipe
			c.waitOnce.Do(func() {
				c.closeLocked()
			})
		}
	}()

	n, err = c.ResponseWriter.Write(b)
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
	if err != nil {
		c.waitOnce.Do(func() {
			c.closeLocked()
		})
	}
	return n, err
}

type splitConn struct {
	writer     io.WriteCloser
	reader     io.ReadCloser
	remoteAddr string
	localAddr  string
}

func (c *splitConn) Write(b []byte) (int, error) { return c.writer.Write(b) }
func (c *splitConn) Read(b []byte) (int, error)  { return c.reader.Read(b) }
func (c *splitConn) Close() error {
	err1 := c.writer.Close()
	err2 := c.reader.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

type dummyAddr string

func (a dummyAddr) Network() string { return "tcp" }
func (a dummyAddr) String() string  { return string(a) }

func (c *splitConn) LocalAddr() net.Addr                { return dummyAddr(c.localAddr) }
func (c *splitConn) RemoteAddr() net.Addr               { return dummyAddr(c.remoteAddr) }
func (c *splitConn) SetDeadline(t time.Time) error      { return nil }
func (c *splitConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *splitConn) SetWriteDeadline(t time.Time) error { return nil }
