package splithttp

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	"github.com/metacubex/mihomo/transport/splithttp/multibuffer"
)

type managedPacketWriter struct {
	id        uint64
	clientID  uint64
	ctx       context.Context
	url       string
	config    *SplitHTTPConfig
	sessionID string
	lease     *xmuxLease

	maxUploadSize   int
	minPostInterval RangeConfig
	pipeline        multibuffer.Pipeline

	mu       sync.Mutex
	seq      int64
	closed   bool
	writeErr error
	done     chan struct{}
	lastPost time.Time
}

func newManagedPacketWriter(ctx context.Context, url string, config *SplitHTTPConfig, sessionID string, lease *xmuxLease) *managedPacketWriter {
	maxUploadSize := config.GetNormalizedScMaxEachPostBytes().rand()
	if maxUploadSize <= 0 {
		maxUploadSize = 1
	}
	clientID := uint64(0)
	if lease != nil && lease.client != nil {
		clientID = lease.client.ID
	}
	w := &managedPacketWriter{
		id:              splitHTTPDiagWriterIDs.Add(1),
		clientID:        clientID,
		ctx:             ctx,
		url:             url,
		config:          config,
		sessionID:       sessionID,
		lease:           lease,
		maxUploadSize:   maxUploadSize,
		minPostInterval: config.GetNormalizedScMinPostsInterval(),
		// Keep packet-up buffering close to Xray's bounded upload pipe.
		// Allowing ScMaxBufferedPosts * maxUploadSize per session causes
		// resident memory to explode under concurrent packet-up uploads.
		pipeline: multibuffer.New(maxUploadSize),
		done:     make(chan struct{}),
	}
	active := splitHTTPDiagActiveWriters.Add(1)
	splitHTTPDiagLog(config, "managed-writer create id=%d session=%s client=%d active=%d url=%s", w.id, sessionID, w.clientID, active, url)
	go w.run()
	return w
}

func (w *managedPacketWriter) nextChunk() ([]byte, string, error) {
	chunk, err := w.pipeline.ReadChunk(w.maxUploadSize)
	if err != nil {
		return nil, "", err
	}
	w.mu.Lock()
	seqStr := strconv.FormatInt(w.seq, 10)
	w.seq++
	w.mu.Unlock()
	return chunk, seqStr, nil
}

func (w *managedPacketWriter) fail(err error) {
	w.mu.Lock()
	if w.writeErr == nil {
		w.writeErr = err
	}
	w.mu.Unlock()
	w.pipeline.Interrupt(err)
}

func (w *managedPacketWriter) run() {
	defer close(w.done)
	defer func() {
		active := splitHTTPDiagActiveWriters.Add(-1)
		w.mu.Lock()
		err := w.writeErr
		closed := w.closed
		seq := w.seq
		w.mu.Unlock()
		splitHTTPDiagLog(w.config, "managed-writer exit id=%d session=%s client=%d active=%d closed=%t seq=%d err=%v", w.id, w.sessionID, w.clientID, active, closed, seq, err)
	}()

	for {
		chunk, seqStr, err := w.nextChunk()
		if err != nil {
			return
		}
		if waitMs := w.minPostInterval.rand(); waitMs > 0 {
			sleepFor := time.Duration(waitMs)*time.Millisecond - time.Since(w.lastPost)
			if sleepFor > 0 {
				timer := time.NewTimer(sleepFor)
				select {
				case <-w.ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}
		w.lastPost = time.Now()
		w.lease.rotateForPacket(w.lastPost)
		if err := w.lease.dialerClient().PostPacket(w.ctx, w.url, w.sessionID, seqStr, chunk); err != nil {
			w.fail(err)
			return
		}
	}
}

func (w *managedPacketWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	err := w.writeErr
	closed := w.closed
	w.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if closed {
		return 0, io.ErrClosedPipe
	}
	return w.pipeline.Write(b)
}

func (w *managedPacketWriter) Close() error {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	splitHTTPDiagLog(w.config, "managed-writer close-request id=%d session=%s client=%d", w.id, w.sessionID, w.clientID)
	_ = w.pipeline.Close()
	<-w.done
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

func openSharedStream(ctx context.Context, config *SplitHTTPConfig, httpVersion, url, sessionID string, body io.Reader, uploadOnly bool) (io.ReadCloser, net.Addr, net.Addr, *XmuxClient, error) {
	lease := newXmuxLease(ctx, config, httpVersion)
	lease.acquire()
	lease.consumeStreamRequest()

	reader, remoteAddr, localAddr, err := lease.dialerClient().OpenStream(ctx, url, sessionID, body, uploadOnly)
	if err != nil {
		lease.release()
		return nil, nil, nil, nil, err
	}
	return reader, remoteAddr, localAddr, lease.client, nil
}

func openSharedStreamWithLease(ctx context.Context, lease *xmuxLease, url, sessionID string, body io.Reader, uploadOnly bool) (io.ReadCloser, net.Addr, net.Addr, error) {
	lease.acquire()
	lease.consumeStreamRequest()

	reader, remoteAddr, localAddr, err := lease.dialerClient().OpenStream(ctx, url, sessionID, body, uploadOnly)
	if err != nil {
		lease.release()
		return nil, nil, nil, err
	}
	return reader, remoteAddr, localAddr, nil
}

func newSharedStreamLease(ctx context.Context, config *SplitHTTPConfig, httpVersion string) *xmuxLease {
	lease := newXmuxLease(ctx, config, httpVersion)
	lease.acquire()
	return lease
}

func dialWithVersion(ctx context.Context, config *SplitHTTPConfig, httpVersion string, runtime DialRuntime) (net.Conn, error) {
	mode := resolveDialMode(config, runtime)

	sessionID := ""
	if mode != "stream-one" {
		sessionID = utils.NewUUIDV4().String()
	}
	splitHTTPDiagLog(config, "dial start session=%s mode=%s http=%s upload_host=%s download_host=%s", sessionID, mode, httpVersion, config.Host, func() string {
		if config.DownloadConfig != nil {
			return config.DownloadConfig.Host
		}
		return config.Host
	}())

	uploadURL := fmt.Sprintf("https://%s%s", config.Host, config.GetNormalizedPath())
	downloadConfig := config
	if config.DownloadConfig != nil {
		downloadConfig = config.DownloadConfig
	}
	downloadURL := fmt.Sprintf("https://%s%s", downloadConfig.Host, downloadConfig.GetNormalizedPath())
	reader, writer := io.Pipe()

	uploadLease := newXmuxLease(ctx, config, httpVersion)
	downloadLease := newXmuxLease(ctx, downloadConfig, httpVersion)

	var remoteAddr net.Addr
	var localAddr net.Addr
	var err error

	releaseAll := func() {
		uploadLease.release()
		downloadLease.release()
	}

	if mode == "stream-one" {
		var body io.ReadCloser
		body, remoteAddr, localAddr, err = openSharedStreamWithLease(ctx, uploadLease, uploadURL, sessionID, reader, false)
		if err != nil {
			_ = reader.Close()
			_ = writer.Close()
			return nil, err
		}
		conn := &managedConn{
			id:         splitHTTPDiagConnIDs.Add(1),
			sessionID:  sessionID,
			mode:       mode,
			config:     config,
			writer:     writer,
			reader:     body,
			remoteAddr: remoteAddr,
			localAddr:  localAddr,
			onClose:    releaseAll,
		}
		active := splitHTTPDiagActiveConns.Add(1)
		splitHTTPDiagLog(config, "managed-conn create id=%d session=%s mode=%s active=%d", conn.id, conn.sessionID, conn.mode, active)
		return conn, nil
	}

	var downBody io.ReadCloser
	downBody, remoteAddr, localAddr, err = openSharedStreamWithLease(ctx, downloadLease, downloadURL, sessionID, nil, false)
	if err != nil {
		_ = reader.Close()
		_ = writer.Close()
		return nil, err
	}

	if mode == "stream-up" {
		_, _, _, err = openSharedStreamWithLease(ctx, uploadLease, uploadURL, sessionID, reader, true)
		if err != nil {
			_ = downBody.Close()
			_ = reader.Close()
			_ = writer.Close()
			releaseAll()
			return nil, err
		}
		conn := &managedConn{
			id:         splitHTTPDiagConnIDs.Add(1),
			sessionID:  sessionID,
			mode:       mode,
			config:     config,
			writer:     writer,
			reader:     downBody,
			remoteAddr: remoteAddr,
			localAddr:  localAddr,
			onClose:    releaseAll,
		}
		active := splitHTTPDiagActiveConns.Add(1)
		splitHTTPDiagLog(config, "managed-conn create id=%d session=%s mode=%s active=%d", conn.id, conn.sessionID, conn.mode, active)
		return conn, nil
	}

	packetWriter := newManagedPacketWriter(ctx, uploadURL, config, sessionID, uploadLease)
	conn := &managedConn{
		id:         splitHTTPDiagConnIDs.Add(1),
		sessionID:  sessionID,
		mode:       mode,
		config:     config,
		writer:     packetWriter,
		reader:     downBody,
		remoteAddr: remoteAddr,
		localAddr:  localAddr,
		onClose:    releaseAll,
	}
	active := splitHTTPDiagActiveConns.Add(1)
	splitHTTPDiagLog(config, "managed-conn create id=%d session=%s mode=%s active=%d", conn.id, conn.sessionID, conn.mode, active)
	return conn, nil
}

func DialContextWithOptions(ctx context.Context, config *SplitHTTPConfig, runtime DialRuntime) (net.Conn, error) {
	if config.DialTransport == nil {
		return nil, fmt.Errorf("splithttp transport dial is not configured")
	}
	var lastErr error
	for _, httpVersion := range candidateHTTPVersions(config) {
		conn, err := dialWithVersion(ctx, config, httpVersion, runtime)
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

func DialContext(ctx context.Context, config *SplitHTTPConfig) (net.Conn, error) {
	return DialContextWithOptions(ctx, config, DialRuntime{})
}
