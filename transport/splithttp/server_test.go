package splithttp

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
)

func TestSplitHTTPServerDownloadHeaders(t *testing.T) {
	tests := []struct {
		name        string
		noSSEHeader bool
		wantType    string
	}{
		{name: "sse-enabled", noSSEHeader: false, wantType: "text/event-stream"},
		{name: "sse-disabled", noSSEHeader: true, wantType: "application/grpc"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := NewSplitHTTPServer(&SplitHTTPConfig{
				Path:        "/xhttp",
				NoSSEHeader: tc.noSSEHeader,
			}, func(conn net.Conn) {
				_ = conn.Close()
			})

			req := httptest.NewRequest(http.MethodGet, "https://example.com/xhttp/session-1", nil)
			recorder := httptest.NewRecorder()

			server.ServeHTTP(recorder, req)

			resp := recorder.Result()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("unexpected status: got %d want %d", resp.StatusCode, http.StatusOK)
			}
			if got := resp.Header.Get("Content-Type"); got != tc.wantType {
				t.Fatalf("unexpected content-type: got %q want %q", got, tc.wantType)
			}
			if got := resp.Header.Get("X-Accel-Buffering"); got != "no" {
				t.Fatalf("unexpected X-Accel-Buffering: got %q want %q", got, "no")
			}
			if got := resp.Header.Get("Cache-Control"); got != "no-store" {
				t.Fatalf("unexpected Cache-Control: got %q want %q", got, "no-store")
			}
		})
	}
}

type keepaliveWriter struct {
	mu     sync.Mutex
	writes [][]byte
	ch     chan struct{}
}

func newKeepaliveWriter() *keepaliveWriter {
	return &keepaliveWriter{ch: make(chan struct{}, 1)}
}

func (w *keepaliveWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.writes = append(w.writes, append([]byte(nil), p...))
	w.mu.Unlock()
	select {
	case w.ch <- struct{}{}:
	default:
	}
	return 0, io.ErrClosedPipe
}

func TestStartStreamUpKeepaliveWritesPadding(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "https://example.com/xhttp/session-1", nil)
	req.Header.Set("Referer", "https://example.com/xhttp/?x_padding=XXX")

	writer := newKeepaliveWriter()
	startStreamUpKeepalive(&SplitHTTPConfig{
		XPaddingBytes:       &RangeConfig{From: 3, To: 3},
		ScStreamUpServerSec: &RangeConfig{From: 1, To: 1},
	}, req, writer)

	select {
	case <-writer.ch:
	case <-time.After(2 * time.Second):
		t.Fatal("expected keepalive padding write")
	}

	writer.mu.Lock()
	defer writer.mu.Unlock()
	if len(writer.writes) == 0 {
		t.Fatal("expected at least one keepalive write")
	}
	if got := string(writer.writes[0]); got != "XXX" {
		t.Fatalf("unexpected keepalive payload: got %q want %q", got, "XXX")
	}
}
