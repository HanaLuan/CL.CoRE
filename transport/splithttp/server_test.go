package splithttp

import (
	"bytes"
	"encoding/base64"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
)

func encodeTestPayload(payload string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(payload))
}

func addTestXPadding(req *http.Request) {
	req.Header.Set("Referer", "https://example.com/?x_padding="+strings.Repeat("X", 100))
}

func TestSplitHTTPConfigDefaultUplinkDataKey(t *testing.T) {
	tests := []struct {
		placement string
		want      string
	}{
		{placement: PlacementHeader, want: "X-Data"},
		{placement: PlacementAuto, want: "X-Data"},
		{placement: PlacementCookie, want: "x_data"},
	}

	for _, tc := range tests {
		t.Run(tc.placement, func(t *testing.T) {
			config := &SplitHTTPConfig{UplinkDataPlacement: tc.placement}
			if got := config.GetNormalizedUplinkDataKey(); got != tc.want {
				t.Fatalf("unexpected uplink data key: got %q want %q", got, tc.want)
			}
		})
	}
}

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
			addTestXPadding(req)
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

func TestSplitHTTPServerPacketUpPayloadPlacement(t *testing.T) {
	tests := []struct {
		name      string
		placement string
		body      string
		configure func(*http.Request)
		want      string
	}{
		{
			name:      "header multi chunk",
			placement: PlacementHeader,
			configure: func(req *http.Request) {
				encoded := encodeTestPayload("hello")
				req.Header.Set("x_data-0", encoded[:3])
				req.Header.Set("x_data-1", encoded[3:])
			},
			want: "hello",
		},
		{
			name:      "cookie multi chunk",
			placement: PlacementCookie,
			configure: func(req *http.Request) {
				encoded := encodeTestPayload("cookie-data")
				req.AddCookie(&http.Cookie{Name: "x_data_0", Value: encoded[:4]})
				req.AddCookie(&http.Cookie{Name: "x_data_1", Value: encoded[4:]})
			},
			want: "cookie-data",
		},
		{
			name:      "auto header cookie body order",
			placement: PlacementAuto,
			body:      "body",
			configure: func(req *http.Request) {
				req.Header.Set("x_data-0", encodeTestPayload("header-"))
				req.AddCookie(&http.Cookie{Name: "x_data_0", Value: encodeTestPayload("cookie-")})
			},
			want: "header-cookie-body",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := NewSplitHTTPServer(&SplitHTTPConfig{
				Path:                "/xhttp",
				UplinkDataPlacement: tc.placement,
				UplinkDataKey:       "x_data",
			}, func(conn net.Conn) {
				_ = conn.Close()
			})

			req := httptest.NewRequest(http.MethodPost, "https://example.com/xhttp/session-1/0", bytes.NewReader([]byte(tc.body)))
			addTestXPadding(req)
			if tc.configure != nil {
				tc.configure(req)
			}
			recorder := httptest.NewRecorder()

			server.ServeHTTP(recorder, req)

			resp := recorder.Result()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("unexpected status: got %d want %d", resp.StatusCode, http.StatusOK)
			}

			sessionAny, ok := server.sessions.Load("session-1")
			if !ok {
				t.Fatal("expected session to be created")
			}
			session := sessionAny.(*httpSession)
			buf := make([]byte, len(tc.want))
			n, err := session.uploadQueue.Read(buf)
			if err != nil {
				t.Fatalf("failed to read packet payload: %v", err)
			}
			if got := string(buf[:n]); got != tc.want {
				t.Fatalf("unexpected payload: got %q want %q", got, tc.want)
			}
		})
	}
}

func TestSplitHTTPServerPacketUpRejectsOversizedPayload(t *testing.T) {
	server := NewSplitHTTPServer(&SplitHTTPConfig{
		Path:                "/xhttp",
		UplinkDataPlacement: PlacementBody,
		ScMaxEachPostBytes:  &RangeConfig{From: 3, To: 3},
	}, func(conn net.Conn) {
		_ = conn.Close()
	})

	req := httptest.NewRequest(http.MethodPost, "https://example.com/xhttp/session-1/0", bytes.NewReader([]byte("toolong")))
	addTestXPadding(req)
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	resp := recorder.Result()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("unexpected status: got %d want %d", resp.StatusCode, http.StatusRequestEntityTooLarge)
	}
}

func TestSplitHTTPServerPacketUpRejectsInvalidBase64(t *testing.T) {
	server := NewSplitHTTPServer(&SplitHTTPConfig{
		Path:                "/xhttp",
		UplinkDataPlacement: PlacementHeader,
		UplinkDataKey:       "x_data",
	}, func(conn net.Conn) {
		_ = conn.Close()
	})

	req := httptest.NewRequest(http.MethodPost, "https://example.com/xhttp/session-1/0", nil)
	addTestXPadding(req)
	req.Header.Set("x_data-0", "%%%")
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	resp := recorder.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestSplitHTTPServerOptionsCORS(t *testing.T) {
	server := NewSplitHTTPServer(&SplitHTTPConfig{
		Path:                "/xhttp",
		SessionPlacement:    PlacementCookie,
		UplinkDataPlacement: PlacementCookie,
	}, func(conn net.Conn) {
		_ = conn.Close()
	})

	req := httptest.NewRequest(http.MethodOptions, "https://example.com/xhttp/session-1", nil)
	req.Header.Set("Origin", "https://browser.example")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "X-Session, X-Seq")
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	resp := recorder.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", resp.StatusCode, http.StatusOK)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://browser.example" {
		t.Fatalf("unexpected allow-origin: got %q", got)
	}
	if got := resp.Header.Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Fatalf("unexpected allow-credentials: got %q", got)
	}
	if got := resp.Header.Get("Access-Control-Allow-Methods"); got != "POST" {
		t.Fatalf("unexpected allow-methods: got %q", got)
	}
	if got := resp.Header.Get("Access-Control-Allow-Headers"); got != "X-Session, X-Seq" {
		t.Fatalf("unexpected allow-headers: got %q", got)
	}
	if got := resp.Header.Get("X-Padding"); got == "" {
		t.Fatal("expected response padding header")
	}
}

func TestSplitHTTPServerRejectsInvalidXPadding(t *testing.T) {
	server := NewSplitHTTPServer(&SplitHTTPConfig{
		Path:          "/xhttp",
		XPaddingBytes: &RangeConfig{From: 4, To: 4},
	}, func(conn net.Conn) {
		_ = conn.Close()
	})

	req := httptest.NewRequest(http.MethodGet, "https://example.com/xhttp/session-1?x_padding=XXX", nil)
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	resp := recorder.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestSplitHTTPServerRejectsDisallowedMode(t *testing.T) {
	server := NewSplitHTTPServer(&SplitHTTPConfig{
		Path: "/xhttp",
		Mode: "stream-one",
	}, func(conn net.Conn) {
		_ = conn.Close()
	})

	req := httptest.NewRequest(http.MethodPost, "https://example.com/xhttp/session-1/0", bytes.NewReader([]byte("payload")))
	addTestXPadding(req)
	recorder := httptest.NewRecorder()

	server.ServeHTTP(recorder, req)

	resp := recorder.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unexpected status: got %d want %d", resp.StatusCode, http.StatusBadRequest)
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
	done := make(chan struct{})
	startStreamUpKeepalive(&SplitHTTPConfig{
		XPaddingBytes:       &RangeConfig{From: 3, To: 3},
		ScStreamUpServerSec: &RangeConfig{From: 1, To: 1},
	}, req, writer, done)
	defer close(done)

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
