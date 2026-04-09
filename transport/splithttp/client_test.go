package splithttp

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/metacubex/http"
)

type fakeRoundTripper struct {
	err            error
	closeIdleCalls int
}

func (f *fakeRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       io.NopCloser(&emptyReader{}),
	}, nil
}

func (f *fakeRoundTripper) CloseIdleConnections() {
	f.closeIdleCalls++
}

type emptyReader struct{}

func (*emptyReader) Read([]byte) (int, error) {
	return 0, io.EOF
}

func TestOpenStreamFailureRetiresClientWithoutImmediateClose(t *testing.T) {
	transport := &fakeRoundTripper{err: errors.New("boom")}
	client := &DefaultDialerClient{
		transportConfig: &SplitHTTPConfig{},
		client:          &http.Client{Transport: transport},
		httpVersion:     "2",
		uploadConns:     map[*H1Conn]struct{}{},
	}

	_, _, _, err := client.OpenStream(context.Background(), "https://example.com/test", "session-1", nil, false)
	if err == nil {
		t.Fatal("expected open stream error")
	}
	if !client.IsClosed() {
		t.Fatal("expected client to be retired after open stream failure")
	}
	if transport.closeIdleCalls != 0 {
		t.Fatalf("expected transport to stay open until Close, got %d CloseIdleConnections calls", transport.closeIdleCalls)
	}

	_ = client.Close()
	if transport.closeIdleCalls != 1 {
		t.Fatalf("expected Close to reap transport once, got %d", transport.closeIdleCalls)
	}
}

func TestPostPacketFailureRetiresClientWithoutImmediateClose(t *testing.T) {
	transport := &fakeRoundTripper{err: errors.New("boom")}
	client := &DefaultDialerClient{
		transportConfig: &SplitHTTPConfig{},
		client:          &http.Client{Transport: transport},
		httpVersion:     "2",
		uploadConns:     map[*H1Conn]struct{}{},
	}

	err := client.PostPacket(context.Background(), "https://example.com/test", "session-1", "0", []byte("payload"))
	if err == nil {
		t.Fatal("expected post packet error")
	}
	if !client.IsClosed() {
		t.Fatal("expected client to be retired after post packet failure")
	}
	if transport.closeIdleCalls != 0 {
		t.Fatalf("expected transport to stay open until Close, got %d CloseIdleConnections calls", transport.closeIdleCalls)
	}

	_ = client.Close()
	if transport.closeIdleCalls != 1 {
		t.Fatalf("expected Close to reap transport once, got %d", transport.closeIdleCalls)
	}
}
