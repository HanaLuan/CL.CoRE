package splithttp

import (
	"context"
	"io"
	"net"
	"testing"
)

type fakeDialerClient struct {
	closed bool
}

func (f *fakeDialerClient) IsClosed() bool {
	return f.closed
}

func (f *fakeDialerClient) OpenStream(context.Context, string, string, io.Reader, bool) (io.ReadCloser, net.Addr, net.Addr, error) {
	return nil, nil, nil, io.EOF
}

func (f *fakeDialerClient) PostPacket(context.Context, string, string, string, []byte) error {
	return io.EOF
}

func TestClientManagerAcquireReuseAndRecreate(t *testing.T) {
	mgr := &clientManager{clients: map[string]*sharedClient{}}
	created := 0
	var firstRaw *fakeDialerClient

	create := func() DialerClient {
		created++
		c := &fakeDialerClient{}
		if firstRaw == nil {
			firstRaw = c
		}
		return c
	}

	s1 := mgr.acquire("k", create)
	s2 := mgr.acquire("k", create)
	if s1 != s2 {
		t.Fatalf("expected same shared client instance for same key")
	}
	if created != 1 {
		t.Fatalf("unexpected create count: got %d want 1", created)
	}
	if got := s1.openUsage.Load(); got != 2 {
		t.Fatalf("open usage mismatch: got %d want 2", got)
	}

	s1.release()
	s2.release()
	if got := s1.openUsage.Load(); got != 0 {
		t.Fatalf("open usage after release mismatch: got %d want 0", got)
	}

	firstRaw.closed = true
	s3 := mgr.acquire("k", create)
	if s3 == s1 {
		t.Fatalf("expected recreate when previous client is closed")
	}
	if created != 2 {
		t.Fatalf("unexpected create count after recreate: got %d want 2", created)
	}
}
