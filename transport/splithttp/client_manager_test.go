package splithttp

import (
	"context"
	"testing"
)

type fakeXmuxConn struct {
	closed bool
}

func (f *fakeXmuxConn) IsClosed() bool { return f.closed }
func (f *fakeXmuxConn) Close() error {
	f.closed = true
	return nil
}

func TestXmuxManagerMaxConnections(t *testing.T) {
	manager := NewXmuxManager(nil, XmuxConfig{
		MaxConnections: &RangeConfig{From: 4, To: 4},
	}, func() XmuxConn {
		return &fakeXmuxConn{}
	})

	clients := map[*XmuxClient]struct{}{}
	for i := 0; i < 8; i++ {
		clients[manager.GetXmuxClient(context.Background())] = struct{}{}
	}

	if len(clients) != 1 {
		t.Fatalf("expected sequential reuse to keep 1 xmux client, got %d", len(clients))
	}
}

func TestXmuxManagerMaxConnectionsUnderConcurrency(t *testing.T) {
	manager := NewXmuxManager(nil, XmuxConfig{
		MaxConnections: &RangeConfig{From: 4, To: 4},
		MaxConcurrency: &RangeConfig{From: 1, To: 1},
	}, func() XmuxConn {
		return &fakeXmuxConn{}
	})

	clients := map[*XmuxClient]struct{}{}
	for i := 0; i < 8; i++ {
		client := manager.GetXmuxClient(context.Background())
		client.OpenUsage.Add(1)
		clients[client] = struct{}{}
	}

	if len(clients) != 4 {
		t.Fatalf("expected 4 distinct xmux clients under concurrency pressure, got %d", len(clients))
	}
}

func TestXmuxManagerDoesNotExceedMaxConnectionsWhenBusy(t *testing.T) {
	manager := NewXmuxManager(nil, XmuxConfig{
		MaxConnections: &RangeConfig{From: 2, To: 2},
		MaxConcurrency: &RangeConfig{From: 1, To: 1},
	}, func() XmuxConn {
		return &fakeXmuxConn{}
	})

	clients := map[*XmuxClient]struct{}{}
	for i := 0; i < 8; i++ {
		client := manager.GetXmuxClient(context.Background())
		client.OpenUsage.Add(1)
		clients[client] = struct{}{}
	}

	if len(clients) != 2 {
		t.Fatalf("expected maxConnections to cap xmux clients at 2, got %d", len(clients))
	}
}

func TestXmuxManagerCMaxReuseTimes(t *testing.T) {
	manager := NewXmuxManager(nil, XmuxConfig{
		CMaxReuseTimes: &RangeConfig{From: 2, To: 2},
	}, func() XmuxConn {
		return &fakeXmuxConn{}
	})

	clients := map[*XmuxClient]struct{}{}
	for i := 0; i < 64; i++ {
		clients[manager.GetXmuxClient(context.Background())] = struct{}{}
	}

	if len(clients) != 32 {
		t.Fatalf("expected 32 distinct xmux clients, got %d", len(clients))
	}
}

func TestXmuxManagerMaxConcurrency(t *testing.T) {
	manager := NewXmuxManager(nil, XmuxConfig{
		MaxConcurrency: &RangeConfig{From: 2, To: 2},
	}, func() XmuxConn {
		return &fakeXmuxConn{}
	})

	clients := map[*XmuxClient]struct{}{}
	for i := 0; i < 64; i++ {
		client := manager.GetXmuxClient(context.Background())
		client.OpenUsage.Add(1)
		clients[client] = struct{}{}
	}

	if len(clients) != 32 {
		t.Fatalf("expected 32 distinct xmux clients, got %d", len(clients))
	}
}

func TestXmuxManagerDefaultReuse(t *testing.T) {
	manager := NewXmuxManager(nil, XmuxConfig{}, func() XmuxConn {
		return &fakeXmuxConn{}
	})

	clients := map[*XmuxClient]struct{}{}
	for i := 0; i < 64; i++ {
		client := manager.GetXmuxClient(context.Background())
		client.OpenUsage.Add(1)
		clients[client] = struct{}{}
	}

	if len(clients) != 1 {
		t.Fatalf("expected 1 distinct xmux client, got %d", len(clients))
	}
}

func TestXmuxManagerClosesRetiredClients(t *testing.T) {
	stale := &fakeXmuxConn{}
	manager := &XmuxManager{
		xmuxConfig: XmuxConfig{},
		newConnFunc: func() XmuxConn {
			return &fakeXmuxConn{}
		},
		xmuxClients: []*XmuxClient{
			{
				XmuxConn:  stale,
				leftUsage: 0,
			},
		},
	}

	client := manager.GetXmuxClient(context.Background())

	if !stale.closed {
		t.Fatal("expected retired xmux client to be closed")
	}
	if client == nil || client.XmuxConn == stale {
		t.Fatal("expected a fresh xmux client")
	}
}
