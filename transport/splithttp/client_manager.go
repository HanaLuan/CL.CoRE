package splithttp

import (
	"sync"
	"sync/atomic"
)

type sharedClient struct {
	client    DialerClient
	openUsage atomic.Int32
}

func (c *sharedClient) release() {
	c.openUsage.Add(-1)
}

type clientManager struct {
	mu      sync.Mutex
	clients map[string]*sharedClient
}

var globalClientManager = &clientManager{
	clients: map[string]*sharedClient{},
}

func (m *clientManager) acquire(key string, create func() DialerClient) *sharedClient {
	m.mu.Lock()
	defer m.mu.Unlock()

	client, ok := m.clients[key]
	if !ok || client.client.IsClosed() {
		client = &sharedClient{client: create()}
		m.clients[key] = client
	}
	client.openUsage.Add(1)
	return client
}
