package splithttp

import (
	"context"
	"crypto/rand"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
)

type XmuxConn interface {
	IsClosed() bool
	Close() error
}

type XmuxClient struct {
	ID           uint64
	XmuxConn     XmuxConn
	OpenUsage    atomic.Int32
	leftUsage    int32
	LeftRequests atomic.Int32
	UnreusableAt time.Time
}

func (c *XmuxClient) dialerClient() DialerClient {
	return c.XmuxConn.(DialerClient)
}

func (c *XmuxClient) release() {
	c.OpenUsage.Add(-1)
}

type XmuxManager struct {
	mu          sync.Mutex
	config      *SplitHTTPConfig
	xmuxConfig  XmuxConfig
	concurrency int32
	connections int32
	newConnFunc func() XmuxConn
	xmuxClients []*XmuxClient
}

func NewXmuxManager(config *SplitHTTPConfig, xmuxConfig XmuxConfig, newConnFunc func() XmuxConn) *XmuxManager {
	return &XmuxManager{
		config:      config,
		xmuxConfig:  xmuxConfig,
		concurrency: int32(xmuxConfig.GetNormalizedMaxConcurrency().rand()),
		connections: int32(xmuxConfig.GetNormalizedMaxConnections().rand()),
		newConnFunc: newConnFunc,
		xmuxClients: make([]*XmuxClient, 0),
	}
}

func (m *XmuxManager) newXmuxClient() *XmuxClient {
	xmuxClient := &XmuxClient{
		ID:        splitHTTPDiagClientIDs.Add(1),
		XmuxConn:  m.newConnFunc(),
		leftUsage: -1,
	}
	if x := m.xmuxConfig.GetNormalizedCMaxReuseTimes().rand(); x > 0 {
		xmuxClient.leftUsage = int32(x - 1)
	}
	xmuxClient.LeftRequests.Store(math.MaxInt32)
	if x := m.xmuxConfig.GetNormalizedHMaxRequestTimes().rand(); x > 0 {
		xmuxClient.LeftRequests.Store(int32(x))
	}
	if x := m.xmuxConfig.GetNormalizedHMaxReusableSecs().rand(); x > 0 {
		xmuxClient.UnreusableAt = time.Now().Add(time.Duration(x) * time.Second)
	}
	m.xmuxClients = append(m.xmuxClients, xmuxClient)
	active := splitHTTPDiagActiveClients.Add(1)
	splitHTTPDiagLog(m.config, "xmux-client create id=%d active=%d open_usage=%d left_usage=%d left_requests=%d", xmuxClient.ID, active, xmuxClient.OpenUsage.Load(), xmuxClient.leftUsage, xmuxClient.LeftRequests.Load())
	return xmuxClient
}

func (m *XmuxManager) GetXmuxClient(ctx context.Context) *XmuxClient {
	m.mu.Lock()
	defer m.mu.Unlock()

	_ = ctx
	for i := 0; i < len(m.xmuxClients); {
		xmuxClient := m.xmuxClients[i]
		reason := ""
		if xmuxClient.XmuxConn.IsClosed() ||
			xmuxClient.leftUsage == 0 ||
			xmuxClient.LeftRequests.Load() <= 0 ||
			(!xmuxClient.UnreusableAt.IsZero() && time.Now().After(xmuxClient.UnreusableAt)) {
			switch {
			case xmuxClient.XmuxConn.IsClosed():
				reason = "closed"
			case xmuxClient.leftUsage == 0:
				reason = "left-usage-exhausted"
			case xmuxClient.LeftRequests.Load() <= 0:
				reason = "request-budget-exhausted"
			default:
				reason = "expired"
			}
			_ = xmuxClient.XmuxConn.Close()
			m.xmuxClients = append(m.xmuxClients[:i], m.xmuxClients[i+1:]...)
			active := splitHTTPDiagActiveClients.Add(-1)
			splitHTTPDiagLog(m.config, "xmux-client retire id=%d reason=%s active=%d open_usage=%d left_usage=%d left_requests=%d", xmuxClient.ID, reason, active, xmuxClient.OpenUsage.Load(), xmuxClient.leftUsage, xmuxClient.LeftRequests.Load())
			continue
		}
		i++
	}

	if len(m.xmuxClients) == 0 {
		return m.newXmuxClient()
	}
	if m.connections > 0 && len(m.xmuxClients) < int(m.connections) {
		return m.newXmuxClient()
	}

	candidates := make([]*XmuxClient, 0, len(m.xmuxClients))
	if m.concurrency > 0 {
		for _, xmuxClient := range m.xmuxClients {
			if xmuxClient.OpenUsage.Load() < m.concurrency {
				candidates = append(candidates, xmuxClient)
			}
		}
	} else {
		candidates = m.xmuxClients
	}

	if len(candidates) == 0 {
		return m.newXmuxClient()
	}

	index, err := rand.Int(rand.Reader, big.NewInt(int64(len(candidates))))
	if err != nil {
		index = big.NewInt(0)
	}
	xmuxClient := candidates[index.Int64()]
	if xmuxClient.leftUsage > 0 {
		xmuxClient.leftUsage--
	}
	return xmuxClient
}

type clientManager struct {
	mu      sync.Mutex
	clients map[string]*XmuxManager
}

var globalClientManager = &clientManager{
	clients: map[string]*XmuxManager{},
}

func (m *clientManager) acquire(ctx context.Context, key string, config *SplitHTTPConfig, create func() DialerClient) *XmuxClient {
	m.mu.Lock()
	manager, ok := m.clients[key]
	if !ok {
		manager = NewXmuxManager(config, config.GetNormalizedXmux(), func() XmuxConn {
			return create()
		})
		m.clients[key] = manager
	}
	m.mu.Unlock()

	client := manager.GetXmuxClient(ctx)
	client.OpenUsage.Add(1)
	return client
}
