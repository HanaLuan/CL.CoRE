package splithttp

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakePacketDialerClient struct {
	mu      sync.Mutex
	closed  bool
	posts   []fakePacketPost
	opens   []fakeOpenStreamCall
	openErr error
	postErr error
}

type fakePacketPost struct {
	sessionID string
	seq       string
	payload   string
}

type fakeOpenStreamCall struct {
	url        string
	sessionID  string
	uploadOnly bool
	hasBody    bool
}

func (f *fakePacketDialerClient) IsClosed() bool { return f.closed }

func (f *fakePacketDialerClient) Close() error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	return nil
}

func (f *fakePacketDialerClient) OpenStream(_ context.Context, url string, sessionID string, body io.Reader, uploadOnly bool) (io.ReadCloser, net.Addr, net.Addr, error) {
	f.mu.Lock()
	f.opens = append(f.opens, fakeOpenStreamCall{
		url:        url,
		sessionID:  sessionID,
		uploadOnly: uploadOnly,
		hasBody:    body != nil,
	})
	f.mu.Unlock()
	if f.openErr != nil {
		return nil, nil, nil, f.openErr
	}
	return io.NopCloser(strings.NewReader("")), nil, nil, nil
}

func (f *fakePacketDialerClient) PostPacket(_ context.Context, _ string, sessionID string, seqStr string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.postErr != nil {
		return f.postErr
	}
	f.posts = append(f.posts, fakePacketPost{
		sessionID: sessionID,
		seq:       seqStr,
		payload:   string(payload),
	})
	return nil
}

func TestResolveDialMode(t *testing.T) {
	tests := []struct {
		name    string
		config  *SplitHTTPConfig
		runtime DialRuntime
		want    string
	}{
		{name: "explicit", config: &SplitHTTPConfig{Mode: "stream-up"}, runtime: DialRuntime{}, want: "stream-up"},
		{name: "auto-default", config: &SplitHTTPConfig{Mode: "auto"}, runtime: DialRuntime{}, want: "packet-up"},
		{name: "auto-reality", config: &SplitHTTPConfig{Mode: "auto"}, runtime: DialRuntime{HasReality: true}, want: "stream-one"},
		{name: "auto-reality-download", config: &SplitHTTPConfig{Mode: "auto", DownloadConfig: &SplitHTTPConfig{}}, runtime: DialRuntime{HasReality: true}, want: "stream-up"},
		{name: "empty-default", config: &SplitHTTPConfig{}, runtime: DialRuntime{}, want: "packet-up"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveDialMode(tt.config, tt.runtime); got != tt.want {
				t.Fatalf("resolveDialMode() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestManagedPacketWriterSplitsPayload(t *testing.T) {
	client := &fakePacketDialerClient{}
	lease := &xmuxLease{
		ctx:    context.Background(),
		key:    "test",
		config: &SplitHTTPConfig{},
		client: &XmuxClient{XmuxConn: client},
	}
	lease.client.LeftRequests.Store(1 << 30)

	writer := newManagedPacketWriter(context.Background(), "https://example.com/test", &SplitHTTPConfig{
		ScMaxEachPostBytes: &RangeConfig{From: 4, To: 4},
		ScMaxBufferedPosts: 4,
		ScMinPostsInterval: &RangeConfig{},
	}, "session-1", lease)

	if _, err := writer.Write([]byte("abcdefghij")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.posts) != 3 {
		t.Fatalf("expected 3 posts, got %d", len(client.posts))
	}
	want := []fakePacketPost{
		{sessionID: "session-1", seq: "0", payload: "abcd"},
		{sessionID: "session-1", seq: "1", payload: "efgh"},
		{sessionID: "session-1", seq: "2", payload: "ij"},
	}
	for i := range want {
		if client.posts[i] != want[i] {
			t.Fatalf("post %d = %+v, want %+v", i, client.posts[i], want[i])
		}
	}
}

func TestXmuxLeaseRotatesAfterRequestBudget(t *testing.T) {
	previousManager := globalClientManager
	globalClientManager = &clientManager{clients: map[string]*XmuxManager{}}
	defer func() { globalClientManager = previousManager }()

	created := make([]*fakePacketDialerClient, 0, 2)
	cfg := &SplitHTTPConfig{
		ClientKey: "rotation",
		Xmux: &XmuxConfig{
			HMaxRequestTimes: &RangeConfig{From: 2, To: 2},
		},
	}
	lease := newXmuxLease(context.Background(), cfg, "2")
	lease.create = func() DialerClient {
		client := &fakePacketDialerClient{}
		created = append(created, client)
		return client
	}

	lease.acquire()
	first := lease.client
	lease.rotateForPacket(time.Now())
	if lease.client != first {
		t.Fatalf("expected first request to keep current client")
	}
	lease.rotateForPacket(time.Now())
	if lease.client == first {
		t.Fatalf("expected request budget exhaustion to rotate client")
	}
	if len(created) != 2 {
		t.Fatalf("expected 2 created clients, got %d", len(created))
	}
}

func TestDialWithVersionStreamUpUsesDownloadConfig(t *testing.T) {
	previousManager := globalClientManager
	previousFactory := dialerClientFactory
	globalClientManager = &clientManager{clients: map[string]*XmuxManager{}}
	defer func() {
		globalClientManager = previousManager
		dialerClientFactory = previousFactory
	}()

	clientByKey := map[string]*fakePacketDialerClient{}
	dialerClientFactory = func(config *SplitHTTPConfig, httpVersion string) DialerClient {
		key := sharedClientKey(config, httpVersion)
		client := &fakePacketDialerClient{}
		clientByKey[key] = client
		return client
	}

	config := &SplitHTTPConfig{
		ClientKey: "upload",
		Host:      "upload.example.com",
		Path:      "/up",
		DialTransport: func(context.Context, string) (net.Conn, error) {
			return nil, nil
		},
		DownloadConfig: &SplitHTTPConfig{
			ClientKey: "download",
			Host:      "download.example.com",
			Path:      "/down",
			DialTransport: func(context.Context, string) (net.Conn, error) {
				return nil, nil
			},
		},
	}

	conn, err := dialWithVersion(context.Background(), config, "2", DialRuntime{HasReality: true})
	if err != nil {
		t.Fatalf("dialWithVersion failed: %v", err)
	}
	_ = conn.Close()

	upClient := clientByKey[sharedClientKey(config, "2")]
	downClient := clientByKey[sharedClientKey(config.DownloadConfig, "2")]
	if upClient == nil || downClient == nil {
		t.Fatalf("expected separate upload and download clients")
	}
	if len(downClient.opens) == 0 || downClient.opens[0].url != "https://download.example.com/down/" || downClient.opens[0].uploadOnly {
		t.Fatalf("unexpected download open call: %+v", downClient.opens)
	}
	if len(upClient.opens) == 0 || upClient.opens[0].url != "https://upload.example.com/up/" || !upClient.opens[0].uploadOnly {
		t.Fatalf("unexpected upload open call: %+v", upClient.opens)
	}
}

func TestDialWithVersionPacketUpUsesDownloadConfigForDownstream(t *testing.T) {
	previousManager := globalClientManager
	previousFactory := dialerClientFactory
	globalClientManager = &clientManager{clients: map[string]*XmuxManager{}}
	defer func() {
		globalClientManager = previousManager
		dialerClientFactory = previousFactory
	}()

	clientByKey := map[string]*fakePacketDialerClient{}
	dialerClientFactory = func(config *SplitHTTPConfig, httpVersion string) DialerClient {
		key := sharedClientKey(config, httpVersion)
		client := &fakePacketDialerClient{}
		clientByKey[key] = client
		return client
	}

	config := &SplitHTTPConfig{
		ClientKey: "upload-packet",
		Host:      "upload.example.com",
		Path:      "/up",
		DialTransport: func(context.Context, string) (net.Conn, error) {
			return nil, nil
		},
		ScMaxEachPostBytes: &RangeConfig{From: 4, To: 4},
		ScMaxBufferedPosts: 4,
		DownloadConfig: &SplitHTTPConfig{
			ClientKey: "download-packet",
			Host:      "download.example.com",
			Path:      "/down",
			DialTransport: func(context.Context, string) (net.Conn, error) {
				return nil, nil
			},
		},
	}

	conn, err := dialWithVersion(context.Background(), config, "2", DialRuntime{})
	if err != nil {
		t.Fatalf("dialWithVersion failed: %v", err)
	}
	if _, err := conn.Write([]byte("abcd")); err != nil {
		t.Fatalf("conn.Write failed: %v", err)
	}
	_ = conn.Close()

	upClient := clientByKey[sharedClientKey(config, "2")]
	downClient := clientByKey[sharedClientKey(config.DownloadConfig, "2")]
	if upClient == nil || downClient == nil {
		t.Fatalf("expected separate upload and download clients")
	}
	if len(downClient.opens) == 0 || downClient.opens[0].url != "https://download.example.com/down/" || downClient.opens[0].uploadOnly {
		t.Fatalf("unexpected downstream open call: %+v", downClient.opens)
	}
	if len(upClient.posts) == 0 || upClient.posts[0].payload != "abcd" {
		t.Fatalf("unexpected upload posts: %+v", upClient.posts)
	}
}

func TestManagedPacketWriterPropagatesPostError(t *testing.T) {
	client := &fakePacketDialerClient{postErr: io.ErrClosedPipe}
	lease := &xmuxLease{
		ctx:    context.Background(),
		key:    "test-error",
		config: &SplitHTTPConfig{},
		client: &XmuxClient{XmuxConn: client},
	}
	lease.client.LeftRequests.Store(1 << 30)

	writer := newManagedPacketWriter(context.Background(), "https://example.com/test", &SplitHTTPConfig{
		ScMaxEachPostBytes: &RangeConfig{From: 4, To: 4},
		ScMaxBufferedPosts: 4,
		ScMinPostsInterval: &RangeConfig{},
	}, "session-1", lease)

	if _, err := writer.Write([]byte("abcd")); err != nil {
		t.Fatalf("initial Write failed early: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if _, err := writer.Write([]byte("efgh")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("follow-up Write error = %v, want %v", err, io.ErrClosedPipe)
	}
	_ = writer.Close()
}
