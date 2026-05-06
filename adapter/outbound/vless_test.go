package outbound

import (
	"context"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/transport/vless"
)

type earlyHandshakeTestConn struct {
	writes    [][]byte
	writeErr  error
	closed    bool
	handshake bool
}

func (c *earlyHandshakeTestConn) Read([]byte) (int, error) { return 0, io.EOF }
func (c *earlyHandshakeTestConn) Write(b []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	c.writes = append(c.writes, append([]byte(nil), b...))
	c.handshake = false
	return len(b), nil
}
func (c *earlyHandshakeTestConn) Close() error                     { c.closed = true; return nil }
func (c *earlyHandshakeTestConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *earlyHandshakeTestConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *earlyHandshakeTestConn) SetDeadline(time.Time) error      { return nil }
func (c *earlyHandshakeTestConn) SetReadDeadline(time.Time) error  { return nil }
func (c *earlyHandshakeTestConn) SetWriteDeadline(time.Time) error { return nil }
func (c *earlyHandshakeTestConn) NeedHandshake() bool              { return c.handshake }

func TestEarlyHandshakePacketConn(t *testing.T) {
	conn := &earlyHandshakeTestConn{handshake: true}

	if err := earlyHandshakePacketConn(conn); err != nil {
		t.Fatalf("earlyHandshakePacketConn failed: %v", err)
	}
	if len(conn.writes) != 1 {
		t.Fatalf("expected exactly one handshake write, got %d", len(conn.writes))
	}
	if len(conn.writes[0]) != 0 {
		t.Fatalf("expected empty early handshake write, got %d bytes", len(conn.writes[0]))
	}
	if conn.closed {
		t.Fatal("expected connection to stay open after successful handshake")
	}
}

func TestEarlyHandshakePacketConnCloseOnError(t *testing.T) {
	conn := &earlyHandshakeTestConn{handshake: true, writeErr: io.ErrClosedPipe}

	err := earlyHandshakePacketConn(conn)
	if err == nil {
		t.Fatal("expected earlyHandshakePacketConn to fail")
	}
	if !conn.closed {
		t.Fatal("expected connection to be closed on handshake failure")
	}
}

func TestVlessXHTTPXUDPVisionRejectedBeforeDial(t *testing.T) {
	out, err := NewVless(VlessOption{
		Name:           "xhttp-xudp-vision",
		Server:         "127.0.0.1",
		Port:           443,
		UUID:           "00000000-0000-0000-0000-000000000001",
		Flow:           vless.XRV,
		Network:        "xhttp",
		UDP:            true,
		PacketEncoding: "xudp",
	})
	if err != nil {
		t.Fatalf("NewVless failed: %v", err)
	}

	_, err = out.ListenPacketContext(context.Background(), &C.Metadata{
		NetWork: C.UDP,
		DstIP:   netip.MustParseAddr("127.0.0.1"),
		DstPort: 53,
	})
	if err == nil {
		t.Fatal("expected xhttp xudp vision to be rejected")
	}
	if !strings.Contains(err.Error(), "does not support xtls-rprx-vision") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildSplitHTTPConfigNoGRPCHeader(t *testing.T) {
	config := buildSplitHTTPConfig(context.Background(), "example.com:443", "example.com", nil, SplitHTTPOptions{
		NoGRPCHeader: true,
	}, SplitHTTPOptions{}, true)
	if !config.NoGRPCHeader {
		t.Fatal("expected xhttp no-grpc-header to propagate")
	}

	config = buildSplitHTTPConfig(context.Background(), "example.com:443", "example.com", nil, SplitHTTPOptions{}, SplitHTTPOptions{
		NoGRPCHeader: true,
	}, true)
	if !config.NoGRPCHeader {
		t.Fatal("expected splithttp no-grpc-header to override")
	}
}
