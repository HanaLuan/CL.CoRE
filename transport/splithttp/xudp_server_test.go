package splithttp

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/metacubex/sing-vmess"
	singbuf "github.com/metacubex/sing/common/buf"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

func makeXUDPTestFrame(t *testing.T, status byte, option byte, destination M.Socksaddr, payload []byte) []byte {
	t.Helper()

	var meta bytes.Buffer
	if destination.IsValid() {
		meta.WriteByte(vmess.NetworkUDP)
		if err := vmess.AddressSerializer.WriteAddrPort(&meta, destination); err != nil {
			t.Fatalf("write test destination: %v", err)
		}
	}

	var frame bytes.Buffer
	binary.Write(&frame, binary.BigEndian, uint16(4+meta.Len()))
	frame.Write([]byte{0, 0, status, option})
	frame.Write(meta.Bytes())
	if option&vmess.OptionData == vmess.OptionData {
		binary.Write(&frame, binary.BigEndian, uint16(len(payload)))
		frame.Write(payload)
	}
	return frame.Bytes()
}

func TestIsXUDPInitialFrame(t *testing.T) {
	destination := M.ParseSocksaddr("127.0.0.1:5353")
	firstFrame := makeXUDPTestFrame(t, vmess.StatusNew, vmess.OptionData, destination, []byte("query"))

	if !isXUDPInitialFrame(firstFrame[:7]) {
		t.Fatal("expected UDP new frame to be detected as XUDP")
	}

	nonXUDP := append([]byte(nil), firstFrame[:7]...)
	nonXUDP[6] = vmess.NetworkTCP
	if isXUDPInitialFrame(nonXUDP) {
		t.Fatal("expected TCP mux frame to be ignored")
	}

	keepFrame := makeXUDPTestFrame(t, vmess.StatusKeep, vmess.OptionData, destination, []byte("query"))
	if isXUDPInitialFrame(keepFrame[:7]) {
		t.Fatal("expected keep frame to be ignored")
	}
}

func TestTryHandleVLESSXUDPMuxReplaysNonXUDP(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload := []byte{0, 0, 1, 2, vmess.StatusNew, vmess.OptionData, vmess.NetworkTCP, 'm', 'u', 'x'}
	errCh := make(chan error, 1)
	go func() {
		_, err := client.Write(payload)
		errCh <- err
	}()

	handled, replayConn, err := TryHandleVLESSXUDPMux(context.Background(), server, M.Metadata{}, nil)
	if err != nil {
		t.Fatalf("TryHandleVLESSXUDPMux failed: %v", err)
	}
	if handled {
		t.Fatal("non-XUDP mux traffic should not be handled by SplitHTTP")
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(replayConn, got); err != nil {
		t.Fatalf("read replay data: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("unexpected replay payload: got %v want %v", got, payload)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("write payload: %v", err)
	}
}

func TestServerXUDPConnReadsNewAndKeepFrames(t *testing.T) {
	destination := M.ParseSocksaddr("127.0.0.1:5353")
	firstPayload := []byte("first")
	secondPayload := []byte("second")
	raw := append(
		makeXUDPTestFrame(t, vmess.StatusNew, vmess.OptionData, destination, firstPayload),
		makeXUDPTestFrame(t, vmess.StatusKeep, vmess.OptionData, destination, secondPayload)...,
	)
	conn := newServerXUDPConn(&readWriteConn{Reader: bytes.NewReader(raw), Writer: io.Discard})

	buffer := make([]byte, 32)
	n, addr, err := conn.ReadFrom(buffer)
	if err != nil {
		t.Fatalf("read new frame: %v", err)
	}
	if got := string(buffer[:n]); got != string(firstPayload) {
		t.Fatalf("unexpected first payload: got %q want %q", got, firstPayload)
	}
	if got := M.SocksaddrFromNet(addr); got != destination {
		t.Fatalf("unexpected first destination: got %v want %v", got, destination)
	}

	n, addr, err = conn.ReadFrom(buffer)
	if err != nil {
		t.Fatalf("read keep frame: %v", err)
	}
	if got := string(buffer[:n]); got != string(secondPayload) {
		t.Fatalf("unexpected second payload: got %q want %q", got, secondPayload)
	}
	if got := M.SocksaddrFromNet(addr); got != destination {
		t.Fatalf("unexpected second destination: got %v want %v", got, destination)
	}
}

func TestServerXUDPConnSkipsKeepAliveAndNoDataFrames(t *testing.T) {
	destination := M.ParseSocksaddr("127.0.0.1:5353")
	payload := []byte("answer")
	raw := append(
		makeXUDPTestFrame(t, vmess.StatusKeepAlive, 0, M.Socksaddr{}, nil),
		makeXUDPTestFrame(t, vmess.StatusKeep, 0, destination, nil)...,
	)
	raw = append(raw, makeXUDPTestFrame(t, vmess.StatusKeep, vmess.OptionData, destination, payload)...)
	conn := newServerXUDPConn(&readWriteConn{Reader: bytes.NewReader(raw), Writer: io.Discard})

	buffer := make([]byte, 32)
	n, addr, err := conn.ReadFrom(buffer)
	if err != nil {
		t.Fatalf("read data frame after skipped frames: %v", err)
	}
	if got := string(buffer[:n]); got != string(payload) {
		t.Fatalf("unexpected payload: got %q want %q", got, payload)
	}
	if got := M.SocksaddrFromNet(addr); got != destination {
		t.Fatalf("unexpected destination: got %v want %v", got, destination)
	}
}

func TestServerXUDPConnWritePacketUsesKeepFrame(t *testing.T) {
	var output bytes.Buffer
	conn := newServerXUDPConn(&readWriteConn{Reader: bytes.NewReader(nil), Writer: &output})
	destination := M.ParseSocksaddr("127.0.0.1:5353")
	payload := []byte("response")
	buffer := singbuf.NewSize(conn.FrontHeadroom() + len(payload))
	buffer.Resize(conn.FrontHeadroom(), 0)
	buffer.Write(payload)

	if err := conn.WritePacket(buffer, destination); err != nil {
		t.Fatalf("write packet: %v", err)
	}

	reader := newServerXUDPConn(&readWriteConn{Reader: bytes.NewReader(output.Bytes()), Writer: io.Discard})
	packet := make([]byte, 32)
	n, addr, err := reader.ReadFrom(packet)
	if err != nil {
		t.Fatalf("read written packet: %v", err)
	}
	if got := string(packet[:n]); got != string(payload) {
		t.Fatalf("unexpected written payload: got %q want %q", got, payload)
	}
	if got := M.SocksaddrFromNet(addr); got != destination {
		t.Fatalf("unexpected written destination: got %v want %v", got, destination)
	}
}

type readWriteConn struct {
	io.Reader
	io.Writer
}

func (c *readWriteConn) Read(p []byte) (int, error)         { return c.Reader.Read(p) }
func (c *readWriteConn) Write(p []byte) (int, error)        { return c.Writer.Write(p) }
func (c *readWriteConn) Close() error                       { return nil }
func (c *readWriteConn) LocalAddr() net.Addr                { return nil }
func (c *readWriteConn) RemoteAddr() net.Addr               { return nil }
func (c *readWriteConn) SetDeadline(t time.Time) error      { return nil }
func (c *readWriteConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *readWriteConn) SetWriteDeadline(t time.Time) error { return nil }

var _ N.PacketConn = (*serverXUDPConn)(nil)
