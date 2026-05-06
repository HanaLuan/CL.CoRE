package splithttp

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"

	"github.com/metacubex/sing-vmess"
	singbuf "github.com/metacubex/sing/common/buf"
	"github.com/metacubex/sing/common/bufio"
	E "github.com/metacubex/sing/common/exceptions"
	M "github.com/metacubex/sing/common/metadata"
	N "github.com/metacubex/sing/common/network"
)

// TryHandleVLESSXUDPMux handles the XUDP-over-VLESS-mux framing that packet-up
// exposes as separate HTTP uploads. Generic sing-vmess mux handling treats the
// XUDP initial frame as ordinary mux metadata, so SplitHTTP needs this narrow
// server-side adapter. Non-XUDP mux traffic is replayed through the returned conn.
func TryHandleVLESSXUDPMux(ctx context.Context, conn net.Conn, metadata M.Metadata, handler N.UDPConnectionHandler) (handled bool, replayConn net.Conn, err error) {
	var first [7]byte
	if _, err = io.ReadFull(conn, first[:]); err != nil {
		return false, nil, err
	}
	replayConn = &prefixConn{
		Conn:   conn,
		reader: io.MultiReader(bytes.NewReader(first[:]), conn),
	}
	if isXUDPInitialFrame(first[:]) {
		return true, nil, handler.NewPacketConnection(ctx, newServerXUDPConn(replayConn), metadata)
	}
	return false, replayConn, nil
}

func isXUDPInitialFrame(first []byte) bool {
	return len(first) >= 7 &&
		first[2] == 0 &&
		first[3] == 0 &&
		first[4] == vmess.StatusNew &&
		first[6] == vmess.NetworkUDP
}

type prefixConn struct {
	net.Conn
	reader io.Reader
}

func (c *prefixConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}

type serverXUDPConn struct {
	net.Conn
	writer          N.ExtendedWriter
	readWaitOptions N.ReadWaitOptions
	destination     M.Socksaddr
}

func newServerXUDPConn(conn net.Conn) *serverXUDPConn {
	return &serverXUDPConn{
		Conn:   conn,
		writer: bufio.NewExtendedWriter(conn),
	}
}

func (c *serverXUDPConn) Read(p []byte) (int, error) {
	n, _, err := c.ReadFrom(p)
	return n, err
}

func (c *serverXUDPConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	buffer := singbuf.With(p)
	var destination M.Socksaddr
	destination, err = c.ReadPacket(buffer)
	if err != nil {
		return
	}
	if destination.IsFqdn() {
		addr = destination
	} else {
		addr = destination.UDPAddr()
	}
	n = buffer.Len()
	return
}

func (c *serverXUDPConn) ReadPacket(buffer *singbuf.Buffer) (destination M.Socksaddr, err error) {
	start := buffer.Start()
	payloadLen, destination, err := c.readFrameMetadata()
	if err != nil {
		return
	}
	c.destination = destination
	buffer.Resize(start, 0)
	_, err = buffer.ReadFullFrom(c.Conn, payloadLen)
	return
}

func (c *serverXUDPConn) InitializeReadWaiter(options N.ReadWaitOptions) (needCopy bool) {
	c.readWaitOptions = options
	return false
}

func (c *serverXUDPConn) WaitReadPacket() (buffer *singbuf.Buffer, destination M.Socksaddr, err error) {
	buffer = c.readWaitOptions.NewPacketBuffer()
	payloadLen, destination, err := c.readFrameMetadata()
	if err != nil {
		buffer.Release()
		return nil, M.Socksaddr{}, err
	}
	c.destination = destination
	buffer.Resize(buffer.Start(), 0)
	_, err = buffer.ReadFullFrom(c.Conn, payloadLen)
	if err != nil {
		buffer.Release()
		return nil, M.Socksaddr{}, err
	}
	c.readWaitOptions.PostReturn(buffer)
	return
}

func (c *serverXUDPConn) readFrameMetadata() (payloadLen int, destination M.Socksaddr, err error) {
	var header [6]byte
	if _, err = io.ReadFull(c.Conn, header[:]); err != nil {
		return
	}
	length := int(binary.BigEndian.Uint16(header[:2]))
	status := header[4]
	option := header[5]
	switch status {
	case vmess.StatusNew, vmess.StatusKeep:
	case vmess.StatusEnd:
		err = io.EOF
		return
	case vmess.StatusKeepAlive:
		return c.readFrameMetadata()
	default:
		err = E.New("unexpected xudp frame: ", status)
		return
	}
	if option&2 == 2 {
		err = E.Cause(net.ErrClosed, "remote closed")
		return
	}

	if length != 4 {
		metaLen := length - 4
		if metaLen < 1 {
			err = E.New("invalid xudp metadata length: ", metaLen)
			return
		}
		meta := make([]byte, metaLen)
		if _, err = io.ReadFull(c.Conn, meta); err != nil {
			return
		}
		metaReader := bytes.NewReader(meta)
		if _, err = metaReader.ReadByte(); err != nil {
			return
		}
		destination, err = vmess.AddressSerializer.ReadAddrPort(metaReader)
		if err != nil {
			return
		}
		destination = destination.Unwrap()
	}
	if option&1 != 1 {
		return c.readFrameMetadata()
	}

	var payloadLength uint16
	err = binary.Read(c.Conn, binary.BigEndian, &payloadLength)
	payloadLen = int(payloadLength)
	return
}

func (c *serverXUDPConn) Write(p []byte) (int, error) {
	return c.WriteTo(p, nil)
}

func (c *serverXUDPConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	destination := M.SocksaddrFromNet(addr)
	if addr == nil {
		destination = c.destination
	}
	return bufio.WritePacketBuffer(c, singbuf.As(p), destination)
}

func (c *serverXUDPConn) LocalAddr() net.Addr {
	return c.destination.UDPAddr()
}

func (c *serverXUDPConn) WritePacket(buffer *singbuf.Buffer, destination M.Socksaddr) error {
	dataLen := buffer.Len()
	addrLen := M.SocksaddrSerializer.AddrPortLen(destination)
	header := buffer.ExtendHeader(7 + addrLen + 2)
	binary.BigEndian.PutUint16(header, uint16(5+addrLen))
	header[2] = 0
	header[3] = 0
	header[4] = vmess.StatusKeep
	header[5] = 1
	header[6] = vmess.NetworkUDP
	if err := vmess.AddressSerializer.WriteAddrPort(singbuf.With(header[7:]), destination); err != nil {
		return err
	}
	binary.BigEndian.PutUint16(header[7+addrLen:], uint16(dataLen))
	return c.writer.WriteBuffer(buffer)
}

func (c *serverXUDPConn) FrontHeadroom() int {
	return 7 + M.MaxSocksaddrLength + 2
}

func (c *serverXUDPConn) NeedAdditionalReadDeadline() bool {
	return true
}
