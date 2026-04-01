package splithttp

import (
	"io"
	"net"
	"sync"
	"time"
)

type managedConn struct {
	writer     io.WriteCloser
	reader     io.ReadCloser
	remoteAddr net.Addr
	localAddr  net.Addr
	onClose    func()
	closeOnce  sync.Once
}

func (c *managedConn) Write(b []byte) (int, error) {
	return c.writer.Write(b)
}

func (c *managedConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}

func (c *managedConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		if c.onClose != nil {
			c.onClose()
		}
		err = c.writer.Close()
		err2 := c.reader.Close()
		if err == nil {
			err = err2
		}
	})
	return err
}

func (c *managedConn) LocalAddr() net.Addr  { return c.localAddr }
func (c *managedConn) RemoteAddr() net.Addr { return c.remoteAddr }
func (c *managedConn) SetDeadline(time.Time) error {
	return nil
}
func (c *managedConn) SetReadDeadline(time.Time) error {
	return nil
}
func (c *managedConn) SetWriteDeadline(time.Time) error {
	return nil
}
