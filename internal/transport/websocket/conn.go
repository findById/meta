package websocket

import (
	"errors"
	"io"
	"net"
	"time"

	gorilla "github.com/gorilla/websocket"
)

type Conn struct {
	conn   *gorilla.Conn
	reader io.Reader
}

func NewConn(conn *gorilla.Conn) *Conn {
	return &Conn{conn: conn}
}

func (c *Conn) Read(p []byte) (int, error) {
	for {
		if c.reader != nil {
			n, err := c.reader.Read(p)
			if err == nil || !errors.Is(err, io.EOF) {
				return n, err
			}
			c.reader = nil
		}
		messageType, reader, err := c.conn.NextReader()
		if err != nil {
			return 0, err
		}
		if messageType != gorilla.BinaryMessage {
			continue
		}
		c.reader = reader
	}
}

func (c *Conn) Write(p []byte) (int, error) {
	if err := c.conn.WriteMessage(gorilla.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *Conn) Close() error {
	return c.conn.Close()
}

func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Conn) SetDeadline(t time.Time) error {
	if err := c.conn.SetReadDeadline(t); err != nil {
		return err
	}
	return c.conn.SetWriteDeadline(t)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}
