package broker

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

const (
	NotAuthorized = 0
	Connected     = 1
	Disconnected  = 2
)

type MetaClient struct {
	Id         string
	Conn       *net.TCPConn
	Ctx        context.Context
	cancelFunc context.CancelFunc
	Lock       sync.RWMutex
	Broker     *MetaBroker
	Reader     *bufio.Reader
	Writer     *bufio.Writer
	Status     int
	TopicMap   sync.Map
}

func NewMetaClient(conn *net.TCPConn, broker *MetaBroker) *MetaClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &MetaClient{
		Conn:       conn,
		Broker:     broker,
		Ctx:        ctx,
		cancelFunc: cancel,
		Reader:     bufio.NewReader(conn),
		Writer:     bufio.NewWriter(conn),
		Status:     NotAuthorized,
	}
}

// TODO not optimal
func (c MetaClient) ReadBuffer(size int) ([]byte, error) {
	buf := make([]byte, size)
	n, err := io.ReadFull(c.Reader, buf)
	if err != nil {
		log.Println("read header", err)
		return nil, err
	}
	if n != len(buf) {
		log.Println("short read.")
		return nil, err
	}
	return buf, nil
}

func (c MetaClient) WriteBuffer(buf []byte) error {
	if c.Status == Disconnected {
		return nil
	}
	if buf == nil {
		return nil
	}
	if c.Conn == nil {
		c.Close()
		return errors.New("connect lost")
	}
	n, err := c.Writer.Write(buf)
	if err != nil {
		log.Println("write err")
		return err
	}
	if n != len(buf) {
		log.Println("short write")
		return err
	}
	if err := c.Writer.Flush(); err != nil {
		return err
	}
	return nil
}

func (c MetaClient) Close() {
	if c.Status == Disconnected {
		return
	}
	c.Status = Disconnected

	c.cancelFunc()
	// wait for message complete
	time.Sleep(1 * time.Second)

	c.Broker.ClientMap.Delete(c.Id)

	if c.Conn != nil {
		c.Conn.Close()
		c.Conn = nil
	}
}
