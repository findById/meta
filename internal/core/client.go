package core

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	StatusNotAuthorized int32 = 0
	StatusConnected     int32 = 1
	StatusDisconnected  int32 = 2
)

type Client struct {
	ID                 string
	Conn               net.Conn
	Ctx                context.Context
	cancelFunc         context.CancelFunc
	Broker             *Broker
	Reader             *bufio.Reader
	Writer             *bufio.Writer
	Status             int32
	TopicMap           sync.Map
	CleanSession       bool
	KeepAlive          time.Duration
	Will               *WillMessage
	Protocol           string
	ReceiveMaximum     uint16
	outbound           chan []byte
	done               chan struct{}
	closeOnce          sync.Once
	writeTimeout       time.Duration
	gracefulDisconnect atomic.Bool
	inflight           sync.Map
	pending            sync.Map
}

type InflightMessage struct {
	PacketID uint16
	Payload  []byte
	LastSent time.Time
	Attempts int
}

func NewClient(conn net.Conn, broker *Broker, protocol string) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	outboundQueueSize := DefaultOutboundQueueSize
	writeTimeout := DefaultWriteTimeout
	if broker != nil {
		outboundQueueSize = broker.OutboundQueueSize()
		writeTimeout = broker.WriteTimeout()
	}
	client := &Client{
		Conn:         conn,
		Broker:       broker,
		Ctx:          ctx,
		cancelFunc:   cancel,
		Reader:       bufio.NewReader(conn),
		Writer:       bufio.NewWriter(conn),
		Status:       StatusNotAuthorized,
		Protocol:     protocol,
		outbound:     make(chan []byte, outboundQueueSize),
		done:         make(chan struct{}),
		writeTimeout: writeTimeout,
		KeepAlive:    DefaultKeepAliveTimeout,
	}
	_ = conn.SetReadDeadline(time.Now().Add(DefaultKeepAliveTimeout))
	go client.writeLoop()
	go client.redeliverLoop()
	return client
}

func (c *Client) ReadBuffer(size int) ([]byte, error) {
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

func (c *Client) WriteBuffer(buf []byte) error {
	if c.IsDisconnected() {
		return nil
	}
	if buf == nil {
		return nil
	}
	if c.Conn == nil {
		c.Close()
		return errors.New("connect lost")
	}
	select {
	case c.outbound <- CloneBytes(buf):
		return nil
	case <-c.done:
		return errors.New("client closed")
	default:
		c.Close()
		return errors.New("client outbound queue full")
	}
}

func (c *Client) WriteDirect(buf []byte) error {
	if c == nil || len(buf) == 0 {
		return nil
	}
	if c.Conn == nil {
		return errors.New("connect lost")
	}
	_ = c.Conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	n, err := c.Writer.Write(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return errors.New("short write")
	}
	return c.Writer.Flush()
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		atomic.StoreInt32(&c.Status, StatusDisconnected)
		c.cancelFunc()
		close(c.done)
		if c.Broker != nil && !c.gracefulDisconnect.Load() && c.Will != nil {
			c.Broker.Publish(*c.Will)
		}
		if c.Broker != nil {
			c.Broker.UnregisterClient(c)
		}
		if c.Conn != nil {
			_ = c.Conn.Close()
		}
	})
}

func (c *Client) SetStatus(status int32) {
	atomic.StoreInt32(&c.Status, status)
}

func (c *Client) MarkGraceful() {
	c.gracefulDisconnect.Store(true)
}

func (c *Client) IsConnected() bool {
	return atomic.LoadInt32(&c.Status) == StatusConnected
}

func (c *Client) IsDisconnected() bool {
	return atomic.LoadInt32(&c.Status) == StatusDisconnected
}

func (c *Client) RefreshDeadline() {
	if c.Conn == nil {
		return
	}
	timeout := c.KeepAlive
	if timeout <= 0 {
		timeout = DefaultKeepAliveTimeout
	}
	_ = c.Conn.SetReadDeadline(time.Now().Add(timeout))
}

func (c *Client) TopicSnapshot() map[string]byte {
	topics := make(map[string]byte)
	c.TopicMap.Range(func(key, value interface{}) bool {
		topic, ok := key.(string)
		if !ok {
			return true
		}
		qos, ok := value.(byte)
		topics[topic] = qos
		return true
	})
	return topics
}

func (c *Client) HasSubscriptionFor(topic string) bool {
	matched := false
	c.TopicMap.Range(func(key, _ interface{}) bool {
		filter, ok := key.(string)
		if !ok {
			return true
		}
		if TopicMatch(filter, topic) {
			matched = true
			return false
		}
		return true
	})
	return matched
}

func (c *Client) TrackInflight(packetID uint16, payload []byte) {
	if packetID == 0 || len(payload) == 0 {
		return
	}
	c.inflight.Store(packetID, &InflightMessage{
		PacketID: packetID,
		Payload:  CloneBytes(payload),
		LastSent: time.Now(),
		Attempts: 1,
	})
}

func (c *Client) AckInflight(packetID uint16) {
	if packetID == 0 {
		return
	}
	c.inflight.Delete(packetID)
}

func (c *Client) StorePending(packetID uint16, value any) {
	if packetID == 0 || value == nil {
		return
	}
	c.pending.Store(packetID, value)
}

func (c *Client) ReleasePending(packetID uint16) (any, bool) {
	if packetID == 0 {
		return nil, false
	}
	return c.pending.LoadAndDelete(packetID)
}

func (c *Client) writeLoop() {
	for {
		select {
		case <-c.done:
			return
		case buf := <-c.outbound:
			if len(buf) == 0 || c.Conn == nil {
				continue
			}
			_ = c.Conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
			n, err := c.Writer.Write(buf)
			if err != nil {
				c.logWriteError("write err", err)
				c.Close()
				return
			}
			if n != len(buf) {
				log.Println("short write")
				c.Close()
				return
			}
			if err := c.Writer.Flush(); err != nil {
				c.logWriteError("flush err", err)
				c.Close()
				return
			}
		}
	}
}

func (c *Client) logWriteError(prefix string, err error) {
	if err == nil || IsQuietNetworkError(err) || c.IsDisconnected() {
		return
	}
	log.Println(prefix, err)
}

func IsQuietNetworkError(err error) bool {
	if err == nil {
		return false
	}
	text := err.Error()
	return strings.Contains(text, "use of closed network connection") ||
		strings.Contains(text, "connection reset by peer") ||
		strings.Contains(text, "broken pipe")
}

func (c *Client) redeliverLoop() {
	ticker := time.NewTicker(DefaultInflightRetry)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			now := time.Now()
			c.inflight.Range(func(key, value interface{}) bool {
				inflight, ok := value.(*InflightMessage)
				if !ok {
					c.inflight.Delete(key)
					return true
				}
				if now.Sub(inflight.LastSent) < DefaultInflightRetry {
					return true
				}
				if inflight.Attempts >= DefaultMaxInflightRetry {
					c.Close()
					return false
				}
				buf := CloneBytes(inflight.Payload)
				if len(buf) > 0 {
					buf[0] |= 0x08
				}
				inflight.LastSent = now
				inflight.Attempts++
				if err := c.WriteBuffer(buf); err != nil {
					c.Close()
					return false
				}
				return true
			})
		}
	}
}
