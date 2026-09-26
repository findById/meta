package tcp

import (
	"log"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

type Listener struct {
	URI      string
	Handler  func(net.Conn)
	listener *net.TCPListener
	stop     chan struct{}
	started  atomic.Bool
	wg       sync.WaitGroup
}

func NewListener(uri string, handler func(net.Conn)) *Listener {
	return &Listener{
		URI:     uri,
		Handler: handler,
		stop:    make(chan struct{}),
	}
}

func (l *Listener) Start() error {
	if !l.started.CompareAndSwap(false, true) {
		return nil
	}
	u, err := url.Parse(l.URI)
	if err != nil {
		return err
	}
	addr, err := net.ResolveTCPAddr(u.Scheme, u.Host)
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP(u.Scheme, addr)
	if err != nil {
		return err
	}
	l.listener = listener
	l.wg.Add(1)
	go l.accept()
	log.Println("tcp listener started", l.URI)
	return nil
}

func (l *Listener) Stop() {
	if !l.started.CompareAndSwap(true, false) {
		return
	}
	close(l.stop)
	if l.listener != nil {
		_ = l.listener.Close()
	}
	l.wg.Wait()
}

func (l *Listener) accept() {
	defer l.wg.Done()
	for {
		conn, err := l.listener.AcceptTCP()
		if err != nil {
			select {
			case <-l.stop:
				return
			default:
			}
			log.Println("accept tcp", err)
			continue
		}
		_ = conn.SetNoDelay(true)
		_ = conn.SetKeepAlive(true)
		_ = conn.SetKeepAlivePeriod(2 * time.Minute)
		if l.Handler != nil {
			go l.Handler(conn)
		} else {
			_ = conn.Close()
		}
	}
}
