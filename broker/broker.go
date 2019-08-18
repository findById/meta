package broker

import (
	"sync"
	"net"
	"net/url"
	"runtime"
	"log"
	"fmt"
	"time"
)

type MetaBroker struct {
	Id        string
	Lock      sync.RWMutex
	ClientMap sync.Map
	TaskQueue chan func()
}

func NewMetaBroker() *MetaBroker {
	return &MetaBroker{
		Id:        fmt.Sprint(time.Now().UnixNano()),
		TaskQueue: make(chan func()),
	}
}

func (b *MetaBroker) Start(uri string) {
	go b.accept(uri)

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		for {
			select {
			case task, ok := <-b.TaskQueue:
				if !ok {
					continue
				}
				task()
			case <-ticker.C:
				log.Println("monitor ", runtime.NumGoroutine())
			}
		}
	}()
}

func (b *MetaBroker) Worker(task func()) {
	if task == nil {
		return
	}
	b.TaskQueue <- task
}

func (b *MetaBroker) accept(uri string) {
	u, err := url.Parse(uri)
	if err != nil {
		panic(err)
	}

	addr, err := net.ResolveTCPAddr(u.Scheme, u.Host)
	if err != nil {
		panic(err)
	}
	listen, err := net.ListenTCP(u.Scheme, addr)
	if err != nil {
		panic(err)
	}
	defer listen.Close()
	fmt.Println("Accepting connections at:", uri)
	for {
		conn, err := listen.AcceptTCP()
		if err != nil {
			continue
		}
		log.Println("Handle connection ", conn.RemoteAddr().String(), runtime.NumGoroutine())
		go b.handleConnection(conn)
	}
}

func (b *MetaBroker) handleConnection(conn *net.TCPConn) {
	client := NewMetaClient(conn, b)

	handler := NewMQTTHandler(client)
	handler.Start()
}
