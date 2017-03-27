package server

import (
	"log"
	"meta/meta-server/client"
	"meta/meta-server/handler"
	"net"
	"net/url"
	"runtime"
)

type MetaServer struct {
	cm       *client.ClientManager
	listener *net.TCPListener
	limiter  string
}

func NewMetaServer() *MetaServer {
	cm := client.NewClientManager()
	return &MetaServer{
		cm: cm,
	}
}

func (this *MetaServer) Start(uri string) {
	u, err := url.Parse(uri)
	if err != nil {
		panic(err)
	}

	addr, err := net.ResolveTCPAddr(u.Scheme, u.Host)
	if err != nil {
		panic(err)
	}
	this.listener, err = net.ListenTCP(u.Scheme, addr)
	if err != nil {
		panic(err)
	}
	defer this.listener.Close()
	log.Println("Accepting connections at:", uri)
	for {
		conn, err := this.listener.AcceptTCP()
		if err != nil {
			continue
		}
		log.Println("Handle connection ", conn.RemoteAddr().String(), this.cm.Size(), runtime.NumGoroutine())
		go this.handleConnection(conn)
	}
}

func (this *MetaServer) Stop() {
	this.listener.Close()
}

func (this *MetaServer) handleConnection(conn *net.TCPConn) {
	h := handler.NewMQTTHandler(conn, this.cm)
	h.Start()
}

func HeartBeat(conn net.TCPConn, timeout int) {

}
