package websocket

import (
	"context"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	gorilla "github.com/gorilla/websocket"
)

type Server struct {
	Addr    string
	Path    string
	Handler func(net.Conn)
	server  *http.Server
	started atomic.Bool
}

func NewServer(addr string, path string, handler func(net.Conn)) *Server {
	if path == "" {
		path = "/mqtt"
	}
	return &Server{
		Addr:    addr,
		Path:    path,
		Handler: handler,
	}
}

func (s *Server) Start() error {
	if !s.started.CompareAndSwap(false, true) {
		return nil
	}
	upgrader := gorilla.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
		Subprotocols: []string{"mqtt"},
	}
	mux := http.NewServeMux()
	mux.HandleFunc(s.Path, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("upgrade websocket", err)
			return
		}
		if s.Handler != nil {
			go s.Handler(NewConn(conn))
		} else {
			_ = conn.Close()
		}
	})
	s.server = &http.Server{
		Addr:              s.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Println("websocket listener started", s.Addr, s.Path)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Println("websocket server", err)
		}
	}()
	return nil
}

func (s *Server) Stop(ctx context.Context) {
	if !s.started.CompareAndSwap(true, false) {
		return
	}
	if s.server != nil {
		_ = s.server.Shutdown(ctx)
	}
}
