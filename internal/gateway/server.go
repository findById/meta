package gateway

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"github.com/findById/meta/internal/core"
	"github.com/findById/meta/internal/protocol/mqtt3"
	"github.com/findById/meta/internal/protocol/mqtt5"
	"github.com/findById/meta/internal/security"
	tcplistener "github.com/findById/meta/internal/transport/tcp"
	wslistener "github.com/findById/meta/internal/transport/websocket"
)

type Config struct {
	MQTT3TCPURI string
	MQTT5TCPURI string
	WebSocket   WebSocketConfig
}

type WebSocketConfig struct {
	Addr     string
	Path     string
	Protocol string
}

type Server struct {
	Broker    *core.Broker
	config    Config
	tcp       []*tcplistener.Listener
	websocket []*wslistener.Server
	lock      sync.Mutex
}

func NewServer(broker *core.Broker, config Config) *Server {
	return &Server{
		Broker: broker,
		config: config,
	}
}

func (s *Server) Start() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.Broker == nil {
		s.Broker = core.NewBroker()
	}
	s.Broker.Start()
	if s.config.MQTT3TCPURI != "" {
		listener := tcplistener.NewListener(s.config.MQTT3TCPURI, s.accept(security.ProtocolMQTT311, func(client *core.Client) core.Handler {
			return mqtt3.NewHandler(client)
		}))
		if err := listener.Start(); err != nil {
			return err
		}
		s.tcp = append(s.tcp, listener)
	}
	if s.config.MQTT5TCPURI != "" {
		listener := tcplistener.NewListener(s.config.MQTT5TCPURI, s.accept(security.ProtocolMQTT5, func(client *core.Client) core.Handler {
			return mqtt5.NewHandler(client)
		}))
		if err := listener.Start(); err != nil {
			return err
		}
		s.tcp = append(s.tcp, listener)
	}
	if s.config.WebSocket.Addr != "" {
		protocol := s.config.WebSocket.Protocol
		if protocol == "" {
			protocol = security.ProtocolMQTT311
		}
		factory := func(client *core.Client) core.Handler {
			return mqtt3.NewHandler(client)
		}
		if protocol == security.ProtocolMQTT5 {
			factory = func(client *core.Client) core.Handler {
				return mqtt5.NewHandler(client)
			}
		}
		server := wslistener.NewServer(s.config.WebSocket.Addr, s.config.WebSocket.Path, s.accept(protocol, factory))
		if err := server.Start(); err != nil {
			return err
		}
		s.websocket = append(s.websocket, server)
	}
	return nil
}

func (s *Server) Stop(ctx context.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, listener := range s.tcp {
		listener.Stop()
	}
	for _, server := range s.websocket {
		server.Stop(ctx)
	}
	if s.Broker != nil {
		s.Broker.Stop()
	}
	log.Println("gateway stopped")
}

func (s *Server) accept(protocol string, factory core.HandlerFactory) func(net.Conn) {
	return func(conn net.Conn) {
		s.Broker.Stats.AcceptedConnections.Add(1)
		s.Broker.Accept(conn, protocol, factory)
	}
}

func ShutdownContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}
