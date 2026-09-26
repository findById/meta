package main

import (
	"flag"
	"fmt"
	"github.com/findById/meta/internal/cache"
	"github.com/findById/meta/internal/core"
	"github.com/findById/meta/internal/gateway"
	"github.com/findById/meta/internal/security"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
)

var (
	host         = flag.String("host", "tcp://0.0.0.0:1883", "mqtt 3.1.1 tcp host, kept for compatibility")
	mqtt5Host    = flag.String("mqtt5", "", "mqtt 5.0 tcp host, for example tcp://0.0.0.0:1885")
	wsAddr       = flag.String("ws", "", "mqtt over websocket listen address, for example :8083")
	wsPath       = flag.String("ws-path", "/mqtt", "mqtt over websocket path")
	wsProtocol   = flag.String("ws-protocol", security.ProtocolMQTT311, "websocket mqtt protocol: mqtt-3.1.1 or mqtt-5.0")
	authMode     = flag.String("auth", "allow-all", "gateway auth mode: allow-all or api")
	authAPI      = flag.String("auth-api", "", "admin service base url for gateway api auth, for example http://127.0.0.1:18080")
	authAPIToken = flag.String("auth-api-token", "", "admin service token for gateway api auth")
	metricsAddr  = flag.String("metrics", "", "metrics and pprof listen address, for example :8080")
	workers      = flag.Int("workers", 0, "broker worker count, default runtime.NumCPU")
	taskQueue    = flag.Int("task-queue", core.DefaultTaskQueueSize, "broker publish task queue size")
	enqueueWait  = flag.Duration("enqueue-wait", core.DefaultTaskEnqueueWait, "max wait for publish task enqueue before rejecting")
	outboundQ    = flag.Int("outbound-queue", core.DefaultOutboundQueueSize, "per-client outbound queue size")
)

func main() {
	flag.Parse()
	if *host == "" && *mqtt5Host == "" && *wsAddr == "" {
		flag.PrintDefaults()
		return
	}

	var securityProvider security.Provider = security.AllowAll{}
	switch *authMode {
	case "allow-all":
		securityProvider = security.AllowAll{}
	case "api":
		if *authAPI == "" {
			log.Fatal("auth api is required when -auth api, for example -auth-api http://127.0.0.1:18080")
		}
		securityProvider = security.NewHTTPProvider(*authAPI, *authAPIToken, cache.NewMemoryCache())
	default:
		log.Fatalf("unsupported gateway auth mode %q, use allow-all or api", *authMode)
	}
	options := []core.Option{
		core.WithSecurity(securityProvider),
		core.WithTaskQueueSize(*taskQueue),
		core.WithTaskEnqueueWait(*enqueueWait),
		core.WithOutboundQueueSize(*outboundQ),
	}
	if *workers > 0 {
		options = append(options, core.WithWorkerCount(*workers))
	}
	meta := core.NewBroker(options...)
	core.PublishStats(meta)
	if *metricsAddr != "" {
		go func() {
			log.Println("metrics listening on", *metricsAddr)
			if err := http.ListenAndServe(*metricsAddr, nil); err != nil {
				log.Println("metrics server", err)
			}
		}()
	}
	server := gateway.NewServer(meta, gateway.Config{
		MQTT3TCPURI: *host,
		MQTT5TCPURI: *mqtt5Host,
		WebSocket: gateway.WebSocketConfig{
			Addr:     *wsAddr,
			Path:     *wsPath,
			Protocol: *wsProtocol,
		},
	})
	if err := server.Start(); err != nil {
		log.Fatal("start gateway", err)
	}

	waitForSignal()
	ctx, cancel := gateway.ShutdownContext()
	defer cancel()
	server.Stop(ctx)
	fmt.Println("Bye")
}

func waitForSignal() os.Signal {
	signalChan := make(chan os.Signal, 1)
	defer close(signalChan)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	s := <-signalChan
	signal.Stop(signalChan)
	return s
}
