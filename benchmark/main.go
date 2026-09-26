package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

type metrics struct {
	connectOK       atomic.Int64
	connectFailed   atomic.Int64
	subscribeOK     atomic.Int64
	subscribeFailed atomic.Int64
	publishOK       atomic.Int64
	publishFailed   atomic.Int64
	received        atomic.Int64
}

func main() {
	broker := flag.String("broker", "tcp://127.0.0.1:1883", "MQTT broker address")
	clients := flag.Int("n", 100, "client count")
	topicCount := flag.Int("topics", 1, "topic count")
	duration := flag.Duration("duration", 30*time.Second, "benchmark duration")
	interval := flag.Duration("interval", 2*time.Second, "publish interval per client")
	qos := flag.Uint("qos", 0, "publish/subscribe qos")
	payloadSize := flag.Int("payload", 64, "payload size in bytes")
	username := flag.String("username", "admin", "username")
	password := flag.String("password", "admin", "password")
	clientID := flag.String("client-id", "", "client id, exact when n=1; suffix is added when n>1")
	connectRamp := flag.Duration("connect-ramp", 0, "spread client connections across this duration")
	connectTimeout := flag.Duration("connect-timeout", 10*time.Second, "per-client connect timeout")
	flag.Parse()

	if *clients <= 0 {
		fmt.Println("client count must be positive")
		return
	}
	if *topicCount <= 0 {
		fmt.Println("topic count must be positive")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	stats := &metrics{}
	wg := sync.WaitGroup{}
	start := time.Now()
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go runClient(ctx, i, *broker, *topicCount, byte(*qos), *payloadSize, *interval, *username, *password, *clientID, *clients, *connectRamp, *connectTimeout, stats, &wg)
	}
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("duration=%s clients=%d topics=%d qos=%d interval=%s payload=%d\n", elapsed.Truncate(time.Millisecond), *clients, *topicCount, *qos, *interval, *payloadSize)
	fmt.Printf("connect_ok=%d connect_failed=%d subscribe_ok=%d subscribe_failed=%d publish_ok=%d publish_failed=%d received=%d\n",
		stats.connectOK.Load(),
		stats.connectFailed.Load(),
		stats.subscribeOK.Load(),
		stats.subscribeFailed.Load(),
		stats.publishOK.Load(),
		stats.publishFailed.Load(),
		stats.received.Load(),
	)
	if elapsed > 0 {
		fmt.Printf("publish_qps=%.2f receive_qps=%.2f\n",
			float64(stats.publishOK.Load())/elapsed.Seconds(),
			float64(stats.received.Load())/elapsed.Seconds(),
		)
	}
}

func runClient(ctx context.Context, id int, broker string, topicCount int, qos byte, payloadSize int, interval time.Duration, username string, password string, clientID string, clientCount int, connectRamp time.Duration, connectTimeout time.Duration, stats *metrics, wg *sync.WaitGroup) {
	defer wg.Done()
	if connectRamp > 0 && clientCount > 1 {
		delay := time.Duration(int64(connectRamp) * int64(id) / int64(clientCount))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}

	if clientID == "" {
		clientID = fmt.Sprintf("meta-bench-%d-%d", id, time.Now().UnixNano())
	} else if clientCount > 1 {
		clientID = fmt.Sprintf("%s-%d", clientID, id)
	}
	opts := MQTT.NewClientOptions().
		AddBroker(broker).
		SetUsername(username).
		SetPassword(password).
		SetClientID(clientID).
		SetConnectTimeout(connectTimeout).
		SetAutoReconnect(false)
	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		stats.received.Add(1)
	})

	client := MQTT.NewClient(opts)
	if token := client.Connect(); !token.WaitTimeout(connectTimeout) || token.Error() != nil {
		stats.connectFailed.Add(1)
		if token.Error() != nil {
			fmt.Printf("connect failed client=%s error=%v\n", clientID, token.Error())
		}
		return
	}
	stats.connectOK.Add(1)
	defer client.Disconnect(250)

	topic := fmt.Sprintf("bench/%d", id%topicCount)
	if token := client.Subscribe(topic, qos, func(client MQTT.Client, msg MQTT.Message) {
		stats.received.Add(1)
	}); !token.WaitTimeout(10*time.Second) || token.Error() != nil {
		stats.subscribeFailed.Add(1)
		if token.Error() != nil {
			fmt.Printf("subscribe failed client=%s topic=%s error=%v\n", clientID, topic, token.Error())
		}
		return
	}
	stats.subscribeOK.Add(1)

	payload := []byte(strings.Repeat("x", payloadSize))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			token := client.Publish(topic, qos, false, payload)
			if !token.WaitTimeout(10*time.Second) || token.Error() != nil {
				stats.publishFailed.Add(1)
				continue
			}
			stats.publishOK.Add(1)
		}
	}
}
