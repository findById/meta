package main

import (
	"meta/mq-server/server"
	"flag"
)

var (
	host string
)

func main() {
	flag.StringVar(&host, "host", "tcp://0.0.0.0:8081", "broker host");
	flag.Parse();
	if host == "" {
		flag.PrintDefaults();
		return
	}

	mq := server.NewMQServer();
	mq.Start(host);
}
