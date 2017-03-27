package main

import (
	"meta/meta-server/server"
	"flag"
)

var (
	host string
)

func main() {
	flag.StringVar(&host, "host", "tcp://0.0.0.0:1883", "broker host");
	flag.Parse();
	if host == "" {
		flag.PrintDefaults();
		return
	}

	mq := server.NewMetaServer();
	mq.Start(host);
}
