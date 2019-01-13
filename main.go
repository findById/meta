package main

import (
	"flag"
	"github.com/findById/meta/storage"
	"fmt"
	"github.com/findById/meta/broker"
	"os"
	"os/signal"
)

var (
	host = flag.String("host", "tcp://0.0.0.0:1883", "broker host")
)

func main() {
	flag.Parse()
	if *host == "" {
		flag.PrintDefaults()
		return
	}

	err := storage.Save("asdfasdf", storage.Message{
		ProducerId: "asdfasdf",
		ConsumerId: "asdfasdf",
	})
	if err != nil {
		panic(err)
	}

	msg, err := storage.Get("asdfasdf")
	if err != nil {
		panic(err)
	}
	fmt.Println(msg)

	meta := broker.NewMetaBroker()
	meta.Start(*host)

	waitForSignal()
	fmt.Println("Bye")
}

func waitForSignal() os.Signal {
	signalChan := make(chan os.Signal, 1)
	defer close(signalChan)
	signal.Notify(signalChan, os.Kill, os.Interrupt)
	s := <-signalChan
	signal.Stop(signalChan)
	return s
}
