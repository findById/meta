package server

import "testing"

func TestRemoteServer(t *testing.T) {
	mq := NewMQServer();
	mq.Start("tcp://0.0.0.0:8081");
	mq.Stop();
}
