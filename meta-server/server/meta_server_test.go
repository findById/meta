package server

import "testing"

func TestRemoteServer(t *testing.T) {
	mq := NewMetaServer();
	mq.Start("tcp://0.0.0.0:1883");
	mq.Stop();
}
