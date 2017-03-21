package handler

import (
	"meta/mq-server/client"
	"net"
)

type AccessHandler struct {
	id string
	cm *client.ClientManager
}

func NewAccessHandler(id string, cm *client.ClientManager) *AccessHandler {
	return &AccessHandler{
		cm:cm,
	}
}

func (this *AccessHandler) HandleMessage(conn *net.TCPConn) {

}

func HeartBeat(conn net.TCPConn, timeout int) {

}

func (this *AccessHandler) Read() {
	for {
		select {

		}
	}
}

func (this *AccessHandler) Write() {
	for {
		select {

		}
	}
}
