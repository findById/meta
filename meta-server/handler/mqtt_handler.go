package handler

import (
	"net"
	"meta/meta-server/client"
)

type MQTTHandler struct {
	cm client.ClientManager
	client client.MetaClient
}

func NewMQTTHandler(conn *net.TCPConn) *MQTTHandler {

	return &MQTTHandler{
	}
}