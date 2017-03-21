package client

import (
	"net"
	"github.com/surgemq/message"
	"bufio"
)

type MQClient struct {
	Id string
	Conn  *net.TCPConn
	Topics []string

	Br *bufio.Reader
	Bw *bufio.Writer
	InChan chan (message.Message)
	OutChan chan (message.Message)
}

func (this *MQClient) Close() {
	this.Conn.Close();
}