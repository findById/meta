package client

import (
	"bufio"
	"net"

	packet "github.com/surgemq/message"
)

type MetaClient struct {
	Id        string
	Conn      *net.TCPConn
	Topics    []string

	Br        *bufio.Reader
	Bw        *bufio.Writer
	InChan    chan (packet.Message)
	OutChan   chan (packet.Message)

	IsClosed  bool
	IsAuthed  bool
}

func (this *MetaClient) Close() {
	this.IsClosed = true
	this.Conn.Close()
}

func NewMetaClient(conn *net.TCPConn) *MetaClient {
	client := &MetaClient{
		Id:         conn.RemoteAddr().String(),
		Conn:       conn,
		Topics:     make([]string, 0),
		Br:         bufio.NewReader(conn),
		Bw:         bufio.NewWriter(conn),
		InChan:     make(chan (packet.Message), 1000),
		OutChan:    make(chan (packet.Message), 1000),
		IsClosed:   false,
		IsAuthed:   false,
	}
	return client
}
