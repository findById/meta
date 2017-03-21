package server

import (
	"net"
	"log"
	"meta/mq-server/client"
	"net/url"
	"io"
	"bufio"
	"encoding/binary"
	"meta/mq-server/handler"
	m "github.com/surgemq/message"
)

type MQServer struct {
	cm       *client.ClientManager
	listener *net.TCPListener
	limiter  string
}

func NewMQServer() *MQServer {
	cm := client.NewClientManager();
	return &MQServer{
		cm:cm,
	}
}

func (this *MQServer) Start(uri string) {
	u, err := url.Parse(uri);
	if err != nil {
		panic(err);
	}

	addr, err := net.ResolveTCPAddr(u.Scheme, u.Host);
	if (err != nil) {
		panic(err);
	}
	this.listener, _ = net.ListenTCP(u.Scheme, addr);
	defer this.listener.Close();
	log.Println("broker started on:", uri);
	for {
		conn, err := this.listener.AcceptTCP();
		if (err != nil) {
			continue;
		}
		log.Println("Handle connection ", conn.RemoteAddr().String(), this.cm.Size());
		go this.handleConnection(conn);
	}
}

func (this *MQServer) Stop() {
	this.listener.Close();
}

func (this *MQServer) handleConnection(conn *net.TCPConn) {
	//defer conn.Close();
	client := &client.MQClient{
		Id:conn.RemoteAddr().String(),
		Conn:conn,
		Br:bufio.NewReader(conn),
		Bw:bufio.NewWriter(conn),
		InChan:make(chan (m.Message), 1000),
		OutChan:make(chan (m.Message), 1000),
	}
	this.cm.AddClient(client);
	// conn.SetDeadline(time.Now().Add(time.Second * 100))

	go handler.NewAccessHandler(client.Id, this.cm).HandleMessage(conn);
	go this.push(client);
	go this.process(client);

	this.handleReceive(client);
}

func HeartBeat(conn net.TCPConn, timeout int) {

}

func (this *MQServer) process(client *client.MQClient) {
	for {
		select {
		case msg := <-client.InChan:
			switch msg.Type() {
			case m.CONNECT:
				rm := msg.(*m.ConnectMessage)
				log.Println("receive", rm)
				ack := m.NewConnackMessage()
				ack.SetReturnCode(m.ConnectionAccepted)
				ack.SetSessionPresent(true)
				client.OutChan <- ack
				break;
			case m.PUBLISH:
				rm := msg.(*m.PublishMessage)

				for _, c := range this.cm.CloneMap() {
					for topic := range c.Topics {
						log.Println(topic);
					}
					c.OutChan <- rm;
				}
				break;
			}
		}
	}
}

func (this *MQServer) handleReceive(client *client.MQClient) {
	b, err := client.Br.Peek(1)
	if err != nil {
		log.Println(err)
		return
	}
	t := m.MessageType(b[0] >> 4)
	msg, err := t.New()
	if err != nil {
		log.Println(err)
		return
	}
	n := 2
	buf, err := client.Br.Peek(n)
	if err != nil {
		log.Println(err)
		return
	}
	for buf[n-1] >= 0x80 {
		n++
		buf, err = client.Br.Peek(n)
		if err != nil {
			log.Println(err)
			return
		}
	}
	l, r := binary.Uvarint(buf[1:])
	buf = make([]byte, int(l)+r+1)
	n, err = io.ReadFull(client.Br, buf)
	if err != nil {
		log.Println(err)
		return
	}
	if n != len(buf) {
		log.Println("short read.")
		return
	}
	_, err = msg.Decode(buf)
	if err != nil {
		log.Println(err)
		return
	}
	client.InChan <- msg
}

func (this *MQServer) push(client *client.MQClient) {
	for {
		select {
		case msg := <-client.OutChan:
			{
				buf := make([]byte, msg.Len())
				n, err := msg.Encode(buf)
				if err != nil {
					log.Println(err)
					continue
				}
				if n != len(buf) {
					log.Println("short encode.")
					continue
				}
				_, err = client.Conn.Write(buf);
				if err != nil {
					this.cm.RemoveClient(client.Id);
				}
				break;
			}
		}
	}
}