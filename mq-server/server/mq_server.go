package server

import (
	"net"
	"log"
	"meta/mq-server/client"
	"net/url"
	"io"
	"bufio"
	"encoding/binary"
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
	log.Println("Accepting connections at:", uri);
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

	go this.process(client);
	go this.push(client);

	this.handleReceive(client);
}

func HeartBeat(conn net.TCPConn, timeout int) {

}

func (this *MQServer) process(client *client.MQClient) {
	for {
		select {
		case msg := <-client.InChan:
			log.Println("receive", msg)
			switch msg.Type() {
			case m.CONNECT:
				ack := m.NewConnackMessage()
				ack.SetReturnCode(m.ConnectionAccepted)
				client.OutChan <- ack
			case m.SUBSCRIBE:
				ack := m.NewSubackMessage()
				ack.SetPacketId(msg.PacketId())
				ack.AddReturnCode(m.QosAtMostOnce)
				client.OutChan <- ack
			case m.PINGREQ:
				ack := m.NewPingrespMessage()
				ack.SetPacketId(msg.PacketId())
				client.OutChan <- ack
			case m.PUBLISH:
				pMsg := msg.(*m.PublishMessage)
				log.Println("PAYLOAD", string(pMsg.Payload()))
				ack := m.NewPubackMessage()
				ack.SetPacketId(msg.PacketId())
				client.OutChan <- ack
			}
		}
	}
}

func (this *MQServer) handleReceive(client *client.MQClient) {
	for {
		b, err := client.Br.Peek(1)
		if err != nil {
			if err == io.EOF {
				continue
			}
			log.Println("peek type", err)
			return
		}
		t := m.MessageType(b[0] >> 4)
		msg, err := t.New()
		if err != nil {
			log.Println("create message", err)
			return
		}
		n := 2
		buf, err := client.Br.Peek(n)
		if err != nil {
			log.Println("peek header", err)
			return
		}
		for buf[n-1] >= 0x80 {
			n++
			buf, err = client.Br.Peek(n)
			if err != nil {
				log.Println("try peek header", err)
				return
			}
		}
		l, r := binary.Uvarint(buf[1:])
		buf = make([]byte, int(l)+r+1)
		n, err = io.ReadFull(client.Br, buf)
		if err != nil {
			log.Println("read header", err)
			return
		}
		if n != len(buf) {
			log.Println("short read.")
			return
		}
		_, err = msg.Decode(buf)
		if err != nil {
			log.Println("decode", err)
			return
		}
		client.InChan <- msg
	}
}

func (this *MQServer) push(client *client.MQClient) {
	for {
		select {
		case msg := <-client.OutChan:
			log.Println("send", msg)
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
			n, err = client.Bw.Write(buf)
			if err != nil {
				return
			}
			if n != len(buf) {
				log.Println("short write")
				return
			}
			err = client.Bw.Flush()
			if err != nil {
				return
			}
		}
	}
}