package client

import (
	"net"
	"bufio"
	"log"
	"io"
	"encoding/binary"
	packet "github.com/surgemq/message"
)

type MQClient struct {
	Id      string
	Conn    *net.TCPConn
	Topics  []string

	Br      *bufio.Reader
	Bw      *bufio.Writer
	InChan  chan (packet.Message)
	OutChan chan (packet.Message)
}

func (this *MQClient) Close() {
	this.Conn.Close();
}

func NewMetaClient(conn *net.TCPConn) *MQClient {
	client := &MQClient{
		Id:conn.RemoteAddr().String(),
		Conn:conn,
		Br:bufio.NewReader(conn),
		Bw:bufio.NewWriter(conn),
		InChan:make(chan (packet.Message), 1000),
		OutChan:make(chan (packet.Message), 1000),
	}
	return client;
}

func (this *MQClient) Start() {
	go this.process();
	go this.WritePacket();
	go this.ReadPacket();
}

func (this *MQClient) process() {
	for {
		select {
		case msg := <-this.InChan:
			log.Println("receive", msg)
			switch msg.Type() {
			case packet.CONNECT:
				data := msg.(*packet.ConnectMessage);
				log.Println(string(data.Username()), string(data.Password()), data.Version());

				ack := packet.NewConnackMessage()
				ack.SetReturnCode(packet.ConnectionAccepted)
				this.OutChan <- ack
				break;
			case packet.SUBSCRIBE:
				data := msg.(*packet.SubscribeMessage);
				log.Println(data.String(), data.Qos());

				ack := packet.NewSubackMessage()
				ack.SetPacketId(msg.PacketId())
				ack.AddReturnCode(packet.QosAtMostOnce)
				this.OutChan <- ack
				break;
			case packet.UNSUBSCRIBE:
				data := msg.(*packet.UnsubscribeMessage);
				log.Println(data.Topics());

				ack := packet.NewUnsubackMessage();
				ack.SetPacketId(msg.PacketId())
				this.OutChan <- ack;
				break
			case packet.PUBLISH:
				data := msg.(*packet.PublishMessage)
				log.Println("payload:", string(data.Payload()))

				ack := packet.NewPubackMessage()
				ack.SetPacketId(msg.PacketId())
				this.OutChan <- ack
				break;
			case packet.PINGREQ:

				ack := packet.NewPingrespMessage()
				ack.SetPacketId(msg.PacketId())
				this.OutChan <- ack
				break;
			case packet.DISCONNECT:

				break;
			default:
				log.Println("unsupported type");
				break
			}
		}
	}
}

func (this *MQClient) ReadPacket() {
	for {
		b, err := this.Br.Peek(1)
		if err != nil {
			if err == io.EOF {
				continue
			}
			log.Println("peek type", err)
			return
		}
		t := packet.MessageType(b[0] >> 4)
		msg, err := t.New()
		if err != nil {
			log.Println("create message", err)
			return
		}
		n := 2
		buf, err := this.Br.Peek(n)
		if err != nil {
			log.Println("peek header", err)
			return
		}
		for buf[n - 1] >= 0x80 {
			n++
			buf, err = this.Br.Peek(n)
			if err != nil {
				log.Println("try peek header", err)
				return
			}
		}
		l, r := binary.Uvarint(buf[1:])
		buf = make([]byte, int(l) + r + 1)
		n, err = io.ReadFull(this.Br, buf)
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
		this.InChan <- msg
	}
}

func (this *MQClient) WritePacket() {
	for {
		select {
		case msg := <-this.OutChan:
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
			n, err = this.Bw.Write(buf)
			if err != nil {
				return
			}
			if n != len(buf) {
				log.Println("short write")
				return
			}
			err = this.Bw.Flush()
			if err != nil {
				return
			}
		}
	}
}

