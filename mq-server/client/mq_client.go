package client

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"net"
	"time"

	packet "github.com/surgemq/message"
)

type MQClient struct {
	Id     string
	Conn   *net.TCPConn
	Topics []string

	Br      *bufio.Reader
	Bw      *bufio.Writer
	InChan  chan (packet.Message)
	OutChan chan (packet.Message)

	dispatcher func(c *MQClient, packet packet.Message)
	IsClosed   bool
}

func (this *MQClient) Close() {
	this.IsClosed = true
	this.Conn.Close()
}

func NewMetaClient(conn *net.TCPConn, dispatcher func(c *MQClient, packet packet.Message)) *MQClient {
	client := &MQClient{
		Id:         conn.RemoteAddr().String(),
		Conn:       conn,
		Topics:     make([]string, 0),
		Br:         bufio.NewReader(conn),
		Bw:         bufio.NewWriter(conn),
		InChan:     make(chan (packet.Message), 1000),
		OutChan:    make(chan (packet.Message), 1000),
		dispatcher: dispatcher,
		IsClosed:   false,
	}
	return client
}

func (this *MQClient) Start() {
	go this.process()
	go this.WritePacket()
	go this.ReadPacket()
}

func (this *MQClient) process() {
	for {
		select {
		case msg := <-this.InChan:
			// log.Println("receive", msg)
			switch msg.Type() {
			case packet.CONNECT:
				data := msg.(*packet.ConnectMessage)
				log.Println(string(data.Username()), string(data.Password()), data.Version())

				ack := packet.NewConnackMessage()
				ack.SetReturnCode(packet.ConnectionAccepted)
				this.OutChan <- ack
				break
			case packet.SUBSCRIBE:
				data := msg.(*packet.SubscribeMessage)
				log.Println(data.String(), data.Qos())

				for _, topic := range data.Topics() {
					isNewTopic := false
					for _, temp := range this.Topics {
						if temp == string(topic) {
							isNewTopic = true
						}
					}
					if !isNewTopic {
						this.Topics = append(this.Topics, string(topic))
					}
				}
				log.Println("Topics:", this.Topics)

				ack := packet.NewSubackMessage()
				ack.SetPacketId(msg.PacketId())
				ack.AddReturnCode(packet.QosAtMostOnce)
				this.OutChan <- ack
				break
			case packet.UNSUBSCRIBE:
				data := msg.(*packet.UnsubscribeMessage)
				log.Println(data.String())
				del := make([]string, 0)
				for _, topic := range data.Topics() {
					for _, temp := range this.Topics {
						if temp == string(topic) {
							del = append(del, temp)
						}
					}
				}
				if len(del) > 0 {
					newTopic := make([]string, 0)
					for _, topic := range this.Topics {
						ignored := false
						for _, delTopic := range del {
							if topic == delTopic {
								ignored = true
							}
						}
						if !ignored {
							newTopic = append(newTopic, topic)
						}
					}
					this.Topics = newTopic
				}
				log.Println("Topics:", this.Topics)

				ack := packet.NewUnsubackMessage()
				ack.SetPacketId(msg.PacketId())
				this.OutChan <- ack
				break
			case packet.PUBLISH:
				//data := msg.(*packet.PublishMessage)
				//log.Println("payload:", string(data.Payload()))

				this.dispatcher(this, msg)

				ack := packet.NewPubackMessage()
				ack.SetPacketId(msg.PacketId())
				this.OutChan <- ack
				break
			case packet.PUBACK:
				break
			case packet.PINGREQ:
				this.Conn.SetDeadline(time.Now().Add(time.Second * 10))

				ack := packet.NewPingrespMessage()
				ack.SetPacketId(msg.PacketId())
				this.OutChan <- ack
				break
			case packet.DISCONNECT:
				this.Close()
				break
			default:
				log.Println("unimplemented message type")
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
			// log.Println("peek type", err)
			this.Close()
			return
		}
		t := packet.MessageType(b[0] >> 4)
		msg, err := t.New()
		if err != nil {
			log.Println("create message", err)
			this.Close()
			return
		}
		n := 2
		buf, err := this.Br.Peek(n)
		if err != nil {
			log.Println("peek header", err)
			this.Close()
			return
		}
		for buf[n-1] >= 0x80 {
			n++
			buf, err = this.Br.Peek(n)
			if err != nil {
				log.Println("try peek header", err)
				this.Close()
				return
			}
		}
		l, r := binary.Uvarint(buf[1:])
		buf = make([]byte, int(l)+r+1)
		n, err = io.ReadFull(this.Br, buf)
		if err != nil {
			log.Println("read header", err)
			this.Close()
			return
		}
		if n != len(buf) {
			log.Println("short read.")
			this.Close()
			return
		}
		_, err = msg.Decode(buf)
		if err != nil {
			log.Println("decode", err)
			this.Close()
			return
		}
		this.InChan <- msg
	}
}

func (this *MQClient) WritePacket() {
	for {
		select {
		case msg := <-this.OutChan:
			// log.Println("send", msg)
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
				this.Close()
				return
			}
			if n != len(buf) {
				log.Println("short write")
				this.Close()
				return
			}
			err = this.Bw.Flush()
			if err != nil {
				this.Close()
				return
			}
		}
	}
}
