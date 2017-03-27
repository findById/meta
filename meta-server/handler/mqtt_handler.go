package handler

import (
	"encoding/binary"
	"io"
	"log"
	"net"
	"time"

	"meta/meta-server/client"

	packet "github.com/surgemq/message"
)

type MQTTHandler struct {
	cm      *client.ClientManager
	client  *client.MetaClient
	OutChan chan (packet.Message)
}

func NewMQTTHandler(conn *net.TCPConn, cm *client.ClientManager) *MQTTHandler {
	client := client.NewMetaClient(conn)
	return &MQTTHandler{
		cm:      cm,
		client:  client,
		OutChan: make(chan (packet.Message), 1000),
	}
}

func (this *MQTTHandler) Start() {
	go this.producer()
	go this.process()
	go this.WritePacket()
	go this.ReadPacket()
}

func (this *MQTTHandler) process() {
	for this.client != nil && !this.client.IsClosed {
		select {
		case msg := <-this.client.InChan:
		// log.Println("receive", msg)
			switch msg.Type() {
			case packet.CONNECT:
				data := msg.(*packet.ConnectMessage)
				log.Println(string(data.Username()), string(data.Password()), data.Version())

				if string(data.Username()) == "" {
					this.client.Close()
					break
				}

				this.client.Id = string(data.ClientId())

				this.cm.AddClient(this.client)
				this.client.IsAuthed = true

				ack := packet.NewConnackMessage()
				ack.SetReturnCode(packet.ConnectionAccepted)
				this.client.OutChan <- ack
				break
			case packet.SUBSCRIBE:
				if !this.client.IsAuthed {
					this.client.Close()
					break
				}
				data := msg.(*packet.SubscribeMessage)
				// log.Println(data.String(), data.Qos())

				for _, topic := range data.Topics() {
					subscribed := false
					for _, temp := range this.client.Topics {
						if temp == string(topic) {
							subscribed = true
						}
					}
					if !subscribed {
						this.client.Topics = append(this.client.Topics, string(topic))
					}
				}
				log.Println("Topics:", this.client.Topics)

				ack := packet.NewSubackMessage()
				ack.SetPacketId(msg.PacketId())
				ack.AddReturnCode(packet.QosAtMostOnce)
				this.client.OutChan <- ack
				break
			case packet.UNSUBSCRIBE:
				if !this.client.IsAuthed {
					this.client.Close()
					break
				}
				data := msg.(*packet.UnsubscribeMessage)
				// log.Println(data.String())

				delTopics := make([]string, 0)
				for _, topic := range data.Topics() {
					for _, temp := range this.client.Topics {
						if temp == string(topic) {
							delTopics = append(delTopics, temp)
						}
					}
				}
				if len(delTopics) > 0 {
					newTopic := make([]string, 0)
					for _, topic := range this.client.Topics {
						unsubscribed := false
						for _, del := range delTopics {
							if topic == del {
								unsubscribed = true
							}
						}
						if !unsubscribed {
							newTopic = append(newTopic, topic)
						}
					}
					this.client.Topics = newTopic
				}
				log.Println("Topics:", this.client.Topics)

				ack := packet.NewUnsubackMessage()
				ack.SetPacketId(msg.PacketId())
				this.client.OutChan <- ack
				break
			case packet.PUBLISH:
				if !this.client.IsAuthed {
					this.client.Close()
					break
				}
				data := msg.(*packet.PublishMessage)
				log.Println("payload:", this.client.Id, data.PacketId(), string(data.Payload()))

				this.OutChan <- msg

				ack := packet.NewPubackMessage()
				ack.SetPacketId(msg.PacketId())
				this.client.OutChan <- ack
				break
			case packet.PUBACK:
				if !this.client.IsAuthed {
					this.client.Close()
					break
				}
				data := msg.(*packet.PubackMessage)
				log.Println("puback:", this.client.Id, data.PacketId())
				break
			case packet.PINGREQ:
				if !this.client.IsAuthed {
					this.client.Close()
					break
				}
				this.client.Conn.SetDeadline(time.Now().Add(time.Second * 60))

				ack := packet.NewPingrespMessage()
				ack.SetPacketId(msg.PacketId())
				this.client.OutChan <- ack
				break
			case packet.DISCONNECT:
				this.client.Close()
				break
			default:
				log.Println("unimplemented message type")
				this.client.Close()
				break
			}
		}
	}
}

func (this *MQTTHandler) ReadPacket() {
	for this.client != nil && !this.client.IsClosed {
		b, err := this.client.Br.Peek(1)
		if err != nil {
			if err == io.EOF {
				continue
			}
			// log.Println("peek type", err)
			this.client.Close()
			return
		}
		t := packet.MessageType(b[0] >> 4)
		msg, err := t.New()
		if err != nil {
			log.Println("create message", err)
			this.client.Close()
			return
		}
		n := 2
		buf, err := this.client.Br.Peek(n)
		if err != nil {
			log.Println("peek header", err)
			this.client.Close()
			return
		}
		for buf[n - 1] >= 0x80 {
			n++
			buf, err = this.client.Br.Peek(n)
			if err != nil {
				log.Println("try peek header", err)
				this.client.Close()
				return
			}
		}
		l, r := binary.Uvarint(buf[1:])
		buf = make([]byte, int(l) + r + 1)
		n, err = io.ReadFull(this.client.Br, buf)
		if err != nil {
			log.Println("read header", err)
			this.client.Close()
			return
		}
		if n != len(buf) {
			log.Println("short read.")
			this.client.Close()
			return
		}
		_, err = msg.Decode(buf)
		if err != nil {
			log.Println("decode", err)
			this.client.Close()
			return
		}
		this.client.InChan <- msg
	}
}

func (this *MQTTHandler) WritePacket() {
	for this.client != nil && !this.client.IsClosed {
		select {
		case msg := <-this.client.OutChan:
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
			n, err = this.client.Bw.Write(buf)
			if err != nil {
				this.client.Close()
				return
			}
			if n != len(buf) {
				log.Println("short write")
				this.client.Close()
				return
			}
			err = this.client.Bw.Flush()
			if err != nil {
				this.client.Close()
				return
			}
		}
	}
}

func (this *MQTTHandler) producer() {
	for this.client != nil && !this.client.IsClosed {
		select {
		case msg := <-this.OutChan:
			data := msg.(*packet.PublishMessage)
			for _, consumer := range this.cm.CloneMap() {
				if consumer == nil || consumer.IsClosed {
					continue
				}
				for _, topic := range consumer.Topics {
					if topic == string(data.Topic()) {
						log.Println("producerId:", this.client.Id, "consumerId:", consumer.Id, "payload:", string(data.Payload()))
						consumer.OutChan <- data
					}
				}
			}
		}
	}
}
