package broker

import (
	"encoding/binary"
	"errors"
	packet "github.com/surgemq/message"
	"io"
	"log"
	"time"
)

type MQTTHandler struct {
	Client *MetaClient
}

func NewMQTTHandler(client *MetaClient) *MQTTHandler {
	return &MQTTHandler{
		Client: client,
	}
}

func (h *MQTTHandler) Start() {
	for {
		select {
		case <-h.Client.Ctx.Done():
			return // end loop
		default:
			if h.ReadPacket() != nil {
				msg := packet.NewDisconnectMessage()
				h.WritePacket(msg)
				h.Client.Close()
				return // end loop
			}
		}
	}
}

func (h *MQTTHandler) ReadPacket() error {
	b, err := h.Client.Reader.Peek(1)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		log.Println("peek type", err)
		return err
	}
	t := packet.MessageType(b[0] >> 4)
	msg, err := t.New()
	if err != nil {
		log.Println("create message", err)
		return err
	}
	n := 2
	buf, err := h.Client.Reader.Peek(n)
	if err != nil {
		log.Println("peek header", err)
		return err
	}
	for buf[n-1] >= 0x80 {
		n++
		buf, err = h.Client.Reader.Peek(n)
		if err != nil {
			log.Println("try peek header", err)
			return err
		}
	}
	l, r := binary.Uvarint(buf[1:])
	buf = make([]byte, int(l)+r+1)
	n, err = io.ReadFull(h.Client.Reader, buf)
	if err != nil {
		log.Println("read header", err)
		return err
	}
	if n != len(buf) {
		log.Println("short read.")
		return err
	}
	_, err = msg.Decode(buf)
	if err != nil {
		log.Println("decode", err)
		return err
	}

	if err := h.processMessage(msg); err != nil {
		return err
	}
	return nil
}

func (h *MQTTHandler) WritePacket(msg packet.Message) error {
	//log.Println("send", msg)
	buf := make([]byte, msg.Len())
	n, err := msg.Encode(buf)
	if err != nil {
		log.Println("encode", err)
		return err
	}
	if n != len(buf) {
		log.Println("short encode.")
		return err
	}
	return h.Client.WriteBuffer(buf)
}

func (h *MQTTHandler) processMessage(msg packet.Message) error {
	//log.Println("receive", msg)
	if msg.Type() != packet.CONNECT {
		if h.Client.Status != Connected {
			return errors.New("permission denied")
		}
	}
	switch msg.Type() {
	case packet.CONNECT:
		data := msg.(*packet.ConnectMessage)
		log.Println(string(data.Username()), string(data.Password()), data.Version())

		// check permission
		if !CheckAuthPermission(data.Username(), data.Password()) {
			ack := packet.NewConnackMessage()
			//ack.SetPacketId(msg.PacketId())
			ack.SetReturnCode(packet.ErrNotAuthorized)
			h.WritePacket(ack)
			return errors.New("permission denied")
		}

		h.Client.Status = Connected
		h.Client.Id = string(data.ClientId())
		h.Client.Conn.SetDeadline(time.Now().Add(time.Minute))

		// 保存当前客户端连接
		h.Client.Broker.ClientMap.Store(h.Client.Id, h.Client)

		ack := packet.NewConnackMessage()
		//ack.SetPacketId(msg.PacketId())
		ack.SetReturnCode(packet.ConnectionAccepted)
		return h.WritePacket(ack)
	case packet.SUBSCRIBE:
		data := msg.(*packet.SubscribeMessage)
		//log.Println(data.String(), data.Qos())

		for _, topic := range data.Topics() {
			if !CheckTopicPermission(string(topic), "subscribe") {
				return errors.New("permission denied")
			}
			// store topic
			_, ok := h.Client.TopicMap.Load(string(topic))
			if !ok {
				//fmt.Print("save before: ")
				//fmt.Println(h.Client.TopicMap.Load(string(topic)))
				h.Client.TopicMap.Store(string(topic), data.Qos())
				//fmt.Print("save after: ")
				//fmt.Println(h.Client.TopicMap.Load(string(topic)))
			}
		}

		ack := packet.NewSubackMessage()
		ack.SetPacketId(msg.PacketId())
		ack.AddReturnCode(packet.QosAtMostOnce)
		return h.WritePacket(ack)
	case packet.UNSUBSCRIBE:
		data := msg.(*packet.UnsubscribeMessage)

		for _, topic := range data.Topics() {
			if !CheckTopicPermission(string(topic), "unsubscribe") {
				return errors.New("permission denied")
			}
			// delete topic
			//fmt.Print("delete before: ")
			//fmt.Println(h.Client.TopicMap.Load(string(topic)))
			h.Client.TopicMap.Delete(string(topic))
			//fmt.Print("delete after: ")
			//fmt.Println(h.Client.TopicMap.Load(string(topic)))
		}

		ack := packet.NewUnsubackMessage()
		ack.SetPacketId(msg.PacketId())
		return h.WritePacket(ack)
	case packet.PUBLISH:
		data := msg.(*packet.PublishMessage)
		//log.Printf("publish >> producerId: %v, packetId: %v, topic: %v, payload: %v\n", h.Client.Id, data.PacketId(), string(data.Topic()), string(data.Payload()))

		if !CheckTopicPermission(string(data.Topic()), "publish") {
			log.Printf("check topic permission failed: publish %v", h.Client.Id)
			return errors.New("permission denied")
		}

		// 消息下发逻辑
		// worker queue process message
		h.Client.Broker.Worker(func() {
			h.processPublishMessage(msg)
		})

		ack := packet.NewPubackMessage()
		ack.SetPacketId(msg.PacketId())
		return h.WritePacket(ack)
	case packet.PUBACK:
		//data := msg.(*packet.PubackMessage)

		// 消息送达逻辑
		h.processPubAckMessage(msg)
		break
	case packet.PINGREQ:
		h.Client.Conn.SetDeadline(time.Now().Add(time.Minute))

		ack := packet.NewPingrespMessage()
		ack.SetPacketId(msg.PacketId())
		return h.WritePacket(ack)
	case packet.DISCONNECT:
		h.Client.Close()
		break
	default:
		return errors.New("unimplemented message type")
	}
	return nil
}

// TODO 当前服务仅作为代理网关，将消息发送给逻辑服务器处理
func (h *MQTTHandler) processPubAckMessage(msg packet.Message) {
	// 6. 消息已送达 删除缓存消息
	//jobId := fmt.Sprintf("PUBLISH_%v_%v", h.Client.Id, msg.PacketId())
	//storage.Remove(jobId)
}

// TODO 当前服务仅作为代理网关，将消息发送给逻辑服务器处理
func (h *MQTTHandler) processPublishMessage(msg packet.Message) {
	data := msg.(*packet.PublishMessage)

	// 1. 保存原始消息
	//temp := storage.Message{
	//	ProducerId:    h.Client.Id,
	//	ConsumerId:    "",
	//	MessageId:     data.PacketId(),
	//	MessageType:   1,
	//	MessageStatus: 0,
	//	Topic:         data.Topic(),
	//	Payload:       data.Payload(),
	//	ProduceTime:   time.Now().Unix(),
	//}
	//err := storage.Save(fmt.Sprintf("PUBLISH_ORIGIN_%v_%v", h.Client.Id, data.PacketId()), temp)
	//if err != nil {
	//	panic(err)
	//}

	// 2. 遍历当前服务器上已连接的客户端
	h.Client.Broker.ClientMap.Range(func(key, value interface{}) bool {
		c := value.(*MetaClient)
		if c.Status != Connected {
			h.Client.Broker.ClientMap.Delete(key)
			return true // continue
		}

		// 3. 当前客户端是否已订阅该主题
		if _, ok := c.TopicMap.Load(string(data.Topic())); ok {
			//log.Printf("consumer >> consumerId: %v, packetId: %v, topic: %v, qos: %v, payload: %v\n", c.Id, data.PacketId(), string(data.Topic()), temp, string(data.Payload()))
			// 4. 已订阅主题 缓存消息等待确认接收
			//jobId := fmt.Sprintf("PUBLISH_%v_%v", h.Client.Id, data.PacketId())
			//err := storage.Save(jobId, storage.Message{
			//	ProducerId:    h.Client.Id,
			//	ConsumerId:    c.Id,
			//	MessageId:     data.PacketId(),
			//	MessageType:   1,
			//	MessageStatus: 0,
			//	Topic:         data.Topic(),
			//	Payload:       data.Payload(),
			//	ProduceTime:   time.Now().Unix(),
			//})
			//if err != nil {
			//	panic(err)
			//}

			// 5. 下发消息
			buf := make([]byte, msg.Len())
			n, err := msg.Encode(buf)
			if err != nil {
				log.Println("encode", err)
				return true
			}
			if n != len(buf) {
				log.Println("short encode.")
				return true
			}
			if err := c.WriteBuffer(buf); err != nil {
				c.Close()
			}
		}
		return true
	})
}
