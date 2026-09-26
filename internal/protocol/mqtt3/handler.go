package mqtt3

import (
	"encoding/binary"
	"errors"
	"io"
	"log"

	"github.com/findById/meta/internal/core"
	"github.com/findById/meta/internal/security"
	packet "github.com/surgemq/message"
)

type Handler struct {
	Client *core.Client
}

func NewHandler(client *core.Client) *Handler {
	return &Handler{Client: client}
}

func (h *Handler) Start() {
	for {
		select {
		case <-h.Client.Ctx.Done():
			return
		default:
			if err := h.ReadPacket(); err != nil {
				h.Client.Close()
				return
			}
		}
	}
}

func (h *Handler) ReadPacket() error {
	b, err := h.Client.Reader.Peek(1)
	if err != nil {
		if err == io.EOF {
			return err
		}
		h.logReadError("peek type", err)
		return err
	}
	t := packet.MessageType(b[0] >> 4)
	msg, err := t.New()
	if err != nil {
		h.logReadError("create message", err)
		return err
	}
	n := 2
	buf, err := h.Client.Reader.Peek(n)
	if err != nil {
		h.logReadError("peek header", err)
		return err
	}
	for buf[n-1] >= 0x80 {
		n++
		buf, err = h.Client.Reader.Peek(n)
		if err != nil {
			h.logReadError("try peek header", err)
			return err
		}
	}
	l, r := binary.Uvarint(buf[1:])
	buf = make([]byte, int(l)+r+1)
	n, err = io.ReadFull(h.Client.Reader, buf)
	if err != nil {
		h.logReadError("read header", err)
		return err
	}
	if n != len(buf) {
		h.logReadError("short read", io.ErrUnexpectedEOF)
		return err
	}
	_, err = msg.Decode(buf)
	if err != nil {
		h.logReadError("decode", err)
		return err
	}
	h.Client.RefreshDeadline()

	return h.processMessage(msg)
}

func (h *Handler) logReadError(prefix string, err error) {
	if h.Client != nil && h.Client.IsConnected() && !core.IsQuietNetworkError(err) {
		log.Println(prefix, err)
	}
}

func (h *Handler) WritePacket(msg packet.Message) error {
	buf, err := encodePacket(msg)
	if err != nil {
		log.Println("encode", err)
		return err
	}
	return h.Client.WriteBuffer(buf)
}

func (h *Handler) processMessage(msg packet.Message) error {
	if msg.Type() != packet.CONNECT {
		if !h.Client.IsConnected() {
			return errors.New("permission denied")
		}
	}
	switch msg.Type() {
	case packet.CONNECT:
		data := msg.(*packet.ConnectMessage)
		if !h.Client.Broker.Security().Authenticate(h.Client.Ctx, security.AuthContext{
			Protocol:   security.ProtocolMQTT311,
			ClientID:   string(data.ClientId()),
			Username:   data.Username(),
			Password:   data.Password(),
			RemoteAddr: h.Client.Conn.RemoteAddr(),
		}) {
			ack := packet.NewConnackMessage()
			ack.SetReturnCode(packet.ErrNotAuthorized)
			if buf, err := encodePacket(ack); err == nil {
				_ = h.Client.WriteDirect(buf)
			}
			return errors.New("permission denied")
		}

		h.Client.SetStatus(core.StatusConnected)
		h.Client.ID = string(data.ClientId())
		h.Client.Protocol = security.ProtocolMQTT311
		h.Client.Broker.RegisterClient(h.Client)
		sessionPresent := h.Client.Broker.BindSession(h.Client, core.SessionOptions{
			CleanSession: data.CleanSession(),
			KeepAlive:    data.KeepAlive(),
			Will:         newWill(data),
		})
		h.Client.RefreshDeadline()

		ack := packet.NewConnackMessage()
		ack.SetReturnCode(packet.ConnectionAccepted)
		ack.SetSessionPresent(sessionPresent)
		return h.WritePacket(ack)
	case packet.SUBSCRIBE:
		data := msg.(*packet.SubscribeMessage)
		topics := data.Topics()
		qosList := data.Qos()
		for i, topic := range topics {
			topicName := string(topic)
			if !h.authorizeTopic(topicName, "subscribe") {
				return errors.New("permission denied")
			}
			qos := packet.QosAtMostOnce
			if i < len(qosList) {
				qos = qosList[i]
			}
			h.Client.Broker.Subscribe(h.Client, topicName, qos)
		}

		ack := packet.NewSubackMessage()
		ack.SetPacketId(msg.PacketId())
		for _, qos := range qosList {
			ack.AddReturnCode(qos)
		}
		if err := h.WritePacket(ack); err != nil {
			return err
		}
		for i, topic := range topics {
			qos := packet.QosAtMostOnce
			if i < len(qosList) {
				qos = qosList[i]
			}
			h.writeRetained(string(topic), qos)
		}
		return nil
	case packet.UNSUBSCRIBE:
		data := msg.(*packet.UnsubscribeMessage)
		for _, topic := range data.Topics() {
			if !h.authorizeTopic(string(topic), "unsubscribe") {
				return errors.New("permission denied")
			}
			h.Client.Broker.Unsubscribe(h.Client, string(topic))
		}
		ack := packet.NewUnsubackMessage()
		ack.SetPacketId(msg.PacketId())
		return h.WritePacket(ack)
	case packet.PUBLISH:
		data := msg.(*packet.PublishMessage)
		if !h.authorizeTopic(string(data.Topic()), "publish") {
			return errors.New("permission denied")
		}
		message := core.Message{
			Topic:   string(data.Topic()),
			Payload: core.CloneBytes(data.Payload()),
			QoS:     data.QoS(),
			Retain:  data.Retain(),
		}
		switch data.QoS() {
		case packet.QosAtMostOnce:
			h.Client.Broker.Publish(message)
			return nil
		case packet.QosAtLeastOnce:
			if !h.Client.Broker.Publish(message) {
				return errors.New("broker busy")
			}
			ack := packet.NewPubackMessage()
			ack.SetPacketId(msg.PacketId())
			return h.WritePacket(ack)
		case packet.QosExactlyOnce:
			h.Client.StorePending(data.PacketId(), message)
			ack := packet.NewPubrecMessage()
			ack.SetPacketId(msg.PacketId())
			return h.WritePacket(ack)
		default:
			return errors.New("invalid publish qos")
		}
	case packet.PUBACK:
		h.Client.AckInflight(msg.PacketId())
	case packet.PUBREC:
		ack := packet.NewPubrelMessage()
		ack.SetPacketId(msg.PacketId())
		buf, err := encodePacket(ack)
		if err != nil {
			return err
		}
		h.Client.TrackInflight(msg.PacketId(), buf)
		return h.Client.WriteBuffer(buf)
	case packet.PUBREL:
		if value, ok := h.Client.ReleasePending(msg.PacketId()); ok {
			if message, ok := value.(core.Message); ok {
				if !h.Client.Broker.Publish(message) {
					return errors.New("broker busy")
				}
			}
		}
		ack := packet.NewPubcompMessage()
		ack.SetPacketId(msg.PacketId())
		return h.WritePacket(ack)
	case packet.PUBCOMP:
		h.Client.AckInflight(msg.PacketId())
	case packet.PINGREQ:
		h.Client.RefreshDeadline()
		ack := packet.NewPingrespMessage()
		ack.SetPacketId(msg.PacketId())
		return h.WritePacket(ack)
	case packet.DISCONNECT:
		h.Client.MarkGraceful()
		h.Client.Close()
	default:
		return errors.New("unimplemented message type")
	}
	return nil
}

func (h *Handler) authorizeTopic(topic string, action string) bool {
	return h.Client.Broker.Security().Authorize(h.Client.Ctx, security.TopicAccessContext{
		Protocol: security.ProtocolMQTT311,
		ClientID: h.Client.ID,
		Topic:    topic,
		Action:   action,
	})
}

func (h *Handler) writeRetained(filter string, subscriptionQoS byte) {
	for _, retained := range h.Client.Broker.RetainedFor(filter) {
		qos := core.MinQoS(retained.QoS, subscriptionQoS)
		buf, packetID, err := EncodePublish(retained, qos, true, 0)
		if err != nil {
			log.Println("encode retained", err)
			continue
		}
		if qos > packet.QosAtMostOnce {
			h.Client.TrackInflight(packetID, buf)
		}
		if err := h.Client.WriteBuffer(buf); err != nil {
			h.Client.Broker.Stats.DeliveryFailures.Add(1)
			return
		}
		h.Client.Broker.Stats.Deliveries.Add(1)
	}
}

func newWill(data *packet.ConnectMessage) *core.WillMessage {
	if data == nil || !data.WillFlag() {
		return nil
	}
	return &core.WillMessage{
		Topic:   string(data.WillTopic()),
		Payload: core.CloneBytes(data.WillMessage()),
		QoS:     data.WillQos(),
		Retain:  data.WillRetain(),
	}
}
