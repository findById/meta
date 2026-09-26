package mqtt5

import (
	"encoding/binary"
	"errors"
	"io"
	"log"

	"github.com/findById/meta/internal/core"
	"github.com/findById/meta/internal/security"
)

type Handler struct {
	Client            *core.Client
	topicAliases      map[uint16]string
	maxPacketSize     uint32
	topicAliasMaximum uint16
}

func NewHandler(client *core.Client) *Handler {
	return &Handler{Client: client, topicAliases: make(map[uint16]string)}
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
	header, err := h.Client.Reader.ReadByte()
	if err != nil {
		if err != io.EOF && h.Client.IsConnected() && !core.IsQuietNetworkError(err) {
			log.Println("mqtt5 read packet", err)
		}
		return err
	}
	remainingLength, err := decodeRemainingLength(h.Client.Reader.ReadByte)
	if err != nil {
		return err
	}
	packetSize := 1 + remainingLengthBytes(remainingLength) + remainingLength
	if h.maxPacketSize > 0 && uint32(packetSize) > h.maxPacketSize {
		return errors.New("mqtt5 maximum packet size exceeded")
	}
	payload := make([]byte, remainingLength)
	if _, err := io.ReadFull(h.Client.Reader, payload); err != nil {
		return err
	}
	h.Client.RefreshDeadline()

	packetType := header >> 4
	flags := header & 0x0f
	return h.processPacket(packetType, flags, payload)
}

func (h *Handler) processPacket(packetType byte, flags byte, data []byte) error {
	if packetType != packetConnect && !h.Client.IsConnected() {
		return errors.New("permission denied")
	}
	switch packetType {
	case packetConnect:
		return h.processConnect(data)
	case packetSubscribe:
		subscribe, err := decodeSubscribe(data)
		if err != nil {
			return err
		}
		codes := make([]byte, 0, len(subscribe.Topics))
		for i, topic := range subscribe.Topics {
			if !h.authorizeTopic(topic, "subscribe") {
				codes = append(codes, 0x87)
				continue
			}
			qos := qosAtMostOnce
			if i < len(subscribe.QoS) {
				qos = subscribe.QoS[i]
			}
			h.Client.Broker.Subscribe(h.Client, topic, qos)
			codes = append(codes, qos)
		}
		if err := h.Client.WriteBuffer(encodeSuback(subscribe.PacketID, codes)); err != nil {
			return err
		}
		for i, topic := range subscribe.Topics {
			qos := qosAtMostOnce
			if i < len(subscribe.QoS) {
				qos = subscribe.QoS[i]
			}
			h.writeRetained(topic, qos)
		}
		return nil
	case packetUnsubscribe:
		packetID, topics, err := decodeUnsubscribe(data)
		if err != nil {
			return err
		}
		for _, topic := range topics {
			if h.authorizeTopic(topic, "unsubscribe") {
				h.Client.Broker.Unsubscribe(h.Client, topic)
			}
		}
		return h.Client.WriteBuffer(encodeUnsuback(packetID, len(topics)))
	case packetPublish:
		publish, err := decodePublish(flags, data)
		if err != nil {
			return err
		}
		if err := h.resolvePublishTopic(&publish); err != nil {
			return err
		}
		if !h.authorizeTopic(publish.Topic, "publish") {
			return errors.New("permission denied")
		}
		message := core.Message{
			Topic:   publish.Topic,
			Payload: core.CloneBytes(publish.Payload),
			QoS:     publish.QoS,
			Retain:  publish.Retain,
		}
		switch publish.QoS {
		case qosAtMostOnce:
			h.Client.Broker.Publish(message)
			return nil
		case qosAtLeastOnce:
			if !h.Client.Broker.Publish(message) {
				return errors.New("broker busy")
			}
			return h.Client.WriteBuffer(encodeAck(packetPuback, publish.PacketID))
		case qosExactlyOnce:
			h.Client.StorePending(publish.PacketID, message)
			return h.Client.WriteBuffer(encodeAck(packetPubrec, publish.PacketID))
		default:
			return errors.New("invalid publish qos")
		}
	case packetPuback:
		h.Client.AckInflight(readPacketID(data))
	case packetPubrec:
		packetID := readPacketID(data)
		buf := encodeAck(packetPubrel, packetID)
		h.Client.TrackInflight(packetID, buf)
		return h.Client.WriteBuffer(buf)
	case packetPubrel:
		packetID := readPacketID(data)
		if value, ok := h.Client.ReleasePending(packetID); ok {
			if message, ok := value.(core.Message); ok {
				if !h.Client.Broker.Publish(message) {
					return errors.New("broker busy")
				}
			}
		}
		return h.Client.WriteBuffer(encodeAck(packetPubcomp, packetID))
	case packetPubcomp:
		h.Client.AckInflight(readPacketID(data))
	case packetPingreq:
		h.Client.RefreshDeadline()
		return h.Client.WriteBuffer([]byte{packetPingresp << 4, 0})
	case packetDisconnect:
		h.Client.MarkGraceful()
		h.Client.Close()
	default:
		return errors.New("unsupported mqtt5 packet")
	}
	return nil
}

func (h *Handler) processConnect(data []byte) error {
	connect, err := decodeConnect(data)
	if err != nil {
		_ = h.Client.WriteDirect(encodeConnack(false, 0x84))
		return err
	}
	if !h.Client.Broker.Security().Authenticate(h.Client.Ctx, security.AuthContext{
		Protocol:   security.ProtocolMQTT5,
		ClientID:   connect.ClientID,
		Username:   connect.Username,
		Password:   connect.Password,
		RemoteAddr: h.Client.Conn.RemoteAddr(),
	}) {
		_ = h.Client.WriteDirect(encodeConnack(false, 0x86))
		return errors.New("permission denied")
	}
	h.Client.SetStatus(core.StatusConnected)
	h.Client.ID = connect.ClientID
	h.Client.Protocol = security.ProtocolMQTT5
	h.Client.ReceiveMaximum = connect.ReceiveMaximum
	h.maxPacketSize = connect.MaxPacketSize
	h.topicAliasMaximum = connect.TopicAliasMaximum
	h.Client.Broker.RegisterClient(h.Client)
	if connect.CleanStart {
		h.Client.Broker.SessionStore().Delete(connect.ClientID)
	}
	sessionPresent := h.Client.Broker.BindSession(h.Client, core.SessionOptions{
		CleanSession: connect.SessionExpiry == 0,
		KeepAlive:    connect.KeepAlive,
		Will:         connect.Will,
	})
	h.Client.RefreshDeadline()
	return h.Client.WriteBuffer(encodeConnack(sessionPresent, 0))
}

func (h *Handler) resolvePublishTopic(publish *publishPacket) error {
	if publish == nil {
		return errors.New("invalid publish")
	}
	if publish.TopicAlias == 0 {
		if publish.Topic == "" {
			return errors.New("invalid publish topic")
		}
		return nil
	}
	if publish.TopicAlias > h.topicAliasMaximum {
		return errors.New("topic alias exceeds maximum")
	}
	if publish.Topic == "" {
		topic := h.topicAliases[publish.TopicAlias]
		if topic == "" {
			return errors.New("unknown topic alias")
		}
		publish.Topic = topic
		return nil
	}
	h.topicAliases[publish.TopicAlias] = publish.Topic
	return nil
}

func (h *Handler) authorizeTopic(topic string, action string) bool {
	return h.Client.Broker.Security().Authorize(h.Client.Ctx, security.TopicAccessContext{
		Protocol: security.ProtocolMQTT5,
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
		if qos > qosAtMostOnce {
			h.Client.TrackInflight(packetID, buf)
		}
		if err := h.Client.WriteBuffer(buf); err != nil {
			h.Client.Broker.Stats.DeliveryFailures.Add(1)
			return
		}
		h.Client.Broker.Stats.Deliveries.Add(1)
	}
}

type connectPacket struct {
	ClientID          string
	Username          []byte
	Password          []byte
	CleanStart        bool
	KeepAlive         uint16
	SessionExpiry     uint32
	ReceiveMaximum    uint16
	MaxPacketSize     uint32
	TopicAliasMaximum uint16
	Will              *core.WillMessage
	WillFlag          bool
	WillQoS           byte
	WillRetain        bool
	WillTopic         string
	WillPayload       []byte
}

func decodeConnect(data []byte) (connectPacket, error) {
	offset := 0
	protocolName, err := readLPBytes(data, &offset)
	if err != nil {
		return connectPacket{}, err
	}
	if string(protocolName) != "MQTT" {
		return connectPacket{}, errors.New("invalid protocol name")
	}
	if offset >= len(data) || data[offset] != 5 {
		return connectPacket{}, errors.New("invalid protocol version")
	}
	offset++
	if offset+3 > len(data) {
		return connectPacket{}, errors.New("short connect header")
	}
	flags := data[offset]
	offset++
	keepAlive := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	propsLen, err := readVarint(data, &offset)
	if err != nil {
		return connectPacket{}, err
	}
	propsStart := offset
	offset += propsLen
	propsEnd := offset
	if offset > len(data) {
		return connectPacket{}, errors.New("short connect properties")
	}
	clientID, err := readLPBytes(data, &offset)
	if err != nil {
		return connectPacket{}, err
	}

	result := connectPacket{
		ClientID:       string(clientID),
		CleanStart:     flags&0x02 != 0,
		KeepAlive:      keepAlive,
		ReceiveMaximum: 65535,
		WillFlag:       flags&0x04 != 0,
		WillQoS:        (flags >> 3) & 0x03,
		WillRetain:     flags&0x20 != 0,
	}
	if err := parseConnectProperties(data[propsStart:propsEnd], &result); err != nil {
		return connectPacket{}, err
	}
	if result.WillFlag {
		willPropsLen, err := readVarint(data, &offset)
		if err != nil {
			return connectPacket{}, err
		}
		offset += willPropsLen
		topic, err := readLPBytes(data, &offset)
		if err != nil {
			return connectPacket{}, err
		}
		payload, err := readLPBytes(data, &offset)
		if err != nil {
			return connectPacket{}, err
		}
		result.Will = &core.WillMessage{
			Topic:   string(topic),
			Payload: core.CloneBytes(payload),
			QoS:     result.WillQoS,
			Retain:  result.WillRetain,
		}
	}
	if flags&0x80 != 0 {
		username, err := readLPBytes(data, &offset)
		if err != nil {
			return connectPacket{}, err
		}
		result.Username = core.CloneBytes(username)
	}
	if flags&0x40 != 0 {
		password, err := readLPBytes(data, &offset)
		if err != nil {
			return connectPacket{}, err
		}
		result.Password = core.CloneBytes(password)
	}
	return result, nil
}

func parseConnectProperties(data []byte, result *connectPacket) error {
	offset := 0
	for offset < len(data) {
		propertyID := data[offset]
		offset++
		switch propertyID {
		case 0x11: // Session Expiry Interval
			if offset+4 > len(data) {
				return errors.New("short session expiry interval")
			}
			result.SessionExpiry = binary.BigEndian.Uint32(data[offset:])
			offset += 4
		case 0x21: // Receive Maximum
			if offset+2 > len(data) {
				return errors.New("short receive maximum")
			}
			result.ReceiveMaximum = binary.BigEndian.Uint16(data[offset:])
			offset += 2
		case 0x27: // Maximum Packet Size
			if offset+4 > len(data) {
				return errors.New("short maximum packet size")
			}
			result.MaxPacketSize = binary.BigEndian.Uint32(data[offset:])
			offset += 4
		case 0x22: // Topic Alias Maximum
			if offset+2 > len(data) {
				return errors.New("short topic alias maximum")
			}
			result.TopicAliasMaximum = binary.BigEndian.Uint16(data[offset:])
			offset += 2
		case 0x19, 0x17: // Request Response Information, Request Problem Information
			offset++
		case 0x26: // User Property
			if _, err := readLPBytes(data, &offset); err != nil {
				return err
			}
			if _, err := readLPBytes(data, &offset); err != nil {
				return err
			}
		case 0x15, 0x16: // Authentication Method, Authentication Data
			if _, err := readLPBytes(data, &offset); err != nil {
				return err
			}
		default:
			return errors.New("unsupported connect property")
		}
		if offset > len(data) {
			return errors.New("short connect property")
		}
	}
	return nil
}

func decodePublish(flags byte, data []byte) (publishPacket, error) {
	offset := 0
	topic, err := readLPBytes(data, &offset)
	if err != nil {
		return publishPacket{}, err
	}
	qos := (flags >> 1) & 0x03
	var packetID uint16
	if qos > qosAtMostOnce {
		if offset+2 > len(data) {
			return publishPacket{}, errors.New("short publish packet id")
		}
		packetID = binary.BigEndian.Uint16(data[offset:])
		offset += 2
	}
	propsLen, err := readVarint(data, &offset)
	if err != nil {
		return publishPacket{}, err
	}
	propsStart := offset
	offset += propsLen
	if offset > len(data) {
		return publishPacket{}, errors.New("short publish properties")
	}
	result := publishPacket{
		Topic:    string(topic),
		Payload:  core.CloneBytes(data[offset:]),
		QoS:      qos,
		Retain:   flags&0x01 != 0,
		PacketID: packetID,
	}
	if err := parsePublishProperties(data[propsStart:offset], &result); err != nil {
		return publishPacket{}, err
	}
	return result, nil
}

func parsePublishProperties(data []byte, result *publishPacket) error {
	offset := 0
	for offset < len(data) {
		propertyID := data[offset]
		offset++
		switch propertyID {
		case 0x23: // Topic Alias
			if offset+2 > len(data) {
				return errors.New("short topic alias")
			}
			result.TopicAlias = binary.BigEndian.Uint16(data[offset:])
			offset += 2
		case 0x01: // Payload Format Indicator
			offset++
		case 0x02: // Message Expiry Interval
			offset += 4
		case 0x08, 0x09: // Response Topic, Correlation Data
			if _, err := readLPBytes(data, &offset); err != nil {
				return err
			}
		case 0x26: // User Property
			if _, err := readLPBytes(data, &offset); err != nil {
				return err
			}
			if _, err := readLPBytes(data, &offset); err != nil {
				return err
			}
		case 0x0b: // Subscription Identifier
			if _, err := readVarint(data, &offset); err != nil {
				return err
			}
		case 0x03: // Content Type
			if _, err := readLPBytes(data, &offset); err != nil {
				return err
			}
		default:
			return errors.New("unsupported publish property")
		}
		if offset > len(data) {
			return errors.New("short publish property")
		}
	}
	return nil
}

func decodeSubscribe(data []byte) (subscribePacket, error) {
	offset := 0
	if len(data) < 3 {
		return subscribePacket{}, errors.New("short subscribe")
	}
	packetID := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	propsLen, err := readVarint(data, &offset)
	if err != nil {
		return subscribePacket{}, err
	}
	offset += propsLen
	result := subscribePacket{PacketID: packetID}
	for offset < len(data) {
		topic, err := readLPBytes(data, &offset)
		if err != nil {
			return subscribePacket{}, err
		}
		if offset >= len(data) {
			return subscribePacket{}, errors.New("short subscribe options")
		}
		options := data[offset]
		offset++
		result.Topics = append(result.Topics, string(topic))
		result.QoS = append(result.QoS, options&0x03)
	}
	return result, nil
}

func decodeUnsubscribe(data []byte) (uint16, []string, error) {
	offset := 0
	if len(data) < 3 {
		return 0, nil, errors.New("short unsubscribe")
	}
	packetID := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	propsLen, err := readVarint(data, &offset)
	if err != nil {
		return 0, nil, err
	}
	offset += propsLen
	var topics []string
	for offset < len(data) {
		topic, err := readLPBytes(data, &offset)
		if err != nil {
			return 0, nil, err
		}
		topics = append(topics, string(topic))
	}
	return packetID, topics, nil
}

func readPacketID(data []byte) uint16 {
	if len(data) < 2 {
		return 0
	}
	return binary.BigEndian.Uint16(data)
}
