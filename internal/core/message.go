package core

type Message struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
}

func (m Message) Clone() Message {
	return Message{
		Topic:   m.Topic,
		Payload: CloneBytes(m.Payload),
		QoS:     m.QoS,
		Retain:  m.Retain,
	}
}

type WillMessage = Message

type SessionOptions struct {
	CleanSession bool
	KeepAlive    uint16
	Will         *WillMessage
}

type RetainedMessage = Message

func MinQoS(a, b byte) byte {
	if a < b {
		return a
	}
	return b
}

func CloneBytes(value []byte) []byte {
	next := make([]byte, len(value))
	copy(next, value)
	return next
}
