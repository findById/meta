package mqtt5

import (
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/findById/meta/internal/core"
	"github.com/findById/meta/internal/security"
)

const (
	packetConnect     byte = 1
	packetConnack     byte = 2
	packetPublish     byte = 3
	packetPuback      byte = 4
	packetPubrec      byte = 5
	packetPubrel      byte = 6
	packetPubcomp     byte = 7
	packetSubscribe   byte = 8
	packetSuback      byte = 9
	packetUnsubscribe byte = 10
	packetUnsuback    byte = 11
	packetPingreq     byte = 12
	packetPingresp    byte = 13
	packetDisconnect  byte = 14

	qosAtMostOnce  byte = 0
	qosAtLeastOnce byte = 1
	qosExactlyOnce byte = 2
)

var publishPacketID atomic.Uint32

func init() {
	core.RegisterPublishEncoder(security.ProtocolMQTT5, EncodePublish)
}

type publishPacket struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	PacketID   uint16
	TopicAlias uint16
}

type subscribePacket struct {
	PacketID uint16
	Topics   []string
	QoS      []byte
}

func EncodePublish(message core.Message, qos byte, retain bool, packetID uint16) ([]byte, uint16, error) {
	if message.Topic == "" {
		return nil, 0, errors.New("invalid publish topic")
	}
	if qos > qosExactlyOnce {
		return nil, 0, errors.New("invalid publish qos")
	}
	if qos > qosAtMostOnce && packetID == 0 {
		packetID = uint16(publishPacketID.Add(1) & 0xffff)
		if packetID == 0 {
			packetID = uint16(publishPacketID.Add(1) & 0xffff)
		}
	}

	varHeaderLen := 2 + len(message.Topic) + 1
	if qos > qosAtMostOnce {
		varHeaderLen += 2
	}
	remainingLength := varHeaderLen + len(message.Payload)
	buf := make([]byte, 1+remainingLengthBytes(remainingLength)+remainingLength)
	buf[0] = packetPublish<<4 | qos<<1
	if retain {
		buf[0] |= 0x01
	}
	offset := 1
	offset += encodeRemainingLength(buf[offset:], remainingLength)
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(message.Topic)))
	offset += 2
	offset += copy(buf[offset:], message.Topic)
	if qos > qosAtMostOnce {
		binary.BigEndian.PutUint16(buf[offset:], packetID)
		offset += 2
	}
	buf[offset] = 0
	offset++
	copy(buf[offset:], message.Payload)
	return buf, packetID, nil
}

func encodeAck(packetType byte, packetID uint16) []byte {
	buf := []byte{packetType << 4, 0x04, 0, 0, 0, 0}
	if packetType == packetPubrel {
		buf[0] |= 0x02
	}
	binary.BigEndian.PutUint16(buf[2:], packetID)
	return buf
}

func encodeConnack(sessionPresent bool, reasonCode byte) []byte {
	flags := byte(0)
	if sessionPresent {
		flags = 1
	}
	return []byte{packetConnack << 4, 0x03, flags, reasonCode, 0x00}
}

func encodeSuback(packetID uint16, codes []byte) []byte {
	remainingLength := 2 + 1 + len(codes)
	buf := make([]byte, 1+remainingLengthBytes(remainingLength)+remainingLength)
	buf[0] = packetSuback << 4
	offset := 1
	offset += encodeRemainingLength(buf[offset:], remainingLength)
	binary.BigEndian.PutUint16(buf[offset:], packetID)
	offset += 2
	buf[offset] = 0
	offset++
	copy(buf[offset:], codes)
	return buf
}

func encodeUnsuback(packetID uint16, count int) []byte {
	codes := make([]byte, count)
	remainingLength := 2 + 1 + len(codes)
	buf := make([]byte, 1+remainingLengthBytes(remainingLength)+remainingLength)
	buf[0] = packetUnsuback << 4
	offset := 1
	offset += encodeRemainingLength(buf[offset:], remainingLength)
	binary.BigEndian.PutUint16(buf[offset:], packetID)
	offset += 2
	buf[offset] = 0
	offset++
	copy(buf[offset:], codes)
	return buf
}

func decodeRemainingLength(reader func() (byte, error)) (int, error) {
	multiplier := 1
	value := 0
	for i := 0; i < 4; i++ {
		encoded, err := reader()
		if err != nil {
			return 0, err
		}
		value += int(encoded&127) * multiplier
		if encoded&128 == 0 {
			return value, nil
		}
		multiplier *= 128
	}
	return 0, errors.New("malformed remaining length")
}

func remainingLengthBytes(length int) int {
	count := 1
	for length >= 128 {
		length /= 128
		count++
	}
	return count
}

func encodeRemainingLength(dst []byte, length int) int {
	offset := 0
	for {
		encoded := byte(length % 128)
		length /= 128
		if length > 0 {
			encoded |= 128
		}
		dst[offset] = encoded
		offset++
		if length == 0 {
			return offset
		}
	}
}

func readLPBytes(data []byte, offset *int) ([]byte, error) {
	if *offset+2 > len(data) {
		return nil, errors.New("short length-prefixed bytes")
	}
	size := int(binary.BigEndian.Uint16(data[*offset:]))
	*offset += 2
	if *offset+size > len(data) {
		return nil, errors.New("short length-prefixed payload")
	}
	value := data[*offset : *offset+size]
	*offset += size
	return value, nil
}

func readVarint(data []byte, offset *int) (int, error) {
	multiplier := 1
	value := 0
	for i := 0; i < 4; i++ {
		if *offset >= len(data) {
			return 0, errors.New("short varint")
		}
		encoded := data[*offset]
		*offset++
		value += int(encoded&127) * multiplier
		if encoded&128 == 0 {
			return value, nil
		}
		multiplier *= 128
	}
	return 0, errors.New("malformed varint")
}
