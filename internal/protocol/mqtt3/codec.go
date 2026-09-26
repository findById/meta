package mqtt3

import (
	"encoding/binary"
	"errors"
	"sync/atomic"

	"github.com/findById/meta/internal/core"
	"github.com/findById/meta/internal/security"
	packet "github.com/surgemq/message"
)

var publishPacketID atomic.Uint32

func init() {
	core.RegisterPublishEncoder(security.ProtocolMQTT311, EncodePublish)
}

func EncodePublish(message core.Message, qos byte, retain bool, packetID uint16) ([]byte, uint16, error) {
	topic := []byte(message.Topic)
	if !packet.ValidTopic(topic) {
		return nil, 0, errors.New("invalid publish topic")
	}
	if !packet.ValidQos(qos) {
		return nil, 0, errors.New("invalid publish qos")
	}
	if qos > packet.QosAtMostOnce && packetID == 0 {
		packetID = uint16(publishPacketID.Add(1) & 0xffff)
		if packetID == 0 {
			packetID = uint16(publishPacketID.Add(1) & 0xffff)
		}
	}

	remainingLength := 2 + len(topic) + len(message.Payload)
	if qos > packet.QosAtMostOnce {
		remainingLength += 2
	}
	buf := make([]byte, 1+remainingLengthBytes(remainingLength)+remainingLength)
	buf[0] = byte(packet.PUBLISH)<<4 | qos<<1
	if retain {
		buf[0] |= 0x01
	}

	offset := 1
	offset += encodeRemainingLength(buf[offset:], remainingLength)
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(topic)))
	offset += 2
	offset += copy(buf[offset:], topic)
	if qos > packet.QosAtMostOnce {
		binary.BigEndian.PutUint16(buf[offset:], packetID)
		offset += 2
	}
	copy(buf[offset:], message.Payload)
	return buf, packetID, nil
}

func encodePacket(msg packet.Message) ([]byte, error) {
	buf := make([]byte, msg.Len())
	n, err := msg.Encode(buf)
	if err != nil {
		return nil, err
	}
	if n != len(buf) {
		return nil, errors.New("short encode")
	}
	return buf, nil
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
