package broker

import (
	packet "github.com/surgemq/message"
)

type Handler interface {
	ReadPacket() error
	WritePacket(msg packet.Message) error
	processMessage(msg packet.Message) error
	processPubAckMessage(msg packet.Message)
	processPublishMessage(msg packet.Message)
}
