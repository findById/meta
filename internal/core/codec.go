package core

import "errors"

var ErrUnsupportedProtocol = errors.New("unsupported protocol encoder")

type PublishEncoder func(message Message, qos byte, retain bool, packetID uint16) ([]byte, uint16, error)

var encoders = map[string]PublishEncoder{}

func RegisterPublishEncoder(protocol string, encoder PublishEncoder) {
	if protocol == "" || encoder == nil {
		return
	}
	encoders[protocol] = encoder
}

func EncodePublish(protocol string, message Message, qos byte, retain bool, packetID uint16) ([]byte, uint16, error) {
	encoder := encoders[protocol]
	if encoder == nil {
		return nil, 0, ErrUnsupportedProtocol
	}
	return encoder(message, qos, retain, packetID)
}
