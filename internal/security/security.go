package security

import (
	"context"
	"net"
)

const (
	ProtocolMQTT311 = "mqtt-3.1.1"
	ProtocolMQTT5   = "mqtt-5.0"
)

type AuthContext struct {
	Protocol   string
	ClientID   string
	Username   []byte
	Password   []byte
	RemoteAddr net.Addr
}

type TopicAccessContext struct {
	Protocol string
	ClientID string
	Topic    string
	Action   string
}

type Provider interface {
	Authenticate(ctx context.Context, auth AuthContext) bool
	Authorize(ctx context.Context, access TopicAccessContext) bool
}

type AllowAll struct{}

func (AllowAll) Authenticate(context.Context, AuthContext) bool {
	return true
}

func (AllowAll) Authorize(context.Context, TopicAccessContext) bool {
	return true
}
