package mqtt3

import (
	"testing"

	"github.com/findById/meta/internal/core"
	packet "github.com/surgemq/message"
)

func TestEncodePublishAllowsEmptyPayload(t *testing.T) {
	buf, packetID, err := EncodePublish(core.Message{Topic: "device/1/empty"}, packet.QosAtLeastOnce, true, 7)
	if err != nil {
		t.Fatal(err)
	}
	if packetID != 7 {
		t.Fatalf("packetID = %d, want 7", packetID)
	}

	msg := packet.NewPublishMessage()
	if _, err := msg.Decode(buf); err != nil {
		t.Fatal(err)
	}
	if string(msg.Topic()) != "device/1/empty" {
		t.Fatalf("topic = %q", msg.Topic())
	}
	if len(msg.Payload()) != 0 {
		t.Fatalf("payload len = %d, want 0", len(msg.Payload()))
	}
	if !msg.Retain() {
		t.Fatalf("retain = false, want true")
	}
	if msg.QoS() != packet.QosAtLeastOnce {
		t.Fatalf("qos = %d, want %d", msg.QoS(), packet.QosAtLeastOnce)
	}
}
