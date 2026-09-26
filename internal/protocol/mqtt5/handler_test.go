package mqtt5

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/findById/meta/internal/core"
	"github.com/findById/meta/internal/security"
)

func TestMQTT5ConnectSubscribePublish(t *testing.T) {
	broker := core.NewBroker()
	broker.Start()
	defer broker.Stop()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	go broker.Accept(serverConn, security.ProtocolMQTT5, func(client *core.Client) core.Handler {
		return NewHandler(client)
	})

	if _, err := clientConn.Write(mqtt5ConnectPacket("client-1")); err != nil {
		t.Fatal(err)
	}
	assertPacket(t, clientConn, []byte{0x20, 0x03, 0x00, 0x00, 0x00})

	if _, err := clientConn.Write(mqtt5SubscribePacket(1, "a/b")); err != nil {
		t.Fatal(err)
	}
	assertPacket(t, clientConn, []byte{0x90, 0x04, 0x00, 0x01, 0x00, 0x00})

	if _, err := clientConn.Write(mqtt5PublishPacket("a/b", []byte("hi"))); err != nil {
		t.Fatal(err)
	}
	got := readPacket(t, clientConn)
	if len(got) < 8 || got[0]>>4 != packetPublish {
		t.Fatalf("unexpected publish packet: %v", got)
	}
	if !bytes.Contains(got, []byte("hi")) {
		t.Fatalf("publish packet missing payload: %v", got)
	}
}

func TestDecodeConnectProperties(t *testing.T) {
	packet := mqtt5ConnectPacketWithProperties("client-props", []byte{
		0x11, 0x00, 0x00, 0x00, 0x3c,
		0x21, 0x00, 0x0a,
		0x27, 0x00, 0x00, 0x04, 0x00,
		0x22, 0x00, 0x08,
	})
	connect, err := decodeConnect(packet[2:])
	if err != nil {
		t.Fatal(err)
	}
	if connect.SessionExpiry != 60 {
		t.Fatalf("session expiry = %d, want 60", connect.SessionExpiry)
	}
	if connect.ReceiveMaximum != 10 {
		t.Fatalf("receive maximum = %d, want 10", connect.ReceiveMaximum)
	}
	if connect.MaxPacketSize != 1024 {
		t.Fatalf("maximum packet size = %d, want 1024", connect.MaxPacketSize)
	}
	if connect.TopicAliasMaximum != 8 {
		t.Fatalf("topic alias maximum = %d, want 8", connect.TopicAliasMaximum)
	}
}

func TestMQTT5TopicAlias(t *testing.T) {
	handler := NewHandler(&core.Client{})
	handler.topicAliasMaximum = 4

	first, err := decodePublish(0, mqtt5PublishBody("a/b", []byte{0x23, 0x00, 0x01}, []byte("first")))
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.resolvePublishTopic(&first); err != nil {
		t.Fatal(err)
	}

	second, err := decodePublish(0, mqtt5PublishBody("", []byte{0x23, 0x00, 0x01}, []byte("second")))
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.resolvePublishTopic(&second); err != nil {
		t.Fatal(err)
	}
	if second.Topic != "a/b" || string(second.Payload) != "second" {
		t.Fatalf("alias publish mismatch: %#v", second)
	}
}

func assertPacket(t *testing.T, conn net.Conn, want []byte) {
	t.Helper()
	got := readPacket(t, conn)
	if !bytes.Equal(got, want) {
		t.Fatalf("packet = %v, want %v", got, want)
	}
}

func readPacket(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, 2)
	if _, err := conn.Read(header[:1]); err != nil {
		t.Fatal(err)
	}
	remainingLength := 0
	multiplier := 1
	for {
		var b [1]byte
		if _, err := conn.Read(b[:]); err != nil {
			t.Fatal(err)
		}
		header = append(header[:1], append(header[1:1], b[0])...)
		remainingLength += int(b[0]&127) * multiplier
		if b[0]&128 == 0 {
			break
		}
		multiplier *= 128
	}
	payload := make([]byte, remainingLength)
	if _, err := conn.Read(payload); err != nil {
		t.Fatal(err)
	}
	return append(header, payload...)
}

func mqtt5ConnectPacket(clientID string) []byte {
	return mqtt5ConnectPacketWithProperties(clientID, nil)
}

func mqtt5ConnectPacketWithProperties(clientID string, properties []byte) []byte {
	var payload []byte
	payload = appendLPString(payload, clientID)
	var vh []byte
	vh = appendLPString(vh, "MQTT")
	vh = append(vh, 0x05, 0x02, 0x00, 0x3c)
	vh = append(vh, byte(len(properties)))
	vh = append(vh, properties...)
	body := append(vh, payload...)
	return appendFixedHeader(0x10, body)
}

func mqtt5SubscribePacket(packetID uint16, topic string) []byte {
	var body []byte
	body = append(body, byte(packetID>>8), byte(packetID), 0x00)
	body = appendLPString(body, topic)
	body = append(body, 0x00)
	return appendFixedHeader(0x82, body)
}

func mqtt5PublishPacket(topic string, payload []byte) []byte {
	return appendFixedHeader(0x30, mqtt5PublishBody(topic, nil, payload))
}

func mqtt5PublishBody(topic string, properties []byte, payload []byte) []byte {
	var body []byte
	body = appendLPString(body, topic)
	body = append(body, byte(len(properties)))
	body = append(body, properties...)
	body = append(body, payload...)
	return body
}

func appendFixedHeader(first byte, body []byte) []byte {
	packet := []byte{first}
	length := len(body)
	for {
		encoded := byte(length % 128)
		length /= 128
		if length > 0 {
			encoded |= 128
		}
		packet = append(packet, encoded)
		if length == 0 {
			break
		}
	}
	return append(packet, body...)
}

func appendLPString(dst []byte, value string) []byte {
	dst = append(dst, byte(len(value)>>8), byte(len(value)))
	return append(dst, value...)
}
