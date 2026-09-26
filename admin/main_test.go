package main

import "testing"

func TestAuthenticateAndAuthorize(t *testing.T) {
	server := NewServer("", Config{
		Devices: []Device{
			{ID: "client-1", Protocol: "mqtt-3.1.1", Username: "admin", Password: "pwd", Enabled: true},
			{ID: "client-2", Protocol: "mqtt-3.1.1", Username: "admin", PasswordHash: hashPassword("secret"), Enabled: true},
		},
		ACLs: []ACL{
			{ClientID: "client-1", TopicFilter: "device/+/status", Action: "publish"},
			{ClientID: "client-2", TopicFilter: "#", Action: "*"},
		},
	})

	if !server.authenticate(AuthRequest{Protocol: "mqtt-3.1.1", ClientID: "client-1", Username: "admin", Password: "pwd"}) {
		t.Fatalf("expected password auth allowed")
	}
	if server.authenticate(AuthRequest{Protocol: "mqtt-3.1.1", ClientID: "client-1", Username: "admin", Password: "bad"}) {
		t.Fatalf("expected password auth denied")
	}
	if !server.authenticate(AuthRequest{Protocol: "mqtt-3.1.1", ClientID: "client-2", Username: "admin", Password: "secret"}) {
		t.Fatalf("expected password hash auth allowed")
	}
	if !server.authorize(AccessRequest{ClientID: "client-1", Topic: "device/1/status", Action: "publish"}) {
		t.Fatalf("expected acl allowed")
	}
	if server.authorize(AccessRequest{ClientID: "client-1", Topic: "device/1/config", Action: "publish"}) {
		t.Fatalf("expected acl denied")
	}
	if !server.authorize(AccessRequest{ClientID: "client-2", Topic: "any/topic", Action: "subscribe"}) {
		t.Fatalf("expected wildcard acl allowed")
	}
}
