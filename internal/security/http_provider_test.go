package security

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestHTTPProviderAuthenticateAndAuthorize(t *testing.T) {
	provider := NewHTTPProvider("http://admin.local", "secret", nil)
	provider.Client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("X-Admin-Token") != "secret" {
			t.Fatalf("missing token")
		}
		switch r.URL.Path {
		case "/api/security/authenticate":
			var req AuthRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatal(err)
			}
			if req.Protocol != ProtocolMQTT311 || req.ClientID != "client-1" || req.Username != "admin" || req.Password != "pwd" {
				t.Fatalf("unexpected auth request: %#v", req)
			}
		case "/api/security/authorize":
			var req AccessRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatal(err)
			}
			if req.Protocol != ProtocolMQTT311 || req.ClientID != "client-1" || req.Topic != "device/1/status" || req.Action != "publish" {
				t.Fatalf("unexpected access request: %#v", req)
			}
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"allowed":true}`)),
			Header:     make(http.Header),
		}, nil
	})}

	if !provider.Authenticate(context.Background(), AuthContext{
		Protocol: ProtocolMQTT311,
		ClientID: "client-1",
		Username: []byte("admin"),
		Password: []byte("pwd"),
	}) {
		t.Fatalf("expected auth allowed")
	}
	if !provider.Authorize(context.Background(), TopicAccessContext{
		Protocol: ProtocolMQTT311,
		ClientID: "client-1",
		Topic:    "device/1/status",
		Action:   "publish",
	}) {
		t.Fatalf("expected access allowed")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
