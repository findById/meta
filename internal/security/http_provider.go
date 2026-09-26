package security

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/findById/meta/internal/cache"
)

const defaultHTTPTimeout = 3 * time.Second
const cacheTTL = 2 * time.Minute

type HTTPProvider struct {
	BaseURL string
	Token   string
	Client  *http.Client
	Cache   cache.Cache
}

type AuthRequest struct {
	Protocol   string `json:"protocol"`
	ClientID   string `json:"clientId"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	RemoteAddr string `json:"remoteAddr,omitempty"`
}

type AccessRequest struct {
	Protocol string `json:"protocol"`
	ClientID string `json:"clientId"`
	Topic    string `json:"topic"`
	Action   string `json:"action"`
}

type DecisionResponse struct {
	Allowed bool `json:"allowed"`
}

func NewHTTPProvider(baseURL, token string, cacheStore cache.Cache) *HTTPProvider {
	if cacheStore == nil {
		cacheStore = cache.NewMemoryCache()
	}
	return &HTTPProvider{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Token:   token,
		Client:  &http.Client{Timeout: defaultHTTPTimeout},
		Cache:   cacheStore,
	}
}

func (p *HTTPProvider) Authenticate(ctx context.Context, auth AuthContext) bool {
	if p == nil || p.BaseURL == "" || auth.ClientID == "" {
		return false
	}
	cacheKey := fmt.Sprintf("security:http:auth:%s:%s:%s:%x", auth.Protocol, auth.ClientID, auth.Username, auth.Password)
	if value, ok := p.Cache.Get(cacheKey); ok {
		return string(value) == "1"
	}
	remoteAddr := ""
	if auth.RemoteAddr != nil {
		remoteAddr = auth.RemoteAddr.String()
	}
	ok := p.postDecision(ctx, "/api/security/authenticate", AuthRequest{
		Protocol:   auth.Protocol,
		ClientID:   auth.ClientID,
		Username:   string(auth.Username),
		Password:   string(auth.Password),
		RemoteAddr: remoteAddr,
	})
	p.Cache.Set(cacheKey, boolBytes(ok), cacheTTL)
	return ok
}

func (p *HTTPProvider) Authorize(ctx context.Context, access TopicAccessContext) bool {
	if p == nil || p.BaseURL == "" || access.ClientID == "" || access.Topic == "" || access.Action == "" {
		return false
	}
	cacheKey := fmt.Sprintf("security:http:acl:%s:%s:%s:%s", access.Protocol, access.ClientID, access.Action, access.Topic)
	if value, ok := p.Cache.Get(cacheKey); ok {
		return string(value) == "1"
	}
	ok := p.postDecision(ctx, "/api/security/authorize", AccessRequest{
		Protocol: access.Protocol,
		ClientID: access.ClientID,
		Topic:    access.Topic,
		Action:   access.Action,
	})
	p.Cache.Set(cacheKey, boolBytes(ok), cacheTTL)
	return ok
}

func (p *HTTPProvider) postDecision(ctx context.Context, path string, body any) bool {
	payload, err := json.Marshal(body)
	if err != nil {
		return false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.BaseURL+path, bytes.NewReader(payload))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	if p.Token != "" {
		req.Header.Set("X-Admin-Token", p.Token)
	}
	client := p.Client
	if client == nil {
		client = &http.Client{Timeout: defaultHTTPTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var result DecisionResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false
	}
	return result.Allowed
}

func boolBytes(value bool) []byte {
	if value {
		return []byte("1")
	}
	return []byte("0")
}
