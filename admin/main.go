package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type Config struct {
	Token   string   `json:"token"`
	Devices []Device `json:"devices"`
	ACLs    []ACL    `json:"acls"`
}

type Device struct {
	ID           string `json:"id"`
	Protocol     string `json:"protocol"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	PasswordHash string `json:"passwordHash"`
	Enabled      bool   `json:"enabled"`
}

type ACL struct {
	ClientID    string `json:"clientId"`
	TopicFilter string `json:"topicFilter"`
	Action      string `json:"action"`
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

type Server struct {
	addr   string
	token  string
	config Config
	server *http.Server
}

func main() {
	addr := flag.String("addr", ":18080", "admin listen address")
	configPath := flag.String("config", "admin/config.example.json", "device and acl config file path")
	flag.Parse()

	config, err := loadConfig(*configPath)
	if err != nil {
		log.Fatal("load config", err)
	}
	server := NewServer(*addr, config)
	if err := server.Start(); err != nil {
		log.Fatal("start admin", err)
	}
	log.Println("admin listening on", *addr)
	waitForSignal()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Stop(ctx)
}

func NewServer(addr string, config Config) *Server {
	return &Server{addr: addr, token: config.Token, config: normalizeConfig(config)}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/api/security/authenticate", s.requireAuth(s.handleAuthenticate))
	mux.HandleFunc("/api/security/authorize", s.requireAuth(s.handleAuthorize))
	s.server = &http.Server{
		Addr:              s.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Println("admin server", err)
		}
	}()
	return nil
}

func (s *Server) Stop(ctx context.Context) {
	if s == nil || s.server == nil {
		return
	}
	_ = s.server.Shutdown(ctx)
}

func (s *Server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s == nil || s.token == "" {
			next(w, r)
			return
		}
		token := strings.TrimSpace(r.Header.Get("X-Admin-Token"))
		if token == "" {
			auth := strings.TrimSpace(r.Header.Get("Authorization"))
			if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
				token = strings.TrimSpace(auth[7:])
			}
		}
		if token != s.token {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next(w, r)
	}
}

func (s *Server) handleAuthenticate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req AuthRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, DecisionResponse{Allowed: s.authenticate(req)})
}

func (s *Server) handleAuthorize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req AccessRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, DecisionResponse{Allowed: s.authorize(req)})
}

func (s *Server) authenticate(req AuthRequest) bool {
	if req.ClientID == "" || req.Protocol == "" {
		return false
	}
	for _, device := range s.config.Devices {
		if device.ID != req.ClientID || device.Protocol != req.Protocol || !device.Enabled {
			continue
		}
		if device.Username != "" && device.Username != req.Username {
			return false
		}
		if device.PasswordHash != "" {
			return device.PasswordHash == hashPassword(req.Password)
		}
		return device.Password == "" || device.Password == req.Password
	}
	return false
}

func (s *Server) authorize(req AccessRequest) bool {
	if req.ClientID == "" || req.Topic == "" || req.Action == "" {
		return false
	}
	action := strings.ToLower(req.Action)
	for _, acl := range s.config.ACLs {
		if acl.ClientID != req.ClientID {
			continue
		}
		if acl.Action != "*" && acl.Action != action {
			continue
		}
		if topicMatch(acl.TopicFilter, req.Topic) {
			return true
		}
	}
	return false
}

func loadConfig(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer file.Close()
	var config Config
	if err := json.NewDecoder(file).Decode(&config); err != nil {
		return Config{}, err
	}
	return config, nil
}

func normalizeConfig(config Config) Config {
	for i := range config.ACLs {
		config.ACLs[i].Action = strings.ToLower(strings.TrimSpace(config.ACLs[i].Action))
	}
	return config
}

func hashPassword(password string) string {
	sum := sha256.Sum256([]byte(password))
	return hex.EncodeToString(sum[:])
}

func topicMatch(filter, topic string) bool {
	if filter == topic || filter == "#" {
		return true
	}
	if filter == "" || topic == "" {
		return false
	}
	filterLevels := strings.Split(filter, "/")
	topicLevels := strings.Split(topic, "/")
	for i, filterLevel := range filterLevels {
		if filterLevel == "#" {
			return i == len(filterLevels)-1
		}
		if i >= len(topicLevels) {
			return false
		}
		if filterLevel == "+" {
			continue
		}
		if filterLevel != topicLevels[i] {
			return false
		}
	}
	return len(filterLevels) == len(topicLevels)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func waitForSignal() os.Signal {
	signalChan := make(chan os.Signal, 1)
	defer close(signalChan)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	s := <-signalChan
	signal.Stop(signalChan)
	fmt.Println("Bye")
	return s
}
