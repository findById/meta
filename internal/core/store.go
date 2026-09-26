package core

import (
	"sync"
	"time"
)

type SessionStore interface {
	Get(clientID string) (ClientSession, bool)
	Save(session ClientSession)
	Delete(clientID string)
	All() []ClientSession
	Count() int
}

type RetainStore interface {
	Store(message Message)
	Delete(topic string)
	Match(filter string) []RetainedMessage
	Count() int
}

type OfflineMessageStore interface {
	Append(clientID string, message Message)
	List(clientID string, limit int) []Message
	Delete(clientID string)
	Count(clientID string) int
	Trim(clientID string, limit int)
	ExpireBefore(timestamp int64)
}

type offlineMessage struct {
	Message   Message
	CreatedAt int64
}

type MemoryOfflineMessageStore struct {
	lock     sync.RWMutex
	messages map[string][]offlineMessage
}

func NewMemoryOfflineMessageStore() *MemoryOfflineMessageStore {
	return &MemoryOfflineMessageStore{messages: make(map[string][]offlineMessage)}
}

func (s *MemoryOfflineMessageStore) Append(clientID string, message Message) {
	if s == nil || clientID == "" || message.Topic == "" {
		return
	}
	s.lock.Lock()
	s.messages[clientID] = append(s.messages[clientID], offlineMessage{
		Message:   message.Clone(),
		CreatedAt: time.Now().Unix(),
	})
	s.lock.Unlock()
}

func (s *MemoryOfflineMessageStore) List(clientID string, limit int) []Message {
	if s == nil || clientID == "" {
		return nil
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	messages := s.messages[clientID]
	if limit > 0 && len(messages) > limit {
		messages = messages[:limit]
	}
	result := make([]Message, 0, len(messages))
	for _, message := range messages {
		result = append(result, message.Message.Clone())
	}
	return result
}

func (s *MemoryOfflineMessageStore) Delete(clientID string) {
	if s == nil || clientID == "" {
		return
	}
	s.lock.Lock()
	delete(s.messages, clientID)
	s.lock.Unlock()
}

func (s *MemoryOfflineMessageStore) Count(clientID string) int {
	if s == nil || clientID == "" {
		return 0
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.messages[clientID])
}

func (s *MemoryOfflineMessageStore) Trim(clientID string, limit int) {
	if s == nil || clientID == "" || limit <= 0 {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	messages := s.messages[clientID]
	if len(messages) <= limit {
		return
	}
	s.messages[clientID] = append([]offlineMessage(nil), messages[len(messages)-limit:]...)
}

func (s *MemoryOfflineMessageStore) ExpireBefore(timestamp int64) {
	if s == nil || timestamp <= 0 {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	for clientID, messages := range s.messages {
		filtered := messages[:0]
		for _, message := range messages {
			if message.CreatedAt >= timestamp {
				filtered = append(filtered, message)
			}
		}
		if len(filtered) == 0 {
			delete(s.messages, clientID)
			continue
		}
		s.messages[clientID] = append([]offlineMessage(nil), filtered...)
	}
}

type MemorySessionStore struct {
	lock     sync.RWMutex
	sessions map[string]ClientSession
}

func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{sessions: make(map[string]ClientSession)}
}

func (s *MemorySessionStore) Get(clientID string) (ClientSession, bool) {
	if s == nil || clientID == "" {
		return ClientSession{}, false
	}
	s.lock.RLock()
	session, ok := s.sessions[clientID]
	s.lock.RUnlock()
	if !ok {
		return ClientSession{}, false
	}
	return session.Clone(), true
}

func (s *MemorySessionStore) Save(session ClientSession) {
	if s == nil || session.ClientID == "" {
		return
	}
	s.lock.Lock()
	s.sessions[session.ClientID] = session.Clone()
	s.lock.Unlock()
}

func (s *MemorySessionStore) Delete(clientID string) {
	if s == nil || clientID == "" {
		return
	}
	s.lock.Lock()
	delete(s.sessions, clientID)
	s.lock.Unlock()
}

func (s *MemorySessionStore) All() []ClientSession {
	if s == nil {
		return nil
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	result := make([]ClientSession, 0, len(s.sessions))
	for _, session := range s.sessions {
		result = append(result, session.Clone())
	}
	return result
}

func (s *MemorySessionStore) Count() int {
	if s == nil {
		return 0
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.sessions)
}

type MemoryRetainStore struct {
	lock     sync.RWMutex
	retained map[string]RetainedMessage
}

func NewMemoryRetainStore() *MemoryRetainStore {
	return &MemoryRetainStore{retained: make(map[string]RetainedMessage)}
}

func (s *MemoryRetainStore) Store(message Message) {
	if s == nil || message.Topic == "" {
		return
	}
	s.lock.Lock()
	s.retained[message.Topic] = message.Clone()
	s.lock.Unlock()
}

func (s *MemoryRetainStore) Delete(topic string) {
	if s == nil || topic == "" {
		return
	}
	s.lock.Lock()
	delete(s.retained, topic)
	s.lock.Unlock()
}

func (s *MemoryRetainStore) Match(filter string) []RetainedMessage {
	if s == nil || filter == "" {
		return nil
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	var result []RetainedMessage
	for topic, retained := range s.retained {
		if TopicMatch(filter, topic) {
			result = append(result, retained.Clone())
		}
	}
	return result
}

func (s *MemoryRetainStore) Count() int {
	if s == nil {
		return 0
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	return len(s.retained)
}

func (s ClientSession) Clone() ClientSession {
	topics := make(map[string]byte, len(s.Topics))
	for topic, qos := range s.Topics {
		topics[topic] = qos
	}
	var will *WillMessage
	if s.Will != nil {
		cloned := s.Will.Clone()
		will = &cloned
	}
	return ClientSession{
		ClientID: s.ClientID,
		Topics:   topics,
		Will:     will,
	}
}
