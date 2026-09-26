package core

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/findById/meta/internal/cache"
	"github.com/findById/meta/internal/security"
)

type Broker struct {
	ID          string
	ClientMap   sync.Map
	TaskQueue   chan func()
	Stats       BrokerStats
	stop        chan struct{}
	workerCount int
	wg          sync.WaitGroup
	options     Options

	topicLock     sync.RWMutex
	clientTopics  map[string]map[string]struct{}
	subscriptions *SubscriptionIndex

	hasPersistentSessions atomic.Bool
	lastOfflineCleanup    atomic.Int64
	lastBusyLog           atomic.Int64
}

type ClientSession struct {
	ClientID string
	Topics   map[string]byte
	Will     *WillMessage
}

type Subscriber struct {
	Client *Client
	Filter string
	QoS    byte
}

func NewBroker(brokerOptions ...Option) *Broker {
	options := DefaultOptions()
	for _, apply := range brokerOptions {
		if apply != nil {
			apply(&options)
		}
	}
	broker := &Broker{
		ID:            fmt.Sprint(time.Now().UnixNano()),
		TaskQueue:     make(chan func(), options.TaskQueueSize),
		stop:          make(chan struct{}),
		workerCount:   options.WorkerCount,
		options:       options,
		clientTopics:  make(map[string]map[string]struct{}),
		subscriptions: NewSubscriptionIndex(),
	}
	if options.SessionStore != nil && options.SessionStore.Count() > 0 {
		broker.hasPersistentSessions.Store(true)
	}
	return broker
}

func (b *Broker) Start() {
	for i := 0; i < b.workerCount; i++ {
		b.wg.Add(1)
		go b.worker(i)
	}
}

func (b *Broker) Stop() {
	select {
	case <-b.stop:
		return
	default:
		close(b.stop)
	}
	b.ClientMap.Range(func(_, value interface{}) bool {
		if client, ok := value.(*Client); ok {
			client.MarkGraceful()
			client.Close()
		}
		return true
	})
	b.wg.Wait()
}

func (b *Broker) Accept(conn net.Conn, protocol string, factory HandlerFactory) {
	client := NewClient(conn, b, protocol)
	handler := factory(client)
	if handler == nil {
		client.Close()
		return
	}
	handler.Start()
}

func (b *Broker) Worker(task func()) bool {
	if task == nil {
		return true
	}
	select {
	case b.TaskQueue <- task:
		return true
	case <-b.stop:
		return false
	default:
	}
	wait := b.options.TaskEnqueueWait
	if wait <= 0 {
		b.Stats.TaskDropped.Add(1)
		return false
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case b.TaskQueue <- task:
		return true
	case <-b.stop:
		return false
	case <-timer.C:
		b.Stats.TaskDropped.Add(1)
		return false
	}
}

func (b *Broker) Security() security.Provider {
	if b.options.Security == nil {
		return security.AllowAll{}
	}
	return b.options.Security
}

func (b *Broker) Cache() cache.Cache {
	return b.options.Cache
}

func (b *Broker) SessionStore() SessionStore {
	if b.options.SessionStore == nil {
		b.options.SessionStore = NewMemorySessionStore()
	}
	return b.options.SessionStore
}

func (b *Broker) RetainStore() RetainStore {
	if b.options.RetainStore == nil {
		b.options.RetainStore = NewMemoryRetainStore()
	}
	return b.options.RetainStore
}

func (b *Broker) OfflineStore() OfflineMessageStore {
	if b.options.OfflineStore == nil {
		b.options.OfflineStore = NewMemoryOfflineMessageStore()
	}
	return b.options.OfflineStore
}

func (b *Broker) OutboundQueueSize() int {
	if b.options.OutboundQueueSize <= 0 {
		return DefaultOutboundQueueSize
	}
	return b.options.OutboundQueueSize
}

func (b *Broker) WriteTimeout() time.Duration {
	if b.options.WriteTimeout <= 0 {
		return DefaultWriteTimeout
	}
	return b.options.WriteTimeout
}

func (b *Broker) worker(index int) {
	defer b.wg.Done()
	for {
		select {
		case task := <-b.TaskQueue:
			if task != nil {
				task()
			}
		case <-b.stop:
			return
		}
	}
}

func (b *Broker) RegisterClient(client *Client) {
	if client == nil || strings.TrimSpace(client.ID) == "" {
		return
	}
	if old, ok := b.ClientMap.Load(client.ID); ok {
		if oldClient, ok := old.(*Client); ok && oldClient != client {
			oldClient.Close()
		}
	}
	b.ClientMap.Store(client.ID, client)
}

func (b *Broker) BindSession(client *Client, sessionOptions SessionOptions) bool {
	if client == nil || strings.TrimSpace(client.ID) == "" {
		return false
	}

	client.CleanSession = sessionOptions.CleanSession
	client.KeepAlive = keepAliveDuration(sessionOptions.KeepAlive)
	client.Will = sessionOptions.Will

	if client.CleanSession {
		b.SessionStore().Delete(client.ID)
		return false
	}
	b.hasPersistentSessions.Store(true)

	session, existed := b.SessionStore().Get(client.ID)
	if !existed {
		session = ClientSession{
			ClientID: client.ID,
			Topics:   make(map[string]byte),
		}
	}
	session.Will = client.Will
	topics := make(map[string]byte, len(session.Topics))
	for topic, qos := range session.Topics {
		topics[topic] = qos
	}
	b.SessionStore().Save(session)

	for topic, qos := range topics {
		client.TopicMap.Store(topic, qos)
		b.Subscribe(client, topic, qos)
	}
	b.deliverOffline(client)
	return existed
}

func (b *Broker) UnregisterClient(client *Client) {
	if client == nil || client.ID == "" {
		return
	}
	b.ClientMap.Delete(client.ID)
	b.topicLock.Lock()
	topics := b.clientTopics[client.ID]
	for topic := range topics {
		b.subscriptions.Remove(client.ID, topic)
	}
	delete(b.clientTopics, client.ID)
	b.topicLock.Unlock()

	if client.CleanSession {
		b.SessionStore().Delete(client.ID)
		return
	}

	session, ok := b.SessionStore().Get(client.ID)
	if !ok {
		session = ClientSession{
			ClientID: client.ID,
			Topics:   make(map[string]byte),
		}
	}
	session.Topics = client.TopicSnapshot()
	session.Will = client.Will
	b.SessionStore().Save(session)
}

func (b *Broker) Subscribe(client *Client, topic string, qos byte) {
	if client == nil || client.ID == "" || topic == "" {
		return
	}
	client.TopicMap.Store(topic, qos)
	b.topicLock.Lock()
	b.subscriptions.Add(client, topic, qos)
	if b.clientTopics[client.ID] == nil {
		b.clientTopics[client.ID] = make(map[string]struct{})
	}
	b.clientTopics[client.ID][topic] = struct{}{}
	b.topicLock.Unlock()

	if !client.CleanSession {
		b.hasPersistentSessions.Store(true)
		session, ok := b.SessionStore().Get(client.ID)
		if !ok {
			session = ClientSession{
				ClientID: client.ID,
				Topics:   make(map[string]byte),
			}
		}
		session.Topics[topic] = qos
		b.SessionStore().Save(session)
	}
}

func (b *Broker) Unsubscribe(client *Client, topic string) {
	if client == nil || client.ID == "" || topic == "" {
		return
	}
	b.topicLock.Lock()
	b.subscriptions.Remove(client.ID, topic)
	if topics := b.clientTopics[client.ID]; topics != nil {
		delete(topics, topic)
		if len(topics) == 0 {
			delete(b.clientTopics, client.ID)
		}
	}
	b.topicLock.Unlock()

	client.TopicMap.Delete(topic)
	if !client.CleanSession {
		if session, ok := b.SessionStore().Get(client.ID); ok {
			delete(session.Topics, topic)
			b.SessionStore().Save(session)
		}
	}
}

func (b *Broker) Subscribers(topic string) []Subscriber {
	return b.subscriptions.Match(topic)
}

func (b *Broker) StoreRetained(msg Message) {
	if msg.Topic == "" {
		return
	}
	if len(msg.Payload) == 0 {
		b.RetainStore().Delete(msg.Topic)
		return
	}
	b.RetainStore().Store(msg)
}

func (b *Broker) RetainedFor(filter string) []RetainedMessage {
	return b.RetainStore().Match(filter)
}

func (b *Broker) Publish(data Message) bool {
	if data.Topic == "" {
		return true
	}
	if data.Retain {
		b.StoreRetained(data)
	}
	if !b.Worker(func() {
		b.publish(data)
	}) {
		b.Stats.PublishRejected.Add(1)
		b.logBusyLimited()
		return false
	}
	return true
}

func (b *Broker) publish(data Message) {
	b.Stats.PublishMessages.Add(1)
	delivered := make(map[string]struct{})
	for _, sub := range b.Subscribers(data.Topic) {
		c := sub.Client
		if c == nil || !c.IsConnected() {
			if c != nil {
				c.Close()
			}
			continue
		}
		if !c.HasSubscriptionFor(data.Topic) {
			b.Unsubscribe(c, sub.Filter)
			continue
		}
		encoded, packetID, err := EncodePublish(c.Protocol, data, MinQoS(data.QoS, sub.QoS), data.Retain, 0)
		if err != nil {
			log.Println("encode publish", err)
			b.Stats.DeliveryFailures.Add(1)
			continue
		}
		if MinQoS(data.QoS, sub.QoS) > 0 {
			c.TrackInflight(packetID, encoded)
		}
		if err := c.WriteBuffer(encoded); err != nil {
			b.Stats.DeliveryFailures.Add(1)
			c.Close()
			continue
		}
		delivered[c.ID] = struct{}{}
		b.Stats.Deliveries.Add(1)
	}
	b.storeOffline(data, delivered)
}

func (b *Broker) storeOffline(data Message, delivered map[string]struct{}) {
	if !b.hasPersistentSessions.Load() {
		return
	}
	if b.options.OfflineMessageTTL > 0 && b.options.OfflineCleanup > 0 {
		now := time.Now()
		last := b.lastOfflineCleanup.Load()
		if now.Unix()-last >= int64(b.options.OfflineCleanup/time.Second) && b.lastOfflineCleanup.CompareAndSwap(last, now.Unix()) {
			b.OfflineStore().ExpireBefore(now.Add(-b.options.OfflineMessageTTL).Unix())
		}
	}
	for _, session := range b.SessionStore().All() {
		if session.ClientID == "" {
			continue
		}
		if _, ok := delivered[session.ClientID]; ok {
			continue
		}
		if _, online := b.ClientMap.Load(session.ClientID); online {
			continue
		}
		for filter := range session.Topics {
			if TopicMatch(filter, data.Topic) {
				store := b.OfflineStore()
				store.Append(session.ClientID, data)
				if b.options.OfflineMessageCap > 0 {
					store.Trim(session.ClientID, b.options.OfflineMessageCap)
				}
				break
			}
		}
	}
}

func (b *Broker) logBusyLimited() {
	now := time.Now().Unix()
	last := b.lastBusyLog.Load()
	if now == last || !b.lastBusyLog.CompareAndSwap(last, now) {
		return
	}
	log.Printf("broker busy: task queue full dropped=%d rejected=%d queue=%d/%d",
		b.Stats.TaskDropped.Load(),
		b.Stats.PublishRejected.Load(),
		len(b.TaskQueue),
		cap(b.TaskQueue),
	)
}

func (b *Broker) deliverOffline(client *Client) {
	if client == nil || client.ID == "" {
		return
	}
	messages := b.OfflineStore().List(client.ID, 0)
	for _, message := range messages {
		qos := message.QoS
		for filter, subQoS := range client.TopicSnapshot() {
			if TopicMatch(filter, message.Topic) {
				qos = MinQoS(qos, subQoS)
				break
			}
		}
		encoded, packetID, err := EncodePublish(client.Protocol, message, qos, message.Retain, 0)
		if err != nil {
			b.Stats.DeliveryFailures.Add(1)
			continue
		}
		if qos > 0 {
			client.TrackInflight(packetID, encoded)
		}
		if err := client.WriteBuffer(encoded); err != nil {
			b.Stats.DeliveryFailures.Add(1)
			return
		}
		b.Stats.Deliveries.Add(1)
	}
	b.OfflineStore().Delete(client.ID)
}

func keepAliveDuration(seconds uint16) time.Duration {
	if seconds == 0 {
		return DefaultKeepAliveTimeout
	}
	return time.Duration(seconds) * time.Second * 3 / 2
}
