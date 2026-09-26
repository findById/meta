package core

import "testing"

func TestSubscribersMatchWildcardAndDeduplicate(t *testing.T) {
	b := NewBroker()
	client := &Client{ID: "client-1", CleanSession: true}
	client.SetStatus(StatusConnected)

	b.Subscribe(client, "device/+/status", 1)
	b.Subscribe(client, "device/1/status", 0)

	subscribers := b.Subscribers("device/1/status")
	if len(subscribers) != 1 {
		t.Fatalf("len(subscribers) = %d, want 1", len(subscribers))
	}
	if subscribers[0].Client != client {
		t.Fatalf("subscriber client mismatch")
	}
}

func TestRetainedForWildcardReturnsClone(t *testing.T) {
	b := NewBroker()
	b.StoreRetained(Message{
		Topic:   "device/1/status",
		Payload: []byte("online"),
		QoS:     1,
		Retain:  true,
	})

	retained := b.RetainedFor("device/+/status")
	if len(retained) != 1 {
		t.Fatalf("len(retained) = %d, want 1", len(retained))
	}
	retained[0].Payload[0] = 'x'

	retained = b.RetainedFor("device/1/status")
	if string(retained[0].Payload) != "online" {
		t.Fatalf("retained payload = %q, want %q", retained[0].Payload, "online")
	}
}

func TestPersistentSessionKeepsSubscriptionsAfterDisconnect(t *testing.T) {
	b := NewBroker()
	client := &Client{ID: "client-1", CleanSession: false}
	client.SetStatus(StatusConnected)

	b.Subscribe(client, "device/#", 1)
	b.UnregisterClient(client)

	next := &Client{ID: "client-1", CleanSession: false}
	next.SetStatus(StatusConnected)
	session, ok := b.SessionStore().Get(next.ID)
	if !ok {
		t.Fatalf("session not found")
	}
	for topic, qos := range session.Topics {
		next.TopicMap.Store(topic, qos)
		b.Subscribe(next, topic, qos)
	}

	subscribers := b.Subscribers("device/1/status")
	if len(subscribers) != 1 || subscribers[0].Client != next {
		t.Fatalf("persistent session was not restored")
	}
}

func TestPublishStoresOfflineMessageForPersistentSession(t *testing.T) {
	b := NewBroker()
	b.SessionStore().Save(ClientSession{
		ClientID: "offline-client",
		Topics:   map[string]byte{"device/#": 1},
	})
	b.hasPersistentSessions.Store(true)

	b.storeOffline(Message{Topic: "device/1/status", Payload: []byte("offline"), QoS: 1}, nil)

	messages := b.OfflineStore().List("offline-client", 10)
	if len(messages) != 1 {
		t.Fatalf("offline messages = %d, want 1", len(messages))
	}
	if string(messages[0].Payload) != "offline" {
		t.Fatalf("offline payload = %q", messages[0].Payload)
	}
}

func TestPublishRejectsWhenTaskQueueIsFull(t *testing.T) {
	b := NewBroker(
		WithWorkerCount(1),
		WithTaskQueueSize(1),
		WithTaskEnqueueWait(0),
	)
	b.TaskQueue <- func() {}

	if b.Publish(Message{Topic: "device/1/status", Payload: []byte("busy"), QoS: 1}) {
		t.Fatalf("publish succeeded with full queue")
	}
	if b.Stats.PublishRejected.Load() != 1 {
		t.Fatalf("publish rejected = %d, want 1", b.Stats.PublishRejected.Load())
	}
	if b.Stats.TaskDropped.Load() != 1 {
		t.Fatalf("task dropped = %d, want 1", b.Stats.TaskDropped.Load())
	}
}

func TestStoreOfflineSkipsWhenNoPersistentSessionObserved(t *testing.T) {
	b := NewBroker()
	b.SessionStore().Save(ClientSession{
		ClientID: "offline-client",
		Topics:   map[string]byte{"device/#": 1},
	})

	b.storeOffline(Message{Topic: "device/1/status", Payload: []byte("offline"), QoS: 1}, nil)
	if b.OfflineStore().Count("offline-client") != 0 {
		t.Fatalf("offline message stored without persistent session marker")
	}
}
