package core

import (
	"expvar"
	"sync"
	"sync/atomic"
)

type BrokerStats struct {
	AcceptedConnections atomic.Int64
	TaskDropped         atomic.Int64
	PublishRejected     atomic.Int64
	PublishMessages     atomic.Int64
	Deliveries          atomic.Int64
	DeliveryFailures    atomic.Int64
}

var publishStatsOnce sync.Once

func PublishStats(broker *Broker) {
	publishStatsOnce.Do(func() {
		expvar.Publish("meta_broker", expvar.Func(func() interface{} {
			if broker == nil {
				return nil
			}
			return broker.StatsSnapshot()
		}))
	})
}

func (b *Broker) StatsSnapshot() map[string]interface{} {
	return map[string]interface{}{
		"accepted_connections": b.Stats.AcceptedConnections.Load(),
		"online_clients":       b.ClientCount(),
		"subscriptions":        b.SubscriptionCount(),
		"sessions":             b.SessionCount(),
		"retained_messages":    b.RetainedCount(),
		"task_queue_len":       len(b.TaskQueue),
		"task_queue_cap":       cap(b.TaskQueue),
		"task_dropped":         b.Stats.TaskDropped.Load(),
		"publish_rejected":     b.Stats.PublishRejected.Load(),
		"publish_messages":     b.Stats.PublishMessages.Load(),
		"deliveries":           b.Stats.Deliveries.Load(),
		"delivery_failures":    b.Stats.DeliveryFailures.Load(),
	}
}

func (b *Broker) ClientCount() int {
	count := 0
	b.ClientMap.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

func (b *Broker) SubscriptionCount() int {
	return b.subscriptions.Count()
}

func (b *Broker) SessionCount() int {
	return b.SessionStore().Count()
}

func (b *Broker) RetainedCount() int {
	return b.RetainStore().Count()
}
