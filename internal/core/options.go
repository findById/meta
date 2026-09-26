package core

import (
	"runtime"
	"time"

	"github.com/findById/meta/internal/cache"
	"github.com/findById/meta/internal/security"
)

const (
	DefaultTaskQueueSize     = 4096
	DefaultOutboundQueueSize = 1024
	DefaultTaskEnqueueWait   = 200 * time.Millisecond
	DefaultWriteTimeout      = 10 * time.Second
	DefaultKeepAliveTimeout  = 2 * time.Minute
	DefaultInflightRetry     = 15 * time.Second
	DefaultMaxInflightRetry  = 5
	DefaultOfflineMessageTTL = 24 * time.Hour
	DefaultOfflineMessageCap = 1000
	DefaultOfflineCleanup    = time.Minute
)

type Handler interface {
	Start()
}

type HandlerFactory func(client *Client) Handler

type Options struct {
	WorkerCount       int
	TaskQueueSize     int
	TaskEnqueueWait   time.Duration
	OutboundQueueSize int
	WriteTimeout      time.Duration
	Security          security.Provider
	Cache             cache.Cache
	SessionStore      SessionStore
	RetainStore       RetainStore
	OfflineStore      OfflineMessageStore
	OfflineMessageTTL time.Duration
	OfflineMessageCap int
	OfflineCleanup    time.Duration
}

type Option func(*Options)

func DefaultOptions() Options {
	workerCount := runtime.NumCPU()
	if workerCount < 1 {
		workerCount = 1
	}
	return Options{
		WorkerCount:       workerCount,
		TaskQueueSize:     DefaultTaskQueueSize,
		TaskEnqueueWait:   DefaultTaskEnqueueWait,
		OutboundQueueSize: DefaultOutboundQueueSize,
		WriteTimeout:      DefaultWriteTimeout,
		Security:          security.AllowAll{},
		Cache:             cache.NewMemoryCache(),
		SessionStore:      NewMemorySessionStore(),
		RetainStore:       NewMemoryRetainStore(),
		OfflineStore:      NewMemoryOfflineMessageStore(),
		OfflineMessageTTL: DefaultOfflineMessageTTL,
		OfflineMessageCap: DefaultOfflineMessageCap,
		OfflineCleanup:    DefaultOfflineCleanup,
	}
}

func WithWorkerCount(count int) Option {
	return func(options *Options) {
		if count > 0 {
			options.WorkerCount = count
		}
	}
}

func WithTaskQueueSize(size int) Option {
	return func(options *Options) {
		if size > 0 {
			options.TaskQueueSize = size
		}
	}
}

func WithTaskEnqueueWait(wait time.Duration) Option {
	return func(options *Options) {
		if wait >= 0 {
			options.TaskEnqueueWait = wait
		}
	}
}

func WithOutboundQueueSize(size int) Option {
	return func(options *Options) {
		if size > 0 {
			options.OutboundQueueSize = size
		}
	}
}

func WithSecurity(provider security.Provider) Option {
	return func(options *Options) {
		if provider != nil {
			options.Security = provider
		}
	}
}

func WithCache(store cache.Cache) Option {
	return func(options *Options) {
		if store != nil {
			options.Cache = store
		}
	}
}

func WithSessionStore(store SessionStore) Option {
	return func(options *Options) {
		if store != nil {
			options.SessionStore = store
		}
	}
}

func WithRetainStore(store RetainStore) Option {
	return func(options *Options) {
		if store != nil {
			options.RetainStore = store
		}
	}
}

func WithOfflineStore(store OfflineMessageStore) Option {
	return func(options *Options) {
		if store != nil {
			options.OfflineStore = store
		}
	}
}

func WithOfflineMessageTTL(ttl time.Duration) Option {
	return func(options *Options) {
		if ttl >= 0 {
			options.OfflineMessageTTL = ttl
		}
	}
}

func WithOfflineMessageCap(cap int) Option {
	return func(options *Options) {
		if cap >= 0 {
			options.OfflineMessageCap = cap
		}
	}
}

func WithOfflineCleanup(interval time.Duration) Option {
	return func(options *Options) {
		if interval >= 0 {
			options.OfflineCleanup = interval
		}
	}
}
