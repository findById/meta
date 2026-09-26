package cache

import (
	"sync"
	"time"
)

type Cache interface {
	Set(key string, value []byte, ttl time.Duration)
	Get(key string) ([]byte, bool)
	Delete(key string)
}

type MemoryCache struct {
	lock sync.RWMutex
	data map[string]cacheEntry
}

type cacheEntry struct {
	value     []byte
	expiresAt time.Time
}

func NewMemoryCache() *MemoryCache {
	return &MemoryCache{
		data: make(map[string]cacheEntry),
	}
}

func (c *MemoryCache) Set(key string, value []byte, ttl time.Duration) {
	if c == nil || key == "" {
		return
	}
	entry := cacheEntry{value: clone(value)}
	if ttl > 0 {
		entry.expiresAt = time.Now().Add(ttl)
	}
	c.lock.Lock()
	c.data[key] = entry
	c.lock.Unlock()
}

func (c *MemoryCache) Get(key string) ([]byte, bool) {
	if c == nil || key == "" {
		return nil, false
	}
	c.lock.RLock()
	entry, ok := c.data[key]
	c.lock.RUnlock()
	if !ok {
		return nil, false
	}
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		c.Delete(key)
		return nil, false
	}
	return clone(entry.value), true
}

func (c *MemoryCache) Delete(key string) {
	if c == nil || key == "" {
		return
	}
	c.lock.Lock()
	delete(c.data, key)
	c.lock.Unlock()
}

func clone(value []byte) []byte {
	next := make([]byte, len(value))
	copy(next, value)
	return next
}
