package film2

import (
	"sync"
	"time"
)

// cache is a tiny TTL map for scrape/search results.
type cache struct {
	mu sync.Mutex
	m  map[string]cacheEntry
}

type cacheEntry struct {
	val any
	exp time.Time
}

func newCache() *cache {
	return &cache{m: map[string]cacheEntry{}}
}

func (c *cache) Get(k string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[k]
	if !ok || time.Now().After(e.exp) {
		return nil, false
	}
	return e.val, true
}

func (c *cache) Set(k string, v any, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[k] = cacheEntry{v, time.Now().Add(ttl)}
}
