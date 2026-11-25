package auth

import (
	"context"
	"sync"
	"time"
)

// Cache remembers the keys it has looked up, for a while, so that an account's requests don't each
// cost a trip to Postgres. The price is that a revoked key keeps working until its entry expires.
type Cache struct {
	store *Store
	ttl   time.Duration

	mu   sync.Mutex
	keys map[string]cached
}

type cached struct {
	key     Key
	expires time.Time
}

// NewCache looks keys up in store, and remembers each one for ttl.
func NewCache(store *Store, ttl time.Duration) *Cache {
	return &Cache{store: store, ttl: ttl, keys: map[string]cached{}}
}

// Lookup returns the key with this ID, from memory if it was looked up less than ttl ago.
func (c *Cache) Lookup(ctx context.Context, id string) (Key, error) {
	c.mu.Lock()
	e, ok := c.keys[id]
	c.mu.Unlock()
	if ok && time.Now().Before(e.expires) {
		return e.key, nil
	}
	k, err := c.store.Lookup(ctx, id)
	if err != nil {
		return Key{}, err // an unknown key isn't remembered, or anyone could fill the map
	}
	c.mu.Lock()
	c.keys[id] = cached{k, time.Now().Add(c.ttl)}
	c.mu.Unlock()
	return k, nil
}
