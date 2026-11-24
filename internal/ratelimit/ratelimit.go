// Package ratelimit gives every key a token bucket.
package ratelimit

import (
	"sync"
	"time"
)

// Limiter lets each key make burst requests at once, and then rate requests a second.
type Limiter struct {
	rate, burst float64
	now         func() time.Time // the clock, so a test can move time by hand

	mu      sync.Mutex
	buckets map[string]*bucket // one per key; keys are accounts, so there's a bounded number
}

// bucket is one key's tokens, as they were the last time it asked.
type bucket struct {
	tokens float64
	last   time.Time
}

func New(rate, burst float64) *Limiter {
	return &Limiter{rate: rate, burst: burst, now: time.Now, buckets: map[string]*bucket{}}
}

// Allow takes a token from key's bucket. If there's none, it says how long until there is.
func (l *Limiter) Allow(key string) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	b.tokens = min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.rate)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / l.rate * float64(time.Second))
}
