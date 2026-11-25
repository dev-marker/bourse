package gateway

import (
	"sync"
	"time"

	"github.com/fiston-user/bourse/internal/market"
)

// replies remembers the answer to every client order ID for a while, so a retry gets that same
// answer, instead of placing a second order. It keeps max answers at most, however busy we are.
type replies struct {
	ttl time.Duration
	max int
	now func() time.Time

	mu    sync.Mutex
	byKey map[string]*pending
	queue []*pending // oldest first, so the expired ones are always at the front
}

// pending is one order's answer, or the promise of one.
type pending struct {
	key    string
	order  market.Order // what was asked for
	at     time.Time
	done   chan struct{} // closed once the answer is in
	status int
	body   []byte
}

func newReplies(ttl time.Duration, max int) *replies {
	return &replies{ttl: ttl, max: max, now: time.Now, byKey: map[string]*pending{}}
}

// claim returns the answer for key, and whether it's new. The caller of a new one must finish it.
func (rs *replies) claim(key string, o market.Order) (*pending, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	now := rs.now()
	for len(rs.queue) > 0 && now.Sub(rs.queue[0].at) >= rs.ttl {
		rs.forget()
	}
	if p, ok := rs.byKey[key]; ok {
		return p, false
	}
	// Full: the oldest answer goes, however young. A retry for it can't place a second order while
	// the first one's open, because its hold is still there, and the hold says 409.
	if len(rs.queue) >= rs.max {
		rs.forget()
	}
	p := &pending{key: key, order: o, at: now, done: make(chan struct{})}
	rs.byKey[key] = p
	rs.queue = append(rs.queue, p)
	return p, true
}

// forget drops the oldest answer.
func (rs *replies) forget() {
	if old := rs.queue[0]; rs.byKey[old.key] == old {
		delete(rs.byKey, old.key)
	}
	rs.queue = rs.queue[1:]
}

// finish stores the answer, and wakes every retry that's waiting for it. Without keep, the key is
// forgotten, so the next retry tries again.
func (rs *replies) finish(p *pending, status int, body []byte, keep bool) {
	rs.mu.Lock()
	p.status, p.body = status, body
	if !keep && rs.byKey[p.key] == p {
		delete(rs.byKey, p.key)
	}
	rs.mu.Unlock()
	close(p.done)
}
