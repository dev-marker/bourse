package ratelimit

import (
	"testing"
	"time"
)

// TestLimiter moves a fake clock by hand, so every answer is exact. No sleeps, and nothing flaky.
func TestLimiter(t *testing.T) {
	now := time.Date(2026, 11, 24, 9, 30, 0, 0, time.UTC)
	l := New(2, 3) // two a second, three at once
	l.now = func() time.Time { return now }

	allow := func(key string, want bool, wantWait time.Duration) {
		t.Helper()
		if ok, wait := l.Allow(key); ok != want || wait != wantWait {
			t.Fatalf("%s at %s: Allow = %v, %v, want %v, %v", key, now.Format("15:04:05.000"),
				ok, wait, want, wantWait)
		}
	}
	for range 3 {
		allow("alice", true, 0) // a full bucket
	}
	allow("alice", false, 500*time.Millisecond) // empty: the next token is half a second away
	allow("bob", true, 0)                       // every account has a bucket of its own

	now = now.Add(500 * time.Millisecond)
	allow("alice", true, 0)
	allow("alice", false, 500*time.Millisecond)

	now = now.Add(time.Hour) // a long rest fills the bucket, but never past three
	for range 3 {
		allow("alice", true, 0)
	}
	allow("alice", false, 500*time.Millisecond)
}
