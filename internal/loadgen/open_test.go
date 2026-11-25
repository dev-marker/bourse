package loadgen

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestOpen sends a thousand a second for a tenth of a second, to an exchange that takes 50 ms to
// answer. An open loop still starts every call on time, and the last one, due at 99 ms, is done at
// 149. A closed loop would have made two.
func TestOpen(t *testing.T) {
	var (
		mu   sync.Mutex
		late time.Duration // the latest any call started
	)
	start := time.Now()
	n := Open(context.Background(), 1000, 100*time.Millisecond, func(due time.Time) {
		mu.Lock()
		late = max(late, time.Since(due))
		mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	})
	took := time.Since(start)
	if n != 100 {
		t.Errorf("made %d calls, want 100", n)
	}
	if late > 20*time.Millisecond {
		t.Errorf("a call started %v after it was due", late)
	}
	if took < 149*time.Millisecond || took > 250*time.Millisecond {
		t.Errorf("took %v, want about 150ms", took)
	}
}
