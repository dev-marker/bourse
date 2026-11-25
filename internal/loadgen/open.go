package loadgen

import (
	"context"
	"sync"
	"time"
)

// Open calls send rate times a second for d, each in a goroutine of its own, and never waits for one
// to finish. Every call gets the time it was due, so its latency counts from then.
func Open(ctx context.Context, rate float64, d time.Duration, send func(due time.Time)) int {
	every := time.Duration(float64(time.Second) / rate)
	start := time.Now()
	var wg sync.WaitGroup
	defer wg.Wait()
	for n := 0; ; n++ {
		due := start.Add(time.Duration(n) * every)
		if due.Sub(start) >= d {
			return n
		}
		select {
		case <-time.After(time.Until(due)):
		case <-ctx.Done():
			return n
		}
		wg.Go(func() { send(due) })
	}
}
