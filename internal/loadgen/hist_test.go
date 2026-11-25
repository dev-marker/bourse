package loadgen

import (
	"testing"
	"time"
)

// TestQuantiles records every latency from 1 to 10,000 microseconds once, so we know every answer.
// Each quantile may be a little too high, never too low, and never by more than a sixteenth.
func TestQuantiles(t *testing.T) {
	var h Histogram
	for i := 1; i <= 10_000; i++ {
		h.Record(time.Duration(i) * time.Microsecond)
	}
	for _, tc := range []struct {
		q    float64
		want time.Duration
	}{
		{0.5, 5 * time.Millisecond},
		{0.9, 9 * time.Millisecond},
		{0.99, 9900 * time.Microsecond},
		{0.999, 9990 * time.Microsecond},
		{1, 10 * time.Millisecond},
	} {
		got := h.Quantile(tc.q)
		if got < tc.want || got > tc.want+tc.want/sub {
			t.Errorf("Quantile(%v) = %v, want %v", tc.q, got, tc.want)
		}
	}
	if h.Max() != 10*time.Millisecond || h.Count() != 10_000 {
		t.Errorf("Max %v, Count %d", h.Max(), h.Count())
	}
}

// TestBuckets checks that every value lands in a bucket that holds it.
func TestBuckets(t *testing.T) {
	for v := uint64(0); v < 1<<20; v += 1 + v/100 {
		i := bucket(v)
		if v > top(i) || i > 0 && v <= top(i-1) {
			t.Fatalf("%d went to bucket %d, which holds %d to %d", v, i, top(i-1)+1, top(i))
		}
	}
}
