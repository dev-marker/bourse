// Package loadgen puts the exchange under load: orders at a fixed rate, and how long each one took.
package loadgen

import (
	"math"
	"math/bits"
	"time"
)

// sub is how many buckets each power of two is cut into. A bucket is never wider than a sixteenth of
// the values in it, so a quantile is never more than about six percent too high.
const sub = 16

// Histogram counts latencies in buckets that grow with the value, from a nanosecond to centuries,
// in 8 KB. It's not safe for concurrent use.
type Histogram struct {
	counts [64 * sub]uint64
	n      uint64
	max    time.Duration
}

// bucket is where a value goes.
func bucket(v uint64) int {
	if v < sub {
		return int(v)
	}
	shift := bits.Len64(v) - 5
	return shift*sub + int(v>>shift)
}

// top is the biggest value that goes in bucket i.
func top(i int) uint64 {
	if i < sub {
		return uint64(i)
	}
	shift := i/sub - 1
	return uint64(i%sub+sub+1)<<shift - 1
}

// Record counts one latency.
func (h *Histogram) Record(d time.Duration) {
	h.counts[bucket(uint64(max(d, 0)))]++
	h.n++
	h.max = max(h.max, d)
}

// Quantile is the latency a fraction q of the samples are at or under.
func (h *Histogram) Quantile(q float64) time.Duration {
	rank := max(1, uint64(math.Ceil(q*float64(h.n))))
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen >= rank {
			return min(time.Duration(top(i)), h.max)
		}
	}
	return h.max
}

// Count is how many latencies were recorded, and Max the biggest.
func (h *Histogram) Count() uint64      { return h.n }
func (h *Histogram) Max() time.Duration { return h.max }
