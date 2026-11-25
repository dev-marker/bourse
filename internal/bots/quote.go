// Package bots trades on the exchange the way its customers do: market makers that quote both sides,
// and takers that cross the book. They go through the gateway, signed and rate-limited, like anyone.
package bots

import (
	"math/rand/v2"

	"github.com/fiston-user/bourse/internal/market"
)

// Walk is a fair value that wanders: every step, a tick up, a tick down, or nowhere.
type Walk struct {
	Price market.Price
	Tick  market.Price
	Rand  *rand.Rand
}

// Step moves the fair value, and returns it. It never goes below one tick.
func (w *Walk) Step() market.Price {
	w.Price = max(w.Tick, w.Price+market.Price(w.Rand.IntN(3)-1)*w.Tick)
	return w.Price
}

// Quoter decides where a market maker's orders rest.
type Quoter struct {
	In     market.Instrument
	Levels int        // prices on each side
	Spread int        // ticks from the fair value to the best bid, and to the best ask
	Size   market.Qty // shares at each price
	Limit  market.Qty // the most it may hold, long or short
	Skew   int        // ticks its quotes lean when it holds the limit
}

// Quote is an order the maker wants resting in the book.
type Quote struct {
	Side  market.Side
	Price market.Price
	Qty   market.Qty
}

// Quotes are the orders to rest around fair, for a maker that holds pos shares, short when negative.
func (q Quoter) Quotes(fair market.Price, pos market.Qty) []Quote {
	tick := q.In.TickSize
	// Long, it leans down: its asks come closer, so it sells, and its bids move away.
	mid := fair - market.Price(int64(q.Skew)*int64(pos)/int64(q.Limit))*tick
	var out []Quote
	for i := range q.Levels {
		gap := market.Price(q.Spread+i) * tick
		if pos < q.Limit && mid > gap {
			out = append(out, Quote{market.Buy, mid - gap, q.Size})
		}
		if pos > -q.Limit {
			out = append(out, Quote{market.Sell, mid + gap, q.Size})
		}
	}
	return out
}

// diff compares what rests with what's wanted. A resting quote nobody wants is stale, and a wanted
// one that isn't resting is missing. The rest stay where they are, and keep their place in the queue.
func diff(resting map[Quote]uint64, want []Quote) (stale, missing []Quote) {
	keep := map[Quote]bool{}
	for _, q := range want {
		keep[q] = true
		if _, ok := resting[q]; !ok {
			missing = append(missing, q)
		}
	}
	for q := range resting {
		if !keep[q] {
			stale = append(stale, q)
		}
	}
	return stale, missing
}
