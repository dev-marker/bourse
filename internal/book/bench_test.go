package book_test

import (
	"math/rand/v2"
	"testing"

	"github.com/fiston-user/bourse/internal/market"
)

// event is one message a busy book receives.
type event struct {
	op    byte // 'q' replaces a quote, 'c' cancels one, 't' takes
	slot  int  // which of our resting quotes it replaces or cancels
	order market.Order
}

// flow is a trading session in miniature, the same on every run.
func flow() []event {
	r := rand.New(rand.NewPCG(3, 4))
	evs := make([]event, 64_000)
	for i := range evs {
		e := &evs[i]
		e.slot = r.IntN(10_000)
		side := market.Side(1 + r.IntN(2))
		switch n := r.IntN(100); {
		case n < 60:
			price := market.Price(9999 - r.IntN(20))
			if side == market.Sell {
				price = market.Price(10001 + r.IntN(20))
			}
			e.op, e.order = 'q', limit(0, side, price, market.Qty(1+r.IntN(500)))
		case n < 90:
			e.op = 'c'
		default:
			e.op = 't'
			e.order = market.Order{Account: "taker", Side: side, Type: market.Market, TIF: market.IOC,
				Qty: market.Qty(1 + r.IntN(1000))}
		}
	}
	return evs
}

// BenchmarkFlow plays the session against a deep book, a thousand messages per op.
func BenchmarkFlow(b *testing.B) {
	bk, evs := deepBook(), flow()
	quotes := make([]uint64, 10_000)
	for i := range quotes {
		quotes[i] = uint64(i + 1)
	}
	id, next := uint64(1_000_000), 0
	for b.Loop() {
		for _, e := range evs[next : next+1000] {
			id++
			o := e.order
			o.ID = id
			switch e.op {
			case 'q':
				bk.Cancel(quotes[e.slot])
				quotes[e.slot] = id
				bk.Submit(o)
			case 'c':
				bk.Cancel(quotes[e.slot])
			case 't':
				bk.Submit(o)
			}
		}
		next = (next + 1000) % len(evs)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*1000), "ns/msg")
}
