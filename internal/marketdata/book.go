// Package marketdata turns the engine's executions into what the public may see: every book's depth,
// the trades, and one-minute candles, as a snapshot and then numbered updates.
package marketdata

import (
	"cmp"
	"slices"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/market"
)

// Level is one price in the book, and all the shares resting there.
type Level struct {
	Price market.Price `json:"price"`
	Qty   market.Qty   `json:"qty"`
}

// depth is a book the way the public sees it: how much rests at each price, on each side.
type depth struct {
	bids, asks map[market.Price]market.Qty
}

func newDepth() depth {
	return depth{bids: map[market.Price]market.Qty{}, asks: map[market.Price]market.Qty{}}
}

// side is one side's levels.
func (d *depth) side(s market.Side) map[market.Price]market.Qty {
	if s == market.Buy {
		return d.bids
	}
	return d.asks
}

// levels returns up to n levels of a side, best first: the highest bids, and the lowest asks. The map
// keeps every change cheap, and sorting happens only here, when someone asks.
func (d *depth) levels(s market.Side, n int) []Level {
	out := make([]Level, 0, len(d.side(s)))
	for p, q := range d.side(s) {
		out = append(out, Level{p, q})
	}
	slices.SortFunc(out, func(a, b Level) int {
		if s == market.Buy {
			return cmp.Compare(b.Price, a.Price)
		}
		return cmp.Compare(a.Price, b.Price)
	})
	return out[:min(n, len(out))]
}

// apply does to the depth what one execution did to the engine's book.
func (d *depth) apply(x *boursev1.Execution) (market.Side, Level, bool) {
	side, qty := market.Side(x.GetSide()), market.Qty(x.GetQty())
	switch x.GetKind() {
	case boursev1.EventKind_EVENT_KIND_RESTED:
		// what's left of the order now waits at its price
	case boursev1.EventKind_EVENT_KIND_TRADED:
		side, qty = side.Opposite(), -qty // the maker, on the other side, lost what traded
	case boursev1.EventKind_EVENT_KIND_CANCELLED:
		if x.GetPrice() == 0 {
			return 0, Level{}, false // what an IOC or a market order didn't fill never rested
		}
		qty = -qty
	default:
		return 0, Level{}, false // accepted and rejected orders change nothing in the book
	}
	levels := d.side(side)
	p := market.Price(x.GetPrice())
	levels[p] += qty
	if levels[p] == 0 {
		delete(levels, p)
	}
	return side, Level{p, levels[p]}, true
}
