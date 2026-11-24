package book

import (
	"errors"

	"github.com/fiston-user/bourse/internal/market"
)

var (
	ErrWouldTake  = errors.New("a post-only order would trade on arrival")
	ErrCannotFill = errors.New("a fill-or-kill order can't be filled in full")
)

// Trade is one fill between a resting order, the maker, and an incoming one, the taker.
type Trade struct {
	Maker, Taker uint64
	Price        market.Price
	Qty          market.Qty

	// Who placed the maker, so the trade can be settled without asking the book.
	MakerAccount, MakerClientID string
}

// Result is what happened to an incoming order.
type Result struct {
	Trades    []Trade
	Rested    market.Qty // left waiting in the book
	Cancelled market.Qty // thrown away: IOC and market leftovers
	// Resting orders of the same account, as they were, cancelled to prevent a self-trade.
	Removed []market.Order
}

// Submit matches an incoming order against the book, and rests what's left if it may.
// The trades in its Result are only valid until the next call, so copy them to keep them.
func (b *Book) Submit(o market.Order) (Result, error) {
	if _, ok := b.orders[o.ID]; ok {
		return Result{}, ErrDuplicateID
	}
	opp := b.sideOf(o.Side.Opposite())
	if best := opp.best(); o.PostOnly && best != nil && o.CanTake(best.price) {
		return Result{}, ErrWouldTake
	}
	if o.TIF == market.FOK && b.available(o, opp) < o.Qty {
		return Result{}, ErrCannotFill
	}

	res := Result{Trades: b.trades[:0]}
	left := o.Qty
	for left > 0 {
		l := opp.best()
		if l == nil || !o.CanTake(l.price) {
			break
		}
		maker := l.head
		if maker.order.Account == o.Account {
			res.Removed = append(res.Removed, maker.order)
			b.remove(maker)
			continue
		}
		qty := min(left, maker.order.Qty)
		res.Trades = append(res.Trades, Trade{Maker: maker.order.ID, Taker: o.ID, Price: l.price, Qty: qty,
			MakerAccount: maker.order.Account, MakerClientID: maker.order.ClientID})
		b.fill(maker, qty)
		left -= qty
	}

	if left > 0 && o.Type == market.Limit && o.TIF == market.GTC {
		o.Qty = left
		b.rest(o)
		res.Rested = left
	} else {
		res.Cancelled = left
	}
	b.trades = res.Trades
	return res, nil
}

// fill takes qty from a resting order, and removes it once nothing is left.
func (b *Book) fill(n *node, qty market.Qty) {
	n.order.Qty -= qty
	n.level.qty -= qty
	if n.order.Qty == 0 {
		b.remove(n)
	}
}

// available is how much o could fill right now, not counting its own account's orders.
func (b *Book) available(o market.Order, opp *side) market.Qty {
	var total market.Qty
	for i := len(opp.levels) - 1; i >= 0 && total < o.Qty; i-- {
		l := opp.levels[i]
		if !o.CanTake(l.price) {
			break
		}
		for n := l.head; n != nil; n = n.next {
			if n.order.Account != o.Account {
				total += n.order.Qty
			}
		}
	}
	return total
}
