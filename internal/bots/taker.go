package bots

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/fiston-user/bourse/internal/market"
)

// Taker crosses the book at random moments: a buy or a sell of a random size, priced a few ticks
// through the best price, and IOC, so it takes what's there and never rests.
type Taker struct {
	In         market.Instrument
	Client     *Client
	MarketData string  // where it reads the book
	Rate       float64 // orders a second, on average
	Lots       int     // the most lots in one order
	Through    int     // how many ticks past the best price it will go
	Rand       *rand.Rand
	Log        *slog.Logger

	ids int // client order IDs used
}

// Run takes until ctx is done.
func (t *Taker) Run(ctx context.Context) error {
	var wait time.Duration
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		// The gaps are random and exponential: a few orders close together, then a quiet moment.
		wait = time.Duration(t.Rand.ExpFloat64() / t.Rate * float64(time.Second))
		side := market.Side(1 + t.Rand.IntN(2))
		qty := t.In.LotSize * market.Qty(1+t.Rand.IntN(t.Lots))
		if _, err := t.Take(ctx, side, qty); err != nil && ctx.Err() == nil {
			t.Log.Warn("taking failed", "symbol", t.In.Symbol, "err", err)
			wait = time.Second // not again right away
		}
	}
}

// Take sends one IOC order through the best price on the other side.
func (t *Taker) Take(ctx context.Context, side market.Side, qty market.Qty) (Placed, error) {
	// A buy takes from the asks, and a sell from the bids.
	best, err := Best(ctx, t.MarketData, t.In.Symbol, side.Opposite())
	if err != nil || best == 0 {
		return Placed{}, err // nothing to take
	}
	through := market.Price(t.Through) * t.In.TickSize
	price := best + through
	if side == market.Sell {
		price = max(t.In.TickSize, best-through)
	}
	t.ids++
	return t.Client.Place(ctx, Order{ClientOrderID: fmt.Sprintf("tk-%s-%d", started, t.ids),
		Symbol: t.In.Symbol, Side: sideName(side), TimeInForce: "ioc", Price: t.In.FormatPrice(price),
		Qty: int64(qty)})
}
