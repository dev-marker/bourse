package bots

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/fiston-user/bourse/internal/market"
)

// Maker keeps a market maker's quotes resting in one book, and moves them when the fair value moves.
type Maker struct {
	Quoter
	Client *Client
	Fair   *Walk
	Every  time.Duration // how often it quotes again
	Log    *slog.Logger

	start   int64            // the shares it had when it started
	resting map[Quote]uint64 // the order ID of every quote in the book
	ids     int              // client order IDs used
}

// Run quotes until ctx is done, and then takes every quote out of the book.
func (m *Maker) Run(ctx context.Context) error {
	start, err := m.Client.Holding(ctx, m.In.Symbol)
	if err != nil {
		return err
	}
	m.start, m.resting = start, map[Quote]uint64{}
	defer m.cancelAll()
	tick := time.NewTicker(m.Every)
	defer tick.Stop()
	for {
		if err := m.requote(ctx); err != nil && ctx.Err() == nil {
			m.Log.Warn("quoting failed", "symbol", m.In.Symbol, "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

// order is a quote as an order: good till cancelled, and post-only, so it only ever adds to the book.
func (m *Maker) order(q Quote) Order {
	m.ids++
	return Order{ClientOrderID: fmt.Sprintf("mm-%s-%d", started, m.ids), Symbol: m.In.Symbol,
		Side: sideName(q.Side), TimeInForce: "gtc", Price: m.In.FormatPrice(q.Price), Qty: int64(q.Qty),
		PostOnly: true}
}

// cancelAll takes every quote out of the book. Run's ctx is done by then, so it gets a few seconds of
// its own.
func (m *Maker) cancelAll() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n := 0
	for _, id := range m.resting {
		if err := m.Client.Cancel(ctx, m.In.Symbol, id); err == nil {
			n++
		}
	}
	m.Log.Info("quotes cancelled", "symbol", m.In.Symbol, "orders", n)
}

// started is in every client order ID, so a bot that starts again never reuses one.
var started = strconv.FormatInt(time.Now().Unix(), 36)

func sideName(s market.Side) string {
	if s == market.Buy {
		return "buy"
	}
	return "sell"
}

// requote moves the fair value, and changes only the orders that have to change.
func (m *Maker) requote(ctx context.Context) error {
	held, err := m.Client.Holding(ctx, m.In.Symbol)
	if err != nil {
		return err
	}
	want := m.Quotes(m.Fair.Step(), market.Qty(held-m.start))
	stale, missing := diff(m.resting, want)
	// Stale quotes go first, so a new one never meets an old one of ours on the other side.
	for _, q := range stale {
		err := m.Client.Cancel(ctx, m.In.Symbol, m.resting[q])
		if err != nil && !errors.Is(err, ErrGone) {
			return err
		}
		delete(m.resting, q)
	}
	for _, q := range missing {
		p, err := m.Client.Place(ctx, m.order(q))
		if err != nil {
			return err
		}
		if p.Rests() {
			m.resting[q] = p.OrderID
		}
	}
	return nil
}
