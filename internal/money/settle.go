package money

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/fiston-user/bourse/internal/market"
)

// Trade is one fill of an order, as the engine reported it.
type Trade struct {
	Maker uint64 // the resting order it traded with
	Price market.Price
	Qty   market.Qty
}

// Settle records what the engine did with order o, in one transaction. id is the order ID the engine
// gave it, or zero if it never got one. Every trade moves cash and shares between the two accounts, out
// of their holds. Unless what's left of o rests in the book, whatever it still holds is released.
func (s *Store) Settle(ctx context.Context, o market.Order, id uint64, trades []Trade,
	rests bool) error {
	return pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		c := &changes{balances: map[key]*change{}}
		taker, err := c.find(ctx, tx, "account = $1 AND client_id = $2", o.Account, o.ClientID)
		if err != nil {
			return err
		}
		if id != 0 {
			taker.orderID = &id
		}
		for _, t := range trades {
			maker, err := c.find(ctx, tx, "symbol = $1 AND order_id = $2", o.Symbol, t.Maker)
			if err != nil {
				return err
			}
			c.use(taker, t)
			c.use(maker, t)
		}
		if !rests {
			c.release(taker)
		}
		return c.apply(ctx, tx)
	})
}

// Release gives back what a resting order still holds, once it has left the book.
func (s *Store) Release(ctx context.Context, symbol string, id uint64) error {
	return pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		c := &changes{balances: map[key]*change{}}
		h, err := c.find(ctx, tx, "symbol = $1 AND order_id = $2", symbol, id)
		if err != nil {
			return err
		}
		c.release(h)
		return c.apply(ctx, tx)
	})
}

// hold is one order's row in holds, locked for the transaction that read it.
type hold struct {
	account, clientID, symbol, asset string
	orderID                          *uint64
	price, amount                    int64
}

// key names one balance: an account's cash, or its shares of one symbol.
type key struct{ account, asset string }

// change is what a transaction adds to one balance. Either part may be negative.
type change struct{ available, held int64 }

// changes collects what one transaction does, and writes it all at the end. Balances are written in
// one fixed order, so two transactions that touch the same accounts can never deadlock.
type changes struct {
	holds    []*hold
	balances map[key]*change
}

// find reads one hold and locks it.
func (c *changes) find(ctx context.Context, tx pgx.Tx, where string, args ...any) (*hold, error) {
	h := &hold{}
	err := tx.QueryRow(ctx, `SELECT account, client_id, symbol, asset, order_id, price, amount
		FROM holds WHERE `+where+` FOR UPDATE`, args...).
		Scan(&h.account, &h.clientID, &h.symbol, &h.asset, &h.orderID, &h.price, &h.amount)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoHold
	}
	if err != nil {
		return nil, err
	}
	c.holds = append(c.holds, h)
	return h, nil
}

// add changes one balance, when the transaction ends.
func (c *changes) add(account, asset string, available, held int64) {
	k := key{account, asset}
	if c.balances[k] == nil {
		c.balances[k] = &change{}
	}
	c.balances[k].available += available
	c.balances[k].held += held
}

// use pays for one trade out of a hold. A buyer's hold pays the seller the trade's price, gets back what
// it held above that price, and gets the shares. A seller's hold hands over the shares, for the cash.
func (c *changes) use(h *hold, t Trade) {
	qty, paid := int64(t.Qty), int64(t.Price)*int64(t.Qty)
	if h.asset == Cash {
		held := h.price * qty // what this many shares held, at the order's own limit
		h.amount -= held
		c.add(h.account, Cash, held-paid, -held)
		c.add(h.account, h.symbol, qty, 0)
		return
	}
	h.amount -= qty
	c.add(h.account, h.symbol, 0, -qty)
	c.add(h.account, Cash, paid, 0)
}

// release gives back everything a hold still holds, and the hold goes.
func (c *changes) release(h *hold) {
	c.add(h.account, h.asset, h.amount, -h.amount)
	h.amount = 0
}

// apply writes the holds, a hold with nothing left is deleted, and then the balances, sorted by account
// and asset. A balance that would go below zero fails its CHECK, and the whole transaction with it.
func (c *changes) apply(ctx context.Context, tx pgx.Tx) error {
	for _, h := range c.holds {
		var err error
		if h.amount == 0 {
			_, err = tx.Exec(ctx, `DELETE FROM holds WHERE account = $1 AND client_id = $2`,
				h.account, h.clientID)
		} else {
			_, err = tx.Exec(ctx, `UPDATE holds SET amount = $3, order_id = $4
				WHERE account = $1 AND client_id = $2`, h.account, h.clientID, h.amount, h.orderID)
		}
		if err != nil {
			return err
		}
	}
	keys := slices.SortedFunc(maps.Keys(c.balances), func(a, b key) int {
		return cmp.Or(cmp.Compare(a.account, b.account), cmp.Compare(a.asset, b.asset))
	})
	for _, k := range keys {
		ch := c.balances[k]
		if err := adjust(ctx, tx, k.account, k.asset, ch.available, ch.held); err != nil {
			return err
		}
	}
	return nil
}
