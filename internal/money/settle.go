package money

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"

	"github.com/jackc/pgx/v5"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/ledger"
)

// Apply settles what the stream says happened, in the store's transaction. A trade gives each side back
// the part of its hold this fill used, and then moves exactly what the ledger books. An order that's
// cancelled or rejected gives back what it still holds. An order with no hold, because it went straight
// to the engine, pays out of what's available.
func (s *Store) Apply(ctx context.Context, tx pgx.Tx, evs []ledger.Event) error {
	c := &changes{holds: map[holdKey]*hold{}, balances: map[key]*change{}}
	for _, ev := range evs {
		if err := c.event(ctx, tx, ev); err != nil {
			return err
		}
	}
	return c.apply(ctx, tx)
}

// event adds up what one event does.
func (c *changes) event(ctx context.Context, tx pgx.Tx, ev ledger.Event) error {
	x := ev.Execution
	switch x.GetKind() {
	case boursev1.EventKind_EVENT_KIND_TRADED:
		taker := holdKey{x.GetAccount(), x.GetClientId()}
		maker := holdKey{x.GetMakerAccount(), x.GetMakerClientId()}
		for _, k := range []holdKey{taker, maker} {
			h, err := c.find(ctx, tx, k)
			if err != nil {
				return err
			}
			if h != nil {
				c.use(h, x.GetQty())
			}
		}
	case boursev1.EventKind_EVENT_KIND_CANCELLED, boursev1.EventKind_EVENT_KIND_REJECTED:
		h, err := c.find(ctx, tx, holdKey{x.GetAccount(), x.GetClientId()})
		if err != nil {
			return err
		}
		if h != nil {
			c.release(h)
		}
	}
	for _, p := range ev.Postings {
		if p.Account != ledger.Bank && p.Account != ledger.Fees {
			c.add(p.Account, p.Asset, p.Amount, 0)
		}
	}
	return nil
}

// Release gives back what an order holds, when it never reached the engine.
func (s *Store) Release(ctx context.Context, account, clientID string) error {
	return pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		c := &changes{holds: map[holdKey]*hold{}, balances: map[key]*change{}}
		h, err := c.find(ctx, tx, holdKey{account, clientID})
		if err != nil {
			return err
		}
		if h == nil {
			return ErrNoHold
		}
		c.release(h)
		return c.apply(ctx, tx)
	})
}

// hold is one order's row in holds, locked for the transaction that read it.
type hold struct {
	holdKey
	symbol, asset string
	price, amount int64
}

// holdKey names an order's hold: its account, and its client order ID.
type holdKey struct{ account, clientID string }

// key names one balance: an account's cash, or its shares of one symbol.
type key struct{ account, asset string }

// change is what a transaction adds to one balance. Either part may be negative.
type change struct{ available, held int64 }

// changes collects what one transaction does, and writes it all at the end. Balances are written in
// one fixed order, so two transactions that touch the same accounts can never deadlock.
type changes struct {
	holds    map[holdKey]*hold
	balances map[key]*change
}

// find reads an order's hold and locks it, or returns nil if the order has none.
func (c *changes) find(ctx context.Context, tx pgx.Tx, k holdKey) (*hold, error) {
	if k.clientID == "" {
		return nil, nil // only orders through the gateway have a client order ID, and a hold
	}
	if h, ok := c.holds[k]; ok {
		return h, nil
	}
	h := &hold{holdKey: k}
	err := tx.QueryRow(ctx, `SELECT symbol, asset, price, amount FROM holds
		WHERE account = $1 AND client_id = $2 FOR UPDATE`, k.account, k.clientID).
		Scan(&h.symbol, &h.asset, &h.price, &h.amount)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c.holds[k] = h
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

// use gives back what a hold kept for qty shares, from held to available: the shares themselves, or
// for a buy, what each share held. The trade's own postings then take what it really cost.
func (c *changes) use(h *hold, qty int64) {
	part := qty
	if h.asset == Cash {
		part = qty * perShare(h.price)
	}
	h.amount -= part
	c.add(h.account, h.asset, part, -part)
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
			_, err = tx.Exec(ctx, `UPDATE holds SET amount = $3 WHERE account = $1 AND client_id = $2`,
				h.account, h.clientID, h.amount)
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
