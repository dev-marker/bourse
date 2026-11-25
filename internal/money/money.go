// Package money keeps what every account owns, and the holds that open orders put on it.
package money

import (
	"context"
	"embed"
	"errors"
	"math"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fiston-user/bourse/internal/ledger"
	"github.com/fiston-user/bourse/internal/market"
)

// Migrations create the tables the store needs.
//
//go:embed migrations/*.sql
var Migrations embed.FS

// Cash is the asset cash is kept in: US dollars, in cents. Shares are kept under their symbol.
const Cash = ledger.Cash

var (
	ErrInsufficient = errors.New("not enough available to place this order")
	ErrNoPrice      = errors.New("a market buy has no price to hold cash for: send a limit order with ioc")
	ErrTooBig       = errors.New("price times quantity is too big")
	ErrHeld         = errors.New("this client_order_id already has an order")
	ErrNoHold       = errors.New("no hold for this order")
)

// cents reads and writes amounts of cash: two decimals, and any cent.
var cents = market.Instrument{Symbol: Cash, Decimals: 2, TickSize: 1}

// Parse reads an amount of an asset: cash like "100000.00", or a number of shares.
func Parse(asset, s string) (int64, error) {
	if asset == Cash {
		p, err := cents.ParsePrice(s)
		return int64(p), err
	}
	return strconv.ParseInt(s, 10, 64)
}

// Format shows an amount of an asset the way a person reads it.
func Format(asset string, amount int64) string {
	if asset == Cash {
		return cents.FormatPrice(market.Price(amount))
	}
	return strconv.FormatInt(amount, 10)
}

// Store keeps balances and holds in Postgres.
type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// Cost is what an order puts on hold. A sell holds its shares. A buy holds what each share can cost,
// times its quantity, in cash, which is cents, because every instrument we trade is priced in cents.
func Cost(o market.Order) (asset string, amount int64, err error) {
	if o.Side == market.Sell {
		return o.Symbol, int64(o.Qty), nil
	}
	if o.Type == market.Market {
		return "", 0, ErrNoPrice
	}
	per := perShare(int64(o.Price))
	if int64(o.Qty) > math.MaxInt64/per {
		return "", 0, ErrTooBig
	}
	return Cash, per * int64(o.Qty), nil
}

// perShare is what a buy at this price holds for each share: the price, and a taker's fee on that one
// share, rounded up. However the order fills, the fees on its trades can never add up to more.
func perShare(price int64) int64 {
	return price + ledger.Fee(price, ledger.TakerBps)
}

// Hold moves what o costs from available to held, and remembers it for o. If the account doesn't have
// enough, nothing changes, and it's ErrInsufficient.
func (s *Store) Hold(ctx context.Context, o market.Order) error {
	asset, amount, err := Cost(o)
	if err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE balances SET available = available - $3, held = held + $3
			WHERE account = $1 AND asset = $2 AND available >= $3`, o.Account, asset, amount)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrInsufficient
		}
		_, err = tx.Exec(ctx, `INSERT INTO holds (account, client_id, symbol, asset, price, amount)
			VALUES ($1, $2, $3, $4, $5, $6)`, o.Account, o.ClientID, o.Symbol, asset, o.Price, amount)
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "23505" { // unique_violation
			return ErrHeld
		}
		return err
	})
}

// Balance is what an account owns of one asset.
type Balance struct {
	Asset     string
	Available int64
	Held      int64
}

// Balances returns everything an account owns, by asset.
func (s *Store) Balances(ctx context.Context, account string) ([]Balance, error) {
	rows, _ := s.db.Query(ctx,
		`SELECT asset, available, held FROM balances WHERE account = $1 ORDER BY asset`, account)
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Balance])
}

// adjust changes one balance, and starts it at zero if the account never had that asset.
func adjust(ctx context.Context, tx pgx.Tx, account, asset string, available, held int64) error {
	_, err := tx.Exec(ctx, `INSERT INTO balances (account, asset) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		account, asset)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE balances SET available = available + $3, held = held + $4
		WHERE account = $1 AND asset = $2`, account, asset, available, held)
	return err
}
