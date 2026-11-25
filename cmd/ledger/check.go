package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fiston-user/bourse/internal/ledger"
	"github.com/fiston-user/bourse/internal/money"
)

// balance is how much of an asset an account has.
type balance struct {
	Account, Asset string
	Amount         int64
}

// check proves the books: in every asset, all the postings add up to zero, and once the accounts have
// read the same events, every trader owns there exactly what the ledger says. It only reads.
func check(ctx context.Context, db *pgxpool.Pool, accountsURL string) error {
	var txs, fees int64
	err := db.QueryRow(ctx, `SELECT (SELECT count(*) FROM transactions),
		(SELECT coalesce(sum(amount), 0) FROM postings WHERE account = $1)`, ledger.Fees).Scan(&txs, &fees)
	if err != nil {
		return err
	}
	fmt.Printf("%d transactions, and %s USD in fees\n", txs, money.Format(ledger.Cash, fees))

	rows, _ := db.Query(ctx, `SELECT '', asset, sum(amount) FROM postings
		GROUP BY asset HAVING sum(amount) <> 0`)
	unbalanced, err := pgx.CollectRows(rows, pgx.RowToStructByPos[balance])
	if err != nil {
		return err
	}
	report("every asset adds up to zero", len(unbalanced) == 0)
	for _, b := range unbalanced {
		fmt.Printf("  %s adds up to %s\n", b.Asset, money.Format(b.Asset, b.Amount))
	}

	accounts, err := pgxpool.New(ctx, accountsURL)
	if err != nil {
		return err
	}
	defer accounts.Close()
	ours, err := applied(ctx, db)
	if err != nil {
		return err
	}
	theirs, err := applied(ctx, accounts)
	if err != nil {
		return err
	}
	if !maps.Equal(ours, theirs) {
		return errors.New("the ledger and the accounts haven't read the same events yet: try again")
	}

	rows, _ = db.Query(ctx, `SELECT account, asset, sum(amount) FROM postings
		WHERE account NOT IN ($1, $2) GROUP BY account, asset`, ledger.Bank, ledger.Fees)
	booked, err := pgx.CollectRows(rows, pgx.RowToStructByPos[balance])
	if err != nil {
		return err
	}
	rows, _ = accounts.Query(ctx, `SELECT account, asset, available + held FROM balances`)
	owned, err := pgx.CollectRows(rows, pgx.RowToStructByPos[balance])
	if err != nil {
		return err
	}
	diff := map[balance]int64{} // by account and asset, with no amount
	for _, b := range booked {
		diff[balance{b.Account, b.Asset, 0}] += b.Amount
	}
	for _, b := range owned {
		diff[balance{b.Account, b.Asset, 0}] -= b.Amount
	}
	maps.DeleteFunc(diff, func(_ balance, d int64) bool { return d == 0 })
	report(fmt.Sprintf("%d trader balances, the same in the accounts", len(booked)), len(diff) == 0)
	for _, b := range slices.SortedFunc(maps.Keys(diff), byName) {
		fmt.Printf("  %s %s is off by %s\n", b.Account, b.Asset, money.Format(b.Asset, diff[b]))
	}
	if len(unbalanced) > 0 || len(diff) > 0 {
		return errors.New("the books are off")
	}
	return nil
}

func report(what string, ok bool) {
	fmt.Printf("%-60s %s\n", what, map[bool]string{true: "ok", false: "OFF"}[ok])
}

// applied is how far a database got in each stream.
func applied(ctx context.Context, db *pgxpool.Pool) (map[string]int64, error) {
	rows, _ := db.Query(ctx, `SELECT stream, seq FROM applied`)
	at := map[string]int64{}
	var stream string
	var seq int64
	_, err := pgx.ForEachRow(rows, []any{&stream, &seq}, func() error {
		at[stream] = seq
		return nil
	})
	return at, err
}

func byName(a, b balance) int {
	return cmp.Or(strings.Compare(a.Account, b.Account), strings.Compare(a.Asset, b.Asset))
}
