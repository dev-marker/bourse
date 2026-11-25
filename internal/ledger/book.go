package ledger

import (
	"context"
	"embed"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Migrations create the ledger's tables.
//
//go:embed migrations/*.sql
var Migrations embed.FS

// Book writes every event that moves something into the ledger, as a transaction with its postings.
// All of them go to Postgres in one round trip.
func Book(ctx context.Context, tx pgx.Tx, evs []Event) error {
	b := &pgx.Batch{}
	for _, ev := range evs {
		if len(ev.Postings) == 0 {
			continue
		}
		if err := balanced(ev.Postings); err != nil {
			return fmt.Errorf("%s seq %d: %w", ev.Stream, ev.Seq, err)
		}
		var accounts, assets []string
		var amounts []int64
		for _, p := range ev.Postings {
			accounts, assets, amounts = append(accounts, p.Account), append(assets, p.Asset),
				append(amounts, p.Amount)
		}
		b.Queue(`WITH t AS (INSERT INTO transactions (stream, seq, time) VALUES ($1, $2, $3) RETURNING id)
			INSERT INTO postings (transaction_id, account, asset, amount)
			SELECT t.id, p.* FROM t, unnest($4::text[], $5::text[], $6::bigint[]) AS p`,
			ev.Stream, ev.Seq, ev.Time, accounts, assets, amounts)
	}
	return tx.SendBatch(ctx, b).Close()
}

// balanced checks that the postings add up to zero, in every asset.
func balanced(ps []Posting) error {
	sum := map[string]int64{}
	for _, p := range ps {
		sum[p.Asset] += p.Amount
	}
	for asset, n := range sum {
		if n != 0 {
			return fmt.Errorf("the postings in %s add up to %d, not zero", asset, n)
		}
	}
	return nil
}
