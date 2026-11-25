package ledger

import (
	"context"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/testutil"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/pgtest"
)

// TestPoison: the second of three deposits can't be applied, the way an unfunded trade can't. The
// other two are booked, the second is skipped and counted, and all three are applied, so the stream
// never reads it again.
func TestPoison(t *testing.T) {
	db := pgtest.Open(t, "ledger", Migrations)
	var evs []Event
	for seq := range uint64(3) {
		d := &boursev1.Deposit{Account: "alice", Asset: Cash, Amount: 100}
		evs = append(evs, FromDeposit(seq+1, time.Now(), d))
	}
	apply := func(ctx context.Context, tx pgx.Tx, evs []Event) error {
		if slices.ContainsFunc(evs, func(ev Event) bool { return ev.Seq == 2 }) {
			return &pgconn.PgError{Code: "23514"} // what a balance below zero gives
		}
		return Book(ctx, tx, evs)
	}
	before := testutil.ToFloat64(poison)
	if err := applyOrSkip(t.Context(), db, evs, apply, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	var booked, seq int
	err := db.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM transactions),
		(SELECT seq FROM applied WHERE stream = $1)`, Deposits).Scan(&booked, &seq)
	if err != nil {
		t.Fatal(err)
	}
	if booked != 2 || seq != 3 || testutil.ToFloat64(poison)-before != 1 {
		t.Fatalf("%d booked, applied up to %d, %v skipped: want 2, 3 and 1", booked, seq,
			testutil.ToFloat64(poison)-before)
	}
}
