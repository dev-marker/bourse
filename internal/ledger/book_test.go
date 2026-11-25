package ledger_test

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/ledger"
	"github.com/fiston-user/bourse/internal/pgtest"
)

// trade is GLBX's seq-th execution: bob sells alice 100 at 101.25.
func trade(seq uint64) ledger.Event {
	return ledger.FromExecution(&boursev1.Execution{Symbol: "GLBX", Seq: seq,
		Kind: boursev1.EventKind_EVENT_KIND_TRADED, Account: "bob", Side: boursev1.Side_SIDE_SELL,
		Price: 101_25, Qty: 100, MakerAccount: "alice"})
}

// count says how many transactions and postings the ledger has.
func count(t *testing.T, db *pgxpool.Pool) (txs, postings int) {
	t.Helper()
	err := db.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM transactions),
		(SELECT count(*) FROM postings)`).Scan(&txs, &postings)
	if err != nil {
		t.Fatal(err)
	}
	return txs, postings
}

// TestApplyOnce books the same trade twice, the way a consumer restarted at the wrong moment would read
// it. The ledger must have it once. Then an event that skips a seq must be refused.
func TestApplyOnce(t *testing.T) {
	db := pgtest.Open(t, "ledger", ledger.Migrations)
	for range 2 {
		err := ledger.ApplyOnce(t.Context(), db, []ledger.Event{trade(1), trade(2)}, ledger.Book)
		if err != nil {
			t.Fatal(err)
		}
	}
	if txs, postings := count(t, db); txs != 2 || postings != 10 {
		t.Fatalf("%d transactions and %d postings, want 2 and 10", txs, postings)
	}
	err := ledger.ApplyOnce(t.Context(), db, []ledger.Event{trade(4)}, ledger.Book)
	if err == nil || !strings.Contains(err.Error(), "jumps from seq 2 to 4") {
		t.Fatalf("seq 4 after seq 2: %v, want a jump", err)
	}
}
