package money_test

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/ledger"
	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/money"
	"github.com/fiston-user/bourse/internal/pgtest"
)

// open gives the test an accounts database of its own.
func open(t *testing.T) (*money.Store, *pgxpool.Pool) {
	db := pgtest.Open(t, "accounts", money.Migrations)
	return money.NewStore(db), db
}

// apply runs events through the store, the way the stream does.
func apply(t *testing.T, s *money.Store, db *pgxpool.Pool, evs ...ledger.Event) {
	t.Helper()
	if err := ledger.ApplyOnce(t.Context(), db, evs, s.Apply); err != nil {
		t.Fatal(err)
	}
}

// deposit is the seq-th deposit.
func deposit(seq uint64, account, asset string, amount int64) ledger.Event {
	d := &boursev1.Deposit{Account: account, Asset: asset, Amount: amount}
	return ledger.FromDeposit(seq, time.Now(), d)
}

// balance is what an account has of one asset.
func balance(t *testing.T, s *money.Store, account, asset string) money.Balance {
	t.Helper()
	all, err := s.Balances(t.Context(), account)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range all {
		if b.Asset == asset {
			return b
		}
	}
	return money.Balance{Asset: asset}
}

// usd shows cents as dollars.
func usd(cents int64) string {
	return fmt.Sprintf("%d.%02d", cents/100, cents%100)
}

// TestDoubleSpend gives alice 1,000.00 and sends twenty buys of 90.00 at once. Fee included, each holds
// 90.05, so eleven of them fit: exactly eleven may get a hold, and alice must still have exactly 1,000.00.
func TestDoubleSpend(t *testing.T) {
	s, db := open(t)
	apply(t, s, db, deposit(1, "alice", money.Cash, 1_000_00))
	var holds atomic.Int64
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			o := market.Order{ClientID: fmt.Sprint("b-", i), Account: "alice", Symbol: "NOVA",
				Side: market.Buy, Type: market.Limit, TIF: market.GTC, Price: 90_00, Qty: 1}
			err := s.Hold(t.Context(), o)
			if err == nil {
				holds.Add(1)
			} else if !errors.Is(err, money.ErrInsufficient) {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	b := balance(t, s, "alice", money.Cash)
	if holds.Load() != 11 || b.Available+b.Held != 1_000_00 {
		t.Fatalf("%d holds, want 11. Alice has %s available and %s held, out of 1000.00",
			holds.Load(), usd(b.Available), usd(b.Held))
	}
}

// TestStream follows one trade, the way the stream tells it. Alice's buy of 300 GLBX at 101.25 rests,
// bob sells her 100, the same trade arrives twice, and then she cancels the rest. Every step must move
// exactly what it should, fees included, and no more.
func TestStream(t *testing.T) {
	s, db := open(t)
	apply(t, s, db, deposit(1, "alice", money.Cash, 100_000_00), deposit(2, "bob", "GLBX", 1000))
	buy := market.Order{ClientID: "a-1", Account: "alice", Symbol: "GLBX", Side: market.Buy,
		Type: market.Limit, TIF: market.GTC, Price: 101_25, Qty: 300}
	sell := market.Order{ClientID: "b-1", Account: "bob", Symbol: "GLBX", Side: market.Sell,
		Type: market.Limit, TIF: market.GTC, Price: 101_00, Qty: 100}
	glbx := func(seq uint64, kind boursev1.EventKind, o market.Order, qty int64) ledger.Event {
		return ledger.FromExecution(&boursev1.Execution{Symbol: "GLBX", Seq: seq, Kind: kind,
			Account: o.Account, ClientId: o.ClientID, Side: boursev1.Side(o.Side), Price: 101_25, Qty: qty})
	}
	trade := glbx(4, boursev1.EventKind_EVENT_KIND_TRADED, sell, 100)
	trade.Execution.MakerAccount, trade.Execution.MakerClientId = "alice", "a-1"
	trade = ledger.FromExecution(trade.Execution) // its postings, with both sides

	steps := []struct {
		name string
		do   func()
		want map[string]money.Balance // by account and asset
	}{
		{"alice's buy rests", func() {
			if err := s.Hold(t.Context(), buy); err != nil {
				t.Fatal(err)
			}
			apply(t, s, db, glbx(1, boursev1.EventKind_EVENT_KIND_ACCEPTED, buy, 300),
				glbx(2, boursev1.EventKind_EVENT_KIND_RESTED, buy, 300))
		}, map[string]money.Balance{"alice USD": {"USD", 69_607_00, 30_393_00}}},
		{"bob sells her 100", func() {
			if err := s.Hold(t.Context(), sell); err != nil {
				t.Fatal(err)
			}
			apply(t, s, db, glbx(3, boursev1.EventKind_EVENT_KIND_ACCEPTED, sell, 100), trade)
		}, map[string]money.Balance{
			"alice USD": {"USD", 69_611_98, 20_262_00}, "alice GLBX": {"GLBX", 100, 0},
			"bob USD": {"USD", 10_119_93, 0}, "bob GLBX": {"GLBX", 900, 0}}},
		{"the same trade again", func() { apply(t, s, db, trade) }, map[string]money.Balance{
			"alice USD": {"USD", 69_611_98, 20_262_00}, "bob USD": {"USD", 10_119_93, 0}}},
		{"alice cancels the rest", func() {
			apply(t, s, db, glbx(5, boursev1.EventKind_EVENT_KIND_CANCELLED, buy, 200))
		}, map[string]money.Balance{"alice USD": {"USD", 89_873_98, 0}}},
	}
	for _, step := range steps {
		step.do()
		for key, want := range step.want {
			account, asset, _ := strings.Cut(key, " ")
			if got := balance(t, s, account, asset); got != want {
				t.Fatalf("%s: %s has %+v, want %+v", step.name, key, got, want)
			}
		}
	}
}
