package money_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/migrate"
	"github.com/fiston-user/bourse/internal/money"
)

// open connects to the accounts database in the Compose stack, and brings its schema up to date.
// Without Postgres, the test is skipped, and says why.
func open(t *testing.T) *money.Store {
	url := cmp.Or(os.Getenv("DATABASE_URL"), "postgres://bourse:bourse@localhost:5432/accounts")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, url)
	if err == nil {
		err = db.Ping(ctx)
	}
	if err != nil {
		t.Skipf("Postgres isn't running (make up starts it): %v", err)
	}
	t.Cleanup(db.Close)
	if err := migrate.Up(ctx, db, money.Migrations); err != nil {
		t.Fatal(err)
	}
	return money.NewStore(db)
}

// account is a name no earlier run of the tests has used, so every run starts from nothing.
func account(name string) string {
	return fmt.Sprintf("%s-%d", name, time.Now().UnixNano())
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

// TestDoubleSpend gives alice 1,000.00 and sends twenty buys of 90.00 at once. Eleven of them fit, so
// exactly eleven may get a hold, and alice must still have exactly 1,000.00.
func TestDoubleSpend(t *testing.T) {
	s := open(t)
	alice := account("alice")
	if err := s.Fund(t.Context(), alice, money.Cash, 1_000_00); err != nil {
		t.Fatal(err)
	}
	var holds atomic.Int64
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			o := market.Order{ClientID: fmt.Sprint("b-", i), Account: alice, Symbol: "NOVA",
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
	b := balance(t, s, alice, money.Cash)
	if holds.Load() != 11 || b.Available+b.Held != 1_000_00 {
		t.Fatalf("%d holds, want 11. Alice has %s available and %s held, out of 1000.00",
			holds.Load(), usd(b.Available), usd(b.Held))
	}
}

// TestSettle follows one trade. Alice's buy of 300 GLBX at 101.25 rests, bob sells her 100, and then
// she cancels the rest. Every step must move exactly what it should, and no more.
func TestSettle(t *testing.T) {
	s, ctx := open(t), t.Context()
	alice, bob := account("alice"), account("bob")
	id := uint64(time.Now().UnixNano()) // order IDs no earlier run has used
	s.Fund(ctx, alice, money.Cash, 100_000_00)
	s.Fund(ctx, bob, "GLBX", 1000)
	buy := market.Order{ClientID: "a-1", Account: alice, Symbol: "GLBX", Side: market.Buy,
		Type: market.Limit, TIF: market.GTC, Price: 101_25, Qty: 300}
	sell := market.Order{ClientID: "b-1", Account: bob, Symbol: "GLBX", Side: market.Sell,
		Type: market.Limit, TIF: market.GTC, Price: 101_00, Qty: 100}

	steps := []struct {
		name string
		do   func() error
		want map[string]money.Balance // by account and asset
	}{
		{"alice's buy rests", func() error {
			if err := s.Hold(ctx, buy); err != nil {
				return err
			}
			return s.Settle(ctx, buy, id, nil, true)
		}, map[string]money.Balance{alice + " USD": {"USD", 69_625_00, 30_375_00}}},
		{"bob sells her 100", func() error {
			if err := s.Hold(ctx, sell); err != nil {
				return err
			}
			return s.Settle(ctx, sell, id+1, []money.Trade{{Maker: id, Price: 101_25, Qty: 100}}, false)
		}, map[string]money.Balance{
			alice + " USD": {"USD", 69_625_00, 20_250_00}, alice + " GLBX": {"GLBX", 100, 0},
			bob + " USD": {"USD", 10_125_00, 0}, bob + " GLBX": {"GLBX", 900, 0}}},
		{"alice cancels the rest", func() error { return s.Release(ctx, "GLBX", id) },
			map[string]money.Balance{alice + " USD": {"USD", 89_875_00, 0}}},
	}
	for _, step := range steps {
		if err := step.do(); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		for key, want := range step.want {
			acct, asset, _ := strings.Cut(key, " ")
			if got := balance(t, s, acct, asset); got != want {
				t.Fatalf("%s: %s has %+v, want %+v", step.name, key, got, want)
			}
		}
	}
}
