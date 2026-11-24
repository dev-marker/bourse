package engine_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fiston-user/bourse/internal/engine"
	"github.com/fiston-user/bourse/internal/market"
)

var acme = market.Instrument{Symbol: "ACME", Decimals: 2, TickSize: 1, LotSize: 1}

// order is client c's i-th order: buys and sells close to 101.00, so most of them trade.
func order(c, i int) market.Order {
	return market.Order{Symbol: "ACME", Account: fmt.Sprint("client", c), Side: market.Side(1 + (c+i)%2),
		Type: market.Limit, TIF: market.GTC, Price: market.Price(10_095 + (c*7+i)%10), Qty: 100}
}

// start runs an engine for ACME until the test ends, with its journal in a folder of the test's own.
func start(t testing.TB) *engine.Engine {
	e, err := engine.New(t.TempDir(), acme)
	if err != nil {
		t.Fatal(err)
	}
	go e.Run(t.Context())
	return e
}

// TestManyClients has eight clients trading at once, each in a goroutine of its own.
func TestManyClients(t *testing.T) {
	e := start(t)
	reports := make([][]engine.Report, 8)
	var wg sync.WaitGroup
	for c := range reports {
		wg.Go(func() {
			for i := range 250 {
				r, err := e.Submit(t.Context(), order(c, i))
				if err != nil {
					t.Error(err)
					return
				}
				reports[c] = append(reports[c], r)
			}
		})
	}
	wg.Wait()

	// Every event has a number of its own, an order's events follow each other, and none is missing.
	seen, last := map[uint64]bool{}, uint64(0)
	for _, r := range slices.Concat(reports...) {
		for i, ev := range r.Events {
			if seen[ev.Seq] || i > 0 && ev.Seq != r.Events[i-1].Seq+1 {
				t.Fatalf("order %d: seq %d is out of place", r.OrderID, ev.Seq)
			}
			seen[ev.Seq], last = true, max(last, ev.Seq)
		}
	}
	if last != uint64(len(seen)) {
		t.Fatalf("%d events, but the last one is seq %d", len(seen), last)
	}
}

// TestStopped sends an order to an engine that has already stopped.
func TestStopped(t *testing.T) {
	e, err := engine.New(t.TempDir(), acme)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		e.Run(ctx)
		close(done)
	}()
	stop()
	<-done
	if _, err := e.Submit(t.Context(), order(0, 0)); err == nil {
		t.Fatal("a stopped engine took an order")
	}
}

// BenchmarkSubmit has ten clients on every core sending orders to one symbol, and measures how long each
// client waits for its answer.
func BenchmarkSubmit(b *testing.B) {
	e := start(b)
	b.SetParallelism(10)
	var clients, waited atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		c := int(clients.Add(1))
		for i := 0; pb.Next(); i++ {
			sent := time.Now()
			e.Submit(b.Context(), order(c, i))
			waited.Add(int64(time.Since(sent)))
		}
	})
	b.ReportMetric(float64(waited.Load())/float64(b.N)/1e6, "ms/order")
}

// TestCancelOwner places an order for client0. Another account's cancel hears it doesn't exist, and only
// then does client0's own cancel take it out.
func TestCancelOwner(t *testing.T) {
	e := start(t)
	r, err := e.Submit(t.Context(), order(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	theirs, _ := e.Cancel(t.Context(), "ACME", r.OrderID, "client1")
	if ev := theirs.Events[0]; ev.Kind != engine.Rejected || !errors.Is(ev.Err, engine.ErrNotFound) {
		t.Fatalf("a cancel from another account: %+v, want rejected, not found", ev)
	}
	mine, _ := e.Cancel(t.Context(), "ACME", r.OrderID, "client0")
	if ev := mine.Events[0]; ev.Kind != engine.Cancelled {
		t.Fatalf("the owner's cancel: %+v, want cancelled", ev)
	}
}
