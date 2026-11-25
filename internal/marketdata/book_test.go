package marketdata_test

import (
	"cmp"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/book"
	"github.com/fiston-user/bourse/internal/engine"
	"github.com/fiston-user/bourse/internal/journal"
	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/marketdata"
	"github.com/fiston-user/bourse/internal/publish"
)

// TestDepth sends random orders and cancels through a real engine, 64 at a time, and rebuilds every
// book from the executions the engine reports, the way they reach Kafka. The same inputs, in the
// engine's order, also go into books of the test's own, and after every batch, the depth rebuilt from
// the executions must be exactly those books' levels.
func TestDepth(t *testing.T) {
	e, err := engine.New(t.TempDir(), market.Listed...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	defer func() {
		stop()
		<-done // its snapshot on the way out goes in the test's folder, before the folder goes
	}()

	markets := map[string]*marketdata.Market{}
	want := map[string]*book.Book{}
	for _, in := range market.Listed {
		markets[in.Symbol], want[in.Symbol] = marketdata.NewMarket(in), book.New(in)
	}
	r := rand.New(rand.NewPCG(1, 2))
	for range 200 {
		inputs := make([]input, 64)
		for i := range inputs {
			in := market.Listed[r.IntN(len(market.Listed))]
			resting := want[in.Symbol].Orders()
			if len(resting) > 0 && r.IntN(4) == 0 {
				inputs[i] = input{order: resting[r.IntN(len(resting))], cancel: true}
			} else {
				inputs[i] = input{order: random(r, in)}
			}
		}
		var wg sync.WaitGroup
		for i := range inputs {
			wg.Go(func() { inputs[i].send(ctx, e) })
		}
		wg.Wait()
		// The engine took them in its own order, and its seqs say which.
		slices.SortFunc(inputs, func(a, b input) int {
			return cmp.Or(cmp.Compare(a.order.Symbol, b.order.Symbol),
				cmp.Compare(a.report.Events[0].Seq, b.report.Events[0].Seq))
		})
		for _, in := range inputs {
			if in.err != nil {
				t.Fatal(in.err)
			}
			b := want[in.order.Symbol]
			if !in.cancel {
				in.order.ID = in.report.OrderID
				b.Submit(in.order)
			} else if in.report.Events[0].Kind == engine.Cancelled {
				b.Cancel(in.order.ID)
			}
			for _, x := range executions(t, in.order.Symbol, in.report) {
				if _, err := markets[in.order.Symbol].Apply(x); err != nil {
					t.Fatal(err)
				}
			}
		}
		for sym, b := range want {
			got := markets[sym].Snapshot(1000)
			sides := map[market.Side][]marketdata.Level{market.Buy: got.Bids, market.Sell: got.Asks}
			for side, levels := range sides {
				var exp []marketdata.Level
				for _, l := range b.Depth(side, 1000) {
					exp = append(exp, marketdata.Level{Price: l.Price, Qty: l.Qty})
				}
				if !slices.Equal(levels, exp) {
					t.Fatalf("%s, side %d:\n got %v\nwant %v", sym, side, levels, exp)
				}
			}
		}
	}
}

// input is one order or cancel, and what the engine said about it.
type input struct {
	order  market.Order
	cancel bool
	report engine.Report
	err    error
}

func (in *input) send(ctx context.Context, e *engine.Engine) {
	if in.cancel {
		in.report, in.err = e.Cancel(ctx, in.order.Symbol, in.order.ID, in.order.Account)
	} else {
		in.report, in.err = e.Submit(ctx, in.order)
	}
}

// random is an order of any kind, from one of four accounts, close to the middle of the book.
func random(r *rand.Rand, in market.Instrument) market.Order {
	o := market.Order{Account: fmt.Sprint("trader", r.IntN(4)), Symbol: in.Symbol,
		Side: market.Side(1 + r.IntN(2)), Type: market.Limit, TIF: market.GTC,
		Price: 100_00 + in.TickSize*market.Price(r.IntN(21)-10), Qty: in.LotSize * market.Qty(1+r.IntN(20))}
	switch r.IntN(10) {
	case 0:
		o.Type, o.TIF, o.Price = market.Market, market.IOC, 0
	case 1:
		o.TIF = market.IOC
	case 2:
		o.TIF = market.FOK
	case 3:
		o.PostOnly = true
	}
	return o
}

// executions turns what the engine reported into the executions the publisher would send to Kafka.
func executions(t *testing.T, sym string, rep engine.Report) []*boursev1.Execution {
	var xs []*boursev1.Execution
	for _, ev := range rep.Events {
		rec, err := publish.Record("executions", journal.Entry{Symbol: sym}, ev)
		if err != nil {
			t.Fatal(err)
		}
		x := new(boursev1.Execution)
		if err := proto.Unmarshal(rec.Value, x); err != nil {
			t.Fatal(err)
		}
		xs = append(xs, x)
	}
	return xs
}
