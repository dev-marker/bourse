package engine

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/fiston-user/bourse/internal/journal"
	"github.com/fiston-user/bourse/internal/market"
)

// instruments are two books with different tick and lot sizes.
var instruments = []market.Instrument{
	{Symbol: "ACME", Decimals: 2, TickSize: 1, LotSize: 1},
	{Symbol: "GLBX", Decimals: 2, TickSize: 5, LotSize: 10},
}

// TestReplay is the journal's promise. An engine rebuilt from the journal has the same books, order IDs and
// sequence numbers as the engine that wrote it, and every entry gives the same events again.
func TestReplay(t *testing.T) {
	dir := t.TempDir()
	live, err := New(dir, instruments...)
	if err != nil {
		t.Fatal(err)
	}
	events := trade(t, live)
	again, err := New(dir, instruments...)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(books(again), books(live)) {
		t.Fatal("the books rebuilt from the journal differ from the live ones")
	}

	// And the same events: every entry in the journal, through a third engine's apply, one at a time.
	fresh, err := New(t.TempDir(), instruments...)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(dir, journalFile))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	replayed, r := map[string][]Event{}, journal.NewReader(f)
	for {
		entry, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		report := fresh.symbols[entry.Symbol].apply(entry.Cancel, entry.Order)
		replayed[entry.Symbol] = append(replayed[entry.Symbol], report.Events...)
	}
	if !maps.EqualFunc(replayed, events, slices.Equal[[]Event]) {
		t.Fatal("the replayed events differ from the live ones")
	}
}

// TestSnapshots does the same with a snapshot every 500 inputs. The rebuilt engine starts from the last
// one, and only replays the journal after it.
func TestSnapshots(t *testing.T) {
	dir := t.TempDir()
	live, err := New(dir, instruments...)
	if err != nil {
		t.Fatal(err)
	}
	live.every = 500
	trade(t, live)
	again, err := New(dir, instruments...)
	if err != nil {
		t.Fatal(err)
	}
	if again.Recovery().Snapshot == 0 {
		t.Fatal("no snapshot was taken")
	}
	if !reflect.DeepEqual(books(again), books(live)) {
		t.Fatal("the books rebuilt from a snapshot differ from the live ones")
	}
}

// trade has eight clients send random orders and cancels to e, all at once, and then stops it. It returns
// every event the clients got back, by symbol, in sequence.
func trade(t *testing.T, e *Engine) map[string][]Event {
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		e.Run(ctx)
		close(done)
	}()
	got := make([]map[string][]Event, 8)
	var wg sync.WaitGroup
	for c := range got {
		got[c] = map[string][]Event{}
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(c), 7))
			for range 400 {
				in := instruments[r.IntN(len(instruments))]
				report, err := send(ctx, e, r, c, in)
				if errors.Is(err, ErrInvalid) {
					continue // turned away at the door, so it's not in the journal either
				}
				if err != nil {
					t.Error(err)
					return
				}
				got[c][in.Symbol] = append(got[c][in.Symbol], report.Events...)
			}
		})
	}
	wg.Wait()
	stop()
	<-done

	events := map[string][]Event{}
	for _, m := range got {
		for sym, evs := range m {
			events[sym] = append(events[sym], evs...)
		}
	}
	for _, evs := range events {
		slices.SortFunc(evs, func(a, b Event) int { return cmp.Compare(a.Seq, b.Seq) })
	}
	return events
}

// send is one random command: a cancel of an order that may or may not exist, or an order of any kind,
// close to 100.00. Some of the orders are invalid on purpose.
func send(ctx context.Context, e *Engine, r *rand.Rand, client int, in market.Instrument) (Report, error) {
	if r.IntN(5) == 0 {
		return e.Cancel(ctx, in.Symbol, uint64(1+r.IntN(400)), fmt.Sprint("client", client%4))
	}
	return e.Submit(ctx, market.Order{
		Symbol: in.Symbol, Account: fmt.Sprint("client", client%4), Side: market.Side(1 + r.IntN(2)),
		Type: market.Type(1 + r.IntN(2)), TIF: market.TimeInForce(1 + r.IntN(3)), PostOnly: r.IntN(10) == 0,
		Price: 10_000 + in.TickSize*market.Price(r.IntN(20)-10), Qty: in.LotSize * market.Qty(1+r.IntN(20)),
	})
}

// shot is what the tests compare of one book: its two counters, and every resting order in it.
type shot struct {
	Seq, ID uint64
	Orders  []market.Order
}

// books takes a shot of every book. Only for an engine that isn't running.
func books(e *Engine) map[string]shot {
	out := map[string]shot{}
	for sym, s := range e.symbols {
		out[sym] = shot{s.seq, s.id, s.book.Orders()}
	}
	return out
}
