package bots

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/fiston-user/bourse/internal/market"
)

var glbx = market.Instrument{Symbol: "GLBX", Decimals: 2, TickSize: 5, LotSize: 10}

// TestQuotes checks where a maker's quotes go: around the fair value when it holds nothing, leaning
// away from what it holds, and on one side only at its limit.
func TestQuotes(t *testing.T) {
	q := Quoter{In: glbx, Levels: 2, Spread: 1, Size: 100, Limit: 1000, Skew: 4}
	buy := func(p market.Price) Quote { return Quote{market.Buy, p, 100} }
	sell := func(p market.Price) Quote { return Quote{market.Sell, p, 100} }
	tests := []struct {
		name string
		pos  market.Qty
		want []Quote
	}{
		{"flat", 0, []Quote{buy(100_95), sell(101_05), buy(100_90), sell(101_10)}},
		{"long leans down", 500, []Quote{buy(100_85), sell(100_95), buy(100_80), sell(101_00)}},
		{"short leans up", -250, []Quote{buy(101_00), sell(101_10), buy(100_95), sell(101_15)}},
		{"at the long limit it only sells", 1000, []Quote{sell(100_85), sell(100_90)}},
		{"at the short limit it only buys", -1000, []Quote{buy(101_15), buy(101_10)}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := q.Quotes(101_00, tc.pos); !slices.Equal(got, tc.want) {
				t.Fatalf("Quotes(101.00, %d) = %v, want %v", tc.pos, got, tc.want)
			}
		})
	}
}

// TestDiff moves the fair value up a tick: one quote on each side is stale, one is missing, and the
// others stay where they are.
func TestDiff(t *testing.T) {
	q := Quoter{In: glbx, Levels: 3, Spread: 1, Size: 100, Limit: 1000, Skew: 4}
	resting := map[Quote]uint64{}
	for i, quote := range q.Quotes(101_00, 0) {
		resting[quote] = uint64(i + 1)
	}
	stale, missing := diff(resting, q.Quotes(101_05, 0))
	byPrice := func(a, b Quote) int { return cmp.Compare(a.Price, b.Price) }
	slices.SortFunc(stale, byPrice)
	slices.SortFunc(missing, byPrice)
	wantStale := []Quote{{market.Buy, 100_85, 100}, {market.Sell, 101_05, 100}}
	wantMissing := []Quote{{market.Buy, 101_00, 100}, {market.Sell, 101_20, 100}}
	if !slices.Equal(stale, wantStale) || !slices.Equal(missing, wantMissing) {
		t.Fatalf("diff = %v, %v, want %v, %v", stale, missing, wantStale, wantMissing)
	}
}

// TestWalk checks that a seed decides the whole walk, and that it stays on the tick and above zero.
func TestWalk(t *testing.T) {
	walk := func(seed uint64) []market.Price {
		w := &Walk{Price: 10, Tick: 5, Rand: rand.New(rand.NewPCG(seed, 0))}
		var out []market.Price
		for range 1000 {
			out = append(out, w.Step())
		}
		return out
	}
	a, b := walk(1), walk(1)
	if !slices.Equal(a, b) {
		t.Fatal("the same seed made two different walks")
	}
	if slices.Equal(a, walk(2)) {
		t.Fatal("two seeds made the same walk")
	}
	for _, p := range a {
		if p < 5 || p%5 != 0 {
			t.Fatalf("the walk went to %d", p)
		}
	}
}
