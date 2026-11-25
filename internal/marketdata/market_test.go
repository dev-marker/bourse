package marketdata_test

import (
	"slices"
	"testing"
	"time"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/marketdata"
)

// TestCandles trades GLBX at times we choose, and checks the one-minute candles: a trade belongs to the
// minute it happened in, by the engine's clock, and a new minute starts a new candle.
func TestCandles(t *testing.T) {
	m := marketdata.NewMarket(market.Instrument{Symbol: "GLBX", Decimals: 2, TickSize: 5, LotSize: 10})
	at := func(s string) time.Time {
		tm, err := time.Parse(time.TimeOnly, s)
		if err != nil {
			t.Fatal(err)
		}
		return time.Date(2026, 11, 25, tm.Hour(), tm.Minute(), tm.Second(), tm.Nanosecond(), time.UTC)
	}
	var xs []*boursev1.Execution
	trade := func(when string, p, q int64) {
		// A sell rests, and a buy takes it: a trade at the sell's price.
		sell := &boursev1.Execution{Symbol: "GLBX", Seq: uint64(len(xs) + 1), Time: at(when).UnixNano(),
			Kind: boursev1.EventKind_EVENT_KIND_RESTED, Side: boursev1.Side_SIDE_SELL, Price: p, Qty: q}
		buy := &boursev1.Execution{Symbol: "GLBX", Seq: sell.Seq + 1, Time: sell.Time,
			Kind: boursev1.EventKind_EVENT_KIND_TRADED, Side: boursev1.Side_SIDE_BUY, Price: p, Qty: q}
		xs = append(xs, sell, buy)
	}
	trade("10:00:05", 101_00, 10)
	trade("10:00:40", 102_00, 20)
	trade("10:00:59.999", 100_50, 30)
	trade("10:01:00", 101_50, 5)
	trade("10:03:30", 101_00, 40)
	for _, x := range xs {
		if _, err := m.Apply(x); err != nil {
			t.Fatal(err)
		}
	}
	got := m.Snapshot(0).Candles
	want := []marketdata.Candle{
		{Time: at("10:00:00"), Open: 101_00, High: 102_00, Low: 100_50, Close: 100_50, Volume: 60},
		{Time: at("10:01:00"), Open: 101_50, High: 101_50, Low: 101_50, Close: 101_50, Volume: 5},
		{Time: at("10:03:00"), Open: 101_00, High: 101_00, Low: 101_00, Close: 101_00, Volume: 40},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got  %+v\nwant %+v", got, want)
	}
}
