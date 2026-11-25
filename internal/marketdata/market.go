package marketdata

import (
	"fmt"
	"slices"
	"time"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/market"
)

// Trade is one trade on the tape. Side is the taker's: buy when a buyer crossed the book.
type Trade struct {
	Time  time.Time    `json:"time"`
	Price market.Price `json:"price"`
	Qty   market.Qty   `json:"qty"`
	Side  string       `json:"side"`
}

// Candle is one minute of trading: the first, highest, lowest and last price, and the shares traded.
type Candle struct {
	Time   time.Time    `json:"time"` // when the minute starts
	Open   market.Price `json:"open"`
	High   market.Price `json:"high"`
	Low    market.Price `json:"low"`
	Close  market.Price `json:"close"`
	Volume market.Qty   `json:"volume"`
}

// Message is what a client gets, as JSON. A snapshot has the whole book, the last trades and the
// candles. After it, every update has one level of the book with its new quantity, zero when it's
// gone, or one trade, or the candle a trade changed. Each one's seq is one more than the last.
type Message struct {
	Type     string   `json:"type"` // snapshot, book, trade or candle
	Symbol   string   `json:"symbol"`
	Seq      uint64   `json:"seq"`
	Decimals int      `json:"decimals,omitempty"` // in a snapshot: 2 means prices are in cents
	Bids     []Level  `json:"bids,omitempty"`
	Asks     []Level  `json:"asks,omitempty"`
	Trades   []Trade  `json:"trades,omitempty"`
	Candles  []Candle `json:"candles,omitempty"`
	Trade    *Trade   `json:"trade,omitempty"`
	Candle   *Candle  `json:"candle,omitempty"`
}

const (
	tapeLen   = 50      // the trades a snapshot has
	candleLen = 24 * 60 // a day of one-minute candles
)

// Market is one symbol's market data, rebuilt from its executions. It isn't safe for concurrent use.
type Market struct {
	in      market.Instrument
	exec    uint64 // the seq of the last execution applied
	seq     uint64 // the seq of the last update
	depth   depth
	tape    [tapeLen]Trade // a ring: trade number n is at n % tapeLen
	trades  int            // trades ever seen
	candles []Candle
}

// NewMarket returns a market for in, with an empty book.
func NewMarket(in market.Instrument) *Market {
	return &Market{in: in, depth: newDepth()}
}

// Apply applies the symbol's next execution, and returns the updates it makes.
func (m *Market) Apply(x *boursev1.Execution) ([]Message, error) {
	if x.GetSeq() <= m.exec {
		return nil, nil // we've had it already
	}
	if x.GetSeq() != m.exec+1 {
		return nil, fmt.Errorf("%s jumps from seq %d to %d", m.in.Symbol, m.exec, x.GetSeq())
	}
	m.exec = x.GetSeq()
	var out []Message
	if side, l, ok := m.depth.apply(x); ok {
		u := Message{Type: "book", Bids: []Level{l}}
		if side == market.Sell {
			u = Message{Type: "book", Asks: []Level{l}}
		}
		out = append(out, m.next(u))
	}
	if x.GetKind() == boursev1.EventKind_EVENT_KIND_TRADED {
		t := m.trade(x)
		c := m.candle(t)
		out = append(out, m.next(Message{Type: "trade", Trade: &t}))
		out = append(out, m.next(Message{Type: "candle", Candle: &c}))
	}
	return out, nil
}

// next numbers u as the symbol's next update.
func (m *Market) next(u Message) Message {
	m.seq++
	u.Symbol, u.Seq = m.in.Symbol, m.seq
	return u
}

// trade puts a traded execution on the tape, and returns it.
func (m *Market) trade(x *boursev1.Execution) Trade {
	t := Trade{Time: time.Unix(0, x.GetTime()).UTC(), Price: market.Price(x.GetPrice()),
		Qty: market.Qty(x.GetQty()), Side: "buy"}
	if x.GetSide() == boursev1.Side_SIDE_SELL {
		t.Side = "sell"
	}
	m.tape[m.trades%tapeLen] = t
	m.trades++
	return t
}

// candle adds a trade to the candle of its minute, and returns that candle. A trade in a new minute
// starts a new one. The minute comes from the engine's time, so a replay makes the same candles.
func (m *Market) candle(t Trade) Candle {
	minute := t.Time.Truncate(time.Minute)
	if n := len(m.candles); n == 0 || m.candles[n-1].Time.Before(minute) {
		if n == candleLen {
			m.candles = slices.Delete(m.candles, 0, 1)
		}
		m.candles = append(m.candles, Candle{Time: minute, Open: t.Price, High: t.Price, Low: t.Price})
	}
	c := &m.candles[len(m.candles)-1]
	c.High, c.Low, c.Close = max(c.High, t.Price), min(c.Low, t.Price), t.Price
	c.Volume += t.Qty
	return *c
}

// Snapshot is everything a new client needs, with up to n levels a side. Its seq is the last update
// it includes, so the next update a client gets has seq + 1.
func (m *Market) Snapshot(n int) Message {
	k := min(m.trades, tapeLen)
	trades := make([]Trade, 0, k)
	for i := m.trades - k; i < m.trades; i++ {
		trades = append(trades, m.tape[i%tapeLen])
	}
	return Message{Type: "snapshot", Symbol: m.in.Symbol, Seq: m.seq, Decimals: m.in.Decimals,
		Bids: m.depth.levels(market.Buy, n), Asks: m.depth.levels(market.Sell, n),
		Trades: trades, Candles: slices.Clone(m.candles)}
}
