package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/marketdata"
)

const stream = "ws://localhost:8081/v1/stream?symbol="

// watch follows a symbol's market data the way a trader's screen does: a snapshot, then every update.
// Once a second, it prints the top of the book, the last trade, how many messages arrived, and how late
// the trades were, from the engine taking the order to here. With stall, it connects and never reads.
// With drop, it loses a message on purpose now and then.
func watch(sym string, clients int, mode string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if mode == "stall" {
		conn, _, err := websocket.Dial(ctx, stream+sym, nil)
		if err != nil {
			return err
		}
		defer conn.CloseNow()
		fmt.Println("following", sym, "and reading nothing")
		<-ctx.Done()
		return nil
	}
	w := &watcher{sym: sym}
	for i := range clients {
		go w.follow(ctx, i == 0, mode == "drop")
	}
	if clients > 1 {
		fmt.Printf("following %s with %d clients\n", sym, clients)
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			fmt.Println(w.line())
		}
	}
}

// buckets is how late a trade can be counted: ten seconds, in tenths of a millisecond.
const buckets = 100_000

// watcher counts what every client got in the last second, and keeps what the first one shows.
type watcher struct {
	sym  string
	msgs atomic.Int64
	late [buckets]atomic.Int64 // trades by how late they were, in tenths of a millisecond

	mu       sync.Mutex
	seq      uint64
	bid, ask marketdata.Level
	last     marketdata.Trade
}

// follow keeps one client on the feed. After a gap, or when the feed drops it, it starts again.
func (w *watcher) follow(ctx context.Context, first, drop bool) {
	for ctx.Err() == nil {
		err := w.session(ctx, first, drop)
		if first && ctx.Err() == nil {
			fmt.Println(why(err))
		}
		if errors.Is(err, marketdata.ErrGap) {
			continue // a new snapshot, right away
		}
		select {
		case <-ctx.Done():
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// session is one connection: a snapshot, and then every update, until something goes wrong.
func (w *watcher) session(ctx context.Context, first, drop bool) error {
	conn, _, err := websocket.Dial(ctx, stream+w.sym, nil)
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20) // a snapshot has a whole day of candles
	var m marketdata.Mirror
	n := 0
	for {
		_, b, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		var msgs []marketdata.Message
		if err := json.Unmarshal(b, &msgs); err != nil {
			return err
		}
		for _, msg := range msgs {
			n++
			if drop && n%20_000 == 0 {
				continue // lost on purpose
			}
			if err := m.Apply(msg); err != nil {
				return err
			}
			w.got(&m, msg, first)
		}
	}
}

func (w *watcher) got(m *marketdata.Mirror, msg marketdata.Message, first bool) {
	w.msgs.Add(1)
	if msg.Trade != nil {
		w.late[min(int(time.Since(msg.Trade.Time)/(100*time.Microsecond)), buckets-1)].Add(1)
	}
	if first {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.seq, w.last = m.Seq, m.Last
		w.bid, _ = m.Best(market.Buy)
		w.ask, _ = m.Best(market.Sell)
	}
}

// line is the last second, on one line, and starts the next second from zero.
func (w *watcher) line() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := fmt.Sprintf("%s seq %-7d %-14s| %-14s last %-12s %6d msg/s", w.sym, w.seq, level(w.bid),
		level(w.ask), level(marketdata.Level{Price: w.last.Price, Qty: w.last.Qty}), w.msgs.Swap(0))
	if p50, p99, ok := w.percentiles(); ok {
		s += fmt.Sprintf("  p50 %v p99 %v", round(p50), round(p99))
	}
	return s
}

// percentiles are the median and the 99th percentile of how late the trades were, and start again.
func (w *watcher) percentiles() (p50, p99 time.Duration, ok bool) {
	var late [buckets]int64
	var total int64
	for i := range w.late {
		late[i] = w.late[i].Swap(0)
		total += late[i]
	}
	if total == 0 {
		return 0, 0, false
	}
	var seen int64
	for i, n := range late {
		seen += n
		at := time.Duration(i+1) * 100 * time.Microsecond
		if p50 == 0 && seen*2 >= total {
			p50 = at
		}
		if seen*100 >= total*99 {
			return p50, at, true
		}
	}
	return p50, 10 * time.Second, true
}

// level shows a price and its quantity. Every symbol we trade is priced in cents.
func level(l marketdata.Level) string {
	if l.Qty == 0 {
		return "-"
	}
	return fmt.Sprintf("%d.%02d x %d", l.Price/100, l.Price%100, l.Qty)
}

// round keeps a duration short enough to read.
func round(d time.Duration) time.Duration {
	if d < 100*time.Millisecond {
		return d
	}
	return d.Round(10 * time.Millisecond)
}

// why says what ended a session.
func why(err error) string {
	switch {
	case errors.Is(err, marketdata.ErrGap):
		return fmt.Sprintf("%v: starting again from a snapshot", err)
	case websocket.CloseStatus(err) == websocket.StatusTryAgainLater:
		return "the feed dropped us: too slow"
	default:
		return "lost the feed: trying again"
	}
}
