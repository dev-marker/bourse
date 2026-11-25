package marketdata

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"strconv"
	"sync"

	"github.com/coder/websocket"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/market"
)

// Feed is the market data service: a Market for every symbol, and the clients following each one.
type Feed struct {
	mu      sync.Mutex
	markets map[string]*Market // never changes after New, so reading it needs no lock
	clients map[string]map[*client]struct{}
	log     *slog.Logger
}

// New returns a feed for these instruments, with every book empty.
func New(log *slog.Logger, instruments ...market.Instrument) *Feed {
	f := &Feed{markets: map[string]*Market{}, clients: map[string]map[*client]struct{}{}, log: log}
	for _, in := range instruments {
		f.markets[in.Symbol] = NewMarket(in)
		f.clients[in.Symbol] = map[*client]struct{}{}
	}
	return f
}

// Apply applies executions in order, and sends every update they make to its symbol's clients.
func (f *Feed) Apply(xs []*boursev1.Execution) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, x := range xs {
		m, ok := f.markets[x.GetSymbol()]
		if !ok {
			continue // a symbol we don't list
		}
		updates, err := m.Apply(x)
		if err != nil {
			return err
		}
		for _, u := range updates {
			if err := f.publish(u); err != nil {
				return err
			}
		}
	}
	return nil
}

// client is one WebSocket connection, following one symbol.
type client struct {
	queue chan []byte             // what's waiting to be written
	drop  context.CancelCauseFunc // ends the connection, and says why
}

// queueLen is how far a client may fall behind before it's dropped.
const queueLen = 8192

// batchLen is about how big one message to a client gets, when many updates are waiting.
const batchLen = 64 << 10

var errTooSlow = errors.New("too slow")

// publish sends u to every client of its symbol. It's encoded once: they all get the same bytes.
func (f *Feed) publish(u Message) error {
	b, err := json.Marshal(u)
	if err != nil {
		return err
	}
	for c := range f.clients[u.Symbol] {
		select {
		case c.queue <- b:
		default:
			// Its queue is full. Waiting for it would hold up everyone else, so it goes.
			delete(f.clients[u.Symbol], c)
			c.drop(errTooSlow)
		}
	}
	return nil
}

// subscribe gives c a snapshot, and adds c to the symbol's clients. Both happen under the lock, so
// the first update c gets is the one right after the snapshot.
func (f *Feed) subscribe(c *client, sym string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, err := json.Marshal(f.markets[sym].Snapshot(math.MaxInt))
	if err != nil {
		return err
	}
	c.queue <- b // the queue is still empty
	f.clients[sym][c] = struct{}{}
	return nil
}

func (f *Feed) unsubscribe(c *client, sym string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.clients[sym], c)
}

// Handler serves the stream, GET /v1/stream?symbol=GLBX, and two plain reads: the book and the candles.
func (f *Feed) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/stream", f.stream)
	mux.HandleFunc("GET /v1/book/{symbol}", f.book)
	mux.HandleFunc("GET /v1/candles/{symbol}", f.candles)
	return mux
}

// stream follows one symbol over a WebSocket: a snapshot, then every update, until the client goes.
func (f *Feed) stream(w http.ResponseWriter, r *http.Request) {
	sym := r.URL.Query().Get("symbol")
	if _, ok := f.markets[sym]; !ok {
		http.Error(w, "no such symbol", http.StatusNotFound)
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept has answered already
	}
	defer conn.CloseNow()
	// We never read: CloseRead answers pings, and ends ctx when the client goes.
	ctx, drop := context.WithCancelCause(conn.CloseRead(r.Context()))
	c := &client{queue: make(chan []byte, queueLen), drop: drop}
	if err := f.subscribe(c, sym); err != nil {
		return
	}
	defer f.unsubscribe(c, sym)
	var batch []byte
	for err == nil {
		select {
		case b := <-c.queue:
			// Everything that's waiting goes in one message, a JSON array: one write, not one each.
			batch = append(batch[:0], '[')
			batch = append(batch, b...)
			for len(c.queue) > 0 && len(batch) < batchLen {
				batch = append(batch, ',')
				batch = append(batch, <-c.queue...)
			}
			batch = append(batch, ']')
			err = conn.Write(ctx, websocket.MessageText, batch)
		case <-ctx.Done():
			err = context.Cause(ctx)
		}
	}
	if errors.Is(context.Cause(ctx), errTooSlow) {
		f.log.Warn("client dropped", "symbol", sym, "reason", "too slow")
		conn.Close(websocket.StatusTryAgainLater, "too slow")
	}
}

// Clients is how many clients follow the symbol.
func (f *Feed) Clients(sym string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.clients[sym])
}

// Book is the symbol's book right now, up to n levels a side, without the trades and the candles.
func (f *Feed) Book(sym string, n int) (Message, bool) {
	m, ok := f.markets[sym]
	if !ok {
		return Message{}, false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	msg := m.Snapshot(n)
	msg.Trades, msg.Candles = nil, nil
	return msg, true
}

// Candles are the symbol's one-minute candles, oldest first.
func (f *Feed) Candles(sym string) ([]Candle, bool) {
	m, ok := f.markets[sym]
	if !ok {
		return nil, false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(m.candles), true
}

// book answers GET /v1/book/{symbol}?depth=10.
func (f *Feed) book(w http.ResponseWriter, r *http.Request) {
	n := 10
	if s := r.URL.Query().Get("depth"); s != "" {
		var err error
		if n, err = strconv.Atoi(s); err != nil || n < 1 {
			http.Error(w, "depth must be a positive number", http.StatusBadRequest)
			return
		}
	}
	msg, ok := f.Book(r.PathValue("symbol"), n)
	if !ok {
		http.Error(w, "no such symbol", http.StatusNotFound)
		return
	}
	reply(w, msg)
}

// candles answers GET /v1/candles/{symbol}.
func (f *Feed) candles(w http.ResponseWriter, r *http.Request) {
	cs, ok := f.Candles(r.PathValue("symbol"))
	if !ok {
		http.Error(w, "no such symbol", http.StatusNotFound)
		return
	}
	reply(w, cs)
}

func reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
