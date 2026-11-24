// Package engine runs the order books, one goroutine per symbol, fed through a queue.
package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/fiston-user/bourse/internal/book"
	"github.com/fiston-user/bourse/internal/market"
)

var (
	ErrInvalid       = errors.New("invalid order")
	ErrUnknownSymbol = errors.New("the engine doesn't trade this symbol")
	ErrNotFound      = errors.New("no resting order with this ID")
	ErrStopped       = errors.New("the engine has stopped")
)

// Kind says what happened to an order.
type Kind uint8

const (
	Accepted  Kind = iota + 1 // it got its ID and reached the book
	Rejected                  // the book refused it, and nothing changed
	Traded                    // it traded with a resting order
	Rested                    // what's left of it waits in the book
	Cancelled                 // what's left of it is gone
)

// Event is one thing that happened to an order. Seq counts the events of a symbol, with no gaps.
type Event struct {
	Seq   uint64
	Kind  Kind
	Order uint64 // the order it's about, the taker in a trade
	Maker uint64 // the resting order, in a trade
	Price market.Price
	Qty   market.Qty
	Err   error // why, for a rejection
}

// Report is everything one command did, in order.
type Report struct {
	OrderID uint64
	Events  []Event
}

// command is one request for a symbol's goroutine, with a channel for the answer.
type command struct {
	order  market.Order
	cancel uint64 // the order to cancel, or zero for a new order
	reply  chan Report
}

// symbol is one book, and everything only its goroutine may touch.
type symbol struct {
	inbox chan command
	book  *book.Book
	seq   uint64 // the last sequence number handed out
	id    uint64 // the last order ID handed out
}

// queue is how many commands can wait for a symbol's goroutine, before their senders have to wait too.
const queue = 1024

// Engine trades many symbols, each in a goroutine of its own.
type Engine struct {
	symbols map[string]*symbol
	stopped chan struct{} // closed when Run returns
}

// New returns an engine for these instruments. Nothing trades until Run.
func New(instruments ...market.Instrument) *Engine {
	e := &Engine{symbols: map[string]*symbol{}, stopped: make(chan struct{})}
	for _, in := range instruments {
		e.symbols[in.Symbol] = &symbol{inbox: make(chan command, queue), book: book.New(in)}
	}
	return e
}

// Run starts every symbol's goroutine, and returns when ctx is done and they have all stopped.
func (e *Engine) Run(ctx context.Context) {
	defer close(e.stopped)
	var wg sync.WaitGroup
	for _, s := range e.symbols {
		wg.Go(func() { s.run(ctx) })
	}
	wg.Wait()
}

// run is the symbol's goroutine, the only code that ever touches its book.
func (s *symbol) run(ctx context.Context) {
	for {
		select {
		case c := <-s.inbox:
			if c.cancel != 0 {
				c.reply <- s.cancel(c.cancel)
			} else {
				c.reply <- s.submit(c.order)
			}
		case <-ctx.Done():
			return
		}
	}
}

// submit gives o the next order ID, matches it, and numbers everything that happened.
func (s *symbol) submit(o market.Order) Report {
	s.id++
	o.ID = s.id
	r := Report{OrderID: o.ID}
	res, err := s.book.Submit(o)
	if err != nil {
		s.emit(&r, Event{Kind: Rejected, Order: o.ID, Err: err})
		return r
	}
	s.emit(&r, Event{Kind: Accepted, Order: o.ID, Price: o.Price, Qty: o.Qty})
	for _, t := range res.Trades {
		s.emit(&r, Event{Kind: Traded, Order: o.ID, Maker: t.Maker, Price: t.Price, Qty: t.Qty})
	}
	for _, id := range res.Removed {
		s.emit(&r, Event{Kind: Cancelled, Order: id})
	}
	if res.Rested > 0 {
		s.emit(&r, Event{Kind: Rested, Order: o.ID, Price: o.Price, Qty: res.Rested})
	}
	if res.Cancelled > 0 {
		s.emit(&r, Event{Kind: Cancelled, Order: o.ID, Qty: res.Cancelled})
	}
	return r
}

// cancel takes an order out of the book, if it's still there.
func (s *symbol) cancel(id uint64) Report {
	r := Report{OrderID: id}
	if !s.book.Cancel(id) {
		s.emit(&r, Event{Kind: Rejected, Order: id, Err: ErrNotFound})
		return r
	}
	s.emit(&r, Event{Kind: Cancelled, Order: id})
	return r
}

// emit gives ev the symbol's next sequence number, and adds it to the report.
func (s *symbol) emit(r *Report, ev Event) {
	s.seq++
	ev.Seq = s.seq
	r.Events = append(r.Events, ev)
}

// Submit sends o to its symbol's book, and waits for what happened to it.
func (e *Engine) Submit(ctx context.Context, o market.Order) (Report, error) {
	s, ok := e.symbols[o.Symbol]
	if !ok {
		return Report{}, ErrUnknownSymbol
	}
	if err := o.Validate(s.book.Instrument); err != nil {
		return Report{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return e.send(ctx, s, command{order: o})
}

// Cancel takes a resting order out of its symbol's book.
func (e *Engine) Cancel(ctx context.Context, sym string, id uint64) (Report, error) {
	s, ok := e.symbols[sym]
	if !ok {
		return Report{}, ErrUnknownSymbol
	}
	return e.send(ctx, s, command{cancel: id})
}

// send puts c in the symbol's queue, and waits for the answer.
func (e *Engine) send(ctx context.Context, s *symbol, c command) (Report, error) {
	c.reply = make(chan Report, 1)
	select {
	case s.inbox <- c:
	case <-ctx.Done():
		return Report{}, ctx.Err()
	case <-e.stopped:
		return Report{}, ErrStopped
	}
	select {
	case r := <-c.reply:
		return r, nil
	case <-e.stopped:
	}
	// It has stopped, but it may have answered first: select picks at random when both are ready.
	select {
	case r := <-c.reply:
		return r, nil
	default:
		return Report{}, ErrStopped
	}
}

// Instrument returns what the engine knows about a symbol.
func (e *Engine) Instrument(sym string) (market.Instrument, bool) {
	s, ok := e.symbols[sym]
	if !ok {
		return market.Instrument{}, false
	}
	return s.book.Instrument, true
}
