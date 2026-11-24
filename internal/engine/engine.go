// Package engine runs the order books, one goroutine per symbol, fed through a queue.
package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/fiston-user/bourse/internal/book"
	"github.com/fiston-user/bourse/internal/journal"
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
	reply  chan answer
	to     *symbol // the book it's for
	// state asks for the book's state instead, for a snapshot.
	state chan<- bookState
}

// answer is what a command gets back: its report, or why it never reached the book.
type answer struct {
	Report
	err error
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
	dir     string // where it keeps its journal and snapshots
	symbols map[string]*symbol
	stopped chan struct{} // closed when Run returns
	inbox   chan command  // every command, on its way to the journal
	journal *journal.Writer
	input   uint64 // the last input written to the journal
	err     error  // why the journal failed, if it did: then nothing more gets through
	every   uint64 // inputs between two snapshots
	snapped uint64 // the last input the snapshot covers
	// startup is how New got the engine back to where it was.
	startup Recovery
}

// Recovery says how the engine got back to where it was, when New made it.
type Recovery struct {
	Snapshot uint64        // the last input the snapshot covered, or zero without one
	Replayed int           // journal entries replayed
	Cut      int64         // bytes of a torn tail cut off the journal
	Took     time.Duration // how long it took
}

// journalFile is the journal's name, in the engine's folder.
const journalFile = "engine.journal"

const (
	snapshotFile  = "engine.snapshot"
	snapshotEvery = 100_000 // inputs between two snapshots
)

// New returns an engine for these instruments, with its journal in dir. If there's a journal there from
// an earlier run, it rebuilds the books from it first, exactly as they were. Nothing trades until Run.
func New(dir string, instruments ...market.Instrument) (*Engine, error) {
	start := time.Now()
	e := &Engine{
		dir:     dir,
		symbols: map[string]*symbol{},
		stopped: make(chan struct{}),
		inbox:   make(chan command, queue),
		every:   snapshotEvery,
	}
	for _, in := range instruments {
		e.symbols[in.Symbol] = &symbol{inbox: make(chan command, queue), book: book.New(in)}
	}
	if err := e.restore(); err != nil {
		return nil, fmt.Errorf("loading the snapshot: %w", err)
	}
	j, cut, err := journal.Open(filepath.Join(dir, journalFile), e.replay)
	if err != nil {
		return nil, fmt.Errorf("recovering the journal: %w", err)
	}
	e.journal = j
	e.startup.Cut, e.startup.Took = cut, time.Since(start)
	return e, nil
}

// replay applies one entry from the journal to its book, through the same code as a live command.
func (e *Engine) replay(entry journal.Entry) error {
	if entry.Seq <= e.input {
		return nil // the snapshot has it already
	}
	if entry.Seq != e.input+1 {
		return fmt.Errorf("the journal jumps from input %d to %d", e.input, entry.Seq)
	}
	s, ok := e.symbols[entry.Symbol]
	if !ok {
		return fmt.Errorf("input %d is for %s, which this engine doesn't trade", entry.Seq, entry.Symbol)
	}
	e.input = entry.Seq
	s.apply(entry.Cancel, entry.Order)
	e.startup.Replayed++
	return nil
}

// Recovery says how the engine got back to where it was, when New made it.
func (e *Engine) Recovery() Recovery {
	return e.startup
}

// Run starts the engine. It returns when ctx is done, and every command in the journal has reached its book.
func (e *Engine) Run(ctx context.Context) {
	defer close(e.stopped)
	var wg sync.WaitGroup
	for _, s := range e.symbols {
		wg.Go(s.run)
	}
	e.record(ctx)
	for _, s := range e.symbols {
		close(s.inbox)
	}
	wg.Wait()
	e.journal.Close()
}

// record is the engine's front door. It writes every command to the journal, waits until the disk
// has it, and only then hands it to its book.
func (e *Engine) record(ctx context.Context) {
	var batch []command
	for {
		if e.err == nil && e.input-e.snapped >= e.every {
			e.err = e.takeSnapshot()
		}
		select {
		case c := <-e.inbox:
			batch = append(batch[:0], c)
		case <-ctx.Done():
			return
		}
		// Group commit: every command that's already waiting joins the batch, and one sync covers them all.
		for len(batch) < queue && len(e.inbox) > 0 {
			batch = append(batch, <-e.inbox)
		}
		if err := e.write(batch); err != nil {
			for _, c := range batch {
				c.reply <- answer{err: err}
			}
			continue
		}
		for _, c := range batch {
			c.to.inbox <- c
		}
	}
}

// write appends the batch to the journal and syncs it. A journal that failed once is never trusted again.
func (e *Engine) write(batch []command) error {
	if e.err != nil {
		return e.err
	}
	for _, c := range batch {
		e.input++
		entry := journal.Entry{Seq: e.input, Time: time.Now().UnixNano(),
			Symbol: c.to.book.Instrument.Symbol, Cancel: c.cancel, Order: c.order}
		if err := e.journal.Append(&entry); err != nil {
			e.err = err
			return err
		}
	}
	e.err = e.journal.Sync()
	return e.err
}

// run is the symbol's goroutine, the only code that ever touches its book. All it gets is in the journal.
func (s *symbol) run() {
	for c := range s.inbox {
		if c.state != nil {
			c.state <- s.state()
			continue
		}
		c.reply <- answer{Report: s.apply(c.cancel, c.order)}
	}
}

// apply does one input to the book. Live or replayed, every input goes through here, and nowhere else.
func (s *symbol) apply(cancel uint64, o market.Order) Report {
	if cancel != 0 {
		return s.cancel(cancel)
	}
	return s.submit(o)
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
	c.to, c.reply = s, make(chan answer, 1)
	select {
	case e.inbox <- c:
	case <-ctx.Done():
		return Report{}, ctx.Err()
	case <-e.stopped:
		return Report{}, ErrStopped
	}
	select {
	case a := <-c.reply:
		return a.Report, a.err
	case <-e.stopped:
	}
	// It has stopped, but it may have answered first: select picks at random when both are ready.
	select {
	case a := <-c.reply:
		return a.Report, a.err
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
