// Package engine runs the order books, one goroutine per symbol, fed through a queue.
package engine

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

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

	// Whose order it is, and its side. In a trade, who placed the maker too.
	Account, ClientID           string
	Side                        market.Side
	MakerAccount, MakerClientID string
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
	journaled
}

// journaled is when the journal took a command, and when its fsync was done, for the order's trace.
type journaled struct {
	taken, synced time.Time
	batch         int // the commands that shared the fsync
}

// answer is what a command gets back: its report, or why it never reached the book.
type answer struct {
	Report
	err error
	journaled
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
	// synced is where the engine says how far the journal is on disk.
	synced *os.File
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

// syncedFile holds the number of the last input the journal has on disk.
const syncedFile = "engine.synced"

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
	// Inputs the last run wrote but never synced were just replayed, so they go to disk first.
	if err := j.Sync(); err != nil {
		return nil, err
	}
	e.synced, err = os.OpenFile(filepath.Join(dir, syncedFile), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := e.markSynced(); err != nil {
		return nil, err
	}
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
// On the way out, it takes a snapshot and seals the journal, so the next start has nothing to replay.
func (e *Engine) Run(ctx context.Context) error {
	defer close(e.stopped)
	var wg sync.WaitGroup
	for _, s := range e.symbols {
		wg.Go(s.run)
	}
	e.record(ctx)
	if e.err == nil && e.input > e.snapped {
		e.err = e.takeSnapshot()
	}
	for _, s := range e.symbols {
		close(s.inbox)
	}
	wg.Wait()
	return errors.Join(e.err, e.journal.Close(), e.synced.Close())
}

// SnapshotEvery sets how many inputs there are between two snapshots. Call it before Run.
func (e *Engine) SnapshotEvery(n uint64) {
	e.every = n
}

// Snapshot is the last input the newest snapshot covers. Call it once Run has returned.
func (e *Engine) Snapshot() uint64 {
	return e.snapped
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
		taken := time.Now()
		if err := e.write(batch); err != nil {
			for _, c := range batch {
				c.reply <- answer{err: err}
			}
			continue
		}
		synced := time.Now()
		for _, c := range batch {
			c.journaled = journaled{taken, synced, len(batch)}
			c.to.inbox <- c
		}
	}
}

// write appends the batch to the journal and syncs it. A journal that failed once is never trusted again.
func (e *Engine) write(batch []command) error {
	if e.err != nil {
		return e.err
	}
	start := time.Now()
	for _, c := range batch {
		e.input++
		entry := journal.Entry{Seq: e.input, Time: time.Now().UnixNano(),
			Symbol: c.to.book.Instrument.Symbol, Cancel: c.cancel, Order: c.order}
		if err := e.journal.Append(&entry); err != nil {
			e.err = err
			return err
		}
	}
	if e.err = e.journal.Sync(); e.err == nil {
		e.err = e.markSynced()
	}
	syncs.Observe(time.Since(start).Seconds())
	batches.Observe(float64(len(batch)))
	return e.err
}

// markSynced tells whoever follows the journal that every input up to e.input is on disk. It's never
// synced itself: after a crash, it can only be behind the journal, never ahead of it.
func (e *Engine) markSynced() error {
	_, err := e.synced.WriteAt(binary.LittleEndian.AppendUint64(nil, e.input), 0)
	return err
}

// run is the symbol's goroutine, the only code that ever touches its book. All it gets is in the journal.
func (s *symbol) run() {
	for c := range s.inbox {
		if c.state != nil {
			c.state <- s.state()
			continue
		}
		r := s.apply(c.cancel, c.order)
		orders.WithLabelValues(kind(c), result(r)).Inc()
		c.reply <- answer{Report: r, journaled: c.journaled}
	}
}

// kind names a command for the metrics: a cancel, or an order by its time in force.
func kind(c command) string {
	if c.cancel != 0 {
		return "cancel"
	}
	return [...]string{market.GTC: "gtc", market.IOC: "ioc", market.FOK: "fok"}[c.order.TIF]
}

// result is what a command came to: rejected, rested, traded, or cancelled.
func result(r Report) string {
	res := "cancelled"
	for _, ev := range r.Events {
		switch ev.Kind {
		case Rejected:
			return "rejected"
		case Rested:
			return "rested"
		case Traded:
			res = "traded"
		}
	}
	return res
}

// apply does one input to the book. Live or replayed, every input goes through here, and nowhere else.
func (s *symbol) apply(cancel uint64, o market.Order) Report {
	if cancel != 0 {
		return s.cancel(cancel, o.Account)
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
		s.emit(&r, &o, Event{Kind: Rejected, Err: err})
		return r
	}
	s.emit(&r, &o, Event{Kind: Accepted, Price: o.Price, Qty: o.Qty})
	for _, t := range res.Trades {
		s.emit(&r, &o, Event{Kind: Traded, Maker: t.Maker, Price: t.Price, Qty: t.Qty,
			MakerAccount: t.MakerAccount, MakerClientID: t.MakerClientID})
	}
	for _, m := range res.Removed {
		s.emit(&r, &m, Event{Kind: Cancelled, Price: m.Price, Qty: m.Qty})
	}
	if res.Rested > 0 {
		s.emit(&r, &o, Event{Kind: Rested, Price: o.Price, Qty: res.Rested})
	}
	if res.Cancelled > 0 {
		s.emit(&r, &o, Event{Kind: Cancelled, Qty: res.Cancelled})
	}
	return r
}

// cancel takes an order out of the book, if it's still there, and if it's this account's.
func (s *symbol) cancel(id uint64, account string) Report {
	r := Report{OrderID: id}
	o, ok := s.book.Order(id)
	if !ok || o.Account != account {
		s.emit(&r, &market.Order{ID: id, Account: account}, Event{Kind: Rejected, Err: ErrNotFound})
		return r
	}
	s.book.Cancel(id)
	s.emit(&r, &o, Event{Kind: Cancelled, Price: o.Price, Qty: o.Qty})
	return r
}

// emit numbers ev, says which order it's about and whose it is, and adds it to the report.
func (s *symbol) emit(r *Report, o *market.Order, ev Event) {
	s.seq++
	ev.Seq, ev.Order = s.seq, o.ID
	ev.Account, ev.ClientID, ev.Side = o.Account, o.ClientID, o.Side
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

// Cancel takes a resting order out of its symbol's book, if the account placed it.
func (e *Engine) Cancel(ctx context.Context, sym string, id uint64, account string) (Report, error) {
	s, ok := e.symbols[sym]
	if !ok {
		return Report{}, ErrUnknownSymbol
	}
	return e.send(ctx, s, command{cancel: id, order: market.Order{Account: account}})
}

// send puts c in the symbol's queue, and waits for the answer.
func (e *Engine) send(ctx context.Context, s *symbol, c command) (Report, error) {
	c.to, c.reply = s, make(chan answer, 1)
	sent := time.Now()
	select {
	case e.inbox <- c:
	case <-ctx.Done():
		return Report{}, ctx.Err()
	case <-e.stopped:
		return Report{}, ErrStopped
	}
	select {
	case a := <-c.reply:
		spans(ctx, sent, a)
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

// Instruments returns every instrument the engine trades, sorted by symbol.
func (e *Engine) Instruments() []market.Instrument {
	out := make([]market.Instrument, 0, len(e.symbols))
	for _, s := range e.symbols {
		out = append(out, s.book.Instrument)
	}
	slices.SortFunc(out, func(a, b market.Instrument) int { return cmp.Compare(a.Symbol, b.Symbol) })
	return out
}

var tracer = otel.Tracer("bourse/engine")

// spans adds a command's time in the engine to its trace, if it has one. It's called afterwards, so
// the engine's goroutines only ever take the time.
func spans(ctx context.Context, sent time.Time, a answer) {
	if !trace.SpanFromContext(ctx).IsRecording() || a.err != nil {
		return
	}
	span := func(name string, from, to time.Time, attrs ...attribute.KeyValue) {
		_, s := tracer.Start(ctx, name, trace.WithTimestamp(from), trace.WithAttributes(attrs...))
		s.End(trace.WithTimestamp(to))
	}
	span("wait for the journal", sent, a.taken)
	span("journal fsync", a.taken, a.synced, attribute.Int("batch", a.batch))
	span("match", a.synced, time.Now())
}
