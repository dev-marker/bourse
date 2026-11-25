package ledger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/fiston-user/bourse/internal/obs"
	"github.com/fiston-user/bourse/internal/publish"
)

// ApplyFunc does what some events mean to a store, inside a transaction of the store's database.
type ApplyFunc func(ctx context.Context, tx pgx.Tx, evs []Event) error

// Consume reads both topics with the client's consumer group, and applies every event to db, until
// ctx is done. Each poll's events go to the database in one transaction. A batch that's been read is
// always applied and committed, even when ctx ends halfway: a stop never throws work away.
func Consume(ctx context.Context, cl *kgo.Client, db *pgxpool.Pool, apply ApplyFunc,
	log *slog.Logger) error {
	work := context.WithoutCancel(ctx)
	var p progress
	for {
		poll, cancel := context.WithTimeout(ctx, time.Second) // so we notice when we've caught up
		fetches := cl.PollRecords(poll, 2000)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := fetches.Err()
		if errors.Is(err, context.DeadlineExceeded) {
			p.idle(log)
			continue
		}
		if err != nil {
			return err
		}
		obs.Lag(fetches)
		evs, err := decode(fetches)
		if err != nil {
			return err
		}
		start := time.Now()
		if err := applyOrSkip(work, db, evs, apply, log); err != nil {
			return err
		}
		// Only now that the database has them do we tell Kafka. A crash right here reads them again.
		if err := cl.CommitUncommittedOffsets(work); err != nil {
			return err
		}
		p.applied(len(evs), start)
	}
}

// ApplyOnce applies the events db hasn't applied yet, in one transaction, along with how far each
// stream got. An event at or below its stream's last seq is skipped, and one past the next is an error.
func ApplyOnce(ctx context.Context, db *pgxpool.Pool, evs []Event, apply ApplyFunc) error {
	return pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		last := map[string]uint64{}
		var fresh []Event
		for _, ev := range evs {
			seq, ok := last[ev.Stream]
			if !ok {
				err := tx.QueryRow(ctx, `SELECT seq FROM applied WHERE stream = $1 FOR UPDATE`,
					ev.Stream).Scan(&seq)
				if err != nil && !errors.Is(err, pgx.ErrNoRows) {
					return err
				}
			}
			if ev.Seq > seq+1 {
				return fmt.Errorf("%s jumps from seq %d to %d", ev.Stream, seq, ev.Seq)
			}
			if ev.Seq == seq+1 {
				fresh = append(fresh, ev)
				seq = ev.Seq
			}
			last[ev.Stream] = seq
		}
		if err := apply(ctx, tx, fresh); err != nil {
			return err
		}
		for stream, seq := range last {
			_, err := tx.Exec(ctx, `INSERT INTO applied (stream, seq) VALUES ($1, $2)
				ON CONFLICT (stream) DO UPDATE SET seq = excluded.seq`, stream, seq)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// applyOrSkip applies the events, all at once. If one of them can't be applied, because it would take a
// balance below zero, it applies them one at a time instead. That poison event is skipped, counted and
// logged, and the stream goes on: one bad event must never stop everyone's settlement.
func applyOrSkip(ctx context.Context, db *pgxpool.Pool, evs []Event, apply ApplyFunc,
	log *slog.Logger) error {
	err := ApplyOnce(ctx, db, evs, apply)
	if !poisoned(err) {
		return err
	}
	for _, ev := range evs {
		err := ApplyOnce(ctx, db, []Event{ev}, apply)
		if poisoned(err) {
			poison.Inc()
			log.Warn("skipped an event that can't be applied", "stream", ev.Stream, "seq", ev.Seq)
			err = ApplyOnce(ctx, db, []Event{ev}, skip) // its seq is applied, so it's never read again
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// skip applies nothing.
func skip(context.Context, pgx.Tx, []Event) error { return nil }

// poisoned reports whether err is a CHECK constraint that failed: a balance that would go below zero.
func poisoned(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23514" // check_violation
}

var poison = promauto.NewCounter(prometheus.CounterOpts{
	Name: "bourse_consumer_poison_total",
	Help: "Events skipped because they can't be applied.",
})

// decode reads every record in fetches.
func decode(fetches kgo.Fetches) ([]Event, error) {
	var evs []Event
	for r := range fetches.RecordsAll() {
		ev, err := Decode(r)
		if err != nil {
			return nil, fmt.Errorf("%s partition %d offset %d: %w", r.Topic, r.Partition, r.Offset, err)
		}
		evs = append(evs, ev)
	}
	return evs, nil
}

// progress counts what the consumer applies, and logs it each time it catches up.
type progress struct {
	n           int
	first, last time.Time
}

func (p *progress) applied(n int, since time.Time) {
	if p.n == 0 {
		p.first = since
	}
	p.n += n
	p.last = time.Now()
}

func (p *progress) idle(log *slog.Logger) {
	if p.n == 0 {
		return
	}
	took := p.last.Sub(p.first)
	log.Info("caught up", "applied", p.n, "took", took.Round(time.Millisecond),
		"per_second", int(float64(p.n)/max(took.Seconds(), 0.001)))
	p.n = 0
}

// EnsureTopics creates both topics, if they aren't there yet. Deposits have one partition, so they
// keep one order, and like executions, they're kept forever.
func EnsureTopics(ctx context.Context, cl *kgo.Client) error {
	if err := publish.EnsureTopic(ctx, cl, Executions); err != nil {
		return err
	}
	forever := map[string]*string{"retention.ms": new("-1")}
	_, err := kadm.NewClient(cl).CreateTopic(ctx, 1, 1, forever, Deposits)
	if errors.Is(err, kerr.TopicAlreadyExists) {
		return nil
	}
	return err
}
