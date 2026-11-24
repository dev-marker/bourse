// Package publish sends the engine's events to Kafka. It reads them back from the journal, so the
// engine never writes anything twice: the journal is the outbox.
package publish

import (
	"cmp"
	"context"
	"hash/fnv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/engine"
	"github.com/fiston-user/bourse/internal/journal"
)

// Partitions is how many partitions the topic has. It can never change: it decides where each symbol
// goes, and a symbol that moved would have its executions in two places, out of order.
const Partitions = 6

// Partition is where a symbol's executions go. All of them go to the same one, so they stay in order.
func Partition(symbol string) int32 {
	h := fnv.New32a()
	h.Write([]byte(symbol))
	return int32(h.Sum32() % Partitions)
}

// Mark is how far a partition got: the input and the seq of its last record.
type Mark struct{ Input, Seq uint64 }

// Covers reports whether the event with this seq, caused by this input, is already out.
func (m Mark) Covers(input, seq uint64) bool {
	return input < m.Input || input == m.Input && seq <= m.Seq
}

// Record turns one event into a Kafka record: keyed by its symbol, in the symbol's partition, and
// stamped with the time the engine took its input.
func Record(topic string, entry journal.Entry, ev engine.Event) (*kgo.Record, error) {
	x := &boursev1.Execution{Symbol: entry.Symbol, Seq: ev.Seq, Input: entry.Seq, Time: entry.Time,
		Kind: boursev1.EventKind(ev.Kind), OrderId: ev.Order, Account: ev.Account, ClientId: ev.ClientID,
		Side: boursev1.Side(ev.Side), Price: int64(ev.Price), Qty: int64(ev.Qty),
		MakerId: ev.Maker, MakerAccount: ev.MakerAccount, MakerClientId: ev.MakerClientID}
	if ev.Err != nil {
		x.Reason = ev.Err.Error()
	}
	value, err := proto.Marshal(x)
	if err != nil {
		return nil, err
	}
	return &kgo.Record{Topic: topic, Partition: Partition(entry.Symbol), Key: []byte(entry.Symbol),
		Value: value, Timestamp: time.Unix(0, entry.Time)}, nil
}

// Producer is the part of a Kafka client the publisher uses, so a test can stand in for it.
type Producer interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Flush(ctx context.Context) error
}

// Publisher sends every event the engine produced to a topic, in order. After a restart, it skips
// what's already out, so a consumer sees every event exactly once.
type Publisher struct {
	follow *engine.Follower
	out    Producer
	topic  string
	marks  map[int32]Mark // how far each partition got before we started
}

func New(follow *engine.Follower, out Producer, topic string, marks map[int32]Mark) *Publisher {
	return &Publisher{follow: follow, out: out, topic: topic, marks: marks}
}

// Run publishes every event as the engine writes it, until ctx is done or a record can't be delivered.
func (p *Publisher) Run(ctx context.Context) error {
	_, err := p.run(ctx, false)
	return err
}

// CatchUp publishes every event the engine has synced so far, waits until Kafka has them all, and says
// how many there were.
func (p *Publisher) CatchUp(ctx context.Context) (int, error) {
	return p.run(ctx, true)
}

func (p *Publisher) run(ctx context.Context, catchUp bool) (int, error) {
	ctx, fail := context.WithCancelCause(ctx)
	defer fail(nil)
	delivered := func(_ *kgo.Record, err error) {
		if err != nil {
			fail(err) // a record that never made it: stop here, and a restart sends it again
		}
	}
	sent := 0
	for !catchUp || p.follow.Behind() {
		entry, report, err := p.follow.Next(ctx)
		if err != nil {
			return sent, cmp.Or(context.Cause(ctx), err)
		}
		part := Partition(entry.Symbol)
		for _, ev := range report.Events {
			if p.marks[part].Covers(entry.Seq, ev.Seq) {
				continue // it went out before we restarted
			}
			r, err := Record(p.topic, entry, ev)
			if err != nil {
				return sent, err
			}
			p.out.Produce(ctx, r, delivered)
			sent++
		}
	}
	err := p.out.Flush(ctx)
	return sent, cmp.Or(context.Cause(ctx), err)
}
