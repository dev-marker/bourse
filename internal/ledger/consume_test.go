package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/fiston-user/bourse/internal/ledger"
	"github.com/fiston-user/bourse/internal/pgtest"
)

// TestConsume runs the consumer against the real Kafka and Postgres, on a topic and a ledger of its
// own. It books 500 trades, and Kafka must have the group's offset. Then a second run gets one new
// trade and one it has seen before, and books exactly the new one.
func TestConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.Ping(ctx); err != nil {
		t.Skipf("Kafka isn't running (make up starts it): %v", err)
	}
	db := pgtest.Open(t, "ledger", ledger.Migrations)
	topic := fmt.Sprint("executions-test-", time.Now().UnixNano())
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { adm.DeleteTopic(context.Background(), topic) })

	send := func(seqs ...uint64) {
		var records []*kgo.Record
		for _, seq := range seqs {
			value, _ := proto.Marshal(trade(seq).Execution)
			records = append(records, &kgo.Record{Topic: topic, Value: value})
		}
		if err := cl.ProduceSync(ctx, records...).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	// consume runs a consumer in the group until the ledger has want transactions.
	consume := func(want int) {
		c, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"), kgo.ConsumerGroup(topic),
			kgo.ConsumeTopics(topic), kgo.DisableAutoCommit())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		run, stop := context.WithCancel(ctx)
		done := make(chan error)
		quiet := slog.New(slog.DiscardHandler)
		go func() { done <- ledger.Consume(run, c, db, ledger.Book, quiet) }()
		for txs, _ := count(t, db); txs < want && ctx.Err() == nil; txs, _ = count(t, db) {
			time.Sleep(20 * time.Millisecond)
		}
		stop()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}

	var seqs []uint64
	for seq := range uint64(500) {
		seqs = append(seqs, seq+1)
	}
	send(seqs...)
	consume(500)
	offsets, err := adm.FetchOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if o, _ := offsets.Lookup(topic, 0); o.At != 500 {
		t.Fatalf("the group's offset is %d, want 500", o.At)
	}

	send(500, 501)
	consume(501)
	time.Sleep(200 * time.Millisecond) // anything booked twice would be in by now
	if txs, postings := count(t, db); txs != 501 || postings != 501*5 {
		t.Fatalf("%d transactions and %d postings, want 501 and %d", txs, postings, 501*5)
	}
}
