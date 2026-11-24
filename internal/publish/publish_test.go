package publish_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/engine"
	"github.com/fiston-user/bourse/internal/journal"
	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/publish"
)

// trade runs an engine on a folder of the test's own. Alice buys all three symbols, bob sells into her
// orders, and then the engine stops. It returns the folder, and every event the engine reported.
func trade(t *testing.T) (string, []engine.Event) {
	dir := t.TempDir()
	e, err := engine.New(dir, market.Listed...)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		e.Run(ctx)
		close(done)
	}()
	var events []engine.Event
	for i := range 60 {
		in := market.Listed[i%len(market.Listed)]
		o := market.Order{ClientID: fmt.Sprint("a-", i), Account: "alice", Symbol: in.Symbol,
			Side: market.Buy, Type: market.Limit, TIF: market.GTC, Qty: 100,
			Price: 100_00 + in.TickSize*market.Price(i%4)}
		if i%3 == 2 {
			o.ClientID, o.Account, o.Side = fmt.Sprint("b-", i), "bob", market.Sell
		}
		r, err := e.Submit(ctx, o)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, r.Events...)
	}
	stop()
	<-done
	return dir, events
}

// topic stands in for Kafka: it keeps every record it's given, in order.
type topic struct{ records []*kgo.Record }

func (t *topic) Produce(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	t.records = append(t.records, r)
	promise(r, nil)
}

func (t *topic) Flush(context.Context) error { return nil }

// publishAll publishes everything in dir's journal, after the marks, and returns the records.
func publishAll(t *testing.T, dir string, marks map[int32]publish.Mark) []*kgo.Record {
	out := &topic{}
	follow := engine.Follow(dir, market.Listed...)
	defer follow.Close()
	if _, err := publish.New(follow, out, "executions", marks).CatchUp(t.Context()); err != nil {
		t.Fatal(err)
	}
	return out.records
}

func decode(t *testing.T, r *kgo.Record) *boursev1.Execution {
	t.Helper()
	x := &boursev1.Execution{}
	if err := proto.Unmarshal(r.Value, x); err != nil {
		t.Fatal(err)
	}
	return x
}

// TestRecord turns a trade into a record, and reads it back the way a consumer would.
func TestRecord(t *testing.T) {
	at := time.Date(2026, 11, 25, 9, 30, 0, 0, time.UTC)
	entry := journal.Entry{Seq: 7, Time: at.UnixNano(), Symbol: "ACME"}
	ev := engine.Event{Seq: 12, Kind: engine.Traded, Order: 5, Maker: 3, Price: 101_25, Qty: 100,
		Account: "bob", ClientID: "b-1", Side: market.Sell, MakerAccount: "alice", MakerClientID: "a-1"}
	r, err := publish.Record("executions", entry, ev)
	if err != nil {
		t.Fatal(err)
	}
	if string(r.Key) != "ACME" || r.Partition != publish.Partition("ACME") || !r.Timestamp.Equal(at) {
		t.Fatalf("key %q, partition %d, time %v", r.Key, r.Partition, r.Timestamp)
	}
	want := &boursev1.Execution{Symbol: "ACME", Seq: 12, Input: 7, Time: entry.Time,
		Kind: boursev1.EventKind_EVENT_KIND_TRADED, OrderId: 5, Account: "bob", ClientId: "b-1",
		Side: boursev1.Side_SIDE_SELL, Price: 101_25, Qty: 100,
		MakerId: 3, MakerAccount: "alice", MakerClientId: "a-1"}
	if got := decode(t, r); !proto.Equal(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
}

// TestResume publishes everything once. Then it starts again as if the publisher had crashed after its
// first 40 records, and Kafka had kept just those. The second run must send exactly the records after
// them: nothing missing, and nothing twice.
func TestResume(t *testing.T) {
	dir, events := trade(t)
	all := publishAll(t, dir, nil)
	if len(all) != len(events) {
		t.Fatalf("%d records, but the engine reported %d events", len(all), len(events))
	}
	marks := map[int32]publish.Mark{}
	for _, r := range all[:40] {
		x := decode(t, r)
		marks[r.Partition] = publish.Mark{Input: x.GetInput(), Seq: x.GetSeq()}
	}
	rest := publishAll(t, dir, marks)
	if len(rest) != len(all)-40 {
		t.Fatalf("after a restart, %d records, want %d", len(rest), len(all)-40)
	}
	for i, r := range rest {
		if !bytes.Equal(r.Value, all[40+i].Value) {
			t.Fatalf("record %d after the restart is %v, want %v", i, decode(t, r), decode(t, all[40+i]))
		}
	}
}

// TestKafka publishes to the real Kafka, on a topic of its own, and then starts again. The second run
// must find everything already out and send nothing, and the topic must have one record per event.
func TestKafka(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := cl.Ping(ctx); err != nil {
		t.Skipf("Kafka isn't running (make up starts it): %v", err)
	}
	name := fmt.Sprint("executions-test-", time.Now().UnixNano())
	if err := publish.EnsureTopic(ctx, cl, name); err != nil {
		t.Fatal(err)
	}
	adm := kadm.NewClient(cl)
	t.Cleanup(func() { adm.DeleteTopic(context.Background(), name) })

	dir, events := trade(t)
	for run := range 2 {
		marks, err := publish.Marks(ctx, cl, name)
		if err != nil {
			t.Fatal(err)
		}
		follow := engine.Follow(dir, market.Listed...)
		n, err := publish.New(follow, cl, name, marks).CatchUp(ctx)
		follow.Close()
		if err != nil {
			t.Fatal(err)
		}
		if want := []int{len(events), 0}[run]; n != want {
			t.Fatalf("run %d published %d records, want %d", run+1, n, want)
		}
	}
	ends, err := adm.ListEndOffsets(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	var records int64
	ends.Each(func(o kadm.ListedOffset) { records += o.Offset })
	if records != int64(len(events)) {
		t.Fatalf("%d records on the topic, want %d, one per event", records, len(events))
	}
}
