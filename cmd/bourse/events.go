package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
)

const (
	kafka = "localhost:9092"
	topic = "executions"
)

// events reads the executions topic. On its own, it prints every new execution, one a line. With stats,
// it says how many arrive each second, and how late. With check, it reads the whole topic.
func events(mode string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if mode == "check" {
		return check(ctx)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafka), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	if err != nil {
		return err
	}
	defer cl.Close()
	fmt.Println("following", topic, "from its end")
	var late []time.Duration
	since := time.Now()
	for {
		poll, cancel := context.WithDeadline(ctx, since.Add(time.Second)) // so stats print every second
		fetches := cl.PollFetches(poll)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		for r := range fetches.RecordsAll() {
			var x boursev1.Execution
			if err := proto.Unmarshal(r.Value, &x); err != nil {
				return err
			}
			if mode == "stats" {
				late = append(late, time.Since(r.Timestamp))
				continue
			}
			fmt.Printf("%s  %v\n", execution(&x), time.Since(r.Timestamp).Round(100*time.Microsecond))
		}
		if time.Since(since) < time.Second {
			continue
		}
		if mode == "stats" && len(late) > 0 {
			slices.Sort(late)
			fmt.Printf("%6d executions in a second, order to here p50 %v, p99 %v, max %v\n", len(late),
				pct(late, 0.5), pct(late, 0.99), pct(late, 1))
		}
		late, since = late[:0], time.Now()
	}
}

// pct is the p-th percentile of durations already sorted.
func pct(sorted []time.Duration, p float64) time.Duration {
	return sorted[int(p*float64(len(sorted)-1))].Round(100 * time.Microsecond)
}

// execution shows one execution on one line, symbol and seq first.
func execution(x *boursev1.Execution) string {
	kind := strings.ToLower(strings.TrimPrefix(x.GetKind().String(), "EVENT_KIND_"))
	side := ""
	if x.GetSide() != boursev1.Side_SIDE_UNSPECIFIED {
		side = strings.ToLower(strings.TrimPrefix(x.GetSide().String(), "SIDE_"))
	}
	s := fmt.Sprintf("%-4s seq %-5d %-9s order %-4d %-4s %-9s", x.GetSymbol(), x.GetSeq(), kind,
		x.GetOrderId(), side, x.GetAccount())
	if x.GetQty() != 0 {
		s += fmt.Sprintf(" %d", x.GetQty())
	}
	if p := x.GetPrice(); p != 0 {
		s += fmt.Sprintf(" @ %d.%02d", p/100, p%100) // every symbol we trade is priced in cents
	}
	if x.GetMakerId() != 0 {
		s += fmt.Sprintf("  maker %d %s", x.GetMakerId(), x.GetMakerAccount())
	}
	if x.GetReason() != "" {
		s += "  " + x.GetReason()
	}
	return s
}

// check reads the whole topic, and checks every symbol's seqs: from one, in order, with no gaps and no
// duplicates.
func check(ctx context.Context) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafka))
	if err != nil {
		return err
	}
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return err
	}
	from := map[int32]kgo.Offset{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			from[o.Partition] = kgo.NewOffset().AtStart()
		}
	})
	cl.AddConsumePartitions(map[string]map[int32]kgo.Offset{topic: from})

	type stream struct{ records, last, gaps, dups int }
	seen := map[string]*stream{}
	for len(from) > 0 {
		fetches := cl.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			return err
		}
		for r := range fetches.RecordsAll() {
			var x boursev1.Execution
			if err := proto.Unmarshal(r.Value, &x); err != nil {
				return err
			}
			s := seen[x.GetSymbol()]
			if s == nil {
				s = &stream{}
				seen[x.GetSymbol()] = s
			}
			s.records++
			switch seq := int(x.GetSeq()); {
			case seq <= s.last:
				s.dups++
			case seq > s.last+1:
				s.gaps += seq - s.last - 1
				s.last = seq
			default:
				s.last = seq
			}
			if end, _ := ends.Lookup(topic, r.Partition); r.Offset+1 >= end.Offset {
				delete(from, r.Partition)
			}
		}
	}
	bad := false
	for _, sym := range slices.Sorted(maps.Keys(seen)) {
		s := seen[sym]
		fmt.Printf("%s  %d records, seq 1 to %d: %d missing, %d duplicates\n", sym, s.records, s.last,
			s.gaps, s.dups)
		bad = bad || s.gaps > 0 || s.dups > 0
	}
	if bad {
		return fmt.Errorf("the topic has gaps or duplicates")
	}
	return nil
}
