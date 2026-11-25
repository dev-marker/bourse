package marketdata

import (
	"context"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
)

// Follow applies every execution on the topic, from the very first one, until ctx is done. The client
// must consume the topic from its start. Once Follow has read what the topic had when it began, it
// calls ready, once, with how many executions that was.
func (f *Feed) Follow(ctx context.Context, cl *kgo.Client, topic string, ready func(n int)) error {
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return err
	}
	behind := map[int32]int64{} // the partitions we haven't read to their end yet
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			behind[o.Partition] = o.Offset
		}
	})
	n := 0
	for {
		if ready != nil && len(behind) == 0 {
			ready(n)
			ready = nil
		}
		fetches := cl.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			return err
		}
		var xs []*boursev1.Execution
		for r := range fetches.RecordsAll() {
			x := new(boursev1.Execution)
			if err := proto.Unmarshal(r.Value, x); err != nil {
				return err
			}
			xs = append(xs, x)
			if end, ok := behind[r.Partition]; ok && r.Offset+1 >= end {
				delete(behind, r.Partition)
			}
		}
		if err := f.Apply(xs); err != nil {
			return err
		}
		n += len(xs)
	}
}
