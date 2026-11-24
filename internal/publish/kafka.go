package publish

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
)

// EnsureTopic creates the topic with its fixed number of partitions, or checks the one that's there.
// Its records are kept forever: they're the exchange's record of every trade.
func EnsureTopic(ctx context.Context, cl *kgo.Client, topic string) error {
	adm := kadm.NewClient(cl)
	forever := map[string]*string{"retention.ms": new("-1")}
	_, err := adm.CreateTopic(ctx, Partitions, 1, forever, topic)
	if !errors.Is(err, kerr.TopicAlreadyExists) {
		return err
	}
	topics, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return err
	}
	if n := len(topics[topic].Partitions); n != Partitions {
		return fmt.Errorf("topic %s has %d partitions, and the publisher needs %d", topic, n, Partitions)
	}
	return nil
}

// Marks reads the last record of every partition: how far the publisher got before it stopped.
func Marks(ctx context.Context, cl *kgo.Client, topic string) (map[int32]Mark, error) {
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return nil, err
	}
	last := map[int32]kgo.Offset{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			last[o.Partition] = kgo.NewOffset().At(o.Offset - 1)
		}
	})
	marks := map[int32]Mark{}
	if len(last) == 0 {
		return marks, nil
	}
	c, err := kgo.NewClient(append(cl.Opts(),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: last}))...)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	for len(marks) < len(last) {
		fetches := c.PollFetches(ctx)
		if err := fetches.Err0(); err != nil {
			return nil, err
		}
		for r := range fetches.RecordsAll() {
			var x boursev1.Execution
			if err := proto.Unmarshal(r.Value, &x); err != nil {
				return nil, err
			}
			marks[r.Partition] = Mark{x.GetInput(), x.GetSeq()}
		}
	}
	return marks, nil
}
