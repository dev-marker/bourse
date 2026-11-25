// Command publisher follows the engine's journal, and publishes every event to Kafka, on the executions
// topic. It runs next to the engine, and can stop and start again at any time.
package main

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/fiston-user/bourse/internal/engine"
	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/publish"
)

const topic = "executions"

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("publisher failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), // we choose each record's partition
		kgo.ProducerLinger(0),                          // send now: under load, batches fill up anyway
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := publish.EnsureTopic(ctx, cl, topic); err != nil {
		return err
	}
	marks, err := publish.Marks(ctx, cl, topic)
	if err != nil {
		return err
	}
	follow := engine.Follow(cmp.Or(os.Getenv("DATA_DIR"), "var"), market.Listed...)
	defer follow.Close()
	p := publish.New(follow, cl, topic, marks)

	start := time.Now()
	n, err := p.CatchUp(ctx)
	if err != nil {
		return err
	}
	took := time.Since(start)
	log.Info("caught up", "published", n, "took", took.Round(time.Millisecond),
		"per_second", int(float64(n)/max(took.Seconds(), 0.001)))
	err = p.Run(ctx)
	// What's still buffered goes out before we stop. Whatever doesn't make it goes out after a restart.
	flush, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = errors.Join(err, cl.Flush(flush))
	log.Info("publisher stopped")
	return err
}
