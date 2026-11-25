// Command marketdata serves the exchange's market data on :8081: every book's depth, the trades and the
// candles, rebuilt from the executions topic, over WebSocket and plain HTTP.
package main

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/marketdata"
)

const topic = "executions"

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("marketdata failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cmp.Or(os.Getenv("KAFKA"), "localhost:9092")),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // every execution there ever was
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	feed := marketdata.New(log, market.Listed...)

	// The books are rebuilt from the whole topic before the port opens, so nobody sees half a book.
	start := time.Now()
	caughtUp := make(chan struct{})
	followed := make(chan error, 1)
	go func() {
		followed <- feed.Follow(ctx, cl, topic, func(n int) {
			took := time.Since(start)
			log.Info("caught up", "applied", n, "took", took.Round(time.Millisecond),
				"per_second", int(float64(n)/max(took.Seconds(), 0.001)))
			close(caughtUp)
		})
	}()
	select {
	case <-caughtUp:
	case err := <-followed:
		return err
	}

	srv := &http.Server{Addr: ":8081", Handler: feed.Handler(),
		BaseContext: func(net.Listener) context.Context { return ctx }} // a stop ends every stream
	served := make(chan error, 1)
	go func() { served <- srv.ListenAndServe() }()
	log.Info("marketdata ready", "addr", srv.Addr)
	select {
	case err = <-followed:
	case err = <-served:
	case <-ctx.Done():
	}
	shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = errors.Join(err, srv.Shutdown(shut))
	log.Info("marketdata stopped")
	return err
}
