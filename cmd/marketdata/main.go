// Command marketdata serves the exchange's market data on :8081: every book's depth, the trades and the
// candles, rebuilt from the executions topic, over WebSocket and plain HTTP.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/fiston-user/bourse/internal/config"
	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/marketdata"
	"github.com/fiston-user/bourse/internal/obs"
	"github.com/fiston-user/bourse/web"
)

const topic = "executions"

// settings is everything marketdata can be told, from its environment: marketdata -h lists it.
type settings struct {
	Addr  string
	Kafka string
}

func load(env *config.Env) settings {
	return settings{
		Addr:  env.String("ADDR", ":8081", "the stream, the books, and the live page"),
		Kafka: env.String("KAFKA", "localhost:9092", "the Kafka brokers"),
	}
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	env := config.New(os.LookupEnv)
	cfg := load(env)
	env.Check("marketdata", os.Args[1:])
	if err := run(log, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("marketdata failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, cfg settings) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Kafka),
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

	// The feed's routes all start with /v1/. Everything else is the live page.
	mux := http.NewServeMux()
	mux.Handle("GET /v1/", feed.Handler())
	mux.Handle("GET /", web.Handler())
	mux.Handle("GET /metrics", promhttp.Handler())
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "bourse_marketdata_clients",
		Help: "WebSocket clients following a symbol, on every symbol.",
	}, func() float64 {
		n := 0
		for _, in := range market.Listed {
			n += feed.Clients(in.Symbol)
		}
		return float64(n)
	})
	srv := &http.Server{Addr: cfg.Addr, Handler: mux,
		BaseContext: func(net.Listener) context.Context { return ctx }} // a stop ends every stream
	served := make(chan error, 1)
	go func() { served <- srv.ListenAndServe() }()
	log.Info("marketdata ready", "addr", srv.Addr, "version", obs.Version)
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
