// Command gateway runs the exchange's front door: a REST API on :8080, in front of the engine.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/auth"
	"github.com/fiston-user/bourse/internal/config"
	"github.com/fiston-user/bourse/internal/gateway"
	"github.com/fiston-user/bourse/internal/ledger"
	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/migrate"
	"github.com/fiston-user/bourse/internal/money"
	"github.com/fiston-user/bourse/internal/obs"
	"github.com/fiston-user/bourse/internal/ratelimit"
)

// settings is everything the gateway can be told, from its environment: gateway -h lists it.
type settings struct {
	Addr, AdminAddr string
	AccountsURL     string
	EngineAddr      string
	Kafka           string
	InstanceID      string
	RateLimit       float64
	Drain           time.Duration
}

func load(env *config.Env) settings {
	return settings{
		Addr:        env.String("ADDR", ":8080", "the public API"),
		AdminAddr:   env.String("ADMIN_ADDR", ":8084", "metrics and profiles, never public"),
		AccountsURL: env.Secret("ACCOUNTS_URL", "the accounts database, with its password"),
		EngineAddr:  env.String("ENGINE_ADDR", "localhost:9083", "the engine's API"),
		Kafka:       env.String("KAFKA", "localhost:9092", "the Kafka brokers"),
		InstanceID:  env.String("INSTANCE_ID", "gateway-1", "this gateway's place in its consumer group"),
		RateLimit:   env.Float("RATE_LIMIT", 10, "requests a second for each account"),
		Drain:       env.Duration("DRAIN", 10*time.Second, "how long requests in flight get to finish"),
	}
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	env := config.New(os.LookupEnv)
	cfg := load(env)
	env.Check("gateway", os.Args[1:])
	if err := run(log, cfg); err != nil {
		log.Error("gateway failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, cfg settings) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, cfg.AccountsURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := migrate.Up(ctx, db, auth.Migrations); err != nil {
		return err
	}
	if err := migrate.Up(ctx, db, money.Migrations); err != nil {
		return err
	}
	keys, balances := auth.NewStore(db), money.NewStore(db)
	if len(os.Args) == 3 && os.Args[1] == "key" { // gateway key ACCOUNT: a new API key, printed once
		k, err := keys.Create(ctx, os.Args[2])
		if err != nil {
			return err
		}
		fmt.Printf("BOURSE_KEY=%s\nBOURSE_SECRET=%s\n", k.ID, k.Secret)
		return nil
	}

	stopTracing, err := obs.Tracing(ctx, "gateway", 0.01)
	if err != nil {
		return err
	}
	defer stopTracing(context.Background())

	conn, err := grpc.NewClient(cfg.EngineAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		// While the engine restarts, a call waits for it, within its deadline, instead of failing.
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))
	if err != nil {
		return err
	}
	defer conn.Close()
	engine := boursev1.NewEngineServiceClient(conn)
	instruments, err := listInstruments(ctx, engine)
	if err != nil {
		return err
	}

	// The stream settles what orders did: trades, cancels, rejections, and deposits.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Kafka),
		kgo.ConsumerGroup("gateway"),
		kgo.InstanceID(cfg.InstanceID), // a restart takes its old place at once, with no rebalance
		kgo.ConsumeTopics(ledger.Executions, ledger.Deposits),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ledger.EnsureTopics(ctx, cl); err != nil {
		return err
	}

	// Each account may send RateLimit requests a second, and twice as many at once.
	cached := auth.NewCache(keys, time.Minute)
	limits := ratelimit.New(cfg.RateLimit, 2*cfg.RateLimit)
	srv := gateway.New(engine, cached, balances, limits, instruments, log)
	traced := obs.Ask(otelhttp.NewHandler(obs.Traced(srv), "gateway", otelhttp.WithPublicEndpointFn(public)))
	hs := &http.Server{Addr: cfg.Addr, Handler: traced, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	admin := obs.Admin(cfg.AdminAddr, nil)
	errs, consumed := make(chan error, 2), make(chan error, 1)
	go func() { errs <- hs.ListenAndServe() }()
	go func() { errs <- admin.ListenAndServe() }()
	consuming, stopConsuming := context.WithCancel(context.Background())
	defer stopConsuming()
	go func() { consumed <- ledger.Consume(consuming, cl, db, balances.Apply, log) }()
	log.Info("gateway ready", "http", cfg.Addr, "admin", cfg.AdminAddr, "version", obs.Version)

	select {
	case <-ctx.Done():
	case err = <-errs:
	case err = <-consumed:
		return err
	}
	log.Info("shutting down")
	// No new connections, and the requests in flight get Drain to finish, the engine's answers included.
	drain, cancel := context.WithTimeout(context.Background(), cfg.Drain)
	defer cancel()
	err = errors.Join(err, hs.Shutdown(drain), admin.Shutdown(drain))
	// Then the consumer stops, once the batch it's on is applied and committed.
	stopConsuming()
	if cerr := <-consumed; !errors.Is(cerr, context.Canceled) {
		err = errors.Join(err, cerr)
	}
	log.Info("gateway stopped")
	return err
}

// listInstruments asks the engine what it trades. The engine may still be starting, so it waits for
// it, for up to half a minute.
func listInstruments(ctx context.Context, engine boursev1.EngineServiceClient) ([]market.Instrument, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	res, err := engine.ListInstruments(ctx, &boursev1.ListInstrumentsRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return nil, fmt.Errorf("asking the engine what it trades: %w", err)
	}
	var out []market.Instrument
	for _, in := range res.GetInstruments() {
		out = append(out, market.Instrument{Symbol: in.GetSymbol(), Decimals: int(in.GetDecimals()),
			TickSize: market.Price(in.GetTickSize()), LotSize: market.Qty(in.GetLotSize())})
	}
	return out, nil
}

// public says every request comes from outside: a trace a client started is linked, never joined, so
// nobody outside decides what we trace.
func public(*http.Request) bool { return true }
