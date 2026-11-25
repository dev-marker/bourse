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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/auth"
	"github.com/fiston-user/bourse/internal/gateway"
	"github.com/fiston-user/bourse/internal/ledger"
	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/migrate"
	"github.com/fiston-user/bourse/internal/money"
	"github.com/fiston-user/bourse/internal/ratelimit"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("gateway failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, env("DATABASE_URL", "postgres://bourse:bourse@localhost:5432/accounts"))
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

	conn, err := grpc.NewClient(env("ENGINE_ADDR", "localhost:9083"),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
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
		kgo.SeedBrokers(env("KAFKA", "localhost:9092")),
		kgo.ConsumerGroup("gateway"),
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

	srv := gateway.New(engine, keys, balances, ratelimit.New(10, 20), instruments, log)
	hs := &http.Server{Addr: ":8080", Handler: srv, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	errs := make(chan error, 2)
	go func() { errs <- hs.ListenAndServe() }()
	go func() { errs <- ledger.Consume(ctx, cl, db, balances.Apply, log) }()
	log.Info("gateway ready", "http", ":8080", "instruments", len(instruments))

	select {
	case <-ctx.Done():
	case err := <-errs:
		return err
	}
	log.Info("shutting down")
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := hs.Shutdown(shutdown); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	log.Info("gateway stopped")
	return nil
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

// env returns an environment variable, or def when it isn't set.
func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
