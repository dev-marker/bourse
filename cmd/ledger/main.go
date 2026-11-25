// Command ledger keeps the exchange's books. It reads every execution and every deposit from Kafka, and
// books them in the ledger database.
//
//	ledger                                 run the ledger
//	ledger deposit alice USD 100000.00     cash or shares into an account, from outside the exchange
//	ledger check                           prove the books balance, and agree with the accounts
package main

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/fiston-user/bourse/internal/ledger"
	"github.com/fiston-user/bourse/internal/migrate"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log, os.Args[1:]); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("ledger failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	url := cmp.Or(os.Getenv("DATABASE_URL"), "postgres://bourse:bourse@localhost:5432/ledger")
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := migrate.Up(ctx, db, ledger.Migrations); err != nil {
		return err
	}
	kafka := kgo.SeedBrokers(cmp.Or(os.Getenv("KAFKA"), "localhost:9092"))
	if len(args) > 0 && args[0] == "check" {
		return check(ctx, db)
	}
	if len(args) > 0 && args[0] == "deposit" {
		return deposit(ctx, kafka, args[1:])
	}

	cl, err := kgo.NewClient(kafka,
		kgo.ConsumerGroup("ledger"),
		kgo.InstanceID("ledger-1"), // after a crash, a restart takes its old place at once
		kgo.ConsumeTopics(ledger.Executions, ledger.Deposits),
		kgo.DisableAutoCommit(), // we commit, once the database has what we read
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := ledger.EnsureTopics(ctx, cl); err != nil {
		return err
	}
	log.Info("ledger ready")
	err = ledger.Consume(ctx, cl, db, ledger.Book, log)
	log.Info("ledger stopped")
	return err
}
