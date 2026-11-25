// Command ledger keeps the exchange's books. It reads every execution and every deposit from Kafka, and
// books them in the ledger database.
//
//	ledger                                 run the ledger
//	ledger deposit alice USD 100000.00     cash or shares into an account, from outside the exchange
//	ledger check                           prove the books balance, and agree with the accounts
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/fiston-user/bourse/internal/config"
	"github.com/fiston-user/bourse/internal/ledger"
	"github.com/fiston-user/bourse/internal/migrate"
	"github.com/fiston-user/bourse/internal/obs"
)

// settings is everything the ledger can be told, from its environment: ledger -h lists it.
type settings struct {
	LedgerURL   string
	AccountsURL string
	Kafka       string
	InstanceID  string
	AdminAddr   string
}

func load(env *config.Env, args []string) settings {
	s := settings{
		LedgerURL:  env.Secret("LEDGER_URL", "the ledger database, with its password"),
		Kafka:      env.String("KAFKA", "localhost:9092", "the Kafka brokers"),
		InstanceID: env.String("INSTANCE_ID", "ledger-1", "this ledger's place in its consumer group"),
		AdminAddr:  env.String("ADMIN_ADDR", ":8082", "health checks, metrics and profiles"),
	}
	if len(args) > 0 && args[0] == "check" {
		s.AccountsURL = env.Secret("ACCOUNTS_URL", "the accounts database, to compare with")
	}
	return s
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	env := config.New(os.LookupEnv)
	cfg := load(env, os.Args[1:])
	env.Check("ledger", os.Args[1:])
	if err := run(log, cfg, os.Args[1:]); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("ledger failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, cfg settings, args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, cfg.LedgerURL)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := migrate.Up(ctx, db, ledger.Migrations); err != nil {
		return err
	}
	kafka := kgo.SeedBrokers(cfg.Kafka)
	if len(args) > 0 && args[0] == "check" {
		return check(ctx, db, cfg.AccountsURL)
	}
	if len(args) > 0 && args[0] == "deposit" {
		return deposit(ctx, kafka, args[1:])
	}

	cl, err := kgo.NewClient(kafka,
		kgo.ConsumerGroup("ledger"),
		kgo.InstanceID(cfg.InstanceID), // after a crash, a restart takes its old place at once
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
	admin := obs.Admin(cfg.AdminAddr, nil)
	go admin.ListenAndServe()
	defer admin.Close()
	log.Info("ledger ready", "admin", cfg.AdminAddr, "version", obs.Version)
	err = ledger.Consume(ctx, cl, db, ledger.Book, log)
	log.Info("ledger stopped")
	return err
}
