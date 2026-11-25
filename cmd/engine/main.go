// Command engine runs the matching engine, with its gRPC API on :9083 and health checks on :8083.
//
//	engine                   run the engine
//	engine backup /backup    copy the snapshot and every sealed journal to a backup
//	engine restore /backup   restore a backup into an empty DATA_DIR
//	engine check             replay every sealed journal, and compare the books with the snapshot
//	engine drill /backup     a restore drill: restore, then check
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/config"
	"github.com/fiston-user/bourse/internal/engine"
	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/obs"
)

// settings is everything the engine can be told, from its environment: engine -h lists it.
type settings struct {
	DataDir       string
	GRPCAddr      string
	AdminAddr     string
	SnapshotEvery int
}

func load(env *config.Env) settings {
	return settings{
		DataDir:       env.String("DATA_DIR", "var", "where the journal and the snapshots are kept"),
		GRPCAddr:      env.String("GRPC_ADDR", ":9083", "the engine's API, for the gateway"),
		AdminAddr:     env.String("ADMIN_ADDR", ":8083", "health checks, metrics and profiles"),
		SnapshotEvery: env.Int("SNAPSHOT_EVERY", 100_000, "inputs between two snapshots"),
	}
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	env := config.New(os.LookupEnv)
	cfg := load(env)
	env.Check("engine", os.Args[1:])
	var err error
	if len(os.Args) > 1 {
		err = tool(cfg, os.Args[1:])
	} else {
		err = run(log, cfg)
	}
	if err != nil {
		log.Error("engine failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, cfg settings) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	gl, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}
	hl, err := net.Listen("tcp", cfg.AdminAddr)
	if err != nil {
		return err
	}

	stopTracing, err := obs.Tracing(ctx, "engine", 0)
	if err != nil {
		return err
	}
	defer stopTracing(context.Background())

	var ready atomic.Bool
	hs := obs.Admin(cfg.AdminAddr, health(&ready))
	errs := make(chan error, 2)
	go func() { errs <- hs.Serve(hl) }()

	eng, err := engine.New(cfg.DataDir, market.Listed...)
	if err != nil {
		return err
	}
	eng.SnapshotEvery(uint64(cfg.SnapshotEvery))
	rec := eng.Recovery()
	if rec.Cut > 0 {
		log.Warn("cut a torn tail off the journal", "bytes", rec.Cut)
	}
	log.Info("recovered", "snapshot", rec.Snapshot, "replayed", rec.Replayed,
		"took", rec.Took.Round(time.Millisecond))
	engineCtx, stopEngine := context.WithCancel(context.Background())
	engineDone := make(chan error, 1)
	go func() { engineDone <- eng.Run(engineCtx) }()

	gs := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	boursev1.RegisterEngineServiceServer(gs, &server{engine: eng})
	reflection.Register(gs)

	go func() { errs <- gs.Serve(gl) }()
	ready.Store(true)
	log.Info("engine ready", "grpc", cfg.GRPCAddr, "admin", cfg.AdminAddr, "version", obs.Version)

	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	log.Info("shutting down")
	start := time.Now()
	// Not ready any more, so nothing new is sent here. The calls in flight finish, so their orders are
	// journaled and answered. Then the engine stops, with a snapshot of every book on the way out.
	ready.Store(false)
	gs.GracefulStop()
	stopEngine()
	err = errors.Join(err, <-engineDone)
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = errors.Join(err, hs.Shutdown(shutdown))
	log.Info("engine stopped", "snapshot", eng.Snapshot(), "took", time.Since(start).Round(time.Millisecond))
	return err
}

// tool runs one of the engine's commands for the people who run it: backup, restore or check.
func tool(cfg settings, args []string) error {
	start := time.Now()
	took := func() time.Duration { return time.Since(start).Round(time.Millisecond) }
	switch {
	case len(args) == 2 && args[0] == "backup":
		b, err := engine.Backup(cfg.DataDir, args[1])
		if err != nil {
			return err
		}
		fmt.Printf("the snapshot at input %d, state %08x, and %s, %d new: %s in %v\n",
			b.Snapshot, b.State, count(b.Journals, "journal"), b.New, size(b.Bytes), took())
	case len(args) == 2 && (args[0] == "restore" || args[0] == "drill"):
		b, err := engine.Restore(args[1], cfg.DataDir)
		if err != nil {
			return err
		}
		fmt.Printf("restored the snapshot and %s, %s in %v, every checksum right\n",
			count(b.Journals, "journal"), size(b.Bytes), took())
		if args[0] == "drill" {
			return tool(cfg, []string{"check"})
		}
	case len(args) == 1 && args[0] == "check":
		c, err := engine.Check(cfg.DataDir, market.Listed...)
		if err != nil {
			return err
		}
		fmt.Printf("replayed %d inputs from %s in %v: the snapshot's books exactly, state %08x\n",
			c.Inputs, count(c.Journals, "journal"), took(), c.State)
	default:
		return errors.New("usage: engine [backup DIR | restore DIR | check | drill DIR]")
	}
	return nil
}

// size shows a number of bytes the way a person reads it.
func size(n int64) string {
	return fmt.Sprintf("%.1f MB", float64(n)/1e6)
}

// count says how many of a thing there are: 1 journal, 2 journals.
func count(n int, thing string) string {
	if n == 1 {
		return "1 " + thing
	}
	return fmt.Sprintf("%d %ss", n, thing)
}

// health answers readiness checks. Admin adds the liveness check, /healthz, to every service.
func health(ready *atomic.Bool) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ready")
	})
	return mux
}
