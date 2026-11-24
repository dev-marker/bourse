// Command engine runs the matching engine, with its gRPC API on :9083 and health checks on :8083.
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

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/engine"
	"github.com/fiston-user/bourse/internal/market"
)

// dataDir is where the engine keeps what it must never lose.
const dataDir = "var"

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("engine failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	gl, err := net.Listen("tcp", ":9083")
	if err != nil {
		return err
	}
	hl, err := net.Listen("tcp", ":8083")
	if err != nil {
		return err
	}

	var ready atomic.Bool
	hs := &http.Server{Handler: health(&ready), ReadHeaderTimeout: 5 * time.Second}
	errs := make(chan error, 2)
	go func() { errs <- hs.Serve(hl) }()

	eng, err := engine.New(dataDir, market.Listed...)
	if err != nil {
		return err
	}
	rec := eng.Recovery()
	if rec.Cut > 0 {
		log.Warn("cut a torn tail off the journal", "bytes", rec.Cut)
	}
	log.Info("recovered", "snapshot", rec.Snapshot, "replayed", rec.Replayed,
		"took", rec.Took.Round(time.Millisecond))
	engineCtx, stopEngine := context.WithCancel(context.Background())
	engineDone := make(chan struct{})
	go func() {
		defer close(engineDone)
		eng.Run(engineCtx)
	}()

	gs := grpc.NewServer()
	boursev1.RegisterEngineServiceServer(gs, &server{engine: eng})
	reflection.Register(gs)

	go func() { errs <- gs.Serve(gl) }()
	ready.Store(true)
	log.Info("engine ready", "grpc", ":9083", "http", ":8083", "symbols", len(market.Listed))

	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	log.Info("shutting down")
	ready.Store(false)
	gs.GracefulStop()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = errors.Join(err, hs.Shutdown(shutdown))
	stopEngine()
	<-engineDone
	log.Info("engine stopped")
	return err
}

// health answers liveness and readiness checks.
func health(ready *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ready")
	})
	return mux
}
