// Package obs is how we see inside the exchange's services: profiles for pprof, metrics for
// Prometheus, and traces for OpenTelemetry.
package obs

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Admin is a service's private port, for the people who run it. It never goes on a public port: a
// profile shows how the code works, and costs CPU to take.
func Admin(addr string, mux *http.ServeMux) *http.Server {
	if mux == nil {
		mux = http.NewServeMux()
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

// Version is the build's version, stamped in by the image's build. Every service says it when it's ready.
var Version = "dev"
