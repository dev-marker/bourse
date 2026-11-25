// Package gateway is the exchange's front door: a REST API that checks every request before the engine
// sees it.
package gateway

import (
	"cmp"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/auth"
	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/money"
	"github.com/fiston-user/bourse/internal/ratelimit"
)

// Keys finds the API key a request says it's signed with.
type Keys interface {
	Lookup(ctx context.Context, id string) (auth.Key, error)
}

// Money holds what orders may spend. What they do with it comes from the stream, not from here.
type Money interface {
	Hold(ctx context.Context, o market.Order) error
	Release(ctx context.Context, account, clientID string) error
	Balances(ctx context.Context, account string) ([]money.Balance, error)
}

// Server is the REST API. It's an http.Handler.
type Server struct {
	engine      boursev1.EngineServiceClient
	keys        Keys
	money       Money
	limits      *ratelimit.Limiter
	replies     *replies
	instruments map[string]market.Instrument
	list        []market.Instrument // the same, sorted by symbol
	log         *slog.Logger
	now         func() time.Time
	mux         *http.ServeMux
}

// New returns the API for an engine that trades these instruments.
func New(engine boursev1.EngineServiceClient, keys Keys, money Money, limits *ratelimit.Limiter,
	instruments []market.Instrument, log *slog.Logger) *Server {
	s := &Server{
		engine:      engine,
		keys:        keys,
		money:       money,
		limits:      limits,
		replies:     newReplies(10 * time.Minute),
		instruments: map[string]market.Instrument{},
		list:        instruments,
		log:         log,
		now:         time.Now,
		mux:         http.NewServeMux(),
	}
	for _, in := range instruments {
		s.instruments[in.Symbol] = in
	}
	s.mux.HandleFunc("GET /v1/instruments", s.listInstruments)
	s.mux.Handle("POST /v1/orders", s.private(s.placeOrder))
	s.mux.Handle("DELETE /v1/orders/{symbol}/{id}", s.private(s.cancelOrder))
	s.mux.Handle("GET /v1/balances", s.private(s.listBalances))
	return s
}

// private is for routes that act for an account. The request must be signed, and within the account's
// limit.
func (s *Server) private(h http.HandlerFunc) http.Handler {
	return s.authenticate(s.limit(h))
}

// ServeHTTP hands the request to its route, and logs how it went.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := s.now()
	rec := &recorder{ResponseWriter: w, status: http.StatusOK}
	s.mux.ServeHTTP(rec, r)
	// The route is the pattern the request matched, never its path: an order ID in a label would
	// make a new time series for every order.
	route := cmp.Or(r.Pattern, "unmatched")
	requests.WithLabelValues(route, strconv.Itoa(rec.status)).Inc()
	latency.WithLabelValues(route).Observe(time.Since(start).Seconds())
	took := time.Since(start).Round(time.Microsecond)
	s.log.Debug(r.Method+" "+r.URL.Path, "status", rec.status, "took", took)
}

// recorder remembers the status a handler wrote, for the log.
type recorder struct {
	http.ResponseWriter
	status int
}

func (r *recorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// problem is how we answer with an error.
type problem struct {
	Error string `json:"error"`
}

// fail answers with an error.
func fail(w http.ResponseWriter, status int, msg string) {
	reply(w, status, problem{msg})
}

// reply answers with v as JSON, and this status.
func reply(w http.ResponseWriter, status int, v any) {
	status, body := encode(status, v)
	write(w, status, body)
}

// encode turns v into JSON, to answer with this status later.
func encode(status int, v any) (int, []byte) {
	b, _ := json.Marshal(v)
	return status, b
}

// write answers with a body that encode made.
func write(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body)
}
