// Command loadgen puts the exchange under load through the gateway, the way its customers would: many
// accounts, signed requests, limit orders near the touch, IOC orders that take them, and cancels. It
// sends at a fixed rate whatever the answers are, and prints the latencies it measured.
//
//	loadgen -rate 1000 -for 30s        a thousand requests a second, for 30 seconds
//	loadgen -rate 500,1000,2000        a step at each rate, one line each
//	loadgen trace                      one order, traced from end to end
//
// Its accounts are the ones in tmp/load-*.env: make load-accounts makes them, and funds them.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fiston-user/bourse/internal/auth"
	"github.com/fiston-user/bourse/internal/bots"
	"github.com/fiston-user/bourse/internal/loadgen"
	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/obs"
)

const gateway = "http://localhost:8080"

// fair is where each symbol's orders go: limits a few ticks either side, IOCs a few ticks through.
var fair = map[string]market.Price{"ACME": 42_00, "GLBX": 101_00, "NOVA": 18_50}

func main() {
	rates := flag.String("rate", "1000", "requests a second; a list makes one step at each rate")
	length := flag.Duration("for", 20*time.Second, "how long each step lasts")
	warmup := flag.Duration("warmup", 5*time.Second, "how long to send at the first rate before measuring")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, *rates, *length, *warmup); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, rates string, length, warmup time.Duration) error {
	traders, err := load("tmp/load-*.env")
	if err != nil {
		return err
	}
	if flag.Arg(0) == "trace" {
		return trace(ctx, traders[0])
	}
	var steps []float64
	for _, s := range strings.Split(rates, ",") {
		r, err := strconv.ParseFloat(s, 64)
		if err != nil || r <= 0 {
			return fmt.Errorf("%q isn't a rate", s)
		}
		steps = append(steps, r)
	}
	fmt.Printf("%d accounts, %v a step, latency in ms from when each request was due\n",
		len(traders), length)
	loadgen.Open(ctx, steps[0], warmup, func(time.Time) { pick(traders).act(ctx) })
	fmt.Println("  rate/s  done/s     p50     p90     p99   p99.9     max  errors")
	for _, rate := range steps {
		if ctx.Err() != nil {
			break
		}
		s := &step{errs: map[string]int{}, start: time.Now()}
		loadgen.Open(ctx, rate, length, func(due time.Time) {
			err := pick(traders).act(ctx)
			s.record(time.Since(due), err)
		})
		fmt.Println(s.line(rate))
	}
	return nil
}

// step is what one rate measured.
type step struct {
	mu    sync.Mutex
	hist  loadgen.Histogram
	errs  map[string]int // by kind: a status code, or timeout
	start time.Time
	last  time.Time // when the last answer came in
}

func (s *step) record(took time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hist.Record(took)
	s.last = time.Now()
	if err != nil {
		s.errs[kind(err)]++
	}
}

// line is the step's row in the table.
func (s *step) line(rate float64) string {
	h := &s.hist
	done := float64(h.Count()) / s.last.Sub(s.start).Seconds()
	var errs []string
	total := 0
	for _, k := range slices.Sorted(maps.Keys(s.errs)) {
		errs, total = append(errs, fmt.Sprintf("%s:%d", k, s.errs[k])), total+s.errs[k]
	}
	return fmt.Sprintf("%8.0f %7.0f %7s %7s %7s %7s %7s %7d  %s", rate, done, ms(h.Quantile(0.5)),
		ms(h.Quantile(0.9)), ms(h.Quantile(0.99)), ms(h.Quantile(0.999)), ms(h.Max()), total,
		strings.Join(errs, " "))
}

// ms shows a latency in milliseconds, to three digits.
func ms(d time.Duration) string {
	v := float64(d) / float64(time.Millisecond)
	switch {
	case v < 10:
		return strconv.FormatFloat(v, 'f', 2, 64)
	case v < 100:
		return strconv.FormatFloat(v, 'f', 1, 64)
	default:
		return strconv.FormatFloat(v, 'f', 0, 64)
	}
}

// statusError is an answer that isn't a success.
type statusError int

func (e statusError) Error() string { return http.StatusText(int(e)) }

// kind names an error in the table: the status code, or what went wrong on the way.
func kind(err error) string {
	var se statusError
	switch {
	case errors.As(err, &se):
		return strconv.Itoa(int(se))
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "conn"
	}
}

// trader is one account, with the orders it has resting.
type trader struct {
	name    string
	http    *http.Client
	ids     atomic.Int64
	mu      sync.Mutex
	resting []order
}

// order is a resting order, so that the trader can cancel it.
type order struct {
	symbol string
	id     uint64
}

// maxResting is how many orders a trader leaves in the book. With that many, it cancels one.
const maxResting = 50

func pick(ts []*trader) *trader { return ts[rand.IntN(len(ts))] }

// act sends one request: a limit order near the touch, an IOC that takes one, or a cancel.
func (t *trader) act(ctx context.Context) error {
	in := market.Listed[rand.IntN(len(market.Listed))]
	t.mu.Lock()
	resting := len(t.resting)
	t.mu.Unlock()
	switch r := rand.Float64(); {
	case resting >= maxResting || resting > 0 && r < 0.3:
		return t.cancel(ctx)
	case r < 0.5:
		return t.place(ctx, in, "ioc", 3)
	default:
		return t.place(ctx, in, "gtc", -1-rand.IntN(5))
	}
}

// place sends an order on a random side, priced ticks through the fair value: a negative number of
// ticks keeps it away from the other side, so it rests.
func (t *trader) place(ctx context.Context, in market.Instrument, tif string, ticks int) error {
	side, sign := "buy", market.Price(1)
	if rand.IntN(2) == 0 {
		side, sign = "sell", -1
	}
	o := bots.Order{
		ClientOrderID: fmt.Sprintf("%s-%s-%d", t.name, started, t.ids.Add(1)),
		Symbol:        in.Symbol, Side: side, TimeInForce: tif,
		Price: in.FormatPrice(fair[in.Symbol] + sign*market.Price(ticks)*in.TickSize),
		Qty:   int64(in.LotSize) * int64(1+rand.IntN(10)),
	}
	var p bots.Placed
	if err := t.do(ctx, http.MethodPost, "/v1/orders", o, &p); err != nil {
		return err
	}
	if p.Rests() {
		t.mu.Lock()
		t.resting = append(t.resting, order{in.Symbol, p.OrderID})
		t.mu.Unlock()
	}
	return nil
}

// cancel takes a random resting order out of the book. One that has traded already is gone, and
// that's fine.
func (t *trader) cancel(ctx context.Context) error {
	t.mu.Lock()
	if len(t.resting) == 0 {
		t.mu.Unlock()
		return nil
	}
	i := rand.IntN(len(t.resting))
	o := t.resting[i]
	t.resting = slices.Delete(t.resting, i, i+1)
	t.mu.Unlock()
	err := t.do(ctx, http.MethodDelete, fmt.Sprintf("/v1/orders/%s/%d", o.symbol, o.id), nil, nil)
	if se := statusError(0); errors.As(err, &se) && se == http.StatusNotFound {
		return nil
	}
	return err
}

// do sends one request, and reads the whole answer.
func (t *trader) do(ctx context.Context, method, path string, in, out any) error {
	var body []byte
	if in != nil {
		body, _ = json.Marshal(in)
	}
	req, err := http.NewRequestWithContext(ctx, method, gateway+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	res, err := t.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode >= 300 {
		return statusError(res.StatusCode)
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

// started is in every client order ID, so a second run never reuses one.
var started = strconv.FormatInt(time.Now().Unix(), 36)

// load reads every account's key. All of them share one pool of connections: Go's default keeps
// only two idle connections to a host, and under load, every other request would open a new one.
func load(pattern string) ([]*trader, error) {
	files, _ := filepath.Glob(pattern)
	if len(files) == 0 {
		return nil, fmt.Errorf("no accounts in %s: make load-accounts makes them", pattern)
	}
	base := &http.Transport{MaxIdleConnsPerHost: 1000, MaxConnsPerHost: 1000}
	var ts []*trader
	for _, f := range files {
		key, err := readKey(f)
		if err != nil {
			return nil, err
		}
		sign := &auth.Transport{Key: key.ID, Secret: key.Secret, Base: base}
		ts = append(ts, &trader{name: strings.TrimSuffix(filepath.Base(f), ".env"),
			http: &http.Client{Transport: sign, Timeout: 10 * time.Second}})
	}
	return ts, nil
}

// readKey reads BOURSE_KEY and BOURSE_SECRET from a file.
func readKey(path string) (auth.Key, error) {
	f, err := os.Open(path)
	if err != nil {
		return auth.Key{}, err
	}
	defer f.Close()
	var key auth.Key
	for s := bufio.NewScanner(f); s.Scan(); {
		name, value, _ := strings.Cut(s.Text(), "=")
		switch name {
		case "BOURSE_KEY":
			key.ID = value
		case "BOURSE_SECRET":
			key.Secret = value
		}
	}
	return key, nil
}

// trace sends one IOC buy that asks to be traced, and says where its trace is.
func trace(ctx context.Context, t *trader) error {
	in := market.Listed[1] // GLBX
	o := bots.Order{ClientOrderID: fmt.Sprintf("%s-%s-trace", t.name, started), Symbol: in.Symbol,
		Side: "buy", TimeInForce: "ioc", Price: in.FormatPrice(fair[in.Symbol] + 3*in.TickSize), Qty: 100}
	body, _ := json.Marshal(o)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gateway+"/v1/orders",
		bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set(obs.TraceHeader, "1")
	start := time.Now()
	res, err := t.http.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	took := time.Since(start).Round(time.Microsecond)
	fmt.Printf("%s in %v\nhttp://localhost:16686/trace/%s\n", res.Status, took, res.Header.Get("Trace-Id"))
	return nil
}
