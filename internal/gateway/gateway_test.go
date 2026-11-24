package gateway_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	boursev1 "github.com/fiston-user/bourse/gen/bourse/v1"
	"github.com/fiston-user/bourse/internal/auth"
	"github.com/fiston-user/bourse/internal/gateway"
	"github.com/fiston-user/bourse/internal/market"
	"github.com/fiston-user/bourse/internal/ratelimit"
)

var glbx = market.Instrument{Symbol: "GLBX", Decimals: 2, TickSize: 5, LotSize: 10}

// engine stands in for the real one. It counts the orders it gets, and accepts every one.
type engine struct {
	boursev1.EngineServiceClient // only the method the gateway calls is here
	mu                           sync.Mutex
	orders                       int
}

func (e *engine) SubmitOrder(_ context.Context, req *boursev1.SubmitOrderRequest,
	_ ...grpc.CallOption) (*boursev1.SubmitOrderResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.orders++
	id := uint64(e.orders)
	accepted := &boursev1.Event{Seq: id, Kind: boursev1.EventKind_EVENT_KIND_ACCEPTED, OrderId: id,
		Price: req.GetPrice(), Qty: req.GetQty()}
	return &boursev1.SubmitOrderResponse{OrderId: id, Events: []*boursev1.Event{accepted}}, nil
}

// keys knows one API key, alice's.
type keys map[string]auth.Key

func (k keys) Lookup(_ context.Context, id string) (auth.Key, error) {
	if key, ok := k[id]; ok {
		return key, nil
	}
	return auth.Key{}, auth.ErrUnknownKey
}

var alice = auth.Key{ID: "ak_ALICE", Account: "alice", Secret: "DEMO-SECRET-FOR-TESTS-ONLY"}

// start runs a gateway with these limits. It returns its address, its engine, and a client signing as alice.
func start(t *testing.T, limits *ratelimit.Limiter) (string, *engine, *http.Client) {
	eng := &engine{}
	log := slog.New(slog.DiscardHandler)
	gw := gateway.New(eng, keys{alice.ID: alice}, limits, []market.Instrument{glbx}, log)
	srv := httptest.NewServer(gw)
	t.Cleanup(srv.Close)
	return srv.URL, eng, &http.Client{Transport: &auth.Transport{Key: alice.ID, Secret: alice.Secret}}
}

// post sends an order, with these headers, and returns the response and its body.
func post(t *testing.T, c *http.Client, url, body string, header ...string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest("POST", url+"/v1/orders", strings.NewReader(body))
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	res, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res, string(b)
}

const order = `{"client_order_id":"a-1","symbol":"GLBX","side":"buy","price":"101.25","qty":300}`

// TestOrders sends one good order, and six that must never reach the engine.
func TestOrders(t *testing.T) {
	url, eng, c := start(t, ratelimit.New(100, 100))
	stale := strconv.FormatInt(time.Now().Add(-time.Minute).Unix(), 10)
	tests := []struct {
		name   string
		client *http.Client
		body   string
		header []string
		want   int
	}{
		{"signed", c, order, nil, http.StatusCreated},
		{"not signed", http.DefaultClient, order, nil, http.StatusUnauthorized},
		{"a minute old", c, order, []string{auth.TimestampHeader, stale}, http.StatusUnauthorized},
		{"off the tick", c, strings.Replace(order, "101.25", "101.27", 1), nil, http.StatusBadRequest},
		{"not a lot", c, strings.Replace(order, "300", "305", 1), nil, http.StatusBadRequest},
		{"no client order ID", c, strings.Replace(order, `"a-1"`, `""`, 1), nil, http.StatusBadRequest},
		{"an unknown word", c, strings.Replace(order, "qty", "quantity", 1), nil, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if res, body := post(t, tt.client, url, tt.body, tt.header...); res.StatusCode != tt.want {
				t.Fatalf("status %d, want %d: %s", res.StatusCode, tt.want, body)
			}
		})
	}
	if eng.orders != 1 {
		t.Fatalf("the engine got %d orders, want only the signed one", eng.orders)
	}
}

// TestRetry sends the same order ten times at once. The engine must see it once, and every answer must
// be the same. Then the same client order ID for another order is a conflict.
func TestRetry(t *testing.T) {
	url, eng, c := start(t, ratelimit.New(100, 100))
	bodies := make([]string, 10)
	var wg sync.WaitGroup
	for i := range bodies {
		wg.Go(func() { _, bodies[i] = post(t, c, url, order) })
	}
	wg.Wait()
	for _, b := range bodies[1:] {
		if b != bodies[0] {
			t.Fatalf("two answers to one order:\n%s\n%s", bodies[0], b)
		}
	}
	if eng.orders != 1 {
		t.Fatalf("the engine got %d orders, want 1", eng.orders)
	}
	res, _ := post(t, c, url, strings.Replace(order, "300", "400", 1))
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("another order with the same client order ID: status %d, want 409", res.StatusCode)
	}
}

// TestRateLimit allows two orders at once, and one a second after that. The third is turned away.
func TestRateLimit(t *testing.T) {
	url, _, c := start(t, ratelimit.New(1, 2))
	for i := range 3 {
		res, body := post(t, c, url, strings.Replace(order, "a-1", "a-"+strconv.Itoa(i), 1))
		want, retry := http.StatusCreated, ""
		if i == 2 {
			want, retry = http.StatusTooManyRequests, "1"
		}
		if got := res.Header.Get("Retry-After"); res.StatusCode != want || got != retry {
			t.Fatalf("order %d: status %d, Retry-After %q: %s", i, res.StatusCode, got, body)
		}
	}
}
