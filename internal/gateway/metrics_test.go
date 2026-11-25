package gateway_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/fiston-user/bourse/internal/ratelimit"
)

// TestMetrics sends an order, and then scrapes the metrics the way Prometheus does. The order is
// counted under its route's pattern, never its path, and its latency is in the histogram.
func TestMetrics(t *testing.T) {
	url, _, _, c := start(t, ratelimit.New(100, 100))
	if res, body := post(t, c, url, order); res.StatusCode != http.StatusCreated {
		t.Fatalf("status %d: %s", res.StatusCode, body)
	}
	srv := httptest.NewServer(promhttp.Handler())
	defer srv.Close()
	res, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	for _, want := range []string{
		`bourse_gateway_requests_total{code="201",route="POST /v1/orders"}`,
		`bourse_gateway_request_duration_seconds_count{route="POST /v1/orders"}`,
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("the metrics have no %s", want)
		}
	}
}
