package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
)

var (
	requests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bourse_gateway_requests_total",
		Help: "Requests the gateway answered, by route and status code.",
	}, []string{"route", "code"})
	latency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "bourse_gateway_request_duration_seconds",
		Help:    "How long the gateway took to answer, by route.",
		Buckets: prometheus.ExponentialBucketsRange(0.0005, 5, 24), // half a millisecond to five seconds
	}, []string{"route"})
	limited = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bourse_gateway_rate_limited_total",
		Help: "Requests turned away with a 429.",
	})
)

// tracer starts the gateway's own spans, inside the request's.
var tracer = otel.Tracer("bourse/gateway")
