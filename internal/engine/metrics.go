package engine

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	syncs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "bourse_engine_journal_sync_seconds",
		Help:    "How long each journal write and fsync took.",
		Buckets: prometheus.ExponentialBuckets(0.00025, 2, 13), // a quarter of a millisecond to a second
	})
	batches = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "bourse_engine_journal_batch_size",
		Help:    "Commands that shared one fsync.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 11), // 1 to 1024
	})
	orders = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "bourse_engine_orders_total",
		Help: "Orders and cancels the books took, by kind and by what happened.",
	}, []string{"kind", "result"})
)
