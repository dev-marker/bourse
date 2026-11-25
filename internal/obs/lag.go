package obs

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
)

var lag = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "bourse_consumer_lag_records",
	Help: "Records on a partition that the consumer hasn't read yet.",
}, []string{"topic", "partition"})

// Lag sets how far behind each partition in the fetches the consumer is: the partition's end, minus
// the offset after the last record it got.
func Lag(fetches kgo.Fetches) {
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if len(p.Records) == 0 {
			return
		}
		next := p.Records[len(p.Records)-1].Offset + 1
		lag.WithLabelValues(p.Topic, strconv.Itoa(int(p.Partition))).Set(float64(p.HighWatermark - next))
	})
}
