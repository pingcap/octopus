package main

import "github.com/prometheus/client_golang/prometheus"

var (
	largeScaleBuckets = []float64{0.5, 1, 5, 10, 20, 30, 40, 50, 60}
	logDurationVec    = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "log_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of log operations.",
			Buckets:   largeScaleBuckets,
		}, []string{"action"})

	logFailedCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "log_write_total",
			Help:      "Counter of log operations.",
		}, []string{"action"})
)

func init() {
	prometheus.MustRegister(logFailedCounterVec)
	prometheus.MustRegister(logDurationVec)
}
