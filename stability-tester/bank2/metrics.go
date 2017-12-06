package main

import "github.com/prometheus/client_golang/prometheus"

var (
	largeScaleBuckets   = []float64{0.5, 1, 5, 10, 20, 30, 40, 50, 60}
	bank2VerifyDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "bank2_verify_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of bank2 verification.",
			Buckets:   largeScaleBuckets,
		})

	bank2VerifyFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "bank2_verify_failed_total",
			Help:      "Counter of failed bank2 verification.",
		})
)

func init() {
	prometheus.MustRegister(bank2VerifyDuration)
	prometheus.MustRegister(bank2VerifyFailedCounter)
}
