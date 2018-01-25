package main

import "github.com/prometheus/client_golang/prometheus"

var (
	largeScaleBuckets = []float64{0.5, 1, 5, 10, 20, 30, 40, 50, 60}

	bank3VerifyDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "bank3_verify_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of bank3 verification.",
			Buckets:   largeScaleBuckets,
		})

	bank3VerifyFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "bank3_verify_failed_total",
			Help:      "Counter of failed bank3 verification.",
		})

	bank3TxnDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "bank3_txn_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of bank3 transcation.",
		})

	bank3TxnFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "bank3_txn_total",
			Help:      "Counter of failed bank3 transcation.",
		})
)

func init() {
	prometheus.MustRegister(bank3VerifyDuration)
	prometheus.MustRegister(bank3VerifyFailedCounter)
	prometheus.MustRegister(bank3TxnDuration)
	prometheus.MustRegister(bank3TxnFailedCounter)
}
