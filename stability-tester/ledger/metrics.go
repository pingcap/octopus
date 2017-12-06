package main

import "github.com/prometheus/client_golang/prometheus"

var (
	largeScaleBuckets = []float64{0.5, 1, 5, 10, 20, 30, 40, 50, 60}
	ledgerTxnDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "ledger_txn_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of ledger txn.",
			Buckets:   largeScaleBuckets,
		})

	ledgerVerifyFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "ledger_verify_failed_total",
			Help:      "Counter of failed ledger verification.",
		})

	ledgerVerifyDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "ledger_verify_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of ledger verification.",
			Buckets:   largeScaleBuckets,
		})
)

func init() {
	prometheus.MustRegister(ledgerVerifyDuration)
	prometheus.MustRegister(ledgerVerifyFailedCounter)
	prometheus.MustRegister(ledgerTxnDuration)
}
