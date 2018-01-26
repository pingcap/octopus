package main

import "github.com/prometheus/client_golang/prometheus"

var (
	bankVerifyDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "bank_verify_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of bank verification.",
			Buckets:   []float64{0.5, 1, 5, 10, 20, 30, 40, 50, 60},
		})

	bankVerifyFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "bank_verify_failed_total",
			Help:      "Counter of failed bank verification.",
		})

	bankTxnDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "bank_txn_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of bank transcation.",
		})

	bankTxnFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "bank_txn_total",
			Help:      "Counter of failed bank transcation.",
		})
)

func init() {
	prometheus.MustRegister(bankVerifyDuration)
	prometheus.MustRegister(bankVerifyFailedCounter)
	prometheus.MustRegister(bankTxnDuration)
	prometheus.MustRegister(bankTxnFailedCounter)
}
