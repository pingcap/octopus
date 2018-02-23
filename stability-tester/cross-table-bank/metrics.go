package main

import "github.com/prometheus/client_golang/prometheus"

var (
	CrossTableBankVerifyDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "cross_table_bank_verify_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of cross_table_bank verification.",
			Buckets:   []float64{0.5, 1, 5, 10, 20, 30, 40, 50, 60},
		})

	CrossTableBankVerifyFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "cross_table_bank_verify_failed_total",
			Help:      "Counter of failed cross_table_bank verification.",
		})

	CrossTableBankTxnDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "cross_table_bank_txn_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of cross_table_bank transcation.",
		})

	CrossTableBankTxnFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "cross_table_bank_txn_total",
			Help:      "Counter of failed cross_table_bank transcation.",
		})
)

func init() {
	prometheus.MustRegister(CrossTableBankVerifyDuration)
	prometheus.MustRegister(CrossTableBankVerifyFailedCounter)
	prometheus.MustRegister(CrossTableBankTxnDuration)
	prometheus.MustRegister(CrossTableBankTxnFailedCounter)
}
