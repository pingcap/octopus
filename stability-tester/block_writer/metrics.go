package main

import "github.com/prometheus/client_golang/prometheus"

var (
	blockBatchWriteDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "block_batch_write_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of block batch write.",
		})

	blockWriteFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "block_write_failed_total",
			Help:      "Counter of failed block write.",
		})
)

func init() {
	prometheus.MustRegister(blockBatchWriteDuration)
	prometheus.MustRegister(blockWriteFailedCounter)
}
