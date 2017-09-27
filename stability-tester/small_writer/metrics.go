package main

import "github.com/prometheus/client_golang/prometheus"

var (
	smallWriteDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "small_data_write_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of small data batch write.",
		})

	smallWriteFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "small_data_write_failed_total",
			Help:      "Counter of failed small data write.",
		})
)

func init() {
	prometheus.MustRegister(smallWriteDuration)
	prometheus.MustRegister(smallWriteFailedCounter)
}
