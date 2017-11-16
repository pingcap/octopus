package main

import "github.com/prometheus/client_golang/prometheus"

var (
	ddlFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "ddl_failed_total",
			Help:      "Counter of failed ddl operations.",
		})
)

func init() {
	prometheus.MustRegister(ddlFailedCounter)
}
