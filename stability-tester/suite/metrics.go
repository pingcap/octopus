// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package suite

import "github.com/prometheus/client_golang/prometheus"

var (
	largeScaleBuckets = []float64{0.5, 1, 5, 10, 20, 30, 40, 50, 60}

	bankVerifyDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "bank_verify_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of bank verification.",
			Buckets:   largeScaleBuckets,
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

	bankMultVerifyDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "bank_mult_verify_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of bank verification.",
			Buckets:   largeScaleBuckets,
		})

	bankMultVerifyFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "bank_mult_verify_failed_total",
			Help:      "Counter of failed bank verification.",
		})

	bankMultTxnDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "bank_mult_txn_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of bank transcation.",
		})

	bankMultTxnFailedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb_test",
			Subsystem: "stability",
			Name:      "bank_mult_txn_total",
			Help:      "Counter of failed bank transcation.",
		})

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

	logDurationVec = prometheus.NewHistogramVec(
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
	prometheus.MustRegister(bankVerifyDuration)
	prometheus.MustRegister(bankVerifyFailedCounter)
	prometheus.MustRegister(bankTxnDuration)
	prometheus.MustRegister(bankTxnFailedCounter)
	prometheus.MustRegister(bank2VerifyDuration)
	prometheus.MustRegister(bank2VerifyFailedCounter)
	prometheus.MustRegister(ledgerTxnDuration)
	prometheus.MustRegister(ledgerVerifyFailedCounter)
	prometheus.MustRegister(ledgerVerifyDuration)
	prometheus.MustRegister(blockBatchWriteDuration)
	prometheus.MustRegister(blockWriteFailedCounter)
	prometheus.MustRegister(smallWriteDuration)
	prometheus.MustRegister(smallWriteFailedCounter)
	prometheus.MustRegister(logDurationVec)
	prometheus.MustRegister(logFailedCounterVec)
}
