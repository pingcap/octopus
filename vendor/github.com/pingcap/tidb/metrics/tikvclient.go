// Copyright 2018 PingCAP, Inc.
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

package metrics

import "github.com/prometheus/client_golang/prometheus"

// TiKVClient metrics.
var (
	TiKVTxnCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_total",
			Help:      "Counter of created txns.",
		})

	TiKVSnapshotCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "snapshot_total",
			Help:      "Counter of snapshots.",
		})

	TiKVTxnCmdCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_cmd_total",
			Help:      "Counter of txn commands.",
		}, []string{"type"})

	TiKVTxnCmdHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_cmd_seconds",
			Help:      "Bucketed histogram of processing time of txn cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
		}, []string{"type"})

	TiKVBackoffCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "backoff_count",
			Help:      "Counter of backoff.",
		}, []string{"type"})

	TiKVBackoffHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "backoff_seconds",
			Help:      "total backoff seconds of a single backoffer.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
		})

	TiKVConnPoolHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "get_conn_seconds",
			Help:      "Bucketed histogram of taking conn from conn pool.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
		}, []string{"type"})

	TiKVSendReqHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "request_seconds",
			Help:      "Bucketed histogram of sending request duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
		}, []string{"type", "store"})

	TiKVCoprocessorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "cop_count",
			Help:      "Counter of coprocessor actions.",
		}, []string{"type"})

	TiKVCoprocessorHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "cop_seconds",
			Help:      "Run duration of a single coprocessor task, includes backoff time.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
		})

	TiKVLockResolverCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "lock_resolver_actions_total",
			Help:      "Counter of lock resolver actions.",
		}, []string{"type"})

	TiKVRegionErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "region_err_total",
			Help:      "Counter of region errors.",
		}, []string{"type"})

	TiKVTxnWriteKVCountHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_write_kv_count",
			Help:      "Count of kv pairs to write in a transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 21),
		})

	TiKVTxnWriteSizeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_write_size",
			Help:      "Size of kv pairs to write in a transaction. (KB)",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 21),
		})

	TiKVRawkvCmdHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "rawkv_cmd_seconds",
			Help:      "Bucketed histogram of processing time of rawkv cmds.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
		}, []string{"type"})

	TiKVRawkvSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "rawkv_kv_size",
			Help:      "Size of key/value to put, in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 21),
		}, []string{"type"})

	TiKVTxnRegionsNumHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "txn_regions_num",
			Help:      "Number of regions in a transaction.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
		}, []string{"type"})

	TiKVLoadSafepointCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "load_safepoint_total",
			Help:      "Counter of load safepoint.",
		}, []string{"type"})

	TiKVSecondaryLockCleanupFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "lock_cleanup_task_total",
			Help:      "failure statistic of secondary lock cleanup task.",
		}, []string{"type"})

	TiKVRegionCacheCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "region_cache",
			Help:      "Counter of region cache.",
		}, []string{"type", "result_state"})
)

func init() {
	prometheus.MustRegister(TiKVTxnCounter)
	prometheus.MustRegister(TiKVSnapshotCounter)
	prometheus.MustRegister(TiKVTxnCmdHistogram)
	prometheus.MustRegister(TiKVBackoffCounter)
	prometheus.MustRegister(TiKVBackoffHistogram)
	prometheus.MustRegister(TiKVSendReqHistogram)
	prometheus.MustRegister(TiKVConnPoolHistogram)
	prometheus.MustRegister(TiKVCoprocessorCounter)
	prometheus.MustRegister(TiKVCoprocessorHistogram)
	prometheus.MustRegister(TiKVLockResolverCounter)
	prometheus.MustRegister(TiKVRegionErrorCounter)
	prometheus.MustRegister(TiKVTxnWriteKVCountHistogram)
	prometheus.MustRegister(TiKVTxnWriteSizeHistogram)
	prometheus.MustRegister(TiKVRawkvCmdHistogram)
	prometheus.MustRegister(TiKVRawkvSizeHistogram)
	prometheus.MustRegister(TiKVTxnRegionsNumHistogram)
	prometheus.MustRegister(TiKVLoadSafepointCounter)
	prometheus.MustRegister(TiKVSecondaryLockCleanupFailureCounter)
	prometheus.MustRegister(TiKVRegionCacheCounter)
}
