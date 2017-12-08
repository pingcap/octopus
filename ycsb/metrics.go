// Copyright 2017 PingCAP, Inc.
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

package main

import "github.com/prometheus/client_golang/prometheus"

var (
	cmdCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "octopus",
			Subsystem: "ycsb",
			Name:      "cmd_total",
			Help:      "Counter of commands.",
		}, []string{"type"})

	cmdErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "octopus",
			Subsystem: "ycsb",
			Name:      "cmd_err_total",
			Help:      "Error counter of commands.",
		}, []string{"type"})

	cmdDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "octopus",
			Subsystem: "ycsb",
			Name:      "cmd_duration_seconds",
			Help:      "Duration of commands.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
		}, []string{"type"})
)

func init() {
	prometheus.MustRegister(cmdCounter)
	prometheus.MustRegister(cmdErrorCounter)
}
