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

import (
	"flag"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-operator/pkg/scheduler/server"
	"github.com/pingcap/tidb-operator/version"
)

var (
	logLevel     string
	printVersion bool
	port         int
	kubeconfig   string
)

func init() {
	flag.StringVar(&logLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	flag.BoolVar(&printVersion, "V", false, "Show version and quit")
	flag.BoolVar(&printVersion, "version", false, "Show version and quit")
	flag.IntVar(&port, "port", 10262, "The port that the tidb scheduler's http service runs on (default 10262)")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file, omit this if run in cluster")
	flag.Parse()

	log.SetLevelByString(logLevel)
	log.SetHighlighting(false)
}

func main() {
	if printVersion {
		version.PrintVersionInfo()
		os.Exit(0)
	}
	version.LogVersionInfo()

	wait.Forever(func() {
		server.StartServer(kubeconfig, port)
	}, 5*time.Second)
}
