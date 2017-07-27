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

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-operator/pkg/client"
	gcc "github.com/pingcap/tidb-operator/pkg/gc/controller"
	tcc "github.com/pingcap/tidb-operator/pkg/tidbcluster/controller"
	"github.com/pingcap/tidb-operator/pkg/tidbset/controller/pd"
	"github.com/pingcap/tidb-operator/pkg/tidbset/controller/tidb"
	"github.com/pingcap/tidb-operator/pkg/tidbset/controller/tikv"
	"github.com/pingcap/tidb-operator/pkg/tpr"
	"github.com/pingcap/tidb-operator/pkg/util/election"
	"github.com/pingcap/tidb-operator/pkg/util/election/resourcelock"
	"github.com/pingcap/tidb-operator/version"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
)

var (
	logLevel     string
	printVersion bool
	kubeconfig   string

	leaseDuration = 15 * time.Second
	renewDuration = 5 * time.Second
	retryPeriod   = 3 * time.Second
)

func init() {
	flag.StringVar(&logLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	flag.BoolVar(&printVersion, "V", false, "Show version and quit")
	flag.BoolVar(&printVersion, "version", false, "Show version and quit")
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

	hostName, err := os.Hostname()
	if err != nil {
		log.Fatalf("failed to get hostname: %v", err)
	}

	cli, err := client.New(kubeconfig)
	if err != nil {
		log.Fatalf("failed to get client: %v", err)
	}

	err = tpr.EnsureTPRExist(cli)
	if err != nil {
		log.Fatalf("failed to ensure TPR exist: %v", err)
	}

	rl := resourcelock.EndpointsLock{
		EndpointsMeta: metav1.ObjectMeta{
			Namespace: metav1.NamespaceSystem,
			Name:      "tidb-operator",
		},
		Client: cli,
		LockConfig: resourcelock.ResourceLockConfig{
			Identity:      hostName,
			EventRecorder: &record.FakeRecorder{},
		},
	}

	duration := 5 * time.Second

	// leader election for multiple operators
	election.RunOrDie(election.LeaderElectionConfig{
		Lock:          &rl,
		LeaseDuration: leaseDuration,
		RenewDeadline: renewDuration,
		RetryPeriod:   retryPeriod,
		Callbacks: election.LeaderCallbacks{
			OnStartedLeading: func(stopCh <-chan struct{}) {
				go wait.Forever(func() { tcc.NewTiDBClusterController(cli).Run(stopCh) }, duration)
				go wait.Forever(func() { pd.NewController(cli).Run(stopCh) }, duration)
				go wait.Forever(func() { tikv.NewController(cli).Run(stopCh) }, duration)
				go wait.Forever(func() { gcc.NewGCController(cli).Run(stopCh) }, duration)
				wait.Forever(func() { tidb.NewController(cli).Run(stopCh) }, duration)
			},
			OnStoppedLeading: func() {
				log.Fatalf("leader election lost")
			},
		},
	})

	panic("unreachable")
}
