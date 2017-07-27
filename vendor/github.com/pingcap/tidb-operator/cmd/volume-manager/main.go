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
	vmc "github.com/pingcap/tidb-operator/pkg/volumemanager/controller"
	"github.com/pingcap/tidb-operator/version"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

var (
	logLevel     string
	printVersion bool
	kubeconfig   string
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

	cfg, err := client.NewConfig(kubeconfig)
	if err != nil {
		log.Fatalf("failed to get kube config: %v", err)
	}
	kubecli := kubernetes.NewForConfigOrDie(cfg)

	// TODO when kubernetes v1.7 released, we should use downward API to get host ip from `status.hostIP`.
	// For details: https://github.com/kubernetes/kubernetes/pull/42717
	ip, err := net.ChooseHostInterface()
	if err != nil {
		log.Fatalf("failed to get host ip: %v", err)
	}

	wait.Forever(func() {
		vmc.NewVolumeManagerController(ip.String(), kubecli).Run(wait.NeverStop)
	}, 5*time.Second)
}
