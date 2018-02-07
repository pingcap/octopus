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
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pingcap/octopus/stability-tester/util"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

var defaultPushMetricsInterval = 15 * time.Second

var (
	logFile    = flag.String("log-file", "", "log file")
	logLevel   = flag.String("L", "info", "log level: info, debug, warn, error, faltal")
	user       = flag.String("user", "root", "db username")
	password   = flag.String("password", "", "user password")
	dbName     = flag.String("db", "test", "database name")
	pds        = flag.String("pds", "", "separated by \",\"")
	tidbs      = flag.String("tidbs", "", "separated by \",\"")
	tikvs      = flag.String("tikvs", "", "separated by \",\"")
	lbService  = flag.String("lb-service", "", "lb")
	metricAddr = flag.String("metric-addr", "", "metric address")
	numRows    = flag.Int("num-rows", 10000, "number of rows")
)

func main() {
	flag.Parse()
	util.InitLog(*logFile, *logLevel)
	if *lbService == "" {
		log.Fatal("lb-service is missing.")
	}
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s)/%s", *user, *password, *lbService, *dbName)
	db, err := util.OpenDB(dbDSN, 3)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sc
		log.Infof("Got signal [%d] to exist.", sig)
		cancel()
		os.Exit(0)
	}()
	onDup := NewOnDupCase(*numRows)
	if err := onDup.Initialize(ctx, db); err != nil {
		log.Fatal(err)
	}
	if err := onDup.Execute(ctx, db); err != nil {
		log.Fatal(err)
	}
}
