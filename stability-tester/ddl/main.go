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

	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/util"
	"golang.org/x/net/context"
)

var defaultPushMetricsInterval = 15 * time.Second

var (
	logFile         = flag.String("log-file", "", "log file")
	logLevel        = flag.String("L", "info", "log level: info, debug, warn, error, faltal")
	user            = flag.String("user", "root", "db username")
	password        = flag.String("password", "", "user password")
	dbName          = flag.String("db", "test", "database name")
	concurrency     = flag.Int("concurrency", 200, "concurrency")
	pds             = flag.String("pds", "", "separated by \",\"")
	tidbs           = flag.String("tidbs", "", "separated by \",\"")
	tikvs           = flag.String("tikvs", "", "separated by \",\"")
	lbService       = flag.String("lb-service", "", "lb")
	metricAddr      = flag.String("metric-addr", "", "metric address")
	tablesToCreate  = flag.Int("tables", 1, "the number of the tables to create")
	mysqlCompatible = flag.Bool("mysql-compatible", false, "disable TiDB-only features")
)

func main() {
	flag.Parse()
	util.InitLog(*logFile, *logLevel)
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s)/%s", *user, *password, *lbService, *dbName)
	db, err := util.OpenDB(dbDSN, *concurrency)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxIdleConns(*concurrency)
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
	if len(*metricAddr) > 0 {
		log.Info("enable metrics")
		go util.PushPrometheus("ddl", *metricAddr, defaultPushMetricsInterval)
	}
	cfg := DDLCaseConfig{
		Concurrency:     *concurrency,
		TablesToCreate:  *tablesToCreate,
		MySQLCompatible: *mysqlCompatible,
	}
	ddl := NewDDLCase(&cfg)
	if err := ddl.Initialize(ctx, db); err != nil {
		log.Fatal(err)
	}
	if err := ddl.Execute(ctx, db); err != nil {
		log.Fatal(err)
	}
}
