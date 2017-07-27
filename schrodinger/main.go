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
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/schrodinger/api"
	"github.com/pingcap/octopus/schrodinger/cat"
	"github.com/pingcap/octopus/schrodinger/cluster"
	"github.com/pingcap/octopus/schrodinger/config"
)

func initLogger(cfg *config.Config) error {
	log.SetLevelByString(cfg.LogLevel)
	if len(cfg.LogFile) > 0 {
		err := log.SetOutputByName(cfg.LogFile)
		if err != nil {
			return errors.Trace(err)
		}
		log.SetHighlighting(false)

		if cfg.LogRotate == "hour" {
			log.SetRotateByDay()
		} else {
			log.SetRotateByDay()
		}
	}
	return nil
}

func main() {
	cfg := config.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Fatalf("parse cmd flags err %s", err)
	}

	err = initLogger(cfg)
	if err != nil {
		log.Fatalf("logger init failed: %s", err.Error())
	}
	clusterManger := cluster.NewClusterManager(cfg)
	catService := cat.NewCatService(clusterManger, cfg)
	go func() {
		r := api.NewRouter(catService)
		addr := fmt.Sprintf(":%d", cfg.Port)
		log.Infof("Starting listen %s", addr)
		if err := http.ListenAndServe(addr, r); err != nil {
			log.Fatalf("start api server failed: %s", err)
		}
	}()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("got signal [%d], exit", sig)
		os.Exit(0)
	}()

	select {}
}
