package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/config"
	"github.com/pingcap/octopus/stability-tester/mvcc_suite"
	"github.com/pingcap/octopus/stability-tester/serial_suite"
	"github.com/pingcap/octopus/stability-tester/suite"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"golang.org/x/net/context"
)

var (
	configFile  = flag.String("c", "config.toml", "stability test config file")
	clusterType = flag.String("cluster", "ssh", "cluster type: [ssh, docker-compose, k8s]")
	pprofAddr   = flag.String("pprof", "0.0.0.0:16060", "pprof address")
	logFile     = flag.String("log-file", "", "log file")
	logLevel    = flag.String("L", "info", "log level: info, debug, warn, error, fatal")
)

func openDB(cfg *config.Config) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/test", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(cfg.Suite.Concurrency)
	log.Info("DB opens successfully")
	return db, nil
}

func main() {
	// Initialize the default random number source.
	rand.Seed(time.Now().UTC().UnixNano())

	flag.Parse()

	log.SetLevelByString(*logLevel)
	log.SetHighlighting(false)
	if len(*logFile) > 0 {
		log.SetOutputByName(*logFile)
		log.SetRotateByHour()
	}
	log.SetLevelByString(*logLevel)

	go func() {
		if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
			log.Fatal(err)
		}
	}()

	cfg, err := config.ParseConfig(*configFile)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("%#v", cfg)

	// Prometheus metrics
	PushMetrics(cfg)

	var (
		db    *sql.DB
		store kv.Storage
		wg    sync.WaitGroup
	)

	// Run all cases, and nemeses
	// Capture signal

	ctx, cancel := context.WithCancel(context.Background())

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("Got signal [%d] to exit.", sig)
		cancel()

		if db != nil {
			db.Close()
		}

		if store != nil {
			store.Close()
		}
	}()

	// Create the database and run TiDB cases
	if len(cfg.Host) > 0 {
		db, err = openDB(cfg)
		if err != nil {
			log.Fatal(err)
		}

		// Initialize suites.
		suiteCases := suite.InitSuite(ctx, cfg, db)
		// Run suites.
		go func() {
			wg.Add(1)
			defer wg.Done()
			suite.RunSuite(ctx, suiteCases, cfg.Suite.Concurrency, db)
		}()

		// Initialize serial suites
		SerialSuites := serial_suite.InitSuite(ctx, cfg)
		// Run serial suites
		go func() {
			wg.Add(1)
			defer wg.Done()
			serial_suite.RunSuite(ctx, SerialSuites)
		}()
	}

	// Create the TiKV storage and run MVCC cases
	if len(cfg.PD) > 0 {
		tidb.RegisterStore("tikv", tikv.Driver{})
		store, err = tidb.NewStore(fmt.Sprintf("tikv://%s?disableGC=true", cfg.PD))
		if err != nil {
			log.Fatal(err)
		}

		// Initialize suites.
		suiteCases := mvcc_suite.InitSuite(ctx, cfg, store)
		// Run suites.
		go func() {
			wg.Add(1)
			defer wg.Done()
			mvcc_suite.RunSuite(ctx, suiteCases, cfg.MVCC.Concurrency, store)
		}()
	}

	// Run all nemeses in background.
	// go nemesis.RunNemeses(ctx, &cfg.Nemeses, cluster.NewCluster(&cfg.Cluster))

	go config.RunConfigScheduler(&cfg.Scheduler)
	wg.Wait()
}
