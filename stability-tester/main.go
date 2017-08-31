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
	"github.com/pingcap/octopus/stability-tester/suite"
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

	var wg sync.WaitGroup

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
		os.Exit(0)
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
		wg.Add(1)
		go func() {
			defer wg.Done()
			suite.RunSuite(ctx, suiteCases, cfg.Suite.Concurrency, db)
		}()
	}

	wg.Wait()
}
