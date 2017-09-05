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
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/octopus/stability-tester/config"
	"github.com/pingcap/octopus/stability-tester/suite"
	"golang.org/x/net/context"
)

var (
	configFile = flag.String("c", "config.toml", "stability test config file")
	pprofAddr  = flag.String("pprof", "0.0.0.0:16060", "pprof address")
	logFile    = flag.String("log-file", "", "log file")
	logLevel   = flag.String("L", "info", "log level: info, debug, warn, error, fatal")
	testCase   = flag.String("t", "", "testcase: bank, bank2, ledger, crud, block_writer, log, mvcc_bank, sysbench, sqllogic_test. mutliple case please use , to divide")
	host       = flag.String("host", "", "db ip")
	port       = flag.Int("port", 4000, "db port")
	pdURL      = flag.String("pd-url", "", "pd information")
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

func initLog() {
	if file, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY, 0666); err == nil {
		log.SetOutput(file)
	} else {
		log.Info("failed to log to file, using default std")
	}

	if lvl, err := log.ParseLevel(*logLevel); err != nil {
		log.SetLevel(lvl)
	} else {
		log.Info("failed to set log level, use info level")
	}

	log.SetLevel(log.InfoLevel)
}

func main() {
	// Initialize the default random number source.
	rand.Seed(time.Now().UTC().UnixNano())

	flag.Parse()
	initLog()

	go func() {
		if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
			log.Fatal(err)
		}
	}()

	cfg, err := config.ParseConfig(*configFile)
	if err != nil {
		log.Fatal(err)
	}

	if len(*testCase) > 0 {
		cfg.Suite.Names = strings.Split(*testCase, ",")
	}

	if len(*host) > 0 {
		cfg.Host = *host
	}
	if *port > 0 {
		cfg.Port = *port
	}
	if len(*pdURL) > 0 {
		cfg.PD = *pdURL
		cfg.Scheduler.PDAddrs = []string{*pdURL}
		cfg.Scheduler.ShuffleLeader = true
		cfg.Scheduler.ShuffleRegion = true
	}

	log.Infof("%#v", cfg)

	// Prometheus metrics
	PushMetrics(cfg)

	var (
		wg sync.WaitGroup
		db *sql.DB
	)

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
	}()

	// Create the database and run TiDB cases
	if len(cfg.Host) > 0 {
		db, err = openDB(cfg)
		if err != nil {
			log.Fatal(err)
		}
		// init case one by one
		suiteCases := suite.InitCase(ctx, cfg, db)
		if len(suiteCases) == 0 {
			return
		}
		// run case
		wg.Add(1)
		go func() {
			defer wg.Done()
			suite.RunSuite(ctx, suiteCases, db)
		}()
	}

	// run scheduler
	go config.RunConfigScheduler(ctx, &cfg.Scheduler)
	wg.Wait()
	if db != nil {
		db.Close()
	}
}
