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

	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/octopus/stability-tester/config"
	"github.com/pingcap/octopus/stability-tester/suite"
	"github.com/pingcap/tidb/kv"
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
		initchan := make(chan int)
		for _, name := range cfg.Suite.Names {
			go func(name string) {
				log := log.New()
				logfile, err := os.OpenFile(name+"tidb-stability-tester.log", os.O_CREATE|os.O_RDWR, 0666)
				if err != nil {
					return
				}
				log.Out = logfile
				log.Level(*logLevel)
				// init case one by one
				runcase := suite.InitCase(ctx, name, cfg, db)
				<-initchan
				// run case
				suite.RunCase(ctx, runcase, cfg.Suite.Concurrency, db)
			}(name)
		}

		for i := 0; i < len(cfg.Suite.Names); i++ {
			initchan <- i
		}

	}

	go config.RunConfigScheduler(&cfg.Scheduler)
	wg.Wait()
}
