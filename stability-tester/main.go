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
	"golang.org/x/net/context"
)

var (
	configFile = flag.String("c", "config.toml", "stability test config file")
	pprofAddr  = flag.String("pprof", "0.0.0.0:16060", "pprof address")
	logFile    = flag.String("log-file", "", "log file")
	logLevel   = flag.String("L", "info", "log level: info, debug, warn, error, fatal")
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
		log.Info("failed to log to file, using default stderr")
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
	}()

	// Create the database and run TiDB cases
	if len(cfg.Host) > 0 {
		db, err = openDB(cfg)
		if err != nil {
			log.Fatal(err)
		}
		// init case one by one
		suiteCases := suite.InitCase(ctx, cfg, db)

		// run case
		wg.Add(1)
		go func() {
			defer wg.Done()
			suite.RunCase(ctx, suiteCases, db)
		}()
	}

	wg.Wait()
}
