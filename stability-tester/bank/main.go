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
	logFile     = flag.String("log-file", "", "log file")
	logLevel    = flag.String("L", "info", "log level: info, debug, warn, error, faltal")
	user        = flag.String("user", "root", "db username")
	password    = flag.String("password", "", "user password")
	dbName      = flag.String("db", "test", "database name")
	accounts    = flag.Int("accounts", 1000000, "the number of accounts")
	interval    = flag.Duration("interval", 2*time.Second, "the interval")
	tables      = flag.Int("tables", 1, "the number of the tables")
	concurrency = flag.Int("concurrency", 200, "concurrency")
	pds         = flag.String("pds", "", "separated by \",\"")
	tidbs       = flag.String("tidbs", "", "separated by \",\"")
	tikvs       = flag.String("tikvs", "", "separated by \",\"")
	lbService   = flag.String("lb-service", "", "lb")
	metricAddr  = flag.String("metric-addr", "", "metric address")
	skipInit    = flag.Bool("skip-init", false, "ski init")
)

var (
	defaultVerifyTimeout = 6 * time.Hour
	remark               = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXVZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXVZlkjsanksqiszndqpijdslnnq"
)

func main() {
	flag.Parse()
	util.InitLog(*logFile, *logLevel)
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s)/%s", *user, *password, *lbService, *dbName)
	db, err := util.OpenDB(dbDSN, *concurrency)
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
	if len(*metricAddr) > 0 {
		log.Info("enable metrics")
		go util.PushPrometheus("bank", *metricAddr, defaultPushMetricsInterval)
	}
	cfg := Config{
		NumAccounts: *accounts,
		Interval:    *interval,
		TableNum:    *tables,
		Concurrency: *concurrency,
	}
	bank := NewBankCase(&cfg)
	if !*skipInit {
		if err := bank.Initialize(ctx, db); err != nil {
			log.Fatal(err)
		}
	}

	if err := bank.Execute(ctx, db); err != nil {
		log.Fatal(err)
	}
}
