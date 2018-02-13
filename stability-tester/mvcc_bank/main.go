package main

import (
	"flag"
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/octopus/stability-tester/util"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

var (
	logFile     string
	logLevel    string
	user        string
	password    string
	dbName      string
	accounts    int
	interval    time.Duration
	tables      int
	concurrency int
	pds         string
	tidbs       string
	tikvs       string
	lb          string
	metricAddr  string
)

var defaultPushMetricsInterval = 15 * time.Second

func init() {
	flag.StringVar(&logFile, "log-file", "", "log file")
	flag.StringVar(&logLevel, "L", "info", "log level: info, debug, warn, error, faltal")
	flag.StringVar(&user, "user", "root", "db username")
	flag.StringVar(&password, "password", "", "user password")
	flag.StringVar(&dbName, "db", "test", "database name")
	flag.IntVar(&accounts, "accounts", 1000000, "the number of accounts")
	flag.DurationVar(&interval, "interval", 2*time.Second, "the interval")
	flag.IntVar(&tables, "tables", 1, "the number of the tables")
	flag.IntVar(&concurrency, "concurrency", 200, "concurrency")
	flag.StringVar(&pds, "pds", "", "separated by \",\"")
	flag.StringVar(&tidbs, "tidbs", "", "separated by \",\"")
	flag.StringVar(&tikvs, "tikvs", "", "separated by \",\"")
	flag.StringVar(&lb, "lb-service", "", "lb")
	flag.StringVar(&metricAddr, "metric-addr", "", "metric address")

}

func main() {
	// Initialize the default random number source.
	rand.Seed(time.Now().UTC().UnixNano())

	flag.Parse()
	util.InitLog(logFile, logLevel)

	ctx, cancel := context.WithCancel(context.Background())
	err := detectParameters()
	if err != nil {
		log.Fatal(err)
	}

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
		os.Exit(0)
	}()

	dbDSN := fmt.Sprintf("%s:%s@tcp(%s)/%s", user, password, lb, dbName)
	db, err := util.OpenDB(dbDSN)
	if err != nil {
		log.Fatal(err)
	}

	cfg := &BankCaseConfig{
		NumAccounts: accounts,
		Interval:    interval,
		Concurrency: concurrency,
		PD:          pds,
	}
	mvccBank := NewMVCCBankCase(cfg)
	err = mvccBank.Initialize(ctx, db)
	if err != nil {
		log.Fatalf("initialize %s error %v", mvccBank, err)
	}

	err = mvccBank.Execute(ctx, db)
	if err != nil {
		log.Fatalf("bank execution error %v", err)
	}
}

func detectParameters() error {
	if len(lb) == 0 {
		return errors.New("lack of lb partermeters")
	}

	if len(pds) == 0 {
		return errors.New("lack of pds partermeters")
	}
	return nil
}
