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
	logFile    string
	logLevel   string
	user       string
	password   string
	dbName     string
	testPath   string
	parallel   int
	skipError  bool
	pds        string
	tidbs      string
	tikvs      string
	lb         string
	metricAddr string
)

var (
	defaultPushMetricsInterval = 15 * time.Second
	defaultConcurrency         = 10
)

func init() {
	flag.StringVar(&logFile, "log-file", "", "log file")
	flag.StringVar(&logLevel, "L", "info", "log level: info, debug, warn, error, faltal")
	flag.StringVar(&user, "user", "root", "db username")
	flag.StringVar(&password, "password", "", "user password")
	flag.StringVar(&dbName, "db", "test", "database name")
	flag.StringVar(&testPath, "test-path", "./sqllogictest", "test data path")
	flag.IntVar(&parallel, "parallel", 8, "parallel count")
	flag.BoolVar(&skipError, "skip-error", true, "skip error")
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
	db, err := util.OpenDB(dbDSN, defaultConcurrency)
	if err != nil {
		log.Fatal(err)
	}

	cfg := &SqllogicTestCaseConfig{
		TestPath:  testPath,
		SkipError: skipError,
		Parallel:  parallel,
		DBName:    dbName,
		Host:      lb,
		User:      user,
		Password:  password,
	}
	sqllogic := NewSqllogictest(cfg)
	err = sqllogic.Initialize(ctx, db)
	if err != nil {
		log.Fatalf("initialize %s error %v", sqllogic, err)
	}

	err = sqllogic.Execute(ctx, db)
	if err != nil {
		log.Fatalf("bank execution error %v", err)
	}
}

func detectParameters() error {
	if len(lb) == 0 {
		return errors.New("lack of lb partermeters")
	}

	return nil
}
