package main

import (
	"flag"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/util"
	"golang.org/x/net/context"
)

var (
	logFile    = flag.String("log-file", "", "log file")
	logLevel   = flag.String("L", "info", "log level: info, debug, warn, error, fatal")
	dbName     = flag.String("db", "test", "database name")
	tableCount = flag.Int("table-count", 16, "number of tables")
	tableSize  = flag.Int("table-size", 1000000, "number of rows per table")
	threads    = flag.Int("threads", 256, "number of threads to use")
	maxTime    = flag.Int("max-time", 2400, "limit fo total exection time in seconds")
	interval   = flag.Duration("interval", 2*time.Hour, "interval to run sysbench")
	luaPath    = flag.String("lua-path", ".", "the path of the lua scripts")
	pds        = flag.String("pds", "", "pds addr, separated by \",\"")
	tidbs      = flag.String("tidbs", "", "tidbs addr, separated by \",\"")
	tikvs      = flag.String("tikvs", "", "tikvs addr, separated by \",\"")
	lbService  = flag.String("lb-service", "", "load balance service")
	metricAddr = flag.String("metic-addr", "", "metric address")
)

func main() {
	flag.Parse()
	util.InitLog(*logFile, *logLevel)
	ctx, cancel := context.WithCancel(context.Background())
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sc
		log.Infof("Got signal [%d] to exist", sig)
		cancel()
		os.Exit(0)
	}()
	addrs := strings.Split(strings.TrimSpace(*lbService), ":")
	if len(addrs) <= 2 {
		log.Fatalf("[lb-service: %s] is invalid", addrs)
	}
	port, err := strconv.Atoi(addrs[1])
	if err != nil {
		log.Fatalf("[lb-service: %s] is invalid", addrs)
	}
	cfg := Config{
		Host:       addrs[0],
		Port:       port,
		User:       "root",
		TableCount: *tableCount,
		TableSize:  *tableSize,
		Threads:    *threads,
		MaxTime:    *maxTime,
		Interval:   *interval,
		DBName:     *dbName,
		LuaPath:    *luaPath,
	}
	sysbench := NewSysbenchCase(&cfg)
	if err := sysbench.Initialize(); err != nil {
		log.Fatal(err)
	}
	if err := sysbench.Execute(ctx); err != nil {
		log.Fatal(err)
	}
	select {}
}
