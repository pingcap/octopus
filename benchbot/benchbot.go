package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/ngaut/log"
	"github.com/pingcap/octopus/benchbot/api"
	"github.com/pingcap/octopus/benchbot/backend"
	"github.com/pingcap/octopus/benchbot/suite"
	_ "github.com/pingcap/octopus/benchbot/suite/sysbench"
)

var (
	configFile = flag.String("c", "benchbot.toml", "benchbot configuration file")
)

const (
	logFileName string = "benchbot.log"
)

func initLogger(cfg *backend.ServerConfig) error {
	if err := os.MkdirAll(cfg.Dir, os.ModePerm); err != nil {
		return err
	}

	logFilepath := path.Join(cfg.Dir, logFileName)
	log.SetRotateByDay()
	log.SetHighlighting(false)
	return log.SetOutputByName(logFilepath)
}

func main() {
	flag.Parse()

	cfg, err := backend.ParseConfig(*configFile)
	if err != nil {
		fmt.Printf("failed to load %s: %s\n", *configFile, err)
		os.Exit(1)
	}

	if err := initLogger(cfg); err != nil {
		fmt.Printf("failed to init logger: %s\n", err)
		os.Exit(1)
	}

	suites, err := suite.NewBenchSuites(*configFile)
	if err != nil {
		fmt.Printf("failed to create suites: %s\n", err)
		os.Exit(1)
	}

	svr, err := backend.NewServer(cfg, suites)
	if err != nil {
		fmt.Printf("failed to create server: %s\n", err)
		os.Exit(1)
	}

	go func() {
		addr := fmt.Sprintf(":%d", cfg.Port)
		r := api.CreateRouter(svr)
		http.ListenAndServe(addr, r)
	}()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	sig := <-sc
	log.Infof("Got signal [%d] to exit.", sig)

	svr.Close()
}
