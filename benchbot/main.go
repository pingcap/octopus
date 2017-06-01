package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/octopus/benchbot/api"
	"github.com/pingcap/octopus/benchbot/backend"
	"github.com/pingcap/octopus/pkg/util"
)

var (
	version    bool
	configFile string
)

func parseArgs() {
	flag.StringVar(&configFile, "config", "./config.toml", "server configuration")
	flag.BoolVar(&version, "V", false, "print version information and exit")
	flag.BoolVar(&version, "version", false, "print version information and exit")
}

func main() {
	parseArgs()

	if version {
		util.PrintInfo()
		return
	}

	cfg, err := backend.ParseConfig(configFile)
	if err != nil {
		fmt.Printf("failed to load config (%s) : %s\n", configFile, err.Error())
		os.Exit(1)
	}

	svr, err := backend.NewServer(cfg)
	if err != nil {
		fmt.Printf("server running failed : %s\n", err.Error())
		os.Exit(1)
	}

	go func() {
		addr := fmt.Sprintf(":%d", cfg.Port)
		r := api.CreateRouter(svr)
		http.ListenAndServe(addr, r)
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	<-sig
	svr.Close()
}
