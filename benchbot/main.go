package main

import (
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/octopus/benchbot/api"
	"github.com/pingcap/octopus/benchbot/backend"
	"github.com/pingcap/octopus/pkg/util"
)

var (
	addr    string
	version bool
)

func parseArgs() {
	flag.StringVar(&addr, "addr", ":20170", "listening address")
	flag.BoolVar(&version, "V", false, "print version information and exit")
	flag.BoolVar(&version, "version", false, "print version information and exit")

	flag.Parse()
}

func main() {
	parseArgs()

	if version {
		util.PrintInfo()
		return
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	cfg := &backend.ServerConfig{} // TODO .. load from toml file
	svr := backend.NewServer(cfg)

	r := api.CreateRouter(svr)
	go func() {
		http.ListenAndServe(addr, r)
	}()

	<-sig
	svr.Close()
}
