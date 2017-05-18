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
	"golang.org/x/net/context"
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

	svr := backend.NewServer()
	go func() {
		select {
		case <-sig:
			svr.Close()
			os.Exit(0)
		}
	}()

	r := api.CreateRouter(svr)
	http.ListenAndServe(":8888", r)

	// Here just to force generating vendor
	// we will remove it later.
	context.Background()
}
