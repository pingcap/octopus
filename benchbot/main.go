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
	addr      string
	version   bool
	storePath string
)

func parseArgs() {
	flag.StringVar(&addr, "addr", ":20170", "listening address")
	flag.StringVar(&storePath, "store", "./store", "path to presist data")
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

	// TODO .. load from toml file
	cfg := &backend.ServerConfig{
		StorePath:           storePath,
		AnsibleDir:          "./ansible",
		AnsibleDownloadsDir: "./ansible/downloads",
		AnsibleBinsDir:      "./ansible/resources/bin",
	}

	svr := backend.NewServer(cfg)
	r := api.CreateRouter(svr)
	go func() {
		http.ListenAndServe(addr, r)
	}()

	// Here just to force generating vendor
	// we will remove it later.
	context.Background()

	<-sig
	svr.Close()
}
