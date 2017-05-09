package main

import (
	"flag"

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

	// Here just to force generating vendor
	// we will remove it later.
	context.Background()
}
