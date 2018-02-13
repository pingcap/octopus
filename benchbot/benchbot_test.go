package main

import (
	"testing"

	. "github.com/pingcap/octopus/benchbot/backend"
	log "github.com/sirupsen/logrus"
)

func TestLogger(t *testing.T) {
	cfg := &ServerConfig{
		Dir:    ".",
		LogDir: "logs",
	}

	if err := initLogger(cfg); err != nil {
		log.Fatal(err)
	}

	log.Info("log from sirupsen/logrus")
}
