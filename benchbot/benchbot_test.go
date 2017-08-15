package main

import (
	"testing"

	"github.com/Sirupsen/logrus"
	"github.com/ngaut/log"

	. "github.com/pingcap/octopus/benchbot/backend"
)

func TestLogger(t *testing.T) {
	cfg := &ServerConfig{
		Dir:    ".",
		LogDir: "logs",
	}

	if err := initLogger(cfg); err != nil {
		log.Fatal(err)
	}

	log.Info("log from ngaut/log")
	logrus.Info("log from sirupsen/logrus")
}
