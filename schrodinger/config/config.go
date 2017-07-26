// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"flag"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/pingcap/octopus/pkg/util"
)

// Config is the configuration
type Config struct {
	*flag.FlagSet `json:"-"`

	LogLevel    string `toml:"log-level" json:"log-level"`
	LogFile     string `toml:"log-file" json:"log-file"`
	LogRotate   string `toml:"log-rotate" json:"log-rotate"`
	Port        int    `toml:"port" json:"port"`
	RepoPrefix  string `toml:"repo-prefix" json:"repo-prefix"`
	ServiceType string `toml:"service-type" json:"service-type"`
	CatDir      string `timl:"cat-dir" json:"cat-dir"`

	configFile   string
	printVersion bool
}

func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("schrodinger", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.StringVar(&cfg.configFile, "config", "", "path to config file")
	fs.IntVar(&cfg.Port, "port", 9088, "port of the web service")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")
	fs.StringVar(&cfg.RepoPrefix, "repo-prefix", "pingcap", "docker repo prefix for tidb related images")
	fs.StringVar(&cfg.ServiceType, "service-type", "NodePort", "service type(NodePort, ClusterIP, LoadBalancer) for tidb")
	fs.StringVar(&cfg.CatDir, "cat-dir", "./cat-data", "path to save cat")
	return cfg
}

// Parse parses flag definitions from the arguments list.
func (c *Config) Parse(arguments []string) error {
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if c.printVersion {
		util.PrintInfo()
		return flag.ErrHelp
	}

	if c.configFile != "" {
		err = c.configFromFile(c.configFile)
		if err != nil {
			return errors.Trace(err)
		}
	}

	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	return nil
}

func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}
