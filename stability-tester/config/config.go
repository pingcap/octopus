// Copyright 2016 PingCAP, Inc.
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
	"io/ioutil"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

const (
	defaultSSHUser = "pingcap"
	defaultSSHPort = 22
	defaultType    = "docker"
)

// Duration is for parsing duration to time.Duration in toml.
type Duration struct {
	time.Duration
}

// UnmarshalText implements toml UnmarshalText interface.
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// Config is the configuration for the stability test.
type Config struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	PD       string `toml:"pd"`

	// Cluster ClusterConfig `toml:"cluster"`
	// Nemeses     NemesesConfig     `toml:"nemeses"`
	Suite       SuiteConfig       `toml:"suite"`
	MVCC        MVCCSuiteConfig   `toml:"mvcc"`
	SerialSuite SerialSuiteConfig `toml:"serial_suite"`
	Scheduler   SchedulerConfig   `toml:"scheduler"`

	Metric MetricConfig `toml:"metric"`
}

// ParseConfig parses the configuration file.
func ParseConfig(path string) (*Config, error) {
	cfg := new(Config)
	if err := parseConfig(path, &cfg); err != nil {
		return nil, errors.Trace(err)
	}

	// adjust Cluster.
	// cfg.Cluster.adjust()

	return cfg, nil
}

func parseConfig(path string, cfg interface{}) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return errors.Trace(err)
	}

	if err = toml.Unmarshal(data, cfg); err != nil {
		return errors.Trace(err)
	}

	return nil
}
