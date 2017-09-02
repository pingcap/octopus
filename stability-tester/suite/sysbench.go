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

package suite

import (
	"bytes"
	"fmt"
	"os/exec"
	"time"

	"database/sql"
	"github.com/pingcap/octopus/stability-tester/config"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

func init() {
	RegisterSuite("sysbench", NewSysbenchCase)
}

// SysbenchCase is configuration for sysbench.
type SysbenchCase struct {
	cfg      *config.SysbenchCaseConfig
	host     string
	port     int
	user     string
	password string
	logger   *log.Logger
}

// NewSysbenchCase returns the SysbenchCase.
func NewSysbenchCase(cfg *config.Config) Case {
	return &SysbenchCase{
		cfg:      &cfg.Suite.Sysbench,
		host:     cfg.Host,
		port:     cfg.Port,
		user:     cfg.User,
		password: cfg.Password,
	}
}

// Initialize implements Case Initialize interface.
func (s *SysbenchCase) Initialize(ctx context.Context, cfg *config.Config, db *sql.DB, logger *log.Logger) error {
	s.logger = logger
	err := s.clean()
	if err != nil {
		return err
	}
	return nil
}

// Execute implements Case Execute interface.
func (s *SysbenchCase) Execute(ctx context.Context, db *sql.DB) error {
	err := s.runAction()
	if err != nil {
		return err
	}
	ticker := time.NewTicker(s.cfg.Interval.Duration)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := s.runAction()
			if err != nil {
				return err
			}
		}
	}
}

// String implements fmt.Stringer interface.
func (s *SysbenchCase) String() string {
	return "sysbench"
}

func (s *SysbenchCase) runAction() error {
	if err := s.prepare(); err != nil {
		return err
	}
	time.Sleep(10 * time.Second)
	if err := s.run(); err != nil {
		return err
	}
	if err := s.clean(); err != nil {
		return err
	}
	return nil
}

func (s *SysbenchCase) prepare() error {
	var err error
	createDBArgs := fmt.Sprintf(`/usr/bin/mysql -h%s -P%d -u%s -e"create database IF NOT EXISTS sbtest"`, s.host, s.port, s.user)
	Log.Infof("prepare command: %s", createDBArgs)
	cmdCreate := exec.Command("/bin/sh", "-c", createDBArgs)
	if err = cmdCreate.Run(); err != nil {
		Log.Errorf("create database failed: %v", err)
		return err
	}

	cmdStr := fmt.Sprintf(`sysbench --test=%s/insert.lua --mysql-host=%s --mysql-port=%d --mysql-user=%s --mysql-password=%s --oltp-tables-count=%d --oltp-table-size=%d --rand-init=on --db-driver=mysql prepare`,
		s.cfg.LuaPath, s.host, s.port, s.user, s.password, s.cfg.TableCount, 0)
	Log.Infof("create tables command: %s", cmdStr)
	cmd := exec.Command("/bin/sh", "-c", cmdStr)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		Log.Errorf("%s\n", out.String())
		return err
	}

	return nil
}

func (s *SysbenchCase) run() error {
	cmdStr := fmt.Sprintf(`sysbench --test=%s/insert.lua --mysql-host=%s --mysql-port=%d --mysql-user=%s --mysql-password=%s --oltp-tables-count=%d --oltp-table-size=%d --num-threads=%d --oltp-read-only=off --report-interval=600 --rand-type=uniform --max-time=%d --percentile=99 --max-requests=1000000000 --db-driver=mysql run`,
		s.cfg.LuaPath, s.host, s.port, s.user, s.password, s.cfg.TableCount, s.cfg.TableSize, s.cfg.Threads, s.cfg.MaxTime)
	cmd := exec.Command("/bin/sh", "-c", cmdStr)
	Log.Infof("run command: %s", cmdStr)

	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		Log.Errorf("%s\n", out.String())
		return err
	}

	return nil
}

func (s *SysbenchCase) clean() error {
	cmdStrArgs := fmt.Sprintf(`/usr/bin/mysql -h%s -P%d -u%s -e"drop database if exists sbtest"`, s.host, s.port, s.user)
	Log.Infof("clean command: ", cmdStrArgs)
	cmd := exec.Command("/bin/sh", "-c", cmdStrArgs)
	if err := cmd.Run(); err != nil {
		Log.Infof("run drop database failed", err)
		return err
	}
	return nil
}
