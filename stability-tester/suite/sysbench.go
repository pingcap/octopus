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
	"database/sql"
	"fmt"
	"os/exec"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/octopus/stability-tester/config"
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
func (c *SysbenchCase) Initialize(ctx context.Context, db *sql.DB, logger *log.Logger) error {
	c.logger = logger
	err, isCancel := c.clean(ctx)
	if isCancel {
		return nil
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Execute implements Case Execute interface.
func (c *SysbenchCase) Execute(ctx context.Context, db *sql.DB) error {
	err, isCancel := c.runAction(ctx)
	if isCancel {
		return nil
	}
	if err != nil {
		return errors.Trace(err)
	}
	ticker := time.NewTicker(Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err, isCancel := c.runAction(ctx)
			if isCancel {
				return nil
			}
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// String implements fmt.Stringer interface.
func (c *SysbenchCase) String() string {
	return "sysbench"
}

func (c *SysbenchCase) runAction(ctx context.Context) (error, bool) {
	if err, isCancel := c.prepare(ctx); isCancel {
		return nil, true
	} else if err != nil {
		return errors.Trace(err), false
	}

	time.Sleep(10 * time.Second)
	if err, isCancel := c.run(ctx); isCancel {
		return nil, true
	} else if err != nil {
		return errors.Trace(err), false
	}
	if err, isCancel := c.clean(ctx); isCancel {
		return nil, true
	} else if err != nil {
		return errors.Trace(err), false
	}
	return nil, false
}

func (c *SysbenchCase) prepare(ctx context.Context) (error, bool) {
	var wg sync.WaitGroup
	createDBArgs := fmt.Sprintf(`/usr/bin/mysql -h%s -P%d -u%s -e"create database IF NOT EXISTS sbtest"`, c.host, c.port, c.user)
	c.logger.Infof("prepare command: %s", createDBArgs)
	cmdCreate := exec.Command("/bin/sh", "-c", createDBArgs)
	if err := cmdCreate.Start(); err != nil {
		c.logger.Errorf("[%s] start failed: %v", createDBArgs, err)
		return errors.Trace(err), false
	}
	done := make(chan error)
	wg.Add(1)
	go func() {
		wg.Done()
		done <- cmdCreate.Wait()
	}()
	wg.Wait()
	select {
	case <-ctx.Done():
		err := cmdCreate.Process.Kill()
		if err != nil {
			c.logger.Errorf("kill [%s] failed: %v", cmdCreate, err)
		}
		return nil, true
	case err := <-done:
		if err != nil {
			c.logger.Errorf("[%s] run failed: %v", createDBArgs, err)
			return errors.Trace(err), false
		}
	}

	cmdStr := fmt.Sprintf(`sysbench --test=%s/insert.lua --mysql-host=%s --mysql-port=%d --mysql-user=%s --mysql-password=%s --oltp-tables-count=%d --oltp-table-size=%d --rand-init=on --db-driver=mysql prepare`,
		c.cfg.LuaPath, c.host, c.port, c.user, c.password, c.cfg.TableCount, 0)
	c.logger.Infof("create tables command: %s", cmdStr)
	cmd := exec.Command("/bin/sh", "-c", cmdStr)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Start(); err != nil {
		c.logger.Errorf("%s\n", out.String())
		return errors.Trace(err), false
	}
	wg.Add(1)
	go func() {
		wg.Done()
		done <- cmd.Wait()
	}()
	wg.Wait()
	select {
	case <-ctx.Done():
		err := cmdCreate.Process.Kill()
		if err != nil {
			c.logger.Errorf("kill [%s] failed: %v", cmd, err)
		}
		return nil, true
	case err := <-done:
		if err != nil {
			c.logger.Errorf("%s\n", out.String())
			return errors.Trace(err), false
		}
	}

	return nil, false
}

func (c *SysbenchCase) run(ctx context.Context) (error, bool) {
	cmdStr := fmt.Sprintf(`sysbench --test=%s/insert.lua --mysql-host=%s --mysql-port=%d --mysql-user=%s --mysql-password=%s --oltp-tables-count=%d --oltp-table-size=%d --num-threads=%d --oltp-read-only=off --report-interval=600 --rand-type=uniform --max-time=%d --percentile=99 --max-requests=1000000000 --db-driver=mysql run`,
		c.cfg.LuaPath, c.host, c.port, c.user, c.password, c.cfg.TableCount, c.cfg.TableSize, c.cfg.Threads, c.cfg.MaxTime)
	cmd := exec.Command("/bin/sh", "-c", cmdStr)
	c.logger.Infof("run command: %s", cmdStr)

	var out bytes.Buffer
	var wg sync.WaitGroup
	done := make(chan error)
	cmd.Stdout = &out
	if err := cmd.Start(); err != nil {
		c.logger.Errorf("%s\n", out.String())
		return errors.Trace(err), false
	}
	wg.Add(1)
	go func() {
		wg.Done()
		done <- cmd.Wait()
	}()
	wg.Wait()
	select {
	case <-ctx.Done():
		err := cmd.Process.Kill()
		if err != nil {
			c.logger.Errorf("kill [%s] failed: %v", cmd, err)
		}
		return nil, true
	case err := <-done:
		if err != nil {
			c.logger.Errorf("%s\n", out.String())
			return errors.Trace(err), false
		}
	}

	return nil, false
}

func (c *SysbenchCase) clean(ctx context.Context) (error, bool) {
	cmdStrArgs := fmt.Sprintf(`/usr/bin/mysql -h%s -P%d -u%s -e"drop database if exists sbtest"`, c.host, c.port, c.user)
	c.logger.Infof("clean command: %s", cmdStrArgs)
	cmd := exec.Command("/bin/sh", "-c", cmdStrArgs)
	var wg sync.WaitGroup
	done := make(chan error)
	if err := cmd.Start(); err != nil {
		c.logger.Error("[%s] start failed: %s", cmdStrArgs, err)
		return errors.Trace(err), false
	}
	wg.Add(1)
	go func() {
		wg.Done()
		done <- cmd.Wait()
	}()
	wg.Wait()
	select {
	case <-ctx.Done():
		err := cmd.Process.Kill()
		if err != nil {
			c.logger.Errorf("kill [%s] failed: %v", cmd, err)
		}
		return nil, true
	case err := <-done:
		if err != nil {
			c.logger.Error("sysbench clean failed: %s", err)
			return errors.Trace(err), false
		}
	}

	return nil, false
}
