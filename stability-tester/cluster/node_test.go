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

package cluster

import (
	"fmt"
	"os/user"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/octopus/stability-tester/config"
)

func TestNode(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testNode{})

type testNode struct {
}

func (n *testNode) SetUpSuite(c *C) {
}

func (n *testNode) TestBinaryNode(c *C) {
	u, _ := user.Current()
	cfg := config.ServiceConfig{
		Name: "testbinary",
		Host: "office.pingcap.net",
		Type: "binary",
		SSH: config.SSHConfig{
			User:    "pingcap",
			Port:    2226,
			KeyPath: fmt.Sprintf("%s/.ssh/id_rsa", u.HomeDir),
		},
		Command: []string{"/data/build/bin/tidb-server --store=tikv --path='127.0.0.1:2379?cluster=1' -P 4009 --status 10088 --binlog-socket /tmp/pump.sock"},
	}
	node := newNode(cfg)

	err := node.Start()
	c.Assert(err, IsNil)

	time.Sleep(1 * time.Second)
	isRunning, err := node.IsRunning()
	c.Assert(err, IsNil)
	c.Assert(isRunning, Equals, true)

	err = node.Stop()
	c.Assert(err, IsNil)

	err = node.Start()
	c.Assert(err, IsNil)

	err = node.Kill()
	c.Assert(err, IsNil)
}

func (n *testNode) TestDockerNode(c *C) {
	u, _ := user.Current()
	cfg := config.ServiceConfig{
		Name: "testdocker",
		Host: "office.pingcap.net",
		Type: "docker",
		SSH: config.SSHConfig{
			User:    "pingcap",
			Port:    2226,
			KeyPath: fmt.Sprintf("%s/.ssh/id_rsa", u.HomeDir),
		},
	}
	node := newNode(cfg)

	err := node.Start()
	c.Assert(err, IsNil)

	time.Sleep(1 * time.Second)
	isRunning, err := node.IsRunning()
	c.Assert(err, IsNil)
	c.Assert(isRunning, Equals, true)

	err = node.Stop()
	c.Assert(err, IsNil)

	err = node.Start()
	c.Assert(err, IsNil)

	err = node.Kill()
	c.Assert(err, IsNil)
}
