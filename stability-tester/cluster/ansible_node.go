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
	"context"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/octopus/stability-tester/config"
)

// ansibleNode is for deploying by tidb_ansible node.
type ansibleNode struct {
	ssh     *sshNode
	name    string
	user    string
	command []string
	wrap    string
	path    string
}

// New ansible Node
func ansibleNodeFactory(cfg config.ServiceConfig) Node {
	b := &ansibleNode{
		ssh:     newSSHNode(cfg),
		name:    cfg.Name,
		command: cfg.Command,
		user:    cfg.SSH.User,
		path:    cfg.Path,
	}

	return b
}

// Start implements Node Start interface.
func (b *ansibleNode) Start() error {
	serverName := b.name[:strings.Index(b.name, "-")]
	command := fmt.Sprintf("%s/bin/svc -u %s/status/%s", b.path, b.path, serverName)
	err := b.ssh.execCmd(command)
	if err != nil {
		return errors.Trace(err)

	}
	return nil
}

// Stop implements Node Stop interface.
func (b *ansibleNode) Stop() error {
	return nil
}

// Kill implements Node Kill interface
func (b *ansibleNode) Kill() error {
	serverName := b.name[:strings.Index(b.name, "-")]
	command := fmt.Sprintf("%s/bin/svc -k  %s/status/%s;%s/bin/svc -d %s/status/%s", b.path, b.path, serverName, b.path, b.path, serverName)
	err := b.ssh.execCmd(command)
	if err != nil {
		return errors.Trace(err)

	}
	return nil
}

// IsRunning implements Node IsRunning interface.
func (b *ansibleNode) IsRunning() (bool, error) {
	return true, nil
}

// Execute implements Node Execute interface.
func (b *ansibleNode) Execute(ctx context.Context, command string) error {
	cmdCh := make(chan error)
	go func() {
		err := b.ssh.execCmd(command)
		cmdCh <- errors.Trace(err)
	}()
	select {
	case err := <-cmdCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Name implements Node Name interface.
func (b *ansibleNode) Name() string {
	return b.name
}

func init() {
	RegisterNode("ansible", ansibleNodeFactory)
}
