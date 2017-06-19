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
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/octopus/stability-tester/config"
)

// binaryNode is for Binary type service in node.
type binaryNode struct {
	ssh     *sshNode
	name    string
	command []string
}

// New bianry Node
func binaryNodeFactory(cfg config.ServiceConfig) Node {
	b := &binaryNode{
		ssh:     newSSHNode(cfg),
		name:    cfg.Name,
		command: cfg.Command,
	}
	return b
}

// Start implements Node Start interface.
func (b *binaryNode) Start() error {
	//if service is running,return directly
	isRunning, err := b.IsRunning()
	if err != nil {
		return errors.Trace(err)
	}
	if isRunning {
		return nil
	}

	//In bianry node,we put all commands to config.Command
	//And start process with a specific name with 'exec -a' for stop this service
	//common will always run in backgroup
	cmd := strings.Join(b.command, " ")
	command := fmt.Sprintf("/usr/bin/nohup bash -c \\\n\"exec -a %s %s\" >/dev/null 2>&1 &", b.name, cmd)
	err = b.ssh.execCmd(command)
	if err != nil {
		return errors.Trace(err)

	}
	return nil
}

// Stop implements Node Stop interface.
func (b *binaryNode) Stop() error {
	//add a blank space at the end to avoid common prefix
	command := fmt.Sprintf("pkill -f '^%s '", b.name)
	err := b.ssh.execCmd(command)
	if err != nil {
		return errors.Trace(err)

	}
	return nil
}

// Kill implements Node Kill interface
func (b *binaryNode) Kill() error {
	command := fmt.Sprintf("pkill -f -9 '^%s '", b.name)
	err := b.ssh.execCmd(command)
	if err != nil {
		return errors.Trace(err)

	}
	return nil
}

// IsRunning implements Node IsRunning interface.
func (b *binaryNode) IsRunning() (bool, error) {
	//add a blank space at the end to avoid common prefix
	command := fmt.Sprintf("pgrep -f '^%s '| wc -l", b.name)
	res := b.ssh.execCmdWithResult(command)
	r := strings.TrimSpace(string(res))
	count, err := strconv.Atoi(r)
	if err != nil {
		return false, errors.Trace(err)
	}
	if count == 1 {
		return true, nil
	}
	if count > 1 {
		return false, errors.Errorf("Services have common prefix with the name")
	}
	return false, nil
}

// Execute implements Node Execute interface.
func (b *binaryNode) Execute(ctx context.Context, command string) error {
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
func (b *binaryNode) Name() string {
	return b.name
}

func init() {
	RegisterNode("binary", binaryNodeFactory)
}
