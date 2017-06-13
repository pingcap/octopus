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

// dockerNode is for docker type service in node.
type dockerNode struct {
	ssh  *sshNode
	name string
}

// New docker Node
func dockerNodeFactory(cfg config.ServiceConfig) Node {
	d := &dockerNode{
		ssh:  newSSHNode(cfg),
		name: cfg.Name,
	}
	return d
}

// Start implements Node Start interface.
func (d *dockerNode) Start() error {
	isRunning, err := d.IsRunning()
	if err != nil {
		return errors.Trace(err)
	}
	if isRunning {
		return nil
	}

	cmd := fmt.Sprintf("docker start %s", d.name)
	err = d.ssh.execCmd(cmd)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Stop implements Node Stop interface.
func (d *dockerNode) Stop() error {
	cmd := fmt.Sprintf("docker stop %s", d.name)
	err := d.ssh.execCmd(cmd)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Kill implements Node Kill interface.
func (d *dockerNode) Kill() error {
	cmd := fmt.Sprintf("docker kill %s", d.name)
	err := d.ssh.execCmd(cmd)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// IsRunning implements Node IsRunning interface
func (d *dockerNode) IsRunning() (bool, error) {
	cmd := fmt.Sprintf("docker ps -f name=%s | grep '%s$' |wc -l", d.name, d.name)
	res := d.ssh.execCmdWithResult(cmd)
	r := strings.TrimSpace(string(res))
	count, err := strconv.Atoi(r)
	if err != nil {
		return false, errors.Trace(err)
	}
	if count == 1 {
		return true, nil
	}
	return false, nil
}

// Execute implements Node Execute interface
func (d *dockerNode) Execute(ctx context.Context, command string) error {
	cmdCh := make(chan error)
	go func() {
		err := d.ssh.execCmd(command)
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
func (d *dockerNode) Name() string {
	return d.name
}

func init() {
	RegisterNode("docker", dockerNodeFactory)
}
