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
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/config"
)

type Node interface {
	// Start the services in this node.
	Start() error
	// Stop the services in this node.
	Stop() error
	// Kill the services in this node.
	Kill() error
	// IsRunning returns whether the services is running or not.
	IsRunning() (bool, error)
	// Execute executes a command in this node.
	Execute(ctx context.Context, command string) error
	// Name returns the name of this service
	Name() string
}

type NodeFactory func(cfg config.ServiceConfig) Node

var nodes = make(map[string]NodeFactory)

func RegisterNode(name string, nodeFunc NodeFactory) error {
	strings.ToLower(name)
	if _, ok := nodes[name]; ok {
		return errors.Errorf("%s is already registered", name)
	}
	nodes[name] = nodeFunc
	return nil
}

func newNode(cfg config.ServiceConfig) Node {
	nodeFactory, ok := nodes[cfg.Type]
	if !ok {
		log.Fatalf("node type %s not found", cfg.Type)
	}
	return nodeFactory(cfg)
}
