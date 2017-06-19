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
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/config"
)

// Cluster control the whole cluster
type Cluster struct {
	nodes map[string]Node
}

// NewCluster return the Cluster
func NewCluster(cfg *config.ClusterConfig) *Cluster {
	cluster := &Cluster{}
	cluster.nodes = make(map[string]Node)

	for _, s := range cfg.Services {
		node := newNode(s)
		cluster.AddNode(node)
	}

	return cluster
}

// AddNode can add the node to the cluster
func (c *Cluster) AddNode(node Node) {
	name := node.Name()
	if _, ok := c.nodes[name]; ok {
		log.Errorf("Duplicated node name %s", name)
		return
	}
	c.nodes[name] = node
}

// GetNode return the node of the cluster
func (c *Cluster) GetNode(name string) (Node, error) {
	node, ok := c.nodes[name]
	if !ok {
		return nil, errors.Errorf("no node named %s in the cluster", name)
	}
	return node, nil
}

// NewEmptyCluster returns an empty cluster, used in tests
func NewEmptyCluster() *Cluster {
	return &Cluster{
		nodes: make(map[string]Node),
	}
}
