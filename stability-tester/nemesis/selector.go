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

package nemesis

import (
	"math/rand"

	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/cluster"
	"github.com/pingcap/octopus/stability-tester/config"
)

// Selector picks a set of nodes.
type Selector func(*cluster.Cluster, *config.Targets) []cluster.Node

func flatTargets(targets *config.Targets) []string {
	var all []string
	table := [][]string{targets.Pd, targets.Tidb, targets.Tikv}
	for _, t := range table {
		for _, i := range t {
			if len(i) != 0 {
				all = append(all, i)
			}
		}
	}
	return all
}

// OneTarget pick one node randomly.
func OneTarget(c *cluster.Cluster, targets *config.Targets) []cluster.Node {
	all := flatTargets(targets)
	t, err := c.GetNode(all[rand.Int()%len(all)])
	if err != nil {
		log.Warning(err)
		return []cluster.Node{}
	}

	return []cluster.Node{t}
}

func AllTarget(c *cluster.Cluster, targets *config.Targets) []cluster.Node {
	all := flatTargets(targets)
	var res []cluster.Node
	for _, name := range all {
		t, err := c.GetNode(name)
		if err != nil {
			log.Warning(err)
			continue
		}
		res = append(res, t)
	}
	return res
}

// TODO: add more Selector

func init() {
	selectors["one"] = OneTarget
	selectors["all"] = AllTarget
}
