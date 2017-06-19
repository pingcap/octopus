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
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/cluster"
	"github.com/pingcap/octopus/stability-tester/config"
)

// Factory is the factory type of Nemesis.
type Factory func(string) (Nemesis, error)

var (
	// TODO: do we need a mutex after all? RegisterNemesis are called in init functions.
	nemesesMu sync.RWMutex
	nemeses   = make(map[string]Factory)

	// For now, ranges is read only once the program was initialized, just same as nemeses above.
	ranges = make(map[string]Range)
)

// RegisterNemesis registers a Nemesis.
func RegisterNemesis(name string, n Factory) {
	nemesesMu.Lock()
	nemeses[name] = n
	nemesesMu.Unlock()
}

// Nemesis introduces failures across the cluster inspired by Jepsen.
//
// TODO: replace Execute() with Start() and Stop()?
type Nemesis interface {
	// Execute executes the nemesis on the targets.
	Execute(ctx context.Context, targets []cluster.Node) error

	// String implements fmt.Stringer interface.
	String() string
}

// RunNemeses runs all nemeses in background.
func RunNemeses(ctx context.Context, ncfg *config.NemesesConfig, c *cluster.Cluster) {
	if len(ncfg.Configs) == 0 {
		log.Info("nemeses is disabled")
		return
	}

	allNemeses := make([]Nemesis, 0, len(ncfg.Configs))
	nemesesMu.RLock()
	for name, cfg := range ncfg.Configs {
		if factory, ok := nemeses[name]; ok {
			n, err := factory(cfg)
			if err != nil {
				log.Warning(err)
			}
			allNemeses = append(allNemeses, n)
		} else {
			log.Warning("unkonw nemesis: %s", name)
		}
	}
	nemesesMu.RUnlock()

	log.Infof("running all nemeses: %v", ncfg)
	scheduler(ctx, c, allNemeses, ncfg.Ranges, ncfg.Targets, ncfg.Wait)
}

func scheduler(ctx context.Context, c *cluster.Cluster, ns []Nemesis, rangesKey []string, targets *config.Targets, duration config.Duration) {
	ticker := time.NewTicker(duration.Duration)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:

			// Choose a nemesis.
			n := ns[rand.Intn(len(ns))]

			// Choose a set of targets.
			strategy := rangesKey[rand.Intn(len(rangesKey))]
			nodes := ranges[strategy](c, targets)

			log.Infof("nemesis %s starts on nodes %s", n, nodesName(nodes))

			err := n.Execute(ctx, nodes)
			if err != nil {
				log.Warning(err)
			}
			log.Infof("nemesis: %s finished", n)
		}
	}
}
