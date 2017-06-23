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

package nemesis

import (
	"context"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/cluster"
)

const (
	// RollingKillNemesisName is the name of RollingKillNemesis.
	RollingKillNemesisName = "rolling_kill"
)

var defaultInterval = 10 * time.Second

// RollingKillNemesis kill a service based on strategies, waits some time and then starts again.
type RollingKillNemesis struct {
}

// rollingUpdateFactory returns a RollingKillNemesis, kills services in the nodeNames randomly.
func rollingUpdateFactory(cfg string) (Nemesis, error) {
	r := new(RollingKillNemesis)
	return r, nil
}

// Register RollingKillNemesis.
func init() {
	RegisterNemesis(RollingKillNemesisName, rollingUpdateFactory)
}

// Execute implements Nemesis Execute interface.
func (n *RollingKillNemesis) Execute(ctx context.Context, targets []cluster.Node) (err error) {
	for _, node := range targets {
		name := node.Name()

		// Kill
		err = retryOnError(func() error {
			return node.Kill()
		})
		if err != nil {
			return errors.Trace(err)
		}
		log.Infof("RollingKillNemesis kills service %s", name)
		time.Sleep(defaultInterval)

		// Start
		err = retryOnError(func() error {
			return node.Start()
		})
		if err != nil {
			return errors.Trace(err)
		}
		log.Infof("RollingKillNemesis starts service %s", name)
	}

	return nil
}

// GetSelector implements Nemesis GetSelector interface.
func (n *RollingKillNemesis) GetSelector() Selector {
	return AllTarget
}

// String implements fmt.Stringer interface.
func (n *RollingKillNemesis) String() string {
	return RollingKillNemesisName
}
