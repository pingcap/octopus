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
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/cluster"
	"github.com/pingcap/octopus/stability-tester/config"
)

const (
	// KillNemesisName is the name of KillNemesis.
	KillNemesisName = "kill"
)

// KillNemesis kill a service based on strategies, waits some time and then starts again.
type KillNemesis struct {
	// WaitTime is the wait time to restart the server again
	// after killing the service, if not set, ignore this nemesis.
	WaitTime config.Duration `toml:"wait"`
}

// killFactory returns a KillNemesis, kills services in the nodeNames randomly.
func killFactory(cfg string) (Nemesis, error) {
	kill := new(KillNemesis)
	if err := toml.Unmarshal([]byte(cfg), kill); err != nil {
		return nil, errors.Trace(err)
	}

	return kill, nil
}

// Register KillNemesis.
func init() {
	RegisterNemesis(KillNemesisName, killFactory)
}

// Execute implements Nemesis Execute interface.
func (n *KillNemesis) Execute(ctx context.Context, targets []cluster.Node) (err error) {
	if n.WaitTime.Duration == time.Duration(0) {
		return errors.New("KillNemesis wait is 0")
	}

	for _, node := range targets {
		name := node.Name()

		// Kill
		err = retryOnError(func() error {
			return node.Kill()
		})
		if err != nil {
			return errors.Trace(err)
		}
		log.Infof("KillNemesis kills service %s", name)

		// Wait
		time.Sleep(n.WaitTime.Duration)

		// Start
		err = retryOnError(func() error {
			return node.Start()
		})
		if err != nil {
			return errors.Trace(err)
		}
		log.Infof("KillNemesis starts service %s", name)
	}

	return nil
}

// String implements fmt.Stringer interface.
func (n *KillNemesis) String() string {
	return KillNemesisName
}
