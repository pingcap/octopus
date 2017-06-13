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
	"fmt"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/cluster"
	"github.com/pingcap/octopus/stability-tester/config"
)

const (
	// LantencyNemesisName is the name of LantencyNemesis.
	LantencyNemesisName = "lantency"
)

// LantencyNemesis lantency a service based on strategies, waits some time and then starts again.
type LantencyNemesis struct {
	// WaitTime is the wait time to restart the server again
	// after lantencying the service, if not set, ignore this nemesis.
	WaitTime      config.Duration `toml:"wait"`
	Device        string          `toml:"dev"`
	Lantency      int             `toml:"lantency"`
	LantencyRange int             `toml:"range"`
}

// lantencyFactory returns a LantencyNemesis, lantencys services in the nodeNames randomly.
func lantencyFactory(cfg string) (Nemesis, error) {
	lantency := new(LantencyNemesis)
	if err := toml.Unmarshal([]byte(cfg), lantency); err != nil {
		return nil, errors.Trace(err)
	}

	return lantency, nil
}

// Register LantencyNemesis.
func init() {
	RegisterNemesis(LantencyNemesisName, lantencyFactory)
}

// Execute implements Nemesis Execute interface.
func (n *LantencyNemesis) Execute(ctx context.Context, targets []cluster.Node) (err error) {
	if n.WaitTime.Duration == time.Duration(0) {
		return errors.New("LantencyNemesis wait is 0")
	}
	if n.Device == "" {
		return errors.New("LantencyNemesis should set device")
	}
	if n.Lantency == 0 {
		return nil
	}

	for _, node := range targets {
		name := node.Name()

		// SetLantency
		err = retryOnError(func() error {
			cmd := fmt.Sprintf("sudo tc qdisc add dev %s root netem delay %dms %dms distribution normal", n.Device, n.Lantency, n.LantencyRange)
			err := node.Execute(ctx, cmd)
			if err != nil {
				cmd = fmt.Sprintf("sudo tc qdisc change dev %s root netem delay %dms %dms distribution normal", n.Device, n.Lantency, n.LantencyRange)
			}
			return node.Execute(ctx, cmd)
		})
		if err != nil {
			return errors.Trace(err)
		}
		log.Infof("LantencyNemesis set lantency(%dms±%dms) in  service %s", n.Lantency, n.LantencyRange, name)

		// Wait
		time.Sleep(n.WaitTime.Duration)

		// RemoveLantency
		err = retryOnError(func() error {
			cmd := fmt.Sprintf("sudo tc qdisc del dev %s root netem", n.Device)
			return node.Execute(ctx, cmd)

		})
		if err != nil {
			return errors.Trace(err)
		}
		log.Infof("LantencyNemesis starts service %s", name)
	}

	return nil
}

// String implements fmt.Stringer interface.
func (n *LantencyNemesis) String() string {
	return LantencyNemesisName
}
