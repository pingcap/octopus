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

package config

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/ngaut/log"
)

var addSchedulerInterval = time.Minute

// SchedulerConfig is used to customize the scheduler configs for PD.
type SchedulerConfig struct {
	PDAddrs       []string `toml:"pd"`
	ShuffleLeader bool     `toml:"shuffle-leader"`
	ShuffleRegion bool     `toml:"shuffle-region"`
}

// RunConfigScheduler continuously adds schedulers using pd api.
func RunConfigScheduler(ctx context.Context, conf *SchedulerConfig) {
	var schedulerNames []string
	if conf.ShuffleLeader {
		schedulerNames = append(schedulerNames, "shuffle-leader-scheduler")
	}
	if conf.ShuffleRegion {
		schedulerNames = append(schedulerNames, "shuffle-region-scheduler")
	}

	for {
		for _, name := range schedulerNames {
			select {
			case <-ctx.Done():
				return
			default:
			}
			data := map[string]string{"name": name}
			b, _ := json.Marshal(data)
			for _, addr := range conf.PDAddrs {
				_, err := http.Post(addr+"pd/api/v1/schedulers", "application/json", bytes.NewBuffer(b))
				if err != nil {
					log.Errorf("add scheduler to pd %v err: %v", addr, err)
				}
			}
		}

		time.Sleep(addSchedulerInterval)
	}
}
