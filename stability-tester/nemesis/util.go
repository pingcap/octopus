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
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/cluster"
)

const (
	// RetryCount is the maximum retry count.
	RetryCount = 10

	// RetryInterval is the interval that each retry must wait.
	RetryInterval = 30 * time.Second
)

func retryOnError(fn func() error) error {
	var err error
	for i := 0; i < RetryCount; i++ {
		err = fn()
		if err == nil {
			break
		}

		log.Warning(err)
		time.Sleep(RetryInterval)
	}

	return errors.Trace(err)
}

func nodesName(ns []cluster.Node) string {
	names := make([]string, 0, len(ns))
	for _, n := range ns {
		names = append(names, n.Name())
	}
	return strings.Join(names, " ")
}
