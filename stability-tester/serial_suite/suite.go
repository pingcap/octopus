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

package serial_suite

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/config"
	"golang.org/x/net/context"
)

// Case is a serial test case for running cluster.
type Case interface {
	// Initialize initializes the case.
	// Because the initialization may take a lot of time,
	// we may output the process periodicity.
	Initialize() error

	// Execute executes the case once.
	Execute(ctx context.Context) error

	// String implements fmt.Stringer interface.
	String() string
}

type SuiteMaker func(cfg *config.Config) Case

var suites = make(map[string]SuiteMaker)

func RegisterSuite(name string, f SuiteMaker) error {
	if _, ok := suites[name]; ok {
		return errors.Errorf("%s is already registerd", name)
	}
	suites[name] = f
	return nil
}

// InitSuite initializes all serial suites.
func InitSuite(ctx context.Context, cfg *config.Config) []Case {
	suiteCases := make([]Case, 0, len(cfg.SerialSuite.Names))

	// Create all cases and initialize them.
	for _, name := range cfg.SerialSuite.Names {
		suiteM, ok := suites[name]
		if !ok {
			log.Warnf("Not found this Suite Case: %s", name)
			continue
		}
		suiteCase := suiteM(cfg)
		err := suiteCase.Initialize()
		if err != nil {
			log.Fatal(err)
		}

		suiteCases = append(suiteCases, suiteCase)
	}

	return suiteCases
}

// RunSuite runs all serial suites.
func RunSuite(ctx context.Context, suiteCases []Case) {
	if len(suiteCases) == 0 {
		return
	}

	for _, c := range suiteCases {
		go func(c Case) {
			if err := c.Execute(ctx); err != nil {
				log.Fatalf("[%s] execute failed %v", c, err)
			}

		}(c)
	}
}
