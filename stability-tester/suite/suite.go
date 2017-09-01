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

package suite

import (
	"context"
	"database/sql"
	"fmt"

	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/pingcap/octopus/stability-tester/config"
)

// Case is a test case for running cluster.
type Case interface {
	// Initialize initializes the case.
	// Because the initialization may take a lot of time,
	// we may output the process periodicity.
	Initialize(ctx context.Context, db *sql.DB, logger *log.Logger) error

	// Execute executes the case once.
	Execute(ctx context.Context, db *sql.DB) error

	// String implements fmt.Stringer interface.
	String() string
}

type SuiteMaker func(cfg *config.Config) Case

var suites = make(map[string]SuiteMaker)

func RegisterSuite(name string, f SuiteMaker) error {
	if _, ok := suites[name]; ok {
		return errors.Errorf("%s is already registerd", name)
	}
	suites[name] =
		f
	return nil
}

// RunSuite runs all suites.
func RunSuite(ctx context.Context, suiteCases []Case, db *sql.DB) {
	for _, c := range suiteCases {
		go func(c Case) {
			if err := c.Execute(ctx, db); err != nil {
				log.Fatalf("[%s] execute failed %v", c, err)
			}
		}(c)
	}

}

// InitCase is init case
func InitCase(ctx context.Context, cfg *config.Config, db *sql.DB) []Case {
	// Create all cases and initialize them.
	var suiteCases []Case
	for _, name := range cfg.Suite.Names {
		select {
		case <-ctx.Done():
			var tempCases []Case
			return tempCases
		default:
			suiteM, ok := suites[name]
			if !ok {
				log.Warnf("Not found this Suite Case: %s", name)
				continue
			}
			logger := newLogger(fmt.Sprintf("%s-stability-tester.log", name))
			suiteCase := suiteM(cfg)
			err := suiteCase.Initialize(ctx, db, logger)
			if err != nil {
				log.Fatal(err)
			}

			suiteCases = append(suiteCases, suiteCase)
		}
	}
	return suiteCases
}
