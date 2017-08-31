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
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/config"
)

// Case is a test case for running cluster.
type Case interface {
	// Initialize initializes the case.
	// Because the initialization may take a lot of time,
	// we may output the process periodicity.
	Initialize(ctx context.Context, db *sql.DB) error

	// Execute executes the case once.
	Execute(db *sql.DB, concurrentIndex int) error

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

// InitSuite initializes all suites.
func InitSuite(ctx context.Context, cfg *config.Config, db *sql.DB) []Case {
	suiteCases := make([]Case, 0, len(cfg.Suite.Names))

	// Create all cases and initialize them.
	for _, name := range cfg.Suite.Names {
		suiteM, ok := suites[name]
		if !ok {
			log.Warnf("Not found this Suite Case: %s", name)
			continue
		}
		suiteCase := suiteM(cfg)
		err := suiteCase.Initialize(ctx, db)
		if err != nil {
			log.Fatal(err)
		}

		suiteCases = append(suiteCases, suiteCase)
	}

	return suiteCases
}

// RunSuite runs all suites.
func RunSuite(ctx context.Context, suiteCases []Case, concurrency int, db *sql.DB) {
	if len(suiteCases) == 0 {
		return
	}

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
					for _, c := range suiteCases {
						if err := c.Execute(db, i); err != nil {
							log.Errorf("[%s] execute failed %v", c, err)
						}
					}
				}
			}
		}(i)
	}

	wg.Wait()
}


func InitCase(ctx context.Context,casename string, cfg *config.Config, db *sql.DB) Case {
	suiteM, ok := suites[casename]
	if !ok {
		log.Warnf("Not found this Suite Case: %s", casename)
		continue
	}
	suiteCase := suiteM(cfg)
	err := suiteCase.Initialize(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	return suiteCase
}


func RunCase(ctx context.Context, case Case, concurrency int, db *sql.DB) {
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
					if err := case.Execute(db, i); err != nil {
						log.Errorf("[%s] execute failed %v", c, err)
					}
				}
			}
		}(i)
	}

	wg.Wait()
}
