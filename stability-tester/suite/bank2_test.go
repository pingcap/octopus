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

package suite

import (
	"context"
	"database/sql"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/octopus/stability-tester/config"
)

var _ = Suite(&testBank2Suite{})

type testBank2Suite struct {
	db *sql.DB
}

func (s *testBank2Suite) SetUpSuite(c *C) {
	s.db = openTestDB(c)
}

func (s *testBank2Suite) TearDownSuite(c *C) {
	s.db.Close()
}

func (s *testBank2Suite) TestTransfer(c *C) {
	caseConf := config.Bank2CaseConfig{
		NumAccounts: 100,
		Interval: config.Duration{
			Duration: 2 * time.Second,
		},
		Contention: "low",
	}
	suiteConf := config.SuiteConfig{
		Concurrency: 10,
		Bank2:       caseConf,
	}
	conf := config.Config{
		Suite: suiteConf,
	}
	bank2Case := NewBank2Case(&conf).(*Bank2Case)
	ctx, cancel := context.WithCancel(context.Background())
	bank2Case.Initialize(ctx, s.db)
	for i := 0; i < 10; i++ {
		err := bank2Case.Execute(s.db, i)
		c.Assert(err, IsNil)
	}
	bank2Case.verify(s.db)
	cancel()
}
