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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/octopus/stability-tester/config"
)

var _ = Suite(&testBankSuite{})

type testBankMultSuite struct {
	db *sql.DB
}

func (s *testBankMultSuite) SetUpSuite(c *C) {
	s.db = openTestDB(c)
}

func (s *testBankMultSuite) TearDownSuite(c *C) {
	s.db.Close()
}

func (s *testBankMultSuite) TestTransfer(c *C) {
	bankMultCase := &BankMultCase{
		cfg: &config.BankMultCaseConfig{
			NumAccounts: 100,
			Interval:    config.Duration{2 * time.Second},
			TableNum:    3,
		},
		concurrency: 10,
	}

	ctx, cancel := context.WithCancel(context.Background())

	bankMultCase.Initialize(ctx, s.db)

	for i := 0; i < 10; i++ {
		err := bankMultCase.Execute(s.db, i)
		c.Assert(err, IsNil)
	}

	bankMultCase.verify(s.db, "")
	cancel()
}
