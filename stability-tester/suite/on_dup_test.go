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

var _ = Suite(&testOnDupSuite{})

type testOnDupSuite struct {
	db *sql.DB
}

func (s *testOnDupSuite) SetUpSuite(c *C) {
	s.db = openTestDB(c)
}

func (s *testOnDupSuite) TearDownSuite(c *C) {
	s.db.Close()
}

func (s *testOnDupSuite) TestOnDup(c *C) {
	onDupCase := NewOnDupCase(&config.Config{
		Suite: config.SuiteConfig{
			OnDup: config.OnDupConfig{
				NumRows: 10000,
			},
		},
	})

	ctx := context.Background()
	err := onDupCase.Initialize(ctx, s.db)
	c.Assert(err, IsNil)
	go func() {
		time.Sleep(5 * time.Second)
		close(onDupCase.(*OnDupCase).Done)
	}()
	err = onDupCase.Execute(s.db, 0)
	c.Assert(err, IsNil)
}
