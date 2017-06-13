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

var _ = Suite(&testCRUDSuite{})

type testCRUDSuite struct {
	db *sql.DB
}

func (s *testCRUDSuite) SetUpSuite(c *C) {
	s.db = openTestDB(c)
}

func (s *testCRUDSuite) TearDownSuite(c *C) {
	s.db.Close()
}

func (s *testCRUDSuite) TestCRUD(c *C) {
	crudCase := NewCRUDCase(&config.Config{
		Suite: config.SuiteConfig{
			CRUD: config.CRUDCaseConfig{
				UserCount:   10,
				PostCount:   10,
				UpdateUsers: 20,
				UpdatePosts: 20,
				Interval:    config.Duration{2 * time.Millisecond},
			},
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	crudCase.Initialize(ctx, s.db)
	for i := 0; i < 10; i++ {
		err := crudCase.Execute(s.db, 0)
		c.Assert(err, IsNil)
	}
	cancel()
}
