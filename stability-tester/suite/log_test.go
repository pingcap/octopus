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

var _ = Suite(&testLogSuite{})

type testLogSuite struct {
	db *sql.DB
}

func (s *testLogSuite) SetUpSuite(c *C) {
	s.db = openTestDB(c)
}

func (s *testLogSuite) TearDownSuite(c *C) {
	s.db.Close()
}

func (s *testLogSuite) TestTransfer(c *C) {
	logCase := &LogCase{
		cfg: &config.LogCaseConfig{
			MaxCount:    100,
			DeleteCount: 10,
			Interval:    config.Duration{1 * time.Second},
			TableNum:    3,
		},
	}

	logCase.initLogWrite(140)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	logCase.Initialize(ctx, s.db)

	for i := 0; i < 10; i++ {
		for j := 0; j < 3; j++ {
			err := logCase.Execute(s.db, i)
			c.Assert(err, IsNil)
		}
	}

	time.Sleep(3 * time.Second)
	cancel()
}
