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

	. "github.com/pingcap/check"
	"github.com/pingcap/octopus/stability-tester/config"
)

var _ = Suite(&testBlockWriterSuite{})

type testBlockWriterSuite struct {
	db *sql.DB
}

func (s *testBlockWriterSuite) SetUpSuite(c *C) {
	s.db = openTestDB(c)
}

func (s *testBlockWriterSuite) TearDownSuite(c *C) {
	s.db.Close()
}

func (s *testBlockWriterSuite) TestTransfer(c *C) {
	blockWriterCase := &BlockWriterCase{
		cfg: &config.BlockWriterCaseConfig{
			TableNum: 3,
		},
	}

	blockWriterCase.initBlocks(10)

	ctx := context.Background()

	blockWriterCase.Initialize(ctx, s.db)

	for i := 0; i < 10; i++ {
		for j := 0; j < 3; j++ {
			err := blockWriterCase.Execute(s.db, i)
			c.Assert(err, IsNil)
		}
	}
}
