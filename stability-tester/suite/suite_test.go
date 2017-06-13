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
	"database/sql"
	"flag"
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
)

var (
	testHost     = flag.String("host", "127.0.0.1", "test MySQL host")
	testPort     = flag.Int("port", 3306, "test MySQL port")
	testUser     = flag.String("user", "root", "test MySQL user")
	testPassword = flag.String("password", "", "test MySQL password")
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

func openTestDB(c *C) *sql.DB {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@(%s:%d)/test", *testUser, *testPassword, *testHost, *testPort))
	c.Assert(err, IsNil)

	return db
}
