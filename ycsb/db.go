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

package main

import (
	"fmt"
	"strings"
)

type Database interface {
	ReadRow(key uint64) (bool, error)
	InsertRow(key uint64, fields []string) error
	Clone() Database
}

// setupDatabase performs initial setup for the example, creating a database
// with a single table. If the desired table already exists on the cluster, the
// existing table will be dropped if the -drop flag was specified.
func SetupDatabase(dbURL string) (Database, error) {
	seps := strings.SplitN(dbURL, "://", 2)
	if len(seps) != 2 {
		return nil, fmt.Errorf("invalid url %s, must be scheme://path", dbURL)
	}

	switch seps[0] {
	case "tidb", "mysql":
		// url is tidb://username:password@protocol(address)
		return setupTiDB(seps[1])
	case "tikv", "txn":
		// url is tikv://pd_addr
		return setupTxnKV(seps[1])
	case "raw":
		// url is raw://pd_addr
		return setupRawKV(seps[1])
	default:
		return nil, fmt.Errorf("unsupported database: %s", seps[0])
	}
}
