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
	"net/url"
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
	parsedURL, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}

	switch parsedURL.Scheme {
	case "tidb", "mysql":
		parsedURL.Scheme = "tidb"
		return setupTiDB(parsedURL)
	default:
		return nil, fmt.Errorf("unsupported database: %s", parsedURL.Scheme)
	}
}
