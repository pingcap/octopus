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

package config

// Configuration for different test cases.

// SuiteConfig is the configuration for all test cases.
type SuiteConfig struct {
	// Names contains all cases to be run later.
	Names []string `toml:"names"`
	// Concurrency is the concurrency to run all cases.
	Concurrency int                   `toml:"concurrency"`
	Bank        BankCaseConfig        `toml:"bank"`
	Bank2       Bank2CaseConfig       `toml:"bank2"`
	Ledger      LedgerConfig          `toml:"ledger"`
	CRUD        CRUDCaseConfig        `toml:"crud"`
	Log         LogCaseConfig         `toml:"log"`
	DDL         DDLCaseConfig         `toml:"ddl"`
	BlockWriter BlockWriterCaseConfig `toml:"block_writer"`
}

// BankCaseConfig is for bank test case.
type BankCaseConfig struct {
	// NumAccounts is total accounts
	NumAccounts int      `toml:"num_accounts"`
	Interval    Duration `toml:"interval"`
	TableNum    int      `toml:"table_num"`
}

// Bank2CaseConfig is for bank2 test case.
type Bank2CaseConfig struct {
	// NumAccounts is total accounts
	NumAccounts int      `toml:"num_accounts"`
	Interval    Duration `toml:"interval"`
	Contention  string   `toml:"contention"`
}

// LedgerConfig is for ledger test case.
type LedgerConfig struct {
	NumAccounts int      `toml:"num_accounts"`
	Interval    Duration `toml:"interval"`
}

// CRUDCaseConfig is for CRUD test case.
type CRUDCaseConfig struct {
	UserCount int `toml:"user_count"`
	PostCount int `toml:"post_count"`
	// Insert/delete users every interval.
	UpdateUsers int `toml:"update_users"`
	// Insert/delete posts every interval.
	UpdatePosts int      `toml:"update_posts"`
	Interval    Duration `toml:"interval"`
}

// LogCaseConfig is for Log test case
type LogCaseConfig struct {
	MaxCount    int      `toml:"max_count"`
	DeleteCount int      `toml:"delete_count"`
	Interval    Duration `toml:"interval"`
	TableNum    int      `toml:"table_num"`
}

// DDLCaseConfig is for DDL test case.
type DDLCaseConfig struct {
	InsertRows              int     `toml:"insert_rows"`
	InitialColumns          int     `toml:"initial_columns"`
	UpdateRatio             float32 `toml:"update_ratio"`
	DeleteRatio             float32 `toml:"delete_ratio"`
	AddIndexProbability     float32 `toml:"add_index_probability"`
	DropIndexProbability    float32 `toml:"drop_index_probability"`
	AddColumnProbability    float32 `toml:"add_column_probability"`
	DropColumnProbability   float32 `toml:"drop_column_probability"`
	RenameColumnProbability float32 `toml:"rename_column_probability"`
}

type BlockWriterCaseConfig struct {
	TableNum int `toml:"table_num"`
}

// MVCCSuiteConfig is the configuration for all MVCC test cases.
type MVCCSuiteConfig struct {
	// Names contains all cases to be run later.
	Names []string `toml:"names"`
	// Concurrency is the concurrency to run all cases.
	Concurrency int            `toml:"concurrency"`
	Bank        BankCaseConfig `toml:"bank"`
}
