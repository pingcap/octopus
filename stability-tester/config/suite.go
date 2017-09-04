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
	Concurrency  int                    `toml:"concurrency"`
	Bank         BankCaseConfig         `toml:"bank"`
	Bank2        Bank2CaseConfig        `toml:"bank2"`
	Ledger       LedgerConfig           `toml:"ledger"`
	CRUD         CRUDCaseConfig         `toml:"crud"`
	Log          LogCaseConfig          `toml:"log"`
	BlockWriter  BlockWriterCaseConfig  `toml:"block_writer"`
	MVCCBank     BankCaseConfig         `toml:"mvcc_bank"`
	Sysbench     SysbenchCaseConfig     `toml:"sysbench"`
	SqllogicTest SqllogicTestCaseConfig `toml:"sqllogic_test"`
	SmallWriter  SmallWriterCaseConfig  `toml:"small_writer"`
}

// BankCaseConfig is for bank test case.
type BankCaseConfig struct {
	// NumAccounts is total accounts
	NumAccounts int      `toml:"num_accounts"`
	Interval    Duration `toml:"interval"`
	TableNum    int      `toml:"table_num"`
	Concurrency int      `toml:"concurrency"`
}

// Bank2CaseConfig is for bank2 test case.
type Bank2CaseConfig struct {
	// NumAccounts is total accounts
	NumAccounts int      `toml:"num_accounts"`
	Interval    Duration `toml:"interval"`
	Contention  string   `toml:"contention"`
	Concurrency int      `toml:"concurrency"`
}

// LedgerConfig is for ledger test case.
type LedgerConfig struct {
	NumAccounts int      `toml:"num_accounts"`
	Interval    Duration `toml:"interval"`
	Concurrency int      `toml:"concurrency"`
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
	Concurrency int      `toml:"concurrency"`
}

// LogCaseConfig is for Log test case
type LogCaseConfig struct {
	MaxCount    int      `toml:"max_count"`
	DeleteCount int      `toml:"delete_count"`
	Interval    Duration `toml:"interval"`
	TableNum    int      `toml:"table_num"`
	Concurrency int      `toml:"concurrency"`
}

// BlockWriterCaseConfig is for block write test case
type BlockWriterCaseConfig struct {
	TableNum    int `toml:"table_num"`
	Concurrency int `toml:"concurrency"`
}

// SysbenchCaseConfig is for sysbench test case
type SysbenchCaseConfig struct {
	TableCount int      `toml:"table_count"`
	TableSize  int      `toml:"table_size"`
	Threads    int      `toml:"threads"`
	MaxTime    int      `toml:"max_time"`
	Interval   Duration `toml:"interval"`
	DBName     string   `toml:"database"`
	LuaPath    string   `toml:"lua_path"`
}

// SqllogictestCaseConfig is for sqllogic_test test case
type SqllogicTestCaseConfig struct {
	TestPath  string `toml:"test_path"`
	SkipError bool   `toml:"skipError"`
	Parallel  int    `toml:"parallel"`
	DBName    string `toml:"database"`
}

// SmallWriterCase is for small write test case
type SmallWriterCaseConfig struct {
	Concurrency int `toml:"concurrency"`
}
