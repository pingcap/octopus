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
	"bytes"
	"database/sql"
	"fmt"

	"github.com/ngaut/log"
)

type tidb struct {
	db *sql.DB
}

func (c *tidb) ReadRow(key uint64) (bool, error) {
	res, err := c.db.Query(fmt.Sprintf("SELECT * FROM ycsb.usertable WHERE ycsb_key=%d", key))
	if err != nil {
		return false, err
	}
	var rowsFound int
	for res.Next() {
		rowsFound++
	}

	log.Debugf("reader found %d rows for key %d", rowsFound, key)

	if err := res.Close(); err != nil {
		return false, err
	}
	return rowsFound == 0, nil
}

func (c *tidb) InsertRow(key uint64, fields []string) error {
	// TODO(arjun): Consider using a prepared statement here.
	var buf bytes.Buffer
	buf.WriteString("INSERT INTO ycsb.usertable VALUES (")
	fmt.Fprintf(&buf, "%d", key)
	for _, s := range fields {
		fmt.Fprintf(&buf, ", '%s'", s)
	}
	buf.WriteString(")")
	_, err := c.db.Exec(buf.String())
	return err
}

func (c *tidb) Clone() Database {
	return c
}

func setupTiDB(url string) (Database, error) {
	// Open connection to server and create a database.
	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}

	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS ycsb"); err != nil {
		log.Fatalf("Failed to create the database %v", err)
	}

	if *drop {
		log.Debugf("Dropping the table")
		if _, err := db.Exec("DROP TABLE IF EXISTS ycsb.usertable"); err != nil {
			log.Fatalf("Failed to drop the table: %s", err)
			return nil, err
		}
	}

	// Create the initial table for storing blocks.
	createStmt := `
CREATE TABLE IF NOT EXISTS ycsb.usertable (
    ycsb_key BIGINT PRIMARY KEY NOT NULL,
    FIELD1 TEXT,
    FIELD2 TEXT,
    FIELD3 TEXT,
    FIELD4 TEXT,
    FIELD5 TEXT,
    FIELD6 TEXT,
    FIELD7 TEXT,
    FIELD8 TEXT,
    FIELD9 TEXT,
    FIELD10 TEXT
)`
	if _, err := db.Exec(createStmt); err != nil {
		return nil, err
	}

	return &tidb{db: db}, nil
}
