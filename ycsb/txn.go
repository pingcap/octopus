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

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	goctx "golang.org/x/net/context"
)

type txnKV struct {
	db kv.Storage
}

func (c *txnKV) ReadKey(k string) (string, error) {
	tx, err := c.db.Begin()
	if err != nil {
		return "", err
	}

	key := []byte(k)
	data, err := tx.Get(key)
	if err != nil {
		return "", err
	}

	defer tx.Rollback()

	if err := tx.Commit(goctx.Background()); err != nil {
		return "", err
	}

	return string(data), nil
}

// func (c *txnKV) ReadKeys(key []byte) ([]byte, error) {
//     tx, err := c.db.Begin()
//     if err != nil {
//         return []byte{}, err
//     }
//
//     ts = tx.StartTS()
//     snapshot, err = c.db.GetSnapshot()
//     snapshot.BatchGet([][]byte)
// }

func (c *txnKV) InsertKey(k, v string) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	key, value := []byte(k), []byte(v)
	if err = tx.Set(key, value); err != nil {
		return err
	}

	return tx.Commit(goctx.Background())
}

func (c *txnKV) Clone() Database {
	return c
}

func setupTxnKV(pdAddr string) (Database, error) {
	// Open connection to server and create a database.
	driver := tikv.Driver{}
	db, err := driver.Open(fmt.Sprintf("tikv://%s?disableGC=true", pdAddr))
	if err != nil {
		return nil, err
	}

	return &txnKV{db: db}, nil
}
