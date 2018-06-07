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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	goctx "golang.org/x/net/context"
)

var colIDs = []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

type txnKV struct {
	db             kv.Storage
	deleteAsInsert bool
}

func (c *txnKV) ReadRow(key uint64) (bool, error) {
	tx, err := c.db.Begin()
	if err != nil {
		return false, err
	}

	rowKey := tablecodec.EncodeRowKeyWithHandle(1, int64(key))
	data, err := tx.Get(rowKey)
	if err != nil {
		return false, err
	}

	defer tx.Rollback()

	if err := tx.Commit(goctx.Background()); err != nil {
		return false, err
	}

	return data == nil, nil
}

func (c *txnKV) InsertRow(key uint64, fields []string) error {
	// Simulate TiDB data
	rowKey := tablecodec.EncodeRowKeyWithHandle(1, int64(key))
	if c.deleteAsInsert {
		tx, err := c.db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := tx.Delete(rowKey); err != nil {
			return err
		}
		return tx.Commit(goctx.Background())
	} else {
		cols := make([]types.Datum, len(fields))
		for i, v := range fields {
			cols[i].SetString(v)
		}

		rowData, err := tablecodec.EncodeRow(&stmtctx.StatementContext{}, cols, colIDs, nil, nil)
		if err != nil {
			return err
		}

		tx, err := c.db.Begin()
		if err != nil {
			return err
		}

		defer tx.Rollback()

		if err = tx.Set(rowKey, rowData); err != nil {
			return err
		}

		return tx.Commit(goctx.Background())
	}
}

func (c *txnKV) Clone() Database {
	return c
}

func setupTxnKV(pdAddr string, deleteAsInsert bool) (Database, error) {
	// Open connection to server and create a database.
	tikv.MaxConnectionCount = 128
	driver := tikv.Driver{}
	db, err := driver.Open(fmt.Sprintf("tikv://%s?disableGC=true", pdAddr))
	if err != nil {
		return nil, err
	}

	return &txnKV{db: db, deleteAsInsert: deleteAsInsert}, nil
}
