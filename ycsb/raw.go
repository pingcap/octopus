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
	"strings"

	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/types"
)

type rawKV struct {
	db *tikv.RawKVClient
}

func (c *rawKV) ReadRow(key uint64) (bool, error) {
	rowKey := tablecodec.EncodeRowKeyWithHandle(1, int64(key))
	data, err := c.db.Get(rowKey)
	if err != nil {
		return false, err
	}

	return data != nil, nil
}

func (c *rawKV) InsertRow(key uint64, fields []string) error {
	// Simulate TiDB data
	rowKey := tablecodec.EncodeRowKeyWithHandle(1, int64(key))
	cols := make([]types.Datum, len(fields))
	for i, v := range fields {
		cols[i].SetString(v)
	}

	rowData, err := tablecodec.EncodeRow(cols, colIDs, nil)
	if err != nil {
		return err
	}

	return c.db.Put([]byte(rowKey), rowData)
}

func (c *rawKV) Clone() Database {
	return c
}

func setupRawKV(pdAddr string) (Database, error) {
	// Open connection to server and create a database.
	db, err := tikv.NewRawKVClient(strings.Split(pdAddr, ","))
	if err != nil {
		return nil, err
	}

	return &rawKV{db: db}, nil
}
