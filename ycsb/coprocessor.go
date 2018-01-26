// Copyright 2018 PingCAP, Inc.
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
	"math"
	"math/rand"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

const (
	TABLESCAN_POINT_GET  = 0
	TABLESCAN_COUNT_STAR = 1
)

type coprocessor struct {
	db       kv.Storage
	table    *simpleTable
	r        *rand.Rand
	readType int
}

type simpleTable struct {
	id      tipb.ColumnInfo
	name    tipb.ColumnInfo
	score   tipb.ColumnInfo
	tableID int64
}

func newSimpleTable() *simpleTable {
	table := &simpleTable{
		tableID: 10001,
	}
	table.id = tipb.ColumnInfo{
		PkHandle:  true,
		ColumnId:  1,
		Collation: int32(mysql.DefaultCollationID),
		ColumnLen: types.UnspecifiedLength,
		Decimal:   types.UnspecifiedLength,
		Tp:        int32(mysql.TypeLong),
	}
	table.name = tipb.ColumnInfo{
		PkHandle:  false,
		ColumnId:  2,
		Collation: int32(mysql.DefaultCollationID),
		ColumnLen: types.UnspecifiedLength,
		Decimal:   types.UnspecifiedLength,
		Tp:        int32(mysql.TypeVarchar),
	}
	table.score = tipb.ColumnInfo{
		PkHandle:  false,
		ColumnId:  3,
		Collation: int32(mysql.DefaultCollationID),
		ColumnLen: types.UnspecifiedLength,
		Decimal:   types.UnspecifiedLength,
		Tp:        int32(mysql.TypeLong),
	}
	return table
}

func (t *simpleTable) colIDs() []int64 {
	return []int64{t.id.ColumnId, t.name.ColumnId, t.score.ColumnId}
}

func (t *simpleTable) genRowData(handle int64, r *rand.Rand) (data *recordData, err error) {
	data = &recordData{}
	colIDs := t.colIDs()
	data.key = tablecodec.EncodeRowKeyWithHandle(t.tableID, handle)
	fields := make([]types.Datum, len(colIDs))
	fields[0].SetInt64(handle)
	fields[1].SetString(randString(r, 30))
	fields[2].SetInt64(rand.Int63n(1000))
	data.value, err = tablecodec.EncodeRow(&stmtctx.StatementContext{}, fields, colIDs, nil, nil)
	return
}

func (t *simpleTable) dagTableScan() *tipb.DAGRequest {
	dag := &tipb.DAGRequest{}
	dag.StartTs = math.MaxUint64
	//dag.TimeZoneOffset =
	dag.Executors = []*tipb.Executor{t.getTableScan()}
	dag.OutputOffsets = []uint32{0, 1, 2}
	return dag
}

func (t *simpleTable) dagCountStar() *tipb.DAGRequest {
	dag := &tipb.DAGRequest{}
	dag.StartTs = math.MaxUint64
	//dag.TimeZoneOffset =
	dag.Executors = []*tipb.Executor{t.getTableScan(), t.aggreCount()}
	dag.OutputOffsets = []uint32{0}
	return dag
}

func (t *simpleTable) getTableScan() *tipb.Executor {
	return &tipb.Executor{
		Tp: tipb.ExecType_TypeTableScan,
		TblScan: &tipb.TableScan{
			TableId: t.tableID,
			Columns: []*tipb.ColumnInfo{&t.id, &t.name, &t.score},
			Desc:    false,
		},
	}
}

func (t *simpleTable) aggreCount() *tipb.Executor {
	expr := &tipb.Expr{
		Tp: tipb.ExprType_Count,
		Children: []*tipb.Expr{{
			Tp:  tipb.ExprType_Int64,
			Val: codec.EncodeInt(nil, 1),
		}},
	}
	return &tipb.Executor{
		Tp: tipb.ExecType_TypeAggregation,
		Aggregation: &tipb.Aggregation{
			AggFunc: []*tipb.Expr{expr},
		},
	}
}

func (t *simpleTable) getPointRange(key int64) kv.KeyRange {
	startKey := tablecodec.EncodeRowKeyWithHandle(t.tableID, key)
	return kv.KeyRange{
		StartKey: startKey,
		EndKey:   startKey.PrefixNext(),
	}
}

func (t *simpleTable) getTableHandleRange() kv.KeyRange {
	startKey, endKey := tablecodec.GetTableHandleKeyRange(t.tableID)
	return kv.KeyRange{
		StartKey: startKey,
		EndKey:   endKey,
	}
}

type recordData struct {
	key   []byte
	value []byte
}

func (c *coprocessor) InsertRow(key uint64, _fields []string) error {
	// Simulate TiDB data
	row, err := c.table.genRowData(int64(key), c.r)
	if err != nil {
		return err
	}

	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if err = tx.Set(row.key, row.value); err != nil {
		return err
	}
	return tx.Commit(goctx.Background())
}

func (c *coprocessor) ReadRow(key uint64) (has bool, err error) {
	client := c.db.GetClient()
	req := kv.Request{}
	req.Concurrency = 1

	req.Tp = kv.ReqTypeDAG
	req.KeyRanges = []kv.KeyRange{c.table.getPointRange(int64(key))}
	dag := c.table.dagTableScan()
	if c.readType == TABLESCAN_COUNT_STAR { //TODO
		dag = c.table.dagCountStar()
		req.KeyRanges = []kv.KeyRange{c.getRangeByKey(key)}
	}
	req.StartTs = dag.StartTs
	req.Data, err = dag.Marshal()
	if err != nil {
		return
	}
	res := client.Send(goctx.Background(), &req)
	defer res.Close()
	data, err := res.Next()
	return len(data) != 0, err
}

/// Get table's maximum range(be covered by one region) which include the handle.
func (c *coprocessor) getRangeByKey(handle uint64) kv.KeyRange {
	backOffer := tikv.NewBackoffer(500, goctx.Background())
	pointRange := c.table.getPointRange(int64(handle))
	kvLocation, err := c.getRegionCache().LocateKey(backOffer, pointRange.StartKey)
	if err != nil {
		panic("get key location from pd")
	}
	handleRange := c.table.getTableHandleRange()
	// range.start_key= max(region.start_key,table_handle_start_key)
	if !kvLocation.Contains(handleRange.StartKey) {
		handleRange.StartKey = kvLocation.StartKey
	}
	// range.end_key = min(region.end_key,table_handle_end_key)
	if !kvLocation.Contains(handleRange.EndKey) {
		handleRange.EndKey = kvLocation.EndKey
	}
	return handleRange
}

func (c *coprocessor) getRegionCache() *tikv.RegionCache {
	type kvStore interface {
		GetRegionCache() *tikv.RegionCache
	}
	tikvStore, ok := c.db.(kvStore)
	if !ok {
		panic("Invalid KvStore with illegal store")
	}
	return tikvStore.GetRegionCache()
}

func (c *coprocessor) Clone() Database {
	return &coprocessor{
		db:    c.db,
		table: c.table,
		r:     rand.New(rand.NewSource(int64(time.Now().UnixNano()))),
	}
}

func setupCoprocessor(pdAddr, readType string) (Database, error) {
	tikv.MaxConnectionCount = 128
	driver := tikv.Driver{}
	db, err := driver.Open(fmt.Sprintf("tikv://%s?disableGC=true", pdAddr))
	if err != nil {
		return nil, err
	}
	tp := TABLESCAN_POINT_GET
	if readType == "count" {
		tp = TABLESCAN_COUNT_STAR
	}
	r := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
	return &coprocessor{db: db, table: newSimpleTable(), r: r, readType: tp}, nil
}
