package ycsb

import (
	"strings"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
)

type rawKV struct {
	db *tikv.RawKVClient
}

func (c *rawKV) Close() error {
	return c.db.Close()
}

func (c *rawKV) ReadRow(id uint64) (bool, error) {
	rowID := tablecodec.EncodeRowKeyWithHandle(1, int64(id))
	data, err := c.db.Get(rowID)
	if err != nil {
		return false, err
	}

	return data != nil, nil
}

func (c *rawKV) InsertRow(id uint64, fields []string) error {
	// Simulate TiDB data
	rowID := tablecodec.EncodeRowKeyWithHandle(1, int64(id))
	cols := make([]types.Datum, len(fields))
	for i, v := range fields {
		cols[i].SetString(v)
	}

	rowData, err := tablecodec.EncodeRow(&stmtctx.StatementContext{}, cols, colIDs, nil, nil)
	if err != nil {
		return err
	}

	return c.db.Put([]byte(rowID), rowData)
}

func setupRawKV(pdAddr string) (Database, error) {
	// Open connection to server and create a database.
	db, err := tikv.NewRawKVClient(strings.Split(pdAddr, ","), config.Security{})
	if err != nil {
		return nil, err
	}

	return &rawKV{db: db}, nil
}
