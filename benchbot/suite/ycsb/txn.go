package ycsb

import (
	"fmt"

	"github.com/juju/errors"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"

	goctx "golang.org/x/net/context"
)

var colIDs = []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

type txnKV struct {
	db kv.Storage
}

func (c *txnKV) Close() error {
	return c.db.Close()
}

func (c *txnKV) ReadRow(id uint64) (bool, error) {
	tx, err := c.db.Begin()
	if err != nil {
		return false, err
	}

	rowID := tablecodec.EncodeRowKeyWithHandle(1, int64(id))
	data, err := tx.Get(rowID)
	if err != nil && errors.Cause(err) != kv.ErrNotExist {
		return false, err
	}

	return data != nil, tx.Commit(goctx.Background())
}

func (c *txnKV) InsertRow(id uint64, fields []string) error {
	// Simulate TiDB data
	rowID := tablecodec.EncodeRowKeyWithHandle(1, int64(id))
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	if err := tx.Delete(rowID); err != nil {
		return err
	}
	return tx.Commit(goctx.Background())
	/*
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

		if err := tx.Set(rowID, rowData); err != nil {
			return err
		}

		return tx.Commit(goctx.Background())
	*/
}

func setupTxnKV(pdAddr string) (Database, error) {
	driver := tikv.Driver{}
	db, err := driver.Open(fmt.Sprintf("tikv://%s?disableGC=true", pdAddr))
	if err != nil {
		return nil, err
	}

	return &txnKV{db: db}, nil
}
