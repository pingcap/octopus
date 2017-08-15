package ycsb

import (
	"fmt"
	"strings"
)

type Database interface {
	Close() error
	ReadRow(id uint64) (bool, error)
	InsertRow(id uint64, fields []string) error
}

func SetupDatabase(dbURL string) (Database, error) {
	seps := strings.SplitN(dbURL, "://", 2)
	if len(seps) != 2 {
		return nil, fmt.Errorf("invalid url %s, must be scheme://path", dbURL)
	}

	switch seps[0] {
	case "tidb", "mysql":
		// url is tidb://username:password@protocol(address)
		return setupTiDB(seps[1])
	case "tikv", "txn":
		// url is tikv://pd_addr
		return setupTxnKV(seps[1])
	case "raw":
		// url is raw://pd_addr
		return setupRawKV(seps[1])
	default:
		return nil, fmt.Errorf("unsupported database: %s", seps[0])
	}
}
