package ycsb

import (
	"bytes"
	"database/sql"
	"fmt"

	. "github.com/pingcap/octopus/benchbot/suite"
)

type tidb struct {
	db *sql.DB
}

func (c *tidb) Close() error {
	return c.db.Close()
}

func (c *tidb) ReadRow(id uint64) (bool, error) {
	stmt := fmt.Sprintf("SELECT * FROM `%s` WHERE id = ?", ycsbTableName)
	res, err := c.db.Query(stmt, id)
	if err != nil {
		return false, err
	}
	var rowsFound int
	for res.Next() {
		rowsFound++
	}

	return rowsFound != 0, res.Close()
}

func (c *tidb) InsertRow(id uint64, fields []string) error {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "INSERT INTO `%s` VALUES (", ycsbTableName)
	fmt.Fprintf(&buf, "%d", id)
	for _, s := range fields {
		fmt.Fprintf(&buf, ", '%s'", s)
	}
	fmt.Fprintf(&buf, ")")
	_, err := c.db.Exec(buf.String())
	return err
}

func setupTiDB(url string) (Database, error) {
	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1024)
	db.SetMaxIdleConns(1024)

	if err := RecreateTable(db, ycsbTableName, ycsbTableSchema); err != nil {
		db.Close()
		return nil, err
	}

	return &tidb{db: db}, nil
}
