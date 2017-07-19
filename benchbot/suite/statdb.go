package suite

import (
	"database/sql"
	"golang.org/x/net/context"

	_ "sync"
	"time"
)

type StatDB struct {
	db   *sql.DB
	stat *statisticManager
}

type StatTx struct {
	tx   *sql.Tx
	stat *statisticManager
}

type StatStmt struct {
	stmt *sql.Stmt
	stat *statisticManager
}

type StatResult struct {
	summary *StatIndex
	stages  []*StatIndex
}

func NewStatDB(db *sql.DB) *StatDB {
	stdb := new(StatDB)
	stdb.db = db
	stdb.stat = newStatisticManager()
	stdb.stat.start()
	return stdb
}

func newStatTx(tx *sql.Tx, stat *statisticManager) *StatTx {
	return &StatTx{
		tx:   tx,
		stat: stat,
	}
}

func newStatStmt(stmt *sql.Stmt, stat *statisticManager) *StatStmt {
	return &StatStmt{
		stmt: stmt,
		stat: stat,
	}
}

//
// wrapper for DB (*sql.db)
//
func (stdb *StatDB) Stat() *StatResult {
	return &StatResult{
		summary: stdb.stat.getSummary(),
		stages:  stdb.stat.getStages(),
	}
}

func (stdb *StatDB) Close() error {
	// NOTICE : here do not close the db in real !!!
	stdb.stat.close()
	return nil
}

func (stdb *StatDB) Prepare(query string) (*StatStmt, error) {
	stmt, err := stdb.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return newStatStmt(stmt, stdb.stat), nil
}

func (stdb *StatDB) PrepareContext(ctx context.Context, query string) (*StatStmt, error) {
	stmt, err := stdb.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return newStatStmt(stmt, stdb.stat), nil
}

func (stdb *StatDB) Begin() (*StatTx, error) {
	s := time.Now()

	tx, err := stdb.db.Begin()
	stdb.stat.record(&dbop{
		class:   opTxBegin,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(), // TODO .. record latency ?
	})

	if err != nil {
		return nil, err
	}

	return newStatTx(tx, stdb.stat), nil
}

func (stdb *StatDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*StatTx, error) {
	s := time.Now()

	tx, err := stdb.db.BeginTx(ctx, opts)
	stdb.stat.record(&dbop{
		class:   opTxBegin,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(), // TODO .. record latency ?
	})

	if err != nil {
		return nil, err
	}

	return newStatTx(tx, stdb.stat), nil
}

func (stdb *StatDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	s := time.Now()
	res, err := stdb.db.Exec(query, args...)
	stdb.stat.record(&dbop{
		class:   opWrite,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(),
	})
	return res, err
}

func (stdb *StatDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	s := time.Now()
	res, err := stdb.db.ExecContext(ctx, query, args...)
	stdb.stat.record(&dbop{
		class:   opWrite,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(),
	})
	return res, err
}

func (stdb *StatDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	s := time.Now()
	rows, err := stdb.db.Query(query, args...)
	stdb.stat.record(&dbop{
		class:   opRead,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(),
	})
	return rows, err
}

func (stdb *StatDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	s := time.Now()
	rows, err := stdb.db.QueryContext(ctx, query, args...)
	stdb.stat.record(&dbop{
		class:   opRead,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(),
	})
	return rows, err
}

func (stdb *StatDB) QueryRow(query string, args ...interface{}) *sql.Row {
	s := time.Now()
	row := stdb.db.QueryRow(query, args...)
	stdb.stat.record(&dbop{
		class:   opRead,
		err:     false, // TODO ... error occours until scan()
		latency: time.Now().Sub(s).Seconds(),
	})
	return row
}

func (stdb *StatDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	s := time.Now()
	row := stdb.db.QueryRowContext(ctx, query, args...)
	stdb.stat.record(&dbop{
		class:   opRead,
		err:     false, // TODO ... error occours until scan()
		latency: time.Now().Sub(s).Seconds(),
	})
	return row
}

//
// wrapper for Trasaction (*sql.Tx)
//
func (sttx *StatTx) Commit() error {
	s := time.Now()
	err := sttx.tx.Commit()
	sttx.stat.record(&dbop{
		class:   opTxCommit,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(),
	})
	return err
}

func (sttx *StatTx) Rollback() error {
	s := time.Now()
	err := sttx.tx.Rollback()
	sttx.stat.record(&dbop{
		class:   opTxRollback,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(),
	})
	return err
}

func (sttx *StatTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	s := time.Now()
	res, err := sttx.tx.Exec(query, args...)
	sttx.stat.record(&dbop{
		class:   opWrite,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(),
	})
	return res, err
}

func (sttx *StatTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	s := time.Now()
	res, err := sttx.tx.ExecContext(ctx, query, args...)
	sttx.stat.record(&dbop{
		class:   opWrite,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(),
	})
	return res, err
}

func (sttx *StatTx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	s := time.Now()
	rows, err := sttx.tx.Query(query, args...)
	sttx.stat.record(&dbop{
		class:   opRead,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(),
	})
	return rows, err
}

func (sttx *StatTx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	s := time.Now()
	rows, err := sttx.tx.QueryContext(ctx, query, args...)
	sttx.stat.record(&dbop{
		class:   opRead,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(),
	})
	return rows, err
}

func (sttx *StatTx) QueryRow(query string, args ...interface{}) *sql.Row {
	s := time.Now()
	row := sttx.tx.QueryRow(query, args...)
	sttx.stat.record(&dbop{
		class:   opRead,
		err:     false,
		latency: time.Now().Sub(s).Seconds(),
	})
	return row
}

func (sttx *StatTx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	s := time.Now()
	row := sttx.tx.QueryRowContext(ctx, query, args...)
	sttx.stat.record(&dbop{
		class:   opRead,
		err:     false,
		latency: time.Now().Sub(s).Seconds(),
	})
	return row
}

//
// wrapper for Statement (*sql.Stmt)
//
func (ss *StatStmt) Exec(args ...interface{}) (sql.Result, error) {
	s := time.Now()
	res, err := ss.stmt.Exec(args...)
	ss.stat.record(&dbop{
		class:   opWrite,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(),
	})
	return res, err
}

func (ss *StatStmt) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	s := time.Now()
	res, err := ss.stmt.ExecContext(ctx, args...)
	ss.stat.record(&dbop{
		class:   opWrite,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(),
	})
	return res, err
}

func (ss *StatStmt) Query(args ...interface{}) (*sql.Rows, error) {
	s := time.Now()
	rows, err := ss.stmt.Query(args...)
	ss.stat.record(&dbop{
		class:   opRead,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(),
	})
	return rows, err
}

func (ss *StatStmt) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	s := time.Now()
	rows, err := ss.stmt.QueryContext(ctx, args...)
	ss.stat.record(&dbop{
		class:   opRead,
		err:     err != nil,
		latency: time.Now().Sub(s).Seconds(),
	})
	return rows, err
}

func (ss *StatStmt) QueryRow(args ...interface{}) *sql.Row {
	s := time.Now()
	row := ss.stmt.QueryRow(args...)
	ss.stat.record(&dbop{
		class:   opRead,
		err:     false,
		latency: time.Now().Sub(s).Seconds(),
	})
	return row
}

func (ss *StatStmt) QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row {
	s := time.Now()
	row := ss.stmt.QueryRowContext(ctx, args...)
	ss.stat.record(&dbop{
		class:   opRead,
		err:     false,
		latency: time.Now().Sub(s).Seconds(),
	})
	return row
}
