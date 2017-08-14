package suite

import (
	"database/sql"
	"time"

	"golang.org/x/net/context"
)

type StatDB struct {
	db   *sql.DB
	stat *StatManager
}

type StatTx struct {
	tx   *sql.Tx
	stat *StatManager
}

type StatStmt struct {
	stmt *sql.Stmt
	stat *StatManager
}

func NewStatDB(db *sql.DB) *StatDB {
	return &StatDB{
		db:   db,
		stat: NewStatManager(),
	}
}

func newStatTx(tx *sql.Tx, stat *StatManager) *StatTx {
	return &StatTx{
		tx:   tx,
		stat: stat,
	}
}

func newStatStmt(stmt *sql.Stmt, stat *StatManager) *StatStmt {
	return &StatStmt{
		stmt: stmt,
		stat: stat,
	}
}

//
// Wrapper for DB (*sql.db)
//
func (s *StatDB) Start() {
	s.stat.Start()
}

func (s *StatDB) Close() error {
	// NOTE: we don't close the db here.
	s.stat.Close()
	return nil
}

func (s *StatDB) Result() *StatResult {
	return s.stat.Result()
}

func (s *StatDB) Prepare(query string) (*StatStmt, error) {
	stmt, err := s.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return newStatStmt(stmt, s.stat), nil
}

func (s *StatDB) PrepareContext(ctx context.Context, query string) (*StatStmt, error) {
	stmt, err := s.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return newStatStmt(stmt, s.stat), nil
}

func (s *StatDB) Begin() (*StatTx, error) {
	start := time.Now()
	tx, err := s.db.Begin()
	s.stat.Record(OPBegin, err, time.Since(start))
	if err != nil {
		return nil, err
	}
	return newStatTx(tx, s.stat), nil
}

func (s *StatDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*StatTx, error) {
	start := time.Now()
	tx, err := s.db.BeginTx(ctx, opts)
	s.stat.Record(OPBegin, err, time.Since(start))
	if err != nil {
		return nil, err
	}
	return newStatTx(tx, s.stat), nil
}

func (s *StatDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	res, err := s.db.Exec(query, args...)
	s.stat.Record(OPWrite, err, time.Since(start))
	return res, err
}

func (s *StatDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	res, err := s.db.ExecContext(ctx, query, args...)
	s.stat.Record(OPWrite, err, time.Since(start))
	return res, err
}

func (s *StatDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := s.db.Query(query, args...)
	s.stat.Record(OPRead, err, time.Since(start))
	return rows, err
}

func (s *StatDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := s.db.QueryContext(ctx, query, args...)
	s.stat.Record(OPRead, err, time.Since(start))
	return rows, err
}

func (s *StatDB) QueryRow(query string, args ...interface{}) *sql.Row {
	start := time.Now()
	row := s.db.QueryRow(query, args...)
	s.stat.Record(OPRead, nil, time.Since(start))
	return row
}

func (s *StatDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	start := time.Now()
	row := s.db.QueryRowContext(ctx, query, args...)
	s.stat.Record(OPRead, nil, time.Since(start))
	return row
}

//
// Wrapper for Trasaction (*sql.Tx)
//
func (s *StatTx) Commit() error {
	start := time.Now()
	err := s.tx.Commit()
	s.stat.Record(OPCommit, err, time.Since(start))
	return err
}

func (s *StatTx) Rollback() error {
	start := time.Now()
	err := s.tx.Rollback()
	s.stat.Record(OPRollback, err, time.Since(start))
	return err
}

func (s *StatTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	res, err := s.tx.Exec(query, args...)
	s.stat.Record(OPWrite, err, time.Since(start))
	return res, err
}

func (s *StatTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	res, err := s.tx.ExecContext(ctx, query, args...)
	s.stat.Record(OPWrite, err, time.Since(start))
	return res, err
}

func (s *StatTx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := s.tx.Query(query, args...)
	s.stat.Record(OPRead, err, time.Since(start))
	return rows, err
}

func (s *StatTx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := s.tx.QueryContext(ctx, query, args...)
	s.stat.Record(OPRead, err, time.Since(start))
	return rows, err
}

func (s *StatTx) QueryRow(query string, args ...interface{}) *sql.Row {
	start := time.Now()
	row := s.tx.QueryRow(query, args...)
	s.stat.Record(OPRead, nil, time.Since(start))
	return row
}

func (s *StatTx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	start := time.Now()
	row := s.tx.QueryRowContext(ctx, query, args...)
	s.stat.Record(OPRead, nil, time.Since(start))
	return row
}

//
// Wrapper for Statement (*sql.Stmt)
//
func (s *StatStmt) Exec(args ...interface{}) (sql.Result, error) {
	start := time.Now()
	res, err := s.stmt.Exec(args...)
	s.stat.Record(OPWrite, err, time.Since(start))
	return res, err
}

func (s *StatStmt) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	res, err := s.stmt.ExecContext(ctx, args...)
	s.stat.Record(OPWrite, err, time.Since(start))
	return res, err
}

func (s *StatStmt) Query(args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := s.stmt.Query(args...)
	s.stat.Record(OPRead, err, time.Since(start))
	return rows, err
}

func (s *StatStmt) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	rows, err := s.stmt.QueryContext(ctx, args...)
	s.stat.Record(OPRead, err, time.Since(start))
	return rows, err
}

func (s *StatStmt) QueryRow(args ...interface{}) *sql.Row {
	start := time.Now()
	row := s.stmt.QueryRow(args...)
	s.stat.Record(OPRead, nil, time.Since(start))
	return row
}

func (s *StatStmt) QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row {
	start := time.Now()
	row := s.stmt.QueryRowContext(ctx, args...)
	s.stat.Record(OPRead, nil, time.Since(start))
	return row
}
