// Copyright 2016 PingCAP, Inc.
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

package suite

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
)

const (
	alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

	// Used by randString
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// reference: http://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
func randString(b []byte, r *rand.Rand) {
	n := len(b)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, r.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = r.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(alphabet) {
			b[i] = alphabet[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
}

func runWithRetry(ctx context.Context, retryCnt int, interval time.Duration, f func() error) (error, bool) {
	var err error
	for i := 0; i < retryCnt; i++ {
		err = f()
		if err == nil {
			return nil, false
		}
		select {
		case <-ctx.Done():
			return nil, true
		case <-time.After(interval):
		}
	}
	return errors.Trace(err), false
}

func mustExec(db *sql.DB, query string, args ...interface{}) (sql.Result, error) {
	r, err := db.Exec(query, args...)
	if err != nil {
		return r, fmt.Errorf("exec %s err %v", query, err)
	}
	return r, nil
}

func mustQuery(db *sql.DB, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return r, fmt.Errorf("exec %s err %v", query, err)
	}
	return rows, nil
}

type queryEntry struct {
	query              string
	args               []interface{}
	expectAffectedRows int64
}

func ExecWithRollback(db *sql.DB, queries []queryEntry) (res sql.Result, err error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, q := range queries {
		res, err = tx.Exec(q.query, q.args...)
		if err != nil {
			tx.Rollback()
			return nil, errors.Trace(err)
		}
		if q.expectAffectedRows >= 0 {
			affected, err := res.RowsAffected()
			if err != nil {
				tx.Rollback()
				return nil, errors.Trace(err)
			}
			if affected != q.expectAffectedRows {
				return nil, fmt.Errorf("expect affectedRows %v, but got %v, query %v", q.expectAffectedRows, affected, q)
			}
		}
	}
	if err = tx.Commit(); err != nil {
		tx.Rollback()
		return nil, errors.Trace(err)
	}
	return
}

func newlogger(filename string) *log.Logger {
	logger := log.New()
	if file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666); err != nil {
		logger.Out = file
	} else {
		log.Infof("failed to log to %s, using default stderr", filename)
	}
	return logger
}
