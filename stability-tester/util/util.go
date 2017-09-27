package util

import (
	"context"
	"database/sql"
	"math/rand"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

const (
	alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

	// Used by RandString
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// RandString reference: http://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
func RandString(b []byte, r *rand.Rand) {
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

// RunWithRetry tries to run func in specified count
func RunWithRetry(ctx context.Context, retryCnt int, interval time.Duration, f func() error) error {
	var err error
	for i := 0; i < retryCnt; i++ {
		err = f()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}
	}
	return errors.Trace(err)
}

// QueryEntry is a query
type QueryEntry struct {
	Query              string
	Args               []interface{}
	ExpectAffectedRows int64
}

// ExecWithRollback exeutes or rollbak
func ExecWithRollback(db *sql.DB, queries []QueryEntry) (res sql.Result, err error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, q := range queries {
		res, err = tx.Exec(q.Query, q.Args...)
		if err != nil {
			tx.Rollback()
			return nil, errors.Trace(err)
		}
		if q.ExpectAffectedRows >= 0 {
			affected, err := res.RowsAffected()
			if err != nil {
				tx.Rollback()
				return nil, errors.Trace(err)
			}
			if affected != q.ExpectAffectedRows {
				log.Fatalf("expect affectedRows %v, but got %v, query %v", q.ExpectAffectedRows, affected, q)
			}
		}
	}
	if err = tx.Commit(); err != nil {
		tx.Rollback()
		return nil, errors.Trace(err)
	}
	return
}

// MustExec must execute sql or fatal
func MustExec(db *sql.DB, query string, args ...interface{}) sql.Result {
	r, err := db.Exec(query, args...)
	if err != nil {
		log.Fatalf("exec %s err %v", query, err)
	}
	return r
}

// OpenDB opens db
func OpenDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	log.Info("DB opens successfully")
	return db, nil
}

// InitLog initials log
func InitLog(file string, level string) {
	log.SetLevelByString(level)
	if len(file) > 0 {
		log.SetOutputByName(file)
		log.SetHighlighting(false)
		log.SetRotateByDay()
	}
}

// PushPrometheus pushes metrics to Prometheus Pushgateway.
func PushPrometheus(job, addr string, interval time.Duration) {
	for {
		err := push.FromGatherer(
			job, push.HostnameGroupingKey(),
			addr,
			prometheus.DefaultGatherer,
		)
		if err != nil {
			log.Errorf("could not push metrics to Prometheus Pushgateway: %v", err)
		}

		time.Sleep(interval)
	}
}
