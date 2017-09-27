package util

import (
	"context"
	"database/sql"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

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
