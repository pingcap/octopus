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
	"strings"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/config"
)

// LogCase is for simulating writing log.
// in this case, we will continuously write data.
// when the count of log entries is more than MaxCount,the specified number of logs are deleted.
type LogCase struct {
	cfg *config.LogCaseConfig
	lws []*logWriter
}

const logWriterBatchSize = 20

//the log size in range minSize and maxSize.
type logWriter struct {
	minSize       int
	maxSize       int
	rand          *rand.Rand
	logDataBuffer []byte
	values        []string
	index         int
}

//NewLogCase returns the LogCase.
func NewLogCase(cfg *config.Config) Case {
	c := &LogCase{
		cfg: &cfg.Suite.Log,
	}
	c.initLogWrite(cfg.Suite.Concurrency)
	if c.cfg.TableNum <= 1 {
		c.cfg.TableNum = 1
	}
	return c
}

func (c *LogCase) initLogWrite(concurrency int) {
	c.lws = make([]*logWriter, concurrency)
	for i := 0; i < concurrency; i++ {
		source := rand.NewSource(int64(time.Now().UnixNano()))
		c.lws[i] = &logWriter{
			minSize:       100,
			maxSize:       1024,
			rand:          rand.New(source),
			logDataBuffer: make([]byte, 1024),
			values:        make([]string, logWriterBatchSize),
		}
	}
}

// Initialize implements Case Initialize interface.
func (c *LogCase) Initialize(ctx context.Context, db *sql.DB) error {
	for i := 0; i < c.cfg.TableNum; i++ {
		var s string
		if i > 0 {
			s = fmt.Sprintf("%d", i)
		}
		mustExec(db, fmt.Sprintf("create table if not exists log%s (id bigint auto_increment,data varchar(1024),primary key(id))", s))
	}

	c.startCheck(ctx, db)
	return nil
}

func (c *LogCase) startCheck(ctx context.Context, db *sql.DB) {
	for i := 0; i < c.cfg.TableNum; i++ {
		go func(i int) {
			ticker := time.NewTicker(c.cfg.Interval.Duration)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					c.reviseLogCount(db, i)
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}
}

func (c *LogCase) reviseLogCount(db *sql.DB, id int) {
	var (
		count int
		index string
	)

	start := time.Now()
	if id > 0 {
		index = fmt.Sprintf("%d", id)
	}
	query := fmt.Sprintf("select count(*) as count from log%s", index)
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		logFailedCounterVec.WithLabelValues("count").Inc()
		log.Errorf("[%s] select count err %v", c, err)
		return
	}
	logDurationVec.WithLabelValues("count").Observe(time.Since(start).Seconds())

	if count > c.cfg.MaxCount {
		var err error
		sql := fmt.Sprintf("delete from log%s where id > 0 limit %d", index, c.cfg.DeleteCount)
		start := time.Now()
		_, err = db.Exec(sql)
		if err != nil {
			logFailedCounterVec.WithLabelValues("delete").Inc()
			log.Errorf("[%s] delete log err %v", c, err)
			return
		}
		logDurationVec.WithLabelValues("delete").Observe(time.Since(start).Seconds())
	}
}

// Execute implements Case Execute interface.
func (c *LogCase) Execute(db *sql.DB, index int) error {
	c.lws[index].batchExecute(db, c.cfg.TableNum)
	return nil
}

// String implements fmt.Stringer interface.
func (c *LogCase) String() string {
	return "log"
}

func (lw *logWriter) randomLogData() []byte {
	size := lw.rand.Intn(lw.maxSize-lw.minSize) + lw.minSize

	randString(lw.logDataBuffer[:size], lw.rand)
	return lw.logDataBuffer[:size]
}

// Insert logWriterBatchSize values in one SQL.
//
// TODO: configure it from outside.

func (lw *logWriter) batchExecute(db *sql.DB, tableNum int) {
	// buffer values
	for i := 0; i < logWriterBatchSize; i++ {
		lw.values[i] = fmt.Sprintf("('%s')", lw.randomLogData())
	}

	start := time.Now()
	var (
		err   error
		index string
	)
	if lw.index > 0 {
		index = fmt.Sprintf("%d", lw.index)
	}

	_, err = db.Exec(
		fmt.Sprintf(
			"INSERT INTO log%s (data) VALUES %s",
			index, strings.Join(lw.values, ",")),
	)

	if err != nil {
		logFailedCounterVec.WithLabelValues("batch_insert").Inc()
		log.Errorf("[log] insert log err %v", err)
		return
	}

	lw.index = (lw.index + 1) % tableNum
	logDurationVec.WithLabelValues("batch_insert").Observe(time.Since(start).Seconds())
}

func init() {
	RegisterSuite("log", NewLogCase)
}