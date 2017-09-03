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
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/octopus/stability-tester/config"
	"golang.org/x/net/context"
)

//this case for test TiKV perform when write small datavery frequently
type SmallWriterCase struct {
	cfg    config.SmallWriterCaseConfig
	sws    []*smallDataWriter
	logger *log.Logger
}

const smallWriterBatchSize = 20

//the small data is a int number
type smallDataWriter struct {
	rand   *rand.Rand
	values []string
	logger *log.Logger
}

//NewSmallWriterCase returns the smallWriterCase.
func NewSmallWriterCase(cfg *config.Config) Case {
	c := &SmallWriterCase{
		cfg: cfg.Suite.SmallWriter,
	}

	c.initSmallDataWriter(c.cfg.Concurrency)
	return c
}

func (c *SmallWriterCase) initSmallDataWriter(concurrency int) {
	c.sws = make([]*smallDataWriter, concurrency)
	for i := 0; i < concurrency; i++ {
		source := rand.NewSource(int64(time.Now().UnixNano()))
		c.sws[i] = &smallDataWriter{
			rand:   rand.New(source),
			values: make([]string, smallWriterBatchSize),
			logger: c.logger,
		}
	}
}

// Initialize implements Case Initialize interface.
func (c *SmallWriterCase) Initialize(ctx context.Context, db *sql.DB, logger *log.Logger) error {
	c.logger = logger
	if _, err := mustExec(db, "create table if not exists small_writer(id bigint auto_increment, data bigint, primary key(id))"); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Execute implements Case Execute interface.
func (c *SmallWriterCase) Execute(ctx context.Context, db *sql.DB) error {
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if err := c.sws[i].batchExecute(db); err != nil {
					smallWriteFailedCounter.Inc()
					c.logger.Errorf("[%s] execute failed %v", c.String(), err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// String implements fmt.Stringer interface.
func (c *SmallWriterCase) String() string {
	return "small_writer"
}

// Insert values.
func (sw *smallDataWriter) batchExecute(db *sql.DB) error {
	var err error
	for i := 0; i < smallWriterBatchSize; i++ {
		start := time.Now()
		_, err = db.Exec(
			fmt.Sprintf(
				"INSERT INTO small_writer (data) VALUES (%d)",
				sw.rand.Int()),
		)

		if err != nil {
			return fmt.Errorf("[small writer] insert err %v", err)
		}
		smallWriteDuration.Observe(time.Since(start).Seconds())
	}
	return nil
}

func init() {
	RegisterSuite("block_writer", NewSmallWriterCase)
}
