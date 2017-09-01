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
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/octopus/stability-tester/config"
)

//this case for test TiKV perform when write small datavery frequently
type SmallWriterCase struct {
	sws    []*smallDataWriter
	logger *log.Logger
}

const smallWriterBatchSize = 20

//the small data is a int number
type smallDataWriter struct {
	rand   *rand.Rand
	values []string
}

//NewSmallWriterCase returns the smallWriterCase.
func NewSmallWriterCase(cfg *config.Config) Case {
	c := &SmallWriterCase{}

	c.initSmallDataWriter(cfg.Suite.Concurrency)
	return c
}

func (c *SmallWriterCase) initSmallDataWriter(concurrency int) {
	c.sws = make([]*smallDataWriter, concurrency)
	for i := 0; i < concurrency; i++ {
		source := rand.NewSource(int64(time.Now().UnixNano()))
		c.sws[i] = &smallDataWriter{
			rand:   rand.New(source),
			values: make([]string, smallWriterBatchSize),
		}
	}
}

// Initialize implements Case Initialize interface.
func (c *SmallWriterCase) Initialize(ctx context.Context, db *sql.DB, logger *log.Logger) error {
	c.logger = logger
	_, err := mustExec(db, "create table if not exists small_writer(id bigint auto_increment, data bigint, primary key(id))")
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Execute implements Case Execute interface.
func (c *SmallWriterCase) Execute(db *sql.DB, index int) error {
	c.sws[index].batchExecute(db)
	return nil
}

// String implements fmt.Stringer interface.
func (c *SmallWriterCase) String() string {
	return "small_writer"
}

// Insert values.
func (sw *smallDataWriter) batchExecute(db *sql.DB) {
	var err error
	for i := 0; i < smallWriterBatchSize; i++ {
		start := time.Now()
		_, err = db.Exec(
			fmt.Sprintf(
				"INSERT INTO small_writer (data) VALUES (%d)",
				sw.rand.Int()),
		)

		if err != nil {
			smallWriteFailedCounter.Inc()
			c.logger.Errorf("[small writer] insert err %v", err)
			return
		}
		smallWriteDuration.Observe(time.Since(start).Seconds())
	}
}

func init() {
	RegisterSuite("block_writer", NewSmallWriterCase)
}
