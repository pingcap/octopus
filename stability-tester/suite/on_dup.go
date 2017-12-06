// Copyright 2017 PingCAP, Inc.
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
	"strconv"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/octopus/stability-tester/config"
)

// OnDupCase tests insert statement with on duplicate update.
// We start insert worker, replace worker and delete worker to
// run concurrently, each worker uses a random id to execute the
// statement at a time, so there will be retries, but should never fail.
type OnDupCase struct {
	NumRows int
	wg      sync.WaitGroup
	errs    []error
	mu      sync.Mutex
	Done    chan bool
}

// NewOnDupCase creates a new OnDupCase.
func NewOnDupCase(cfg *config.Config) Case {
	return &OnDupCase{
		NumRows: cfg.Suite.OnDup.NumRows,
		Done:    make(chan bool),
	}
}

// Initialize creates the tables.
func (c *OnDupCase) Initialize(ctx context.Context, db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS on_dup (
  id INT,
  name VARCHAR(64),
  score INT,
  PRIMARY KEY (id),
  UNIQUE INDEX byName (name)
)`)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = db.Exec(`TRUNCATE TABLE on_dup`)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *OnDupCase) String() string {
	return "on_dup"
}

func (c *OnDupCase) Execute(db *sql.DB, concurrentIndex int) error {
	c.wg.Add(3)
	go c.runInsert(db)
	go c.runDelete(db)
	go c.runReplace(db)
	c.wg.Wait()
	if len(c.errs) != 0 {
		return c.errs[0]
	}
	return nil
}

func (c *OnDupCase) runInsert(db *sql.DB) {
	defer c.wg.Done()
	ran := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	for {
		id := ran.Intn(c.NumRows)
		name := strconv.Itoa(int(ran.Intn(c.NumRows)))
		score := ran.Intn(c.NumRows)
		query := fmt.Sprintf("INSERT on_dup VALUES (%d, '%d', %d) ON DUPLICATE KEY UPDATE score = values(score)", id, name, score)
		_, err := db.Exec(query)
		if err != nil {
			c.mu.Lock()
			c.errs = append(c.errs, err)
			c.mu.Unlock()
			return
		}
		select {
		case <-c.Done:
			return
		}
	}
}

func (c *OnDupCase) runDelete(db *sql.DB) {
	defer c.wg.Done()
	ran := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	for {
		id := ran.Intn(c.NumRows)
		query := fmt.Sprintf("DELETE from on_dup where id = %d", id)
		_, err := db.Exec(query)
		if err != nil {
			c.mu.Lock()
			c.errs = append(c.errs, err)
			c.mu.Unlock()
			return
		}
		select {
		case <-c.Done:
			return
		}
	}
}

func (c *OnDupCase) runReplace(db *sql.DB) {
	defer c.wg.Done()
	ran := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	for {
		id := ran.Intn(c.NumRows)
		name := strconv.Itoa(int(ran.Intn(c.NumRows)))
		score := ran.Intn(c.NumRows)
		query := fmt.Sprintf("REPLACE on_dup VALUES (%d, '%d', %d)", id, name, score)
		_, err := db.Exec(query)
		if err != nil {
			c.errs = append(c.errs, errors.Trace(err))
			return
		}
		select {
		case <-c.Done:
			return
		}
	}
}

func init() {
	RegisterSuite("on_dup", NewOnDupCase)
}
