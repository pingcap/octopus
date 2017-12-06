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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/juju/errors"
)

// OnDupCase tests insert statement with on duplicate update.
// We start insert worker, replace worker and delete worker to
// run concurrently, each worker uses a random id to execute the
// statement at a time, so there will be retries, but should never fail.
type OnDupCase struct {
	NumRows int
	errCh   chan error
}

// NewOnDupCase creates a new OnDupCase.
func NewOnDupCase(numRows int) *OnDupCase {
	return &OnDupCase{
		NumRows: numRows,
		errCh:   make(chan error, 3),
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

func (c *OnDupCase) Execute(ctx context.Context, db *sql.DB) error {
	childCtx, cancelFunc := context.WithCancel(ctx)
	go c.runInsert(childCtx, db)
	go c.runDelete(childCtx, db)
	go c.runReplace(childCtx, db)
	select {
	case err := <-c.errCh:
		cancelFunc()
		return errors.Trace(err)
	case <-ctx.Done():
		return nil
	}
}

func (c *OnDupCase) runInsert(ctx context.Context, db *sql.DB) {
	ran := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	for {
		id := ran.Intn(c.NumRows)
		name := strconv.Itoa(int(ran.Intn(c.NumRows)))
		score := ran.Intn(c.NumRows)
		query := fmt.Sprintf("INSERT on_dup VALUES (%d, '%d', %d) ON DUPLICATE KEY UPDATE score = values(score)", id, name, score)
		_, err := db.Exec(query)
		if err != nil {
			c.errCh <- errors.Trace(err)
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (c *OnDupCase) runDelete(ctx context.Context, db *sql.DB) {
	ran := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	for {
		id := ran.Intn(c.NumRows)
		query := fmt.Sprintf("DELETE from on_dup where id = %d", id)
		_, err := db.Exec(query)
		if err != nil {
			c.errCh <- errors.Trace(err)
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (c *OnDupCase) runReplace(ctx context.Context, db *sql.DB) {
	ran := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	for {
		id := ran.Intn(c.NumRows)
		name := strconv.Itoa(int(ran.Intn(c.NumRows)))
		score := ran.Intn(c.NumRows)
		query := fmt.Sprintf("REPLACE on_dup VALUES (%d, '%d', %d)", id, name, score)
		_, err := db.Exec(query)
		if err != nil {
			c.errCh <- errors.Trace(err)
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
