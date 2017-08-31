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
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/octopus/stability-tester/config"
)

var (
	defaultVerifyTimeout = 20 * time.Minute
)

// BackCase is for concurrent balance transfer.
type BankCase struct {
	mu          sync.RWMutex
	cfg         *config.BankCaseConfig
	concurrency int
	wg          sync.WaitGroup
	stopped     int32
}

// NewBankCase returns the BankCase.
func NewBankCase(cfg *config.Config) Case {
	b := &BankCase{
		cfg:         &cfg.Suite.Bank,
		concurrency: cfg.Suite.Concurrency,
	}
	if b.cfg.TableNum <= 1 {
		b.cfg.TableNum = 1
	}
	return b
}

// Initialize implements Case Initialize interface.
func (c *BankCase) Initialize(ctx context.Context, db *sql.DB) error {
	for i := 0; i < c.cfg.TableNum; i++ {
		err := c.initDB(ctx, db, i)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *BankCase) initDB(ctx context.Context, db *sql.DB, id int) error {
	var index string
	if id > 0 {
		index = fmt.Sprintf("%d", id)
	}
	isDropped := c.tryDrop(db, index)
	if !isDropped {
		c.startVerify(ctx, db, index)
		return nil
	}

	mustExec(db, fmt.Sprintf("create table if not exists accounts%s (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)", index))
	mustExec(db, `create table if not exists record (id BIGINT AUTO_INCREMENT,
        from_id BIGINT NOT NULL,
        to_id BIGINT NOT NULL,
        from_balance BIGINT NOT NULL,
        to_balance BIGINT NOT NULL,
        amount BIGINT NOT NULL,
        tso BIGINT UNSIGNED NOT NULL,
        PRIMARY KEY(id))`)
	var wg sync.WaitGroup

	// TODO: fix the error is NumAccounts can't be divided by batchSize.
	// Insert batchSize values in one SQL.
	batchSize := 100
	jobCount := c.cfg.NumAccounts / batchSize
	wg.Add(jobCount)

	ch := make(chan int, jobCount)
	for i := 0; i < c.concurrency; i++ {
		start := time.Now()
		var execInsert []string
		go func() {
			args := make([]string, batchSize)

			for {
				startIndex, ok := <-ch
				if !ok {
					return
				}

				for i := 0; i < batchSize; i++ {
					args[i] = fmt.Sprintf("(%d, %d)", startIndex+i, 1000)
				}

				query := fmt.Sprintf("INSERT IGNORE INTO accounts%s (id, balance) VALUES %s", index, strings.Join(args, ","))
				insertF := func() error {
					_, err := db.Exec(query)
					return err
				}
				err := runWithRetry(ctx, 100, 3*time.Second, insertF)
				if err != nil {
					Log.Fatalf("[%s]exec %s  err %s", c, query, err)
				}

				execInsert = append(execInsert, fmt.Sprintf("%d_%d", startIndex, startIndex+batchSize))
				wg.Done()
			}
		}()
		Log.Infof("[%s] insert %s accounts%s, takes %s", c, strings.Join(execInsert, ","), index, time.Now().Sub(start))
	}

	for i := 0; i < jobCount; i++ {
		ch <- i * batchSize
	}

	wg.Wait()
	close(ch)

	c.startVerify(ctx, db, index)
	return nil
}

func (c *BankCase) startVerify(ctx context.Context, db *sql.DB, index string) {
	c.verify(db, index)
	start := time.Now()
	go func(index string) {
		ticker := time.NewTicker(c.cfg.Interval.Duration)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				err := c.verify(db, index)
				if err != nil {
					Log.Infof("[%s] verify error: %s in: %s", c, err, time.Now())
					if time.Now().Sub(start) > defaultVerifyTimeout {
						atomic.StoreInt32(&c.stopped, 1)
						Log.Info("stop bank execute")
						c.wg.Wait()
						Log.Fatalf("[%s] verify timeout since %s, error: %s", c, start, err)
					}
				} else {
					start = time.Now()
					Log.Infof("[%s] verify success in %s", c, time.Now())
				}
			case <-ctx.Done():
				return
			}
		}
	}(index)
}

// Execute implements Case Execute interface.
func (c *BankCase) Execute(db *sql.DB, index int) error {
	if atomic.LoadInt32(&c.stopped) != 0 {
		// too many log print in here if return error
		return nil
	}
	c.wg.Add(1)
	c.moveMoney(db)
	c.wg.Done()
	return nil
}

// String implements fmt.Stringer interface.
func (c *BankCase) String() string {
	return "bank"
}

//tryDrop will drop table if data incorrect and panic error likes Bad connect.
func (c *BankCase) tryDrop(db *sql.DB, index string) bool {
	var (
		count int
		table string
	)
	//if table is not exist ,return true directly
	query := fmt.Sprintf("show tables like 'accounts%s'", index)
	err := db.QueryRow(query).Scan(&table)
	switch {
	case err == sql.ErrNoRows:
		return true
	case err != nil:
		Log.Fatal(err)
	}

	query = fmt.Sprintf("select count(*) as count from accounts%s", index)
	err = db.QueryRow(query).Scan(&count)
	if err != nil {
		Log.Fatal(err)
	}
	if count == c.cfg.NumAccounts {
		return false
	}

	Log.Infof("[%s] we need %d accounts%s but got %d, re-initialize the data again", c, c.cfg.NumAccounts, index, count)
	mustExec(db, fmt.Sprintf("drop table if exists accounts%s", index))
	mustExec(db, "DROP TABLE IF EXISTS record")
	return true
}

func (c *BankCase) verify(db *sql.DB, index string) error {
	var total int

	start := time.Now()

	tx, err := db.Begin()
	if err != nil {
		bankVerifyFailedCounter.Inc()
		return errors.Trace(err)
	}

	defer tx.Rollback()

	query := fmt.Sprintf("select sum(balance) as total from accounts%s", index)
	err = tx.QueryRow(query).Scan(&total)
	if err != nil {
		bankVerifyFailedCounter.Inc()
		Log.Errorf("[%s] select sum error %v", c, err)
		return errors.Trace(err)
	}
	var tso uint64 = 0
	if err = tx.QueryRow("select @@tidb_current_ts").Scan(&tso); err != nil {
		return errors.Trace(err)
	}
	Log.Infof("select sum(balance) to verify use tso %d", tso)
	tx.Commit()
	bankVerifyDuration.Observe(time.Since(start).Seconds())

	check := c.cfg.NumAccounts * 1000
	if total != check {
		Log.Errorf("[%s]accouts%s total must %d, but got %d", c, index, check, total)
		atomic.StoreInt32(&c.stopped, 1)
		c.wg.Wait()
		Log.Fatalf("[%s]accouts%s total must %d, but got %d", c, index, check, total)
	}

	return nil
}

func (c *BankCase) moveMoney(db *sql.DB) {
	var (
		from, to, id int
		index        string
	)
	for {
		from, to, id = rand.Intn(c.cfg.NumAccounts), rand.Intn(c.cfg.NumAccounts), rand.Intn(c.cfg.TableNum)
		if from == to {
			continue
		}
		break
	}
	if id > 0 {
		index = fmt.Sprintf("%d", id)
	}

	amount := rand.Intn(999)

	start := time.Now()

	err := c.execTransaction(db, from, to, amount, index)

	if err != nil {
		bankTxnFailedCounter.Inc()
		return
	}
	bankTxnDuration.Observe(time.Since(start).Seconds())
}

func (c *BankCase) execTransaction(db *sql.DB, from, to int, amount int, index string) error {
	tx, err := db.Begin()
	if err != nil {
		return errors.Trace(err)
	}

	defer tx.Rollback()

	rows, err := tx.Query(fmt.Sprintf("SELECT id, balance FROM accounts%s WHERE id IN (%d, %d) FOR UPDATE", index, from, to))
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	var (
		fromBalance int
		toBalance   int
		count       int = 0
	)

	for rows.Next() {
		var id, balance int
		if err = rows.Scan(&id, &balance); err != nil {
			return errors.Trace(err)
		}
		switch id {
		case from:
			fromBalance = balance
		case to:
			toBalance = balance
		default:
			Log.Fatalf("[%s] got unexpected account %d", c, id)
		}

		count++
	}

	if err = rows.Err(); err != nil {
		return errors.Trace(err)
	}

	if count != 2 {
		Log.Fatalf("[%s] select %d(%d) -> %d(%d) invalid count %d", c, from, fromBalance, to, toBalance, count)
	}

	var update string
	if fromBalance >= amount {
		update = fmt.Sprintf(`
UPDATE accounts%s
  SET balance = CASE id WHEN %d THEN %d WHEN %d THEN %d END
  WHERE id IN (%d, %d)
`, index, to, toBalance+amount, from, fromBalance-amount, from, to)
		_, err = tx.Exec(update)
		if err != nil {
			return errors.Trace(err)
		}

		var tso uint64 = 0
		if err = tx.QueryRow("select @@tidb_current_ts").Scan(&tso); err != nil {
			return err
		}
		if _, err = tx.Exec(fmt.Sprintf(`
INSERT INTO record (from_id, to_id, from_balance, to_balance, amount, tso)
    VALUES (%d, %d, %d, %d, %d, %d)`, from, to, fromBalance, toBalance, amount, tso)); err != nil {
			return err
		}
		Log.Infof("[bank] exec pre: %s\n", update)
	}

	err = tx.Commit()
	if fromBalance >= amount {
		if err != nil {
			Log.Infof("[bank] exec commit error: %s\n err:%s\n", update, err)
		}
		if err == nil {
			Log.Infof("[bank] exec commit success: %s\n", update)
		}
	}
	return err
}

func init() {
	RegisterSuite("bank", NewBankCase)
}
