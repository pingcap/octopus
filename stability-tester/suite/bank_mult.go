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
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/config"
)

// involvedNum is the number of accounts involved in one transaciton
const involvedNum int = 10

// BackCase is for concurrent balance transfer.
type BankMultCase struct {
	mu          sync.RWMutex
	cfg         *config.BankMultCaseConfig
	concurrency int
	wg          sync.WaitGroup
	stopped     int32
}

// NewBankMultCase returns the BankMultCase.
func NewBankMultCase(cfg *config.Config) Case {
	b := &BankMultCase{
		cfg:         &cfg.Suite.BankMult,
		concurrency: cfg.Suite.Concurrency,
	}
	if b.cfg.TableNum <= 1 {
		b.cfg.TableNum = 1
	}
	return b
}

// Initialize implements Case Initialize interface.
func (c *BankMultCase) Initialize(ctx context.Context, db *sql.DB) error {
	for i := 0; i < c.cfg.TableNum; i++ {
		err := c.initDB(ctx, db, i)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *BankMultCase) initDB(ctx context.Context, db *sql.DB, id int) error {
	var index string
	if id > 0 {
		index = fmt.Sprintf("%d", id)
	}
	isDropped := c.tryDrop(db, index)
	if !isDropped {
		c.startVerify(ctx, db, index)
		return nil
	}

	mustExec(db, fmt.Sprintf("create table if not exists bank_mult_accounts%s (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL, remark VARCHAR(128))", index))
	mustExec(db, `create table if not exists bank_mult_record (id BIGINT AUTO_INCREMENT,
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

	maxLen := len(remark)
	ch := make(chan int, jobCount)
	for i := 0; i < c.concurrency; i++ {
		go func() {
			args := make([]string, batchSize)

			for {
				startIndex, ok := <-ch
				if !ok {
					return
				}

				start := time.Now()
				for i := 0; i < batchSize; i++ {
					args[i] = fmt.Sprintf("(%d, %d, \"%s\")", startIndex+i, 1000, remark[:rand.Intn(maxLen)])
				}

				query := fmt.Sprintf("INSERT IGNORE INTO bank_mult_accounts%s (id, balance, remark) VALUES %s", index, strings.Join(args, ","))
				insertF := func() error {
					_, err := db.Exec(query)
					return err
				}
				err := runWithRetry(ctx, 100, 3*time.Second, insertF)
				if err != nil {
					log.Fatalf("exec %s err %s", query, err)
				}

				log.Infof("[%s] insert %d bank_mult_accounts%s, takes %s", c, batchSize, index, time.Now().Sub(start))

				wg.Done()
			}
		}()
	}

	for i := 0; i < jobCount; i++ {
		ch <- i * batchSize
	}

	wg.Wait()
	close(ch)

	c.startVerify(ctx, db, index)
	return nil
}

func (c *BankMultCase) startVerify(ctx context.Context, db *sql.DB, index string) {
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
					log.Infof("[%s] verify error: %s in: %s", c, err, time.Now())
					if time.Now().Sub(start) > defaultVerifyTimeout {
						atomic.StoreInt32(&c.stopped, 1)
						log.Info("stop bankMult execute")
						c.wg.Wait()
						log.Fatalf("[%s] verify timeout since %s, error: %s", c, start, err)
					}
				} else {
					start = time.Now()
					log.Infof("[%s] verify success in %s", c, time.Now())
				}
			case <-ctx.Done():
				return
			}
		}
	}(index)
}

// Execute implements Case Execute interface.
func (c *BankMultCase) Execute(db *sql.DB, index int) error {
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
func (c *BankMultCase) String() string {
	return "bankMult"
}

//tryDrop will drop table if data incorrect and panic error likes Bad connect.
func (c *BankMultCase) tryDrop(db *sql.DB, index string) bool {
	var (
		count int
		table string
	)
	//if table is not exist ,return true directly
	query := fmt.Sprintf("show tables like 'bank_mult_accounts%s'", index)
	err := db.QueryRow(query).Scan(&table)
	switch {
	case err == sql.ErrNoRows:
		return true
	case err != nil:
		log.Fatal(err)
	}

	query = fmt.Sprintf("select count(*) as count from bank_mult_accounts%s", index)
	err = db.QueryRow(query).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	if count == c.cfg.NumAccounts {
		return false
	}

	log.Infof("[%s] we need %d bank_mult_accounts%s but got %d, re-initialize the data again", c, c.cfg.NumAccounts, index, count)
	mustExec(db, fmt.Sprintf("drop table if exists bank_mult_accounts%s", index))
	mustExec(db, "DROP TABLE IF EXISTS bank_mult_record")
	return true
}

func (c *BankMultCase) verify(db *sql.DB, index string) error {
	var total int

	start := time.Now()

	tx, err := db.Begin()
	if err != nil {
		bankMultVerifyFailedCounter.Inc()
		return errors.Trace(err)
	}

	defer tx.Rollback()

	query := fmt.Sprintf("select sum(balance) as total from bank_mult_accounts%s", index)
	err = tx.QueryRow(query).Scan(&total)
	if err != nil {
		bankMultVerifyFailedCounter.Inc()
		log.Errorf("[%s] select sum error %v", c, err)
		return errors.Trace(err)
	}
	var tso uint64 = 0
	if err = tx.QueryRow("select @@tidb_current_ts").Scan(&tso); err != nil {
		return errors.Trace(err)
	}
	log.Infof("select sum(balance) to verify use tso %d", tso)
	tx.Commit()
	bankMultVerifyDuration.Observe(time.Since(start).Seconds())

	check := c.cfg.NumAccounts * 1000
	if total != check {
		log.Errorf("[%s]accouts%s total must %d, but got %d", c, index, check, total)
		atomic.StoreInt32(&c.stopped, 1)
		c.wg.Wait()
		log.Fatalf("[%s]accouts%s total must %d, but got %d", c, index, check, total)
	}

	return nil
}

func (c *BankMultCase) moveMoney(db *sql.DB) {
	var (
		index      string
		froms, tos []int
		amounts    []int
	)

	from := rand.Intn(c.cfg.NumAccounts)
	for len(froms) < involvedNum {
		to, amount := rand.Intn(c.cfg.NumAccounts), rand.Intn(999)
		if from == to {
			continue
		}
		froms, tos, amounts = append(froms, from), append(tos, to), append(amounts, amount)
		from = to
	}
	// make the transfer links to be a loop
	froms, tos, amounts = append(froms, tos[len(tos)-1]), append(tos, froms[0]), append(amounts, rand.Intn(999))

	if id := rand.Intn(c.cfg.TableNum); id > 0 {
		index = fmt.Sprintf("%d", id)
	}

	start := time.Now()

	err := c.execTransaction(db, froms, tos, amounts, index)

	if err != nil {
		bankMultTxnFailedCounter.Inc()
		return
	}
	bankMultTxnDuration.Observe(time.Since(start).Seconds())
}

func (c *BankMultCase) execTransaction(db *sql.DB, froms, tos []int, amounts []int, index string) error {
	tx, err := db.Begin()
	if err != nil {
		return errors.Trace(err)
	}

	defer tx.Rollback()

	selectStmt, err := tx.Prepare(fmt.Sprintf(`SELECT id, balance FROM bank_mult_accounts%s
		 WHERE id IN (?, ?) FOR UPDATE`, index))
	if err != nil {
		return errors.Trace(err)
	}

	updateStmt, err := tx.Prepare(fmt.Sprintf(`UPDATE bank_mult_accounts%s
		SET balance = CASE id WHEN ? THEN ? WHEN ? THEN ? END
		WHERE id IN (?, ?)`, index))
	if err != nil {
		return errors.Trace(err)
	}

	insertStmt, err := tx.Prepare(fmt.Sprintf(`
		INSERT INTO bank_mult_record (from_id, to_id, from_balance, to_balance, amount, tso)
			VALUES (?, ?, ?, ?, ?, ?)`))
	if err != nil {
		return errors.Trace(err)
	}

	for i := 0; i < len(froms); i++ {
		rows, err := selectStmt.Query(froms[i], tos[i])
		if err != nil {
			return errors.Trace(err)
		}

		var (
			fromBalance int
			toBalance   int
			count       int
		)

		for rows.Next() {
			var id, balance int
			if err = rows.Scan(&id, &balance); err != nil {
				return errors.Trace(err)
			}
			switch id {
			case froms[i]:
				fromBalance = balance
			case tos[i]:
				toBalance = balance
			default:
				log.Fatalf("[%s] got unexpected account %d", c, id)
			}

			count += 1
		}

		if err = rows.Err(); err != nil {
			return errors.Trace(err)
		}

		if count != 2 {
			log.Fatalf("[%s] select %d(%d) -> %d(%d) invalid count %d", c, froms[i], fromBalance, tos[i], toBalance, count)
		}

		rows.Close()

		update := fmt.Sprintf(`UPDATE bank_mult_accounts%s
		SET balance = CASE id WHEN %d THEN %d WHEN %d THEN %d END
		WHERE id IN (%d, %d)`, index, tos[i], toBalance+amounts[i], froms[i], fromBalance-amounts[i], froms[i], tos[i])

		if fromBalance >= amounts[i] {
			_, err = updateStmt.Exec(tos[i], toBalance+amounts[i], froms[i], fromBalance-amounts[i], froms[i], tos[i])
			if err != nil {
				return errors.Trace(err)
			}

			var tso uint64 = 0
			if err = tx.QueryRow("select @@tidb_current_ts").Scan(&tso); err != nil {
				return err
			}
			if _, err = insertStmt.Exec(froms[i], tos[i], fromBalance, toBalance, amounts[i], tso); err != nil {
				return err
			}
			log.Infof("[bank_mult] exec pre: %s\n", update)
		} else {
			log.Infof("[bank_mult] %v %v", fromBalance, amounts[i])
		}
	}

	selectStmt.Close()
	updateStmt.Close()
	insertStmt.Close()
	err = tx.Commit()
	if err != nil {
		log.Infof("[bank_mult] exec commit error: %s\n", err)
	} else {
		log.Infof("[bank_mult] exec commit success\n")
	}

	return err
}

func init() {
	RegisterSuite("bank_mult", NewBankMultCase)
}
