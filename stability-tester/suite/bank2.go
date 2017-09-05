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
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/octopus/stability-tester/config"
	"golang.org/x/net/context"
)

const (
	initialBalance    = 1000
	insertBatchSize   = 100
	insertConcurrency = 100
	maxTransfer       = 100
	systemAccountID   = 0
)

// Bank2Case transfers money between accounts, creating new ledger transactions
// in the form of a transaction record and two transaction "legs" per database
// transaction.
type Bank2Case struct {
	cfg    *config.Bank2CaseConfig
	wg     sync.WaitGroup
	stop   int32
	txnID  int32
	logger *log.Logger
}

// NewBank2Case returns a Bank2Case.
func NewBank2Case(cfg *config.Config) Case {
	b := &Bank2Case{
		cfg: &cfg.Suite.Bank2,
	}
	return b
}

// Initialize creates the tables and inserts initial balances.
func (c *Bank2Case) Initialize(ctx context.Context, db *sql.DB, logger *log.Logger) error {
	c.logger = logger
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS bank2_accounts (
  id INT,
  balance INT NOT NULL,
  name VARCHAR(32),
  remark VARCHAR(128),
  PRIMARY KEY (id),
  UNIQUE INDEX byName (name)
);

CREATE TABLE IF NOT EXISTS bank2_transaction (
  id INT,
  booking_date TIMESTAMP DEFAULT NOW(),
  txn_date TIMESTAMP DEFAULT NOW(),
  txn_ref VARCHAR(32),
  PRIMARY KEY (id),
  UNIQUE INDEX byTxnRef (txn_ref)
);

CREATE TABLE IF NOT EXISTS bank2_transaction_leg (
  id INT AUTO_INCREMENT,
  account_id INT,
  amount INT NOT NULL,
  running_balance INT NOT NULL,
  txn_id INT,
  PRIMARY KEY (id)
);

TRUNCATE TABLE bank2_accounts;
TRUNCATE TABLE bank2_transaction;
TRUNCATE TABLE bank2_transaction_leg;
	`)
	if err != nil {
		return errors.Trace(err)
	}

	var wg sync.WaitGroup
	type Job struct {
		begin, end int
	}
	ch := make(chan Job)
	maxLen := len(remark)
	for i := 0; i < insertConcurrency; i++ {
		wg.Add(1)
		start := time.Now()
		var execInsert []string
		go func() {
			defer wg.Done()
			for job := range ch {
				select {
				case <-ctx.Done():
					return
				default:
				}
				args := make([]string, 0, insertBatchSize)
				for i := job.begin; i < job.end; i++ {
					args = append(args, fmt.Sprintf(`(%d, %d, "account %d", "%s")`, i, initialBalance, i, remark[:rand.Intn(maxLen)]))
				}

				query := fmt.Sprintf("INSERT IGNORE INTO bank2_accounts (id, balance, name, remark) VALUES %s", strings.Join(args, ","))
				err, isCancel := runWithRetry(ctx, 200, 5*time.Second, func() error {
					_, err := db.Exec(query)
					return err
				})
				if isCancel {
					return
				}
				if err != nil {
					c.logger.Fatalf("exec %s err %s", query, err)
				}
				execInsert = append(execInsert, fmt.Sprintf("%d_%d", job.begin, job.end))
			}
		}()
		c.logger.Infof("[%s] insert %s accounts, takes %s", c, strings.Join(execInsert, ","), time.Since(start))
	}

	var begin, end int
	for begin = 1; begin <= c.cfg.NumAccounts; begin = end {
		end = begin + insertBatchSize
		if end > c.cfg.NumAccounts {
			end = c.cfg.NumAccounts + 1
		}
		select {
		case <-ctx.Done():
			return nil
		case ch <- Job{begin: begin, end: end}:
		}
	}
	close(ch)
	wg.Wait()

	select {
	case <-ctx.Done():
		c.logger.Warn("bank2 initialize is cancel")
		return nil
	default:
	}
	query := fmt.Sprintf(`INSERT IGNORE INTO bank2_accounts (id, balance, name) VALUES (%d, %d, "system account")`, systemAccountID, int64(c.cfg.NumAccounts*initialBalance))
	err, isCancel := runWithRetry(ctx, 100, 3*time.Second, func() error {
		_, err := db.Exec(query)
		return err
	})
	if isCancel {
		return nil
	}
	if err != nil {
		c.logger.Fatalf("[%s] insert system account err: %v", c, err)
	}

	c.startVerify(ctx, db)
	return nil
}

func (c *Bank2Case) startVerify(ctx context.Context, db *sql.DB) {
	c.verify(db)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.cfg.Interval.Duration):
				c.verify(db)
			}
		}
	}()
}

func (c *Bank2Case) verify(db *sql.DB) {
	start := time.Now()

	tx, err := db.Begin()
	if err != nil {
		bank2VerifyFailedCounter.Inc()
		return
	}
	defer tx.Rollback()

	var tso uint64
	if err = tx.QueryRow("SELECT @@tidb_current_ts").Scan(&tso); err == nil {
		c.logger.Infof("SELECT SUM(balance) to verify use tso %d", tso)
	}

	var total int64
	err = tx.QueryRow("SELECT SUM(balance) AS total FROM bank2_accounts").Scan(&total)
	if err != nil {
		bank2VerifyFailedCounter.Inc()
		return
	}

	bank2VerifyDuration.Observe(time.Since(start).Seconds())

	expectTotal := (int64(c.cfg.NumAccounts) * initialBalance) * 2
	if total != expectTotal {
		c.logger.Errorf("[bank2] bank2_accounts total should be %d, but got %d", expectTotal, total)
		atomic.StoreInt32(&c.stop, 1)
		c.wg.Wait()
		c.logger.Fatalf("[bank2] bank2_accounts total should be %d, but got %d", expectTotal, total)
	}
}

// Execute implements Case Execute interface.
func (c *Bank2Case) Execute(ctx context.Context, db *sql.DB) error {
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
				if atomic.LoadInt32(&c.stop) != 0 {
					c.logger.Error("bank2 stopped")
					return
				}
				c.wg.Add(1)
				c.moveMoney(db)
				c.wg.Done()
			}
		}(i)
	}
	wg.Wait()
	return nil
}

func (c *Bank2Case) moveMoney(db *sql.DB) {
	from, to := rand.Intn(c.cfg.NumAccounts), rand.Intn(c.cfg.NumAccounts)
	if from == to {
		return
	}
	if c.cfg.Contention == "high" {
		// Use the first account number we generated as a coin flip to
		// determine whether we're transferring money into or out of
		// the system account.
		if from > c.cfg.NumAccounts/2 {
			from = systemAccountID
		} else {
			to = systemAccountID
		}
	}
	amount := rand.Intn(maxTransfer)
	start := time.Now()
	if err := c.execTransaction(db, from, to, amount); err != nil {
		bank2VerifyFailedCounter.Inc()
		c.logger.Errorf("[bank2] move money err %v", err)
		return
	}
	bank2VerifyDuration.Observe(time.Since(start).Seconds())
}

func (c *Bank2Case) execTransaction(db *sql.DB, from, to int, amount int) error {
	tx, err := db.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()
	rows, err := tx.Query(fmt.Sprintf("SELECT id, balance FROM bank2_accounts WHERE id IN (%d, %d) FOR UPDATE", from, to))
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

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
		case from:
			fromBalance = balance
		case to:
			toBalance = balance
		default:
			c.logger.Fatalf("[%s] got unexpected account %d", c, id)
		}
		count++
	}

	if err = rows.Err(); err != nil {
		return errors.Trace(err)
	}

	if count != 2 {
		c.logger.Fatalf("[%s] select %d(%d) -> %d(%d) invalid count %d", c, from, fromBalance, to, toBalance, count)
	}

	if fromBalance < amount {
		return nil
	}

	insertTxn := `INSERT INTO bank2_transaction (id, txn_ref) VALUES (?, ?)`
	insertTxnLeg := `INSERT INTO bank2_transaction_leg (account_id, amount, running_balance, txn_id) VALUES (?, ?, ?, ?)`
	updateAcct := `UPDATE bank2_accounts SET balance = ? WHERE id = ?`
	txnID := atomic.AddInt32(&c.txnID, 1)
	if _, err := tx.Exec(insertTxn, txnID, fmt.Sprintf("txn %d", txnID)); err != nil {
		tx.Rollback()
		return errors.Trace(err)
	}
	if _, err = tx.Exec(insertTxnLeg, from, -amount, fromBalance-amount, txnID); err != nil {
		tx.Rollback()
		return errors.Trace(err)
	}
	if _, err = tx.Exec(insertTxnLeg, to, amount, toBalance+amount, txnID); err != nil {
		tx.Rollback()
		return errors.Trace(err)
	}
	if _, err = tx.Exec(updateAcct, toBalance+amount, to); err != nil {
		tx.Rollback()
		return errors.Trace(err)
	}
	if _, err = tx.Exec(updateAcct, fromBalance-amount, from); err != nil {
		tx.Rollback()
		return errors.Trace(err)
	}

	err = tx.Commit()
	c.logger.Infof("[bank2] %d(%d) transfer %d to %d(%d) err: %v", from, fromBalance, amount, to, toBalance, err)
	return nil
}

func (c *Bank2Case) String() string {
	return "bank2"
}

func init() {
	RegisterSuite("bank2", NewBank2Case)
}
