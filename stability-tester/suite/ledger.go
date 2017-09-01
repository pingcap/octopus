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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/octopus/stability-tester/config"
)

// LedgerCase simulates a complete record of financial transactions over the
// life of a bank (or other company).
type LedgerCase struct {
	cfg    *config.LedgerConfig
	wg     sync.WaitGroup
	stop   int32
	logger *log.Logger
}

// NewLedgerCase returns the case for test.
func NewLedgerCase(cfg *config.Config) Case {
	c := &LedgerCase{
		cfg: &cfg.Suite.Ledger,
	}
	return c
}

const stmtCreate = `
CREATE TABLE IF NOT EXISTS ledger_accounts (
  causality_id BIGINT NOT NULL,
  posting_group_id BIGINT NOT NULL,
  amount BIGINT,
  balance BIGINT,
  currency VARCHAR(32),
  created TIMESTAMP,
  value_date TIMESTAMP,
  account_id VARCHAR(32),
  transaction_id VARCHAR(32),
  scheme VARCHAR(32),
  PRIMARY KEY (account_id, posting_group_id),
  INDEX (transaction_id),
  INDEX (posting_group_id)
);
TRUNCATE TABLE ledger_accounts;
`

// Initialize creates the table for ledger test.
func (c *LedgerCase) Initialize(ctx context.Context, db *sql.DB, logger *log.Logger) error {
	c.logger = logger
	if _, err := db.Exec(stmtCreate); err != nil {
		c.logger.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(insertConcurrency)
	type Job struct {
		begin, end int
	}
	ch := make(chan Job)
	for i := 0; i < insertConcurrency; i++ {
		start := time.Now()
		var execInsert []string
		go func() {
			defer wg.Done()
			for job := range ch {
				args := make([]string, 0, insertBatchSize)
				for i := job.begin; i < job.end; i++ {
					args = append(args, fmt.Sprintf(`(%v, 0, "acc%v", 0, 0)`, rand.Int63(), i))
				}

				query := fmt.Sprintf(`INSERT INTO ledger_accounts (posting_group_id, amount,account_id, causality_id, balance) VALUES %s`, strings.Join(args, ","))
				err, isCancel := runWithRetry(ctx, 100, 3*time.Second, func() error {
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
		c.logger.Infof("[%s] insert %d accounts, takes %s", c, strings.Join(execInsert, ","), time.Since(start))
	}

	var begin, end int
	for begin = 1; begin <= c.cfg.NumAccounts; begin = end {
		end = begin + insertBatchSize
		if end > c.cfg.NumAccounts {
			end = c.cfg.NumAccounts + 1
		}
		ch <- Job{
			begin: begin,
			end:   end,
		}
	}
	close(ch)
	wg.Wait()

	c.startVerify(ctx, db)
	return nil
}

type postingRequest struct {
	Group               int64
	AccountA, AccountB  string
	Amount              int    // deposited on AccountA, removed from AccountB
	Transaction, Scheme string // opaque
}

// Execute implements Case Execute interface.
func (c *LedgerCase) Execute(ctx context.Context, db *sql.DB) error {
	var wg sync.WaitGroup
	wg.Add(c.cfg.Concurrency)
	for i := 0; i < c.cfg.Concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if err := c.ExecuteLedger(db); err != nil {
					c.logger.Errorf("[%s] exec failed %v", c.String(), err)
				}
			}
		}(i)
	}

	wg.Wait()
	return nil
}

// ExecuteLedger is run case
func (c *LedgerCase) ExecuteLedger(db *sql.DB) error {
	if atomic.LoadInt32(&c.stop) != 0 {
		return errors.New("ledger stopped")
	}

	c.wg.Add(1)
	defer c.wg.Done()

	req := postingRequest{
		Group:    rand.Int63(),
		AccountA: fmt.Sprintf("acc%d", rand.Intn(c.cfg.NumAccounts)+1),
		AccountB: fmt.Sprintf("acc%d", rand.Intn(c.cfg.NumAccounts)+1),
		Amount:   rand.Intn(maxTransfer),
	}

	if req.AccountA == req.AccountB {
		// The code we use throws a unique constraint violation since we
		// try to insert two conflicting primary keys. This isn't the
		// interesting case.
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()

	if err = c.doPosting(tx, req); err != nil {
		c.logger.Errorf("[ledger] doPosting error: %v", err)
		return errors.Trace(err)
	}

	return nil
}

func getLast(tx *sql.Tx, accountID string) (lastCID int64, lastBalance int64, err error) {
	err = tx.QueryRow(fmt.Sprintf(`SELECT causality_id, balance FROM ledger_accounts
		WHERE account_id = "%v" ORDER BY causality_id DESC LIMIT 1 FOR UPDATE`, accountID)).
		Scan(&lastCID, &lastBalance)
	return
}

func (c *LedgerCase) doPosting(tx *sql.Tx, req postingRequest) error {
	start := time.Now()
	var cidA, balA, cidB, balB int64
	var err error
	cidA, balA, err = getLast(tx, req.AccountA)
	if err != nil {
		return err
	}
	cidB, balB, err = getLast(tx, req.AccountB)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(`
INSERT INTO ledger_accounts (
  posting_group_id,
  amount,
  account_id,
  causality_id,
  balance
)
VALUES (
  %[1]v,	-- posting_group_id
  %[2]v, 	-- amount
  "%[3]v", 	-- account_id (A)
  %[4]v, 	-- causality_id
  %[5]v+%[2]v	-- (new) balance
), (
  %[1]v,   -- posting_group_id
 -%[2]v,   -- amount
  "%[6]v", -- account_id (B)
  %[7]v,   -- causality_id
  %[8]v-%[2]v -- (new) balance
)`, req.Group, req.Amount,
		req.AccountA, cidA+1, balA,
		req.AccountB, cidB+1, balB)
	if _, err := tx.Exec(query); err != nil {
		return errors.Trace(err)
	}
	if err := tx.Commit(); err != nil {
		return errors.Trace(err)
	}

	ledgerTxnDuration.Observe(time.Since(start).Seconds())
	return nil
}

func (c *LedgerCase) startVerify(ctx context.Context, db *sql.DB) {
	c.verify(db)

	go func() {
		for {
			select {
			case <-time.After(c.cfg.Interval.Duration):
				c.verify(db)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (c *LedgerCase) verify(db *sql.DB) error {
	start := time.Now()
	tx, err := db.Begin()
	if err != nil {
		ledgerVerifyFailedCounter.Inc()
		return errors.Trace(err)
	}
	defer tx.Rollback()

	var total int64
	err = tx.QueryRow(`
select sum(balance) total from
  (select account_id, max(causality_id) max_causality_id from ledger_accounts group by account_id) last
  join ledger_accounts
    on last.account_id = ledger_accounts.account_id and last.max_causality_id = ledger_accounts.causality_id`).Scan(&total)
	if err != nil {
		ledgerVerifyFailedCounter.Inc()
		return errors.Trace(err)
	}

	ledgerVerifyDuration.Observe(time.Since(start).Seconds())
	if total != 0 {
		c.logger.Errorf("[ledger] check total balance got %v", total)
		atomic.StoreInt32(&c.stop, 1)
		c.wg.Wait()
		c.logger.Fatalf("[ledger] check total balance got %v", total)
	}

	c.logger.Infof("verify ok, cost time: %v", time.Since(start))
	return nil
}

func (c *LedgerCase) String() string {
	return "Ledger"
}

func init() {
	RegisterSuite("ledger", NewLedgerCase)
}
