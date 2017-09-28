package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/util"
	"golang.org/x/net/context"
)

var (
	insertConcurrency = 100
	insertBatchSize   = 100
	maxTransfer       = 100
)

// LedgerConfig is for ledger test case.
type LedgerConfig struct {
	NumAccounts int           `toml:"num_accounts"`
	Interval    time.Duration `toml:"interval"`
	Concurrency int           `toml:"concurrency"`
}

// LedgerCase simulates a complete record of financial transactions over the
// life of a bank (or other company).
type LedgerCase struct {
	*LedgerConfig
	wg   sync.WaitGroup
	stop int32
}

// NewLedgerCase returns the case for test.
func NewLedgerCase(cfg *LedgerConfig) *LedgerCase {
	c := &LedgerCase{
		LedgerConfig: cfg,
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
func (c *LedgerCase) Initialize(ctx context.Context, db *sql.DB) error {
	log.Infof("[%s] start to init...", c.String())
	defer func() {
		log.Infof("[%s] init end...", c.String())
	}()
	if _, err := db.Exec(stmtCreate); err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	type Job struct {
		begin, end int
	}
	ch := make(chan Job)
	for i := 0; i < insertConcurrency; i++ {
		wg.Add(1)
		start := time.Now()
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
					args = append(args, fmt.Sprintf(`(%v, 0, "acc%v", 0, 0)`, rand.Int63(), i))
				}

				query := fmt.Sprintf(`INSERT INTO ledger_accounts (posting_group_id, amount,account_id, causality_id, balance) VALUES %s`, strings.Join(args, ","))
				err := util.RunWithRetry(ctx, 100, 3*time.Second, func() error {
					_, err := db.Exec(query)
					return err
				})
				if err != nil {
					log.Fatalf("exec %s err %s", query, err)
				}
				log.Infof("[%s] insert %d accounts, takes %s", c, job.end-job.begin, time.Since(start))
			}
		}()
	}

	var begin, end int
	for begin = 1; begin <= c.NumAccounts; begin = end {
		end = begin + insertBatchSize
		if end > c.NumAccounts {
			end = c.NumAccounts + 1
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
		log.Warn("ledger initialize is cancel")
		return nil
	default:
	}

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
	log.Infof("[%s] start to test...", c)
	defer func() {
		log.Infof("[%s] test end...", c)
	}()
	var wg sync.WaitGroup
	for i := 0; i < c.Concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if err := c.ExecuteLedger(db); err != nil {
					log.Errorf("[%s] exec failed %v", c, err)
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
		AccountA: fmt.Sprintf("acc%d", rand.Intn(c.NumAccounts)+1),
		AccountB: fmt.Sprintf("acc%d", rand.Intn(c.NumAccounts)+1),
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
		log.Errorf("[ledger] doPosting error: %v", err)
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
			case <-ctx.Done():
				return
			case <-time.After(c.Interval):
				c.verify(db)
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
		log.Errorf("[ledger] check total balance got %v", total)
		atomic.StoreInt32(&c.stop, 1)
		c.wg.Wait()
		log.Fatalf("[ledger] check total balance got %v", total)
	}

	log.Infof("verify ok, cost time: %v", time.Since(start))
	return nil
}

func (c *LedgerCase) String() string {
	return "Ledger"
}
