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

// BankCase is for concurrent balance transfer.
type BankCase struct {
	mu      sync.RWMutex
	cfg     *Config
	wg      sync.WaitGroup
	stopped int32
}

type Config struct {
	// NumAccounts is total accounts
	NumAccounts int           `toml:"num_accounts"`
	Interval    time.Duration `toml:"interval"`
	TableNum    int           `toml:"table_num"`
	Concurrency int           `toml:"concurrency"`
}

// NewBankCase returns the BankCase.
func NewBankCase(cfg *Config) *BankCase {
	b := &BankCase{
		cfg: cfg,
	}
	if b.cfg.TableNum <= 1 {
		b.cfg.TableNum = 1
	}
	return b
}

// Initialize implements Case Initialize interface.
func (c *BankCase) Initialize(ctx context.Context, db *sql.DB) error {
	log.Infof("[%s] start to init...", c.String())
	defer func() {
		log.Infof("[%s] init end...", c.String())
	}()
	for i := 0; i < c.cfg.TableNum; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
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

	util.MustExec(db, fmt.Sprintf("create table if not exists accounts%s (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL, remark VARCHAR(128))", index))
	util.MustExec(db, `create table if not exists record (id BIGINT AUTO_INCREMENT,
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

	maxLen := len(remark)
	ch := make(chan int, jobCount)
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			args := make([]string, batchSize)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				startIndex, ok := <-ch
				if !ok {
					break
				}
				start := time.Now()
				for i := 0; i < batchSize; i++ {
					args[i] = fmt.Sprintf("(%d, %d, \"%s\")", startIndex+i, 1000, remark[:rand.Intn(maxLen)])
				}

				query := fmt.Sprintf("INSERT IGNORE INTO accounts%s (id, balance, remark) VALUES %s", index, strings.Join(args, ","))
				insertF := func() error {
					_, err := db.Exec(query)
					return err
				}
				err := util.RunWithRetry(ctx, 200, 5*time.Second, insertF)
				if err != nil {
					log.Fatalf("[%s]exec %s  err %s", c, query, err)
				}
				log.Infof("[%s] insert %d accounts%s, takes %s", c, batchSize, index, time.Now().Sub(start))
			}
		}()
	}

	for i := 0; i < jobCount; i++ {
		ch <- i * batchSize
	}

	close(ch)
	wg.Wait()

	select {
	case <-ctx.Done():
		log.Warn("bank initialize is cancel")
		return nil
	default:
	}

	c.startVerify(ctx, db, index)
	return nil
}

func (c *BankCase) startVerify(ctx context.Context, db *sql.DB, index string) {
	c.verify(db, index)
	start := time.Now()
	go func(index string) {
		ticker := time.NewTicker(c.cfg.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := c.verify(db, index)
				if err != nil {
					log.Infof("[%s] verify error: %s in: %s", c, err, time.Now())
					if time.Now().Sub(start) > defaultVerifyTimeout {
						atomic.StoreInt32(&c.stopped, 1)
						log.Info("stop bank execute")
						c.wg.Wait()
						log.Fatalf("[%s] verify timeout since %s, error: %s", c, start, err)
					}
				} else {
					start = time.Now()
					log.Infof("[%s] verify success in %s", c, time.Now())
				}
			}
		}
	}(index)
}

// Execute implements Case Execute interface.
func (c *BankCase) Execute(ctx context.Context, db *sql.DB) error {
	log.Infof("[%s] start to test...", c.String())
	defer func() {
		log.Infof("[%s] test end...", c.String())
	}()
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
				if atomic.LoadInt32(&c.stopped) != 0 {
					// too many log print in here if return error
					log.Error("bank stopped")
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
		log.Fatal(err)
	}

	query = fmt.Sprintf("select count(*) as count from accounts%s", index)
	err = db.QueryRow(query).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	if count == c.cfg.NumAccounts {
		return false
	}

	log.Infof("[%s] we need %d accounts%s but got %d, re-initialize the data again", c, c.cfg.NumAccounts, index, count)
	util.MustExec(db, fmt.Sprintf("drop table if exists accounts%s", index))
	util.MustExec(db, "DROP TABLE IF EXISTS record")
	return true
}

func (c *BankCase) verify(db *sql.DB, index string) error {
	var total int

	tx, err := db.Begin()
	if err != nil {
		return errors.Trace(err)
	}

	defer tx.Rollback()

	query := fmt.Sprintf("select sum(balance) as total from accounts%s", index)
	err = tx.QueryRow(query).Scan(&total)
	if err != nil {
		log.Errorf("[%s] select sum error %v", c, err)
		return errors.Trace(err)
	}
	var tso uint64 = 0
	if err = tx.QueryRow("select @@tidb_current_ts").Scan(&tso); err != nil {
		return errors.Trace(err)
	}
	log.Infof("select sum(balance) to verify use tso %d", tso)
	tx.Commit()
	check := c.cfg.NumAccounts * 1000
	if total != check {
		log.Errorf("[%s]accouts%s total must %d, but got %d", c, index, check, total)
		atomic.StoreInt32(&c.stopped, 1)
		c.wg.Wait()
		log.Fatalf("[%s]accouts%s total must %d, but got %d", c, index, check, total)
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

	err := c.execTransaction(db, from, to, amount, index)

	if err != nil {
		return
	}
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
			log.Fatalf("[%s] got unexpected account %d", c, id)
		}

		count++
	}

	if err = rows.Err(); err != nil {
		return errors.Trace(err)
	}

	if count != 2 {
		log.Fatalf("[%s] select %d(%d) -> %d(%d) invalid count %d", c, from, fromBalance, to, toBalance, count)
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
		log.Infof("[bank] exec pre: %s\n", update)
	}

	err = tx.Commit()
	if fromBalance >= amount {
		if err != nil {
			log.Infof("[bank] exec commit error: %s\n err:%s\n", update, err)
		}
		if err == nil {
			log.Infof("[bank] exec commit success: %s\n", update)
		}
	}
	return err
}
