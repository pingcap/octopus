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
	log "github.com/sirupsen/logrus"
	"github.com/pingcap/octopus/stability-tester/util"
	"golang.org/x/net/context"
)

// BankCase is for concurrent balance transfer.
type BankCase struct {
	cfg     *Config
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
	if b.cfg.TableNum < 10 {
		b.cfg.TableNum = 10
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
	c.startVerify(ctx, db)
	return nil
}

func (c *BankCase) initDB(ctx context.Context, db *sql.DB, id int) error {
	var index string
	index = fmt.Sprintf("%d", id)
	isDropped := c.tryDrop(db, index)
	if !isDropped {
		return nil
	}

	// remark column here is to make the row size larger or smaller than 64 bytes,
	// if larger than 64, the row will be written into default cf in tikv rocksdb;
	// if smaller than 64, the row will be written into write cf in tikv rocksdb.
	util.MustExec(db, fmt.Sprintf("create table if not exists accounts%s (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL, remark VARCHAR(128))", index))
	util.MustExec(db, `create table if not exists record (id BIGINT AUTO_INCREMENT,
				from_table BIGINT NOT NULL,
        from_id BIGINT NOT NULL,
        to_table BIGINT NOT NULL,
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
					args[i] = fmt.Sprintf("(%d, %d, \"%s\")", startIndex+i, 10000, remark[:rand.Intn(maxLen)])
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
				log.Infof("[%s] insert %d accounts into accounts%s, takes %s", c, batchSize, index, time.Now().Sub(start))
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
		log.Warn("bank initialize is canceled")
		return nil
	default:
	}

	return nil
}

func (c *BankCase) startVerify(ctx context.Context, db *sql.DB) {
	start := time.Now()
	go func() {
		ticker := time.NewTicker(c.cfg.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := c.verify(db)
				if err != nil {
					log.Errorf("[%s] verify error: %s in: %s", c, err, time.Now())
					if time.Now().Sub(start) > defaultVerifyTimeout {
						atomic.StoreInt32(&c.stopped, 1)
						log.Fatalf("[%s] verify timeout since %s, error: %s", c, start, err)
					}
				} else {
					start = time.Now()
					log.Infof("[%s] verify success in %s", c, time.Now())
				}
			}
		}
	}()
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
					return
				}
				c.transferMoney(db)
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// String implements fmt.Stringer interface.
func (c *BankCase) String() string {
	return "cross-table-bank"
}

// tryDrop will drop table if data is incorrect or there is a panic error like bad connection.
func (c *BankCase) tryDrop(db *sql.DB, index string) bool {
	var (
		count int
		table string
	)
	// if table does not exist, return true directly
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

func (c *BankCase) verify(db *sql.DB) error {
	total := uint64(0)

	tx, err := db.Begin()
	if err != nil {
		return errors.Trace(err)
	}

	defer tx.Rollback()

	for i := 0; i < c.cfg.TableNum; i++ {
		query := fmt.Sprintf("select sum(balance) as total from accounts%d", i)
		var tmp int
		err = tx.QueryRow(query).Scan(&tmp)
		if err != nil {
			log.Errorf("[%s] select sum error %v", c, err)
			return errors.Trace(err)
		}
		total += uint64(tmp)
	}
	var tso uint64 = 0
	if err = tx.QueryRow("select @@tidb_current_ts").Scan(&tso); err != nil {
		return errors.Trace(err)
	}
	log.Infof("select sum(balance) to verify use tso %d", tso)
	tx.Commit()
	check := uint64(c.cfg.NumAccounts) * 10000 * uint64(c.cfg.TableNum)
	if total != check {
		atomic.StoreInt32(&c.stopped, 1)
		log.Fatalf("[%s]accouts total must be %d, but got %d", c, check, total)
	}

	return nil
}

func (c *BankCase) transferMoney(db *sql.DB) {
	var (
		from_table, from_id, to_table, to_id int
	)
	for {
		from_table, to_table = rand.Intn(c.cfg.TableNum), rand.Intn(c.cfg.TableNum)
		if from_table == to_table {
			continue
		}
		break
	}
	from_id, to_id = rand.Intn(c.cfg.NumAccounts), rand.Intn(c.cfg.NumAccounts)

	amount := rand.Intn(999)

	err := c.execTransaction(db, from_table, from_id, to_table, to_id, amount)

	if err != nil {
		return
	}
}

// get balance based on row id
func (c *BankCase) getRowInfo(tx *sql.Tx, table, expected_id int) (int, error) {
	row, err := tx.Query(fmt.Sprintf("SELECT id, balance FROM accounts%d WHERE id = %d FOR UPDATE", table, expected_id))
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer row.Close()

	var balance int = 0
	if row.Next() {
		var id int
		if err = row.Scan(&id, &balance); err != nil {
			return balance, errors.Trace(err)
		}
		if id != expected_id {
			log.Fatalf("[%s] got unexpected id %d", c, id)
		}
	} else {
		log.Fatalf("[%s] got no id %d", c, expected_id)
	}

	if err = row.Err(); err != nil {
		return balance, errors.Trace(err)
	}
	return balance, nil
}

func (c *BankCase) execTransaction(db *sql.DB, from_table, from_id, to_table, to_id int, amount int) error {
	tx, err := db.Begin()
	if err != nil {
		return errors.Trace(err)
	}

	defer tx.Rollback()

	var (
		fromBalance int
		toBalance   int
	)

	fromBalance, err = c.getRowInfo(tx, from_table, from_id)
	if err != nil {
		return err
	}
	toBalance, err = c.getRowInfo(tx, to_table, to_id)
	if err != nil {
		return err
	}

	var update string
	if fromBalance >= amount {
		update = fmt.Sprintf(`
UPDATE accounts%d SET balance = %d WHERE id = %d;
UPDATE accounts%d SET balance = %d WHERE id = %d;
`, from_table, fromBalance-amount, from_id, to_table, toBalance+amount, to_id)
		_, err = tx.Exec(update)
		if err != nil {
			return errors.Trace(err)
		}

		var tso uint64 = 0
		if err = tx.QueryRow("select @@tidb_current_ts").Scan(&tso); err != nil {
			return err
		}
		if _, err = tx.Exec(fmt.Sprintf(`
INSERT INTO record (from_table, from_id, to_table, to_id, from_balance, to_balance, amount, tso)
    VALUES (%d, %d, %d, %d, %d, %d, %d, %d)`, from_table, from_id, to_table, to_id, fromBalance, toBalance, amount, tso)); err != nil {
			return err
		}
	}

	err = tx.Commit()
	return err
}
