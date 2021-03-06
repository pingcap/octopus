package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// BankCaseConfig is for bank test case.
type BankCaseConfig struct {
	// NumAccounts is total accounts
	NumAccounts int           `toml:"num_accounts"`
	Interval    time.Duration `toml:"interval"`
	TableNum    int           `toml:"table_num"`
	Concurrency int           `toml:"concurrency"`
	PD          string        `toml:"pd"`
}

// MVCCBankCase is for concurrent balance transfer.
type MVCCBankCase struct {
	cfg   *BankCaseConfig
	store kv.Storage
	pd    string
}

// Because TiDB use prefix `m`, `t`, to avoid the conflict with TiDB,
// here we use `#` prefix.
const mvccPrefix = "#"

// NewMVCCBankCase returns the MVCCBankCase.
func NewMVCCBankCase(cfg *BankCaseConfig) *MVCCBankCase {
	c := &MVCCBankCase{
		cfg: cfg,
		pd:  cfg.PD,
	}
	return c
}

// Initialize implements Case Initialize interface.
func (c *MVCCBankCase) Initialize(ctx context.Context, store *sql.DB) error {
	log.Infof("[%s] start to init...", c)
	defer func() {
		log.Infof("[%s] init end...", c)
	}()
	if len(c.pd) <= 0 {
		return errors.New("mvcc bank init failed: pd is empty")
	}
	tidb.RegisterStore("tikv", tikv.Driver{})
	var err error
	c.store, err = tidb.NewStore(fmt.Sprintf("tikv://%s?disableGC=true", c.pd))
	if err != nil {
		return errors.Trace(err)
	}
	var wg sync.WaitGroup

	// TODO: fix the error is NumAccounts can't be divided by batchSize.
	// Insert batchSize values in one SQL.
	batchSize := 100
	jobCount := c.cfg.NumAccounts / batchSize
	ch := make(chan int, jobCount)
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				startIndex, ok := <-ch
				if !ok {
					return
				}

				start := time.Now()
				err := kv.RunInNewTxn(c.store, true, func(txn kv.Transaction) error {
					for i := 0; i < batchSize; i++ {
						if err := txn.Set(bankKey(startIndex+i), []byte("1000")); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					log.Errorf("[%s] initialize failed %v", c, err)
					return
				}
				log.Infof("[%s] insert %d accounts, takes %s", c, batchSize, time.Now().Sub(start))

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
		log.Errorf("mvcc bank initialize cancel")
		return nil
	default:
	}

	c.startVerify(ctx)
	return nil
}

func (c *MVCCBankCase) startVerify(ctx context.Context) {
	c.verify()

	go func() {
		ticker := time.NewTicker(c.cfg.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.verify()
			}
		}
	}()
}

// Execute implements Case Execute interface.
func (c *MVCCBankCase) Execute(ctx context.Context, db *sql.DB) error {
	log.Infof("[%s] start to test...", c)
	defer func() {
		log.Infof("[%s] test end...", c)
	}()
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			default:
			}
			c.moveMoney(ctx)
		}()
	}

	wg.Wait()
	return nil
}

// String implements fmt.Stringer interface.
func (c *MVCCBankCase) String() string {
	return "bank"
}

func (c *MVCCBankCase) verify() {
	prefix := bankPrefix()

	total := 0
	err := kv.RunInNewTxn(c.store, false, func(txn kv.Transaction) error {
		it, err := txn.Seek(bankKey(0))
		if err != nil {
			return err
		}

		for it.Valid() && it.Key().HasPrefix(prefix) {

			if v, err := bankValue(it.Key(), it.Value()); err != nil {
				return errors.Trace(err)
			} else {
				total += v
			}
			if err := it.Next(); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		log.Errorf("[%s] select sum err %v", c, err)
		return
	}

	check := c.cfg.NumAccounts * 1000
	if total != check {
		log.Fatalf("[%s] total must %d, but got %d", c, check, total)
	}
}

func (c *MVCCBankCase) moveMoney(ctx context.Context) {
	var from, to int
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		from, to = rand.Intn(c.cfg.NumAccounts), rand.Intn(c.cfg.NumAccounts)
		if from == to {
			continue
		}
		break
	}

	amount := rand.Intn(999)

	err := c.execTransaction(from, to, amount)

	if err != nil {
		log.Errorf("[%s] move money err %v", c, err)
	}
}

func (c *MVCCBankCase) execTransaction(from, to int, amount int) error {
	err := kv.RunInNewTxn(c.store, false, func(txn kv.Transaction) error {
		fromKey := bankKey(from)
		toKey := bankKey(to)

		if err := txn.LockKeys(fromKey, toKey); err != nil {
			return errors.Trace(err)
		}

		fromValue, err := txn.Get(fromKey)
		if err != nil {
			return errors.Trace(err)
		}

		toValue, err := txn.Get(toKey)
		if err != nil {
			return errors.Trace(err)
		}

		fromBalance, err := bankValue(fromKey, fromValue)
		if err != nil {
			return errors.Trace(err)
		}
		toBalance, err := bankValue(toKey, toValue)
		if err != nil {
			return errors.Trace(err)
		}

		if fromBalance >= amount {
			if err := txn.Set(fromKey, []byte(strconv.Itoa(fromBalance-amount))); err != nil {
				return errors.Trace(err)
			}
			if err := txn.Set(toKey, []byte(strconv.Itoa(toBalance+amount))); err != nil {
				return errors.Trace(err)
			}
		}

		return nil
	})

	return err
}

func bankKey(id int) kv.Key {
	return kv.Key(fmt.Sprintf("%sbank_%020d", mvccPrefix, id))
}

func bankPrefix() kv.Key {
	return kv.Key(fmt.Sprintf("%sbank_", mvccPrefix))
}

func bankValue(key kv.Key, value []byte) (int, error) {
	if value == nil {
		return 0, fmt.Errorf("[bank] value can't be nil for %q", key)
	}

	v, err := strconv.Atoi(string(value))
	if err != nil {
		return 0, fmt.Errorf("[bank] parse value err for %q, err: %v", key, err)
	}

	return v, nil
}
