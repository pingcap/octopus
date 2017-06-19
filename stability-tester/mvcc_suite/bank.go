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

package mvcc_suite

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/config"
	"github.com/pingcap/tidb/kv"
	"golang.org/x/net/context"
)

// BackCase is for concurrent balance transfer.
type BankCase struct {
	cfg         *config.BankCaseConfig
	concurrency int
}

// NewBankCase returns the BankCase.
func NewBankCase(cfg *config.Config) Case {
	return &BankCase{
		cfg:         &cfg.MVCC.Bank,
		concurrency: cfg.MVCC.Concurrency,
	}
}

// Initialize implements Case Initialize interface.
func (c *BankCase) Initialize(ctx context.Context, db kv.Storage) error {
	var wg sync.WaitGroup

	// TODO: fix the error is NumAccounts can't be divided by batchSize.
	// Insert batchSize values in one SQL.
	batchSize := 100
	jobCount := c.cfg.NumAccounts / batchSize
	wg.Add(jobCount)

	ch := make(chan int, jobCount)
	for i := 0; i < c.concurrency; i++ {
		go func() {
			for {
				startIndex, ok := <-ch
				if !ok {
					return
				}

				start := time.Now()

				err := kv.RunInNewTxn(db, true, func(txn kv.Transaction) error {
					for i := 0; i < batchSize; i++ {
						if err := txn.Set(bankKey(startIndex+i), []byte("1000")); err != nil {
							return err
						}
					}

					return nil
				})
				if err != nil {
					log.Fatalf("[%s] initialize failed %v", c, err)
				}

				log.Infof("[%s] insert %d accounts, takes %s", c, batchSize, time.Now().Sub(start))

				wg.Done()
			}
		}()
	}

	for i := 0; i < jobCount; i++ {
		ch <- i * batchSize
	}

	wg.Wait()
	close(ch)

	c.startVerify(ctx, db)
	return nil
}

func (c *BankCase) startVerify(ctx context.Context, db kv.Storage) {
	c.verify(db)

	go func() {
		ticker := time.NewTicker(c.cfg.Interval.Duration)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.verify(db)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Execute implements Case Execute interface.
func (c *BankCase) Execute(db kv.Storage, _index int) error {
	c.moveMoney(db)
	return nil
}

// String implements fmt.Stringer interface.
func (c *BankCase) String() string {
	return "bank"
}

func (c *BankCase) verify(db kv.Storage) {
	prefix := bankPrefix()

	total := 0
	err := kv.RunInNewTxn(db, false, func(txn kv.Transaction) error {
		it, err := txn.Seek(bankKey(0))
		if err != nil {
			return err
		}

		for it.Valid() && it.Key().HasPrefix(prefix) {
			total += bankValue(it.Key(), it.Value())
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

func (c *BankCase) moveMoney(db kv.Storage) {
	var from, to int
	for {
		from, to = rand.Intn(c.cfg.NumAccounts), rand.Intn(c.cfg.NumAccounts)
		if from == to {
			continue
		}
		break
	}

	amount := rand.Intn(999)

	err := c.execTransaction(db, from, to, amount)

	if err != nil {
		log.Errorf("[%s] move money err %v", c, err)
	}
}

func (c *BankCase) execTransaction(db kv.Storage, from, to int, amount int) error {
	err := kv.RunInNewTxn(db, false, func(txn kv.Transaction) error {
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

		fromBalance := bankValue(fromKey, fromValue)
		toBalance := bankValue(toKey, toValue)

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

func bankValue(key kv.Key, value []byte) int {
	if value == nil {
		log.Fatalf("[bank] value can't be nil for %q", key)
	}

	v, err := strconv.Atoi(string(value))
	if err != nil {
		log.Fatalf("[bank] parse value err for %q, err: %v", key, err)
	}

	return v
}

func init() {
	RegisterSuite("bank", NewBankCase)
}
