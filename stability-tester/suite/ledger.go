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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/config"
)

// LedgerCase simulates a complete record of financial transactions over the
// life of a bank (or other company).
type LedgerCase struct {
	cfg *config.LedgerConfig
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
`

// Initialize creates the table for ledger test.
func (c *LedgerCase) Initialize(ctx context.Context, db *sql.DB) error {
	if _, err := db.Exec(stmtCreate); err != nil {
		log.Fatal(err)
	}
	return nil
}

type postingRequest struct {
	Group              int64
	AccountA, AccountB string
	Amount             int64 // deposited on AccountA, removed from AccountB
	Currency           string

	Transaction, Scheme string // opaque
}

// Execute implements Case Execute interface.
func (c *LedgerCase) Execute(db *sql.DB, index int) error {
	req := postingRequest{
		Group:    rand.Int63(),
		AccountA: fmt.Sprintf("acc%d", rand.Int63n(int64(c.cfg.NumAccounts))),
		AccountB: fmt.Sprintf("acc%d", rand.Int63n(int64(c.cfg.NumAccounts))),
		Amount:   1,
		Currency: "USD",
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

	if err = c.doPosting(tx, req); err != nil {
		log.Errorf("[ledger] doPosting error: %v", err)
		return errors.Trace(err)
	}

	return nil
}

func getLast(tx *sql.Tx, accountID string) (lastCID int64, lastBalance int64, err error) {
	err = tx.QueryRow(`SELECT causality_id, balance FROM ledger_accounts `+
		`WHERE account_id = ? ORDER BY causality_id DESC LIMIT 1`, accountID).
		Scan(&lastCID, &lastBalance)

	if err == sql.ErrNoRows {
		err = nil
		// Paranoia about unspecified semantics.
		lastBalance = 0
		lastCID = 0
	}
	return
}

func (c *LedgerCase) doPosting(tx *sql.Tx, req postingRequest) error {
	var cidA, balA, cidB, balB int64
	if c.cfg.RunBalance {
		var err error
		cidA, balA, err = getLast(tx, req.AccountA)
		if err != nil {
			return err
		}
		cidB, balB, err = getLast(tx, req.AccountB)
		if err != nil {
			return err
		}
	} else {
		// For Cockroach, unique_rowid() would be the better choice.
		cidA, cidB = rand.Int63(), rand.Int63()
		// Want the running balance to always be zero in this case without
		// special-casing below.
		balA = -req.Amount
		balB = req.Amount
	}
	query := fmt.Sprintf(`
INSERT INTO ledger_accounts (
  posting_group_id,
  amount,
  account_id,
  causality_id, -- strictly increasing in absolute time. Only used for running balance.
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
	return nil
}

func (c *LedgerCase) String() string {
	return "Ledger"
}

func init() {
	RegisterSuite("ledger", NewLedgerCase)
}
