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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

// The DDL test case is intended to test the correctness of DDL operations. It
// generates test cases by probability so that it should be run in background for
// enough time to see if there are any issues.
//
// The DDL test case have two go routines run in parallel, one for DDL operations,
// one for DML operations. The feature of each operation (for example, covering
// what kind of scenario) is determined and generated at start up time (See
// `generateDMLOps`, `generateDDLOps``), while the order of each operation is
// randomized in each round.
//
// If there are remaining DDL operations while all DML operations are performed, a
// new round of DML operations will be started (with new randomized order) and when
// all DDL operations are done, the remaining DML operations are discarded. vice
// versa.
//
// Since there are some conflicts between some DDL operations and DML operations,
// for example, inserting a row while removing a column may cause errors in
// inserting because of incorrect column numbers, some locks and some conflicting
// detections are introduced. The conflicting detection will ignore errors raised
// in such scenarios. In addition, the data in memory is stored by column instead
// of by row to minimize data conflicts in adding and removing columns.

type DDLCaseConfig struct {
	Concurrency     int  `toml:"concurrency"`
	MySQLCompatible bool `toml:"mysql_compactible"`
	TablesToCreate  int  `toml:"tables_to_create"`
}

type DDLCase struct {
	cfg   *DDLCaseConfig
	cases []*testCase
}

func (c *DDLCase) String() string {
	return "ddl"
}

// Execute executes each goroutine (i.e. `testCase`) concurrently.
func (c *DDLCase) Execute(ctx context.Context, db *sql.DB) error {
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
				err := c.cases[i].execute(db)
				if err != nil {
					log.Fatalf("[ddl] [instance %d] ERROR: %s", i, errors.ErrorStack(err))
				}
			}
		}(i)
	}
	wg.Wait()
	return nil
}

// Initialize initializes each concurrent goroutine (i.e. `testCase`).
func (c *DDLCase) Initialize(ctx context.Context, db *sql.DB) error {
	for i := 0; i < c.cfg.Concurrency; i++ {
		err := c.cases[i].initialize(db)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// NewDDLCase returns a DDLCase, which contains specified `testCase`s.
func NewDDLCase(cfg *DDLCaseConfig) *DDLCase {
	cases := make([]*testCase, cfg.Concurrency)
	for i := 0; i < cfg.Concurrency; i++ {
		cases[i] = &testCase{
			cfg:       cfg,
			tables:    make(map[string]*ddlTestTable),
			ddlOps:    make([]ddlTestOpExecutor, 0),
			dmlOps:    make([]ddlTestOpExecutor, 0),
			caseIndex: i,
		}
	}
	b := &DDLCase{
		cfg:   cfg,
		cases: cases,
	}
	return b
}

const (
	ddlTestValueNull    int32 = -1
	ddlTestValueInvalid int32 = -99
)

type ddlTestOpExecutor struct {
	executeFunc func(interface{}) error
	config      interface{}
}

// initialize generates possible DDL and DML operations for one `testCase`.
// Different `testCase`s will be run in parallel according to the concurrent configuration.
func (c *testCase) initialize(db *sql.DB) error {
	c.db = db
	if err := c.generateDDLOps(); err != nil {
		return errors.Trace(err)
	}
	if err := c.generateDMLOps(); err != nil {
		return errors.Trace(err)
	}
	if err := c.executeAddTable(nil); err != nil {
		return errors.Trace(err)
	}
	if err := c.executeAddTable(nil); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// executeOperations randomly executes a list of operations, with each operation being exeucted
// once. It will sleep for a random & small amount of time after each operation to make sure that
// other concurrent tasks have chances to be scheduled.
func (c *testCase) executeOperations(ops []ddlTestOpExecutor, postOp func() error) error {
	perm := rand.Perm(len(ops))
	for _, idx := range perm {
		op := ops[idx]
		err := op.executeFunc(op.config)
		if err != nil {
			if err.Error() != "Conflict operation" {
				return errors.Trace(err)
			}
		} else {
			if postOp != nil {
				err = postOp()
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
		time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
	}
	return nil
}

// execute iterates over two list of operations concurrently, one is
// ddl operations, one is dml operations.
// When one list completes, it starts over from the beginning again.
// When both of them ONCE complete, it exits.
func (c *testCase) execute(db *sql.DB) error {
	ddlAllComplete, dmlAllComplete := false, false

	err := parallel(func() error {
		var err error
		for {
			err = c.executeOperations(c.ddlOps, nil)
			ddlAllComplete = true
			if ddlAllComplete && dmlAllComplete || err != nil {
				break
			}
		}
		return errors.Trace(err)
	}, func() error {
		var err error
		for {
			err = c.executeOperations(c.dmlOps, func() error {
				return c.executeVerifyIntegrity()
			})
			dmlAllComplete = true
			if ddlAllComplete && dmlAllComplete || err != nil {
				break
			}
		}
		return errors.Trace(err)
	})

	if err != nil {
		ddlFailedCounter.Inc()
		return errors.Trace(err)
	}

	log.Infof("[ddl] [instance %d] Round completed", c.caseIndex)
	log.Infof("[ddl] [instance %d] Executing post round operations...", c.caseIndex)

	if !c.cfg.MySQLCompatible {
		err := c.executeAdminCheck()
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// executeVerifyIntegrity verifies the integrity of the data in the database
// by comparing the data in memory (that we expected) with the data in the database.
func (c *testCase) executeVerifyIntegrity() error {
	c.tablesLock.RLock()
	tablesSnapshot := make([]*ddlTestTable, 0)
	for _, table := range c.tables {
		tablesSnapshot = append(tablesSnapshot, table)
	}
	c.tablesLock.RUnlock()

	for _, table := range tablesSnapshot {
		table.lock.RLock()
		columnsSnapshot := table.filterColumns(table.predicateAll)
		table.lock.RUnlock()

		// build SQL
		sql := "SELECT "
		for i, column := range columnsSnapshot {
			if i > 0 {
				sql += ", "
			}
			sql += fmt.Sprintf("`%s`", column.name)
		}
		sql += fmt.Sprintf(" FROM `%s`", table.name)

		// execute
		rows, err := c.db.Query(sql)
		if err == nil {
			defer rows.Close()
		}
		// When column is removed, SELECT statement may return error so that we ignore them here.
		if table.deleted {
			return nil
		}
		for _, column := range columnsSnapshot {
			if column.deleted {
				return nil
			}
		}
		if err != nil {
			return errors.Trace(err)
		}

		// Read all rows.
		var actualRows [][]int32
		for rows.Next() {
			cols, err1 := rows.Columns()
			if err1 != nil {
				return errors.Trace(err)
			}

			// See https://stackoverflow.com/questions/14477941/read-select-columns-into-string-in-go
			rawResult := make([][]byte, len(cols))
			result := make([]int32, len(cols))
			dest := make([]interface{}, len(cols))
			for i := range rawResult {
				dest[i] = &rawResult[i]
			}

			err1 = rows.Scan(dest...)
			if err1 != nil {
				return errors.Trace(err)
			}

			for i, raw := range rawResult {
				if raw == nil {
					result[i] = ddlTestValueNull
				} else {
					val, err1 := strconv.Atoi(string(raw))
					if err1 != nil {
						return errors.Trace(err)
					}
					result[i] = int32(val)
				}
			}

			actualRows = append(actualRows, result)
		}

		// Even if SQL executes successfully, column deletion will cause different data as well
		if table.deleted {
			return nil
		}
		for _, column := range columnsSnapshot {
			if column.deleted {
				return nil
			}
		}

		// Make signatures for actual rows.
		actualRowsMap := make(map[string]int)
		for _, row := range actualRows {
			rowString := ""
			for _, col := range row {
				rowString += fmt.Sprintf("%d,", col)
			}
			_, ok := actualRowsMap[rowString]
			if !ok {
				actualRowsMap[rowString] = 0
			}
			actualRowsMap[rowString]++
		}

		// Compare with expecting rows.
		for i := 0; i < table.numberOfRows; i++ {
			rowString := ""
			for _, column := range columnsSnapshot {
				rowString += fmt.Sprintf("%d,", column.rows[i])
			}
			_, ok := actualRowsMap[rowString]
			if !ok {
				return errors.Trace(fmt.Errorf("Expecting row %s in table `%s` but not found", rowString, table.name))
			}
			actualRowsMap[rowString]--
			if actualRowsMap[rowString] < 0 {
				return errors.Trace(fmt.Errorf("Expecting row %s in table `%s` but not found", rowString, table.name))
			}
		}
		for rowString, occurs := range actualRowsMap {
			if occurs > 0 {
				return errors.Trace(fmt.Errorf("Unexpected row %s in table `%s`", rowString, table.name))
			}
		}
	}
	return nil
}

func (c *testCase) executeAdminCheck() error {
	// build SQL
	sql := "ADMIN CHECK TABLE "
	i := 0
	for _, table := range c.tables {
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("`%s`", table.name)
		i++
	}

	// execute
	log.Infof("[ddl] [instance %d] %s", c.caseIndex, sql)
	_, err := c.db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
