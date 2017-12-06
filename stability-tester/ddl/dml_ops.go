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
	"fmt"
	"math/rand"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

func (c *testCase) generateDMLOps() error {
	if err := c.generateInsert(); err != nil {
		return errors.Trace(err)
	}
	if err := c.generateUpdate(); err != nil {
		return errors.Trace(err)
	}
	if err := c.generateDelete(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

type ddlTestInsertColumnStrategy int
type ddlTestInsertMissingValueStrategy int

const (
	ddlTestInsertColumnStrategyBegin ddlTestInsertColumnStrategy = iota
	ddlTestInsertColumnStrategyZeroNonPk
	ddlTestInsertColumnStrategyAllNonPk
	ddlTestInsertColumnStrategyRandomNonPk
	ddlTestInsertColumnStrategyEnd
)

const (
	ddlTestInsertMissingValueStrategyBegin ddlTestInsertMissingValueStrategy = iota
	ddlTestInsertMissingValueStrategyAllNull
	ddlTestInsertMissingValueStrategyAllDefault
	ddlTestInsertMissingValueStrategyRandom
	ddlTestInsertMissingValueStrategyEnd
)

type ddlTestInsertConfig struct {
	useSetStatement      bool                              // whether to use SET or VALUE statement
	columnStrategy       ddlTestInsertColumnStrategy       // how non-Primary-Key columns are picked
	missingValueStrategy ddlTestInsertMissingValueStrategy // how columns are filled when they are not picked in VALUE statement
}

func (c *testCase) generateInsert() error {
	for i := 0; i < 5; i++ {
		for columnStrategy := ddlTestInsertColumnStrategyBegin + 1; columnStrategy < ddlTestInsertColumnStrategyEnd; columnStrategy++ {
			// Note: `useSetStatement` is commented out since `... VALUES ...` SQL will generates column conflicts with add / drop column.
			// We always use `... SET ...` syntax currently.

			// for useSetStatement := 0; useSetStatement < 2; useSetStatement++ {
			config := ddlTestInsertConfig{
				useSetStatement: true, // !(useSetStatement == 0),
				columnStrategy:  columnStrategy,
			}
			//	if config.useSetStatement {
			c.dmlOps = append(c.dmlOps, ddlTestOpExecutor{c.executeInsert, config})
			// 	} else {
			// 		for missingValueStrategy := ddlTestInsertMissingValueStrategyBegin + 1; missingValueStrategy < ddlTestInsertMissingValueStrategyEnd; missingValueStrategy++ {
			// 			config.missingValueStrategy = missingValueStrategy
			// 			c.dmlOps = append(c.dmlOps, ddlTestOpExecutor{c.executeInsert, config})
			// 		}
			// 	}
			// }
		}
	}
	return nil
}

func (c *testCase) executeInsert(cfg interface{}) error {
	c.tablesLock.RLock()
	table := c.pickupRandomTable()
	if table == nil {
		c.tablesLock.RUnlock()
		return nil
	}
	table.lock.RLock()
	columns := table.filterColumns(table.predicateAll)
	nonPkColumns := table.filterColumns(table.predicateNonPrimaryKey)
	table.lock.RUnlock()
	c.tablesLock.RUnlock()

	config := cfg.(ddlTestInsertConfig)

	// build assignments
	assigns := make([]*ddlTestColumnDescriptor, 0)
	for _, column := range columns {
		pick := false
		if column.isPrimaryKey {
			// PrimaryKey Column is always assigned values
			pick = true
		} else {
			// NonPrimaryKey Column is assigned by strategy
			switch config.columnStrategy {
			case ddlTestInsertColumnStrategyAllNonPk:
				pick = true
			case ddlTestInsertColumnStrategyZeroNonPk:
				pick = false
			case ddlTestInsertColumnStrategyRandomNonPk:
				if rand.Float64() <= float64(1)/float64(len(nonPkColumns)) {
					pick = true
				}
			}
		}
		if pick {
			assigns = append(assigns, &ddlTestColumnDescriptor{column, rand.Int31()})
		}
	}

	// build SQL
	sql := ""
	if config.useSetStatement {
		if len(assigns) == 0 {
			return nil
		}
		sql = fmt.Sprintf("INSERT INTO `%s` SET ", table.name)
		perm := rand.Perm(len(assigns))
		for i, idx := range perm {
			assign := assigns[idx]
			if i > 0 {
				sql += ", "
			}
			sql += fmt.Sprintf("`%s` = '%d'", assign.column.name, assign.value)
		}
	} else {
		sql = fmt.Sprintf("INSERT INTO `%s` VALUE (", table.name)
		for colIdx, column := range columns {
			if colIdx > 0 {
				sql += ", "
			}
			cd := column.getMatchedColumnDescriptor(assigns)
			if cd != nil {
				sql += fmt.Sprintf("'%d'", cd.value)
			} else {
				var missingValueSQL string
				switch config.missingValueStrategy {
				case ddlTestInsertMissingValueStrategyAllDefault:
					missingValueSQL = "DEFAULT"
				case ddlTestInsertMissingValueStrategyAllNull:
					missingValueSQL = "NULL"
				case ddlTestInsertMissingValueStrategyRandom:
					if rand.Float64() <= 0.5 {
						missingValueSQL = "DEFAULT"
					} else {
						missingValueSQL = "NULL"
					}
				}
				sql += missingValueSQL
				var missingValue int32
				if missingValueSQL == "DEFAULT" {
					missingValue = column.defaultValue
				} else if missingValueSQL == "NULL" {
					missingValue = ddlTestValueNull
				} else {
					panic("invalid missing value")
				}
				// add column to ref list
				assigns = append(assigns, &ddlTestColumnDescriptor{column, missingValue})
			}
		}
		sql += ")"
	}

	// execute SQL
	log.Infof("[ddl] [instance %d] %s", c.caseIndex, sql)
	_, err := c.db.Exec(sql)
	if err != nil {
		if table.deleted {
			return ddlTestErrorConflict{}
		}
		for _, cd := range assigns {
			if cd.column.deleted {
				return ddlTestErrorConflict{}
			}
		}
		return errors.Trace(err)
	}

	// append row
	table.lock.Lock()
	for _, column := range table.columns {
		cd := column.getMatchedColumnDescriptor(assigns)
		if cd == nil {
			// only happens when using SET
			column.rows = append(column.rows, column.defaultValue)
		} else {
			column.rows = append(column.rows, cd.value)
		}
	}
	table.numberOfRows++
	table.lock.Unlock()

	return nil
}

type ddlTestWhereStrategy int

const (
	ddlTestWhereStrategyBegin ddlTestWhereStrategy = iota
	ddlTestWhereStrategyNone
	ddlTestWhereStrategyRandomInPk
	ddlTestWhereStrategyRandomInNonPk
	ddlTestWhereStrategyRandomMixed
	ddlTestWhereStrategyEnd
)

type ddlTestUpdateTargetStrategy int

const (
	ddlTestUpdateTargetStrategyBegin ddlTestUpdateTargetStrategy = iota
	ddlTestUpdateTargetStrategyAllColumns
	ddlTestUpdateTargetStrategyRandom
	ddlTestUpdateTargetStrategyEnd
)

type ddlTestUpdateConfig struct {
	whereStrategy  ddlTestWhereStrategy        // how "where" statement is generated
	targetStrategy ddlTestUpdateTargetStrategy // which column to update
}

func (c *testCase) generateUpdate() error {
	for i := 0; i < 5; i++ {
		for whereStrategy := ddlTestWhereStrategyBegin + 1; whereStrategy < ddlTestWhereStrategyEnd; whereStrategy++ {
			for targetStrategy := ddlTestUpdateTargetStrategyBegin + 1; targetStrategy < ddlTestUpdateTargetStrategyEnd; targetStrategy++ {
				config := ddlTestUpdateConfig{
					whereStrategy:  whereStrategy,
					targetStrategy: targetStrategy,
				}
				c.dmlOps = append(c.dmlOps, ddlTestOpExecutor{c.executeUpdate, config})
			}
		}
	}
	return nil
}

func (c *testCase) buildWhereColumns(whereStrategy ddlTestWhereStrategy, pkColumns, nonPkColumns []*ddlTestColumn, numberOfRows int) []*ddlTestColumnDescriptor {
	// build where conditions
	whereColumns := make([]*ddlTestColumnDescriptor, 0)
	if whereStrategy == ddlTestWhereStrategyRandomInPk || whereStrategy == ddlTestWhereStrategyRandomMixed {
		if len(pkColumns) > 0 {
			picks := rand.Intn(len(pkColumns))
			perm := rand.Perm(picks)
			for _, idx := range perm {
				// value will be filled later
				whereColumns = append(whereColumns, &ddlTestColumnDescriptor{pkColumns[idx], -1})
			}
		}
	}
	if whereStrategy == ddlTestWhereStrategyRandomInNonPk || whereStrategy == ddlTestWhereStrategyRandomMixed {
		if len(nonPkColumns) > 0 {
			picks := rand.Intn(len(nonPkColumns))
			perm := rand.Perm(picks)
			for _, idx := range perm {
				// value will be filled later
				whereColumns = append(whereColumns, &ddlTestColumnDescriptor{nonPkColumns[idx], -1})
			}
		}
	}

	// fill values of where statements
	if len(whereColumns) > 0 {
		rowToUpdate := rand.Intn(numberOfRows)
		for _, cd := range whereColumns {
			cd.value = cd.column.rows[rowToUpdate]
		}
	}

	return whereColumns
}

func (c *testCase) executeUpdate(cfg interface{}) error {
	c.tablesLock.RLock()
	table := c.pickupRandomTable()
	if table == nil {
		c.tablesLock.RUnlock()
		return nil
	}
	table.lock.RLock()
	pkColumns := table.filterColumns(table.predicatePrimaryKey)
	nonPkColumns := table.filterColumns(table.predicateNonPrimaryKey)
	table.lock.RUnlock()
	c.tablesLock.RUnlock()

	if table.numberOfRows == 0 {
		return nil
	}

	config := cfg.(ddlTestUpdateConfig)

	// build where conditions
	whereColumns := c.buildWhereColumns(config.whereStrategy, pkColumns, nonPkColumns, table.numberOfRows)

	// build assignments
	assigns := make([]*ddlTestColumnDescriptor, 0)
	picks := 0
	switch config.targetStrategy {
	case ddlTestUpdateTargetStrategyRandom:
		if len(nonPkColumns) > 0 {
			picks = rand.Intn(len(nonPkColumns))
		}
	case ddlTestUpdateTargetStrategyAllColumns:
		picks = len(nonPkColumns)
	}
	if picks == 0 {
		return nil
	}
	perm := rand.Perm(picks)
	for _, idx := range perm {
		assigns = append(assigns, &ddlTestColumnDescriptor{nonPkColumns[idx], rand.Int31()})
	}

	// build SQL
	sql := fmt.Sprintf("UPDATE `%s` SET ", table.name)
	for i, cd := range assigns {
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("`%s` = '%d'", cd.column.name, cd.value)
	}
	if len(whereColumns) > 0 {
		sql += " WHERE "
		for i, cd := range whereColumns {
			if i > 0 {
				sql += " AND "
			}
			sql += buildConditionSQL(cd.column.name, cd.value)
		}
	}

	// execute SQL
	log.Infof("[ddl] [instance %d] %s", c.caseIndex, sql)
	_, err := c.db.Exec(sql)
	if err != nil {
		if table.deleted {
			return ddlTestErrorConflict{}
		}
		for _, cd := range assigns {
			if cd.column.deleted {
				return ddlTestErrorConflict{}
			}
		}
		for _, cd := range whereColumns {
			if cd.column.deleted {
				return ddlTestErrorConflict{}
			}
		}
		return errors.Trace(err)
	}

	// update values
	table.lock.RLock()
	for i := 0; i < table.numberOfRows; i++ {
		match := true
		for _, cd := range whereColumns {
			if cd.value != cd.column.rows[i] {
				match = false
				break
			}
		}
		if match {
			for _, cd := range assigns {
				cd.column.rows[i] = cd.value
			}
		}
	}
	table.lock.RUnlock()

	return nil
}

type ddlTestDeleteConfig struct {
	whereStrategy ddlTestWhereStrategy // how "where" statement is generated
}

func (c *testCase) generateDelete() error {
	for i := 0; i < 5; i++ {
		for whereStrategy := ddlTestWhereStrategyBegin + 1; whereStrategy < ddlTestWhereStrategyEnd; whereStrategy++ {
			config := ddlTestDeleteConfig{
				whereStrategy: whereStrategy,
			}
			c.dmlOps = append(c.dmlOps, ddlTestOpExecutor{c.executeDelete, config})
		}
	}
	return nil
}

func (c *testCase) executeDelete(cfg interface{}) error {
	c.tablesLock.RLock()
	table := c.pickupRandomTable()
	if table == nil {
		c.tablesLock.RUnlock()
		return nil
	}
	table.lock.RLock()
	pkColumns := table.filterColumns(table.predicatePrimaryKey)
	nonPkColumns := table.filterColumns(table.predicateNonPrimaryKey)
	table.lock.RUnlock()
	c.tablesLock.RUnlock()

	if table.numberOfRows == 0 {
		return nil
	}

	config := cfg.(ddlTestDeleteConfig)
	whereColumns := c.buildWhereColumns(config.whereStrategy, pkColumns, nonPkColumns, table.numberOfRows)

	// build SQL
	sql := fmt.Sprintf("DELETE FROM `%s`", table.name)
	if len(whereColumns) > 0 {
		sql += " WHERE "
		for i, cd := range whereColumns {
			if i > 0 {
				sql += " AND "
			}
			sql += buildConditionSQL(cd.column.name, cd.value)
		}
	}

	// execute SQL
	log.Infof("[ddl] [instance %d] %s", c.caseIndex, sql)
	_, err := c.db.Exec(sql)
	if err != nil {
		if table.deleted {
			return ddlTestErrorConflict{}
		}
		for _, cd := range whereColumns {
			if cd.column.deleted {
				return ddlTestErrorConflict{}
			}
		}
		return errors.Trace(err)
	}

	// update values
	table.lock.Lock()
	for i := table.numberOfRows - 1; i >= 0; i-- {
		match := true
		for _, cd := range whereColumns {
			if cd.value != cd.column.rows[i] {
				match = false
				break
			}
		}
		if match {
			// we must use `table.columns` here, since there might be new columns after deletion
			for _, column := range table.columns {
				column.rows = append(column.rows[:i], column.rows[i+1:]...)
			}
			table.numberOfRows--
		}
	}
	table.lock.Unlock()

	return nil
}
