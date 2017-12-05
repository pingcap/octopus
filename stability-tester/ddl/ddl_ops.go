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
	"github.com/twinj/uuid"
)

func (c *testCase) generateDDLOps() error {
	if err := c.generateAddTable(); err != nil {
		return errors.Trace(err)
	}
	if err := c.generateDropTable(); err != nil {
		return errors.Trace(err)
	}
	if err := c.generateAddIndex(); err != nil {
		return errors.Trace(err)
	}
	if err := c.generateDropIndex(); err != nil {
		return errors.Trace(err)
	}
	if err := c.generateAddColumn(); err != nil {
		return errors.Trace(err)
	}
	if err := c.generateDropColumn(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *testCase) generateAddTable() error {
	c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.executeAddTable, nil})
	return nil
}

func (c *testCase) executeAddTable(cfg interface{}) error {
	columnCount := rand.Intn(c.cfg.TablesToCreate) + 2
	tableColumns := make([]*ddlTestColumn, columnCount)
	for i := 0; i < columnCount; i++ {
		column := ddlTestColumn{
			name:         uuid.NewV4().String(),
			fieldType:    "int", // TODO: Add more field types and values
			defaultValue: rand.Int31(),
			rows:         make([]int32, 0),
		}
		tableColumns[i] = &column
	}

	// Generate primary key with [0, 3) size
	primaryKeyFields := rand.Intn(3)
	primaryKeys := make([]int, 0)
	if primaryKeyFields > 0 {
		primaryKeys = rand.Perm(len(tableColumns))[0:primaryKeyFields]
		for _, columnIndex := range primaryKeys {
			tableColumns[columnIndex].isPrimaryKey = true
		}
	}
	tableInfo := ddlTestTable{
		name:         uuid.NewV4().String(),
		columns:      tableColumns,
		indexes:      make([]*ddlTestIndex, 0),
		numberOfRows: 0,
	}

	sql := fmt.Sprintf("CREATE TABLE `%s` (", tableInfo.name)
	for i := 0; i < len(tableInfo.columns); i++ {
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("`%s` %s", tableColumns[i].name, tableColumns[i].getDefinition())
	}
	if primaryKeyFields > 0 {
		sql += ", PRIMARY KEY ("
		for i, columnIndex := range primaryKeys {
			if i > 0 {
				sql += ", "
			}
			sql += fmt.Sprintf("`%s`", tableColumns[columnIndex].name)
		}
		sql += ")"
	}
	sql += ")"

	log.Infof("[ddl] [instance %d] %s", c.caseIndex, sql)
	_, err := c.db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}

	c.tablesLock.Lock()
	c.tables[tableInfo.name] = &tableInfo
	c.tablesLock.Unlock()

	return nil
}

func (c *testCase) generateDropTable() error {
	c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.executeDropTable, nil})
	return nil
}

func (c *testCase) executeDropTable(cfg interface{}) error {
	c.tablesLock.Lock()
	tableToDrop := c.pickupRandomTable()
	if tableToDrop == nil {
		c.tablesLock.Unlock()
		return nil
	}
	delete(c.tables, tableToDrop.name)
	tableToDrop.deleted = true
	c.tablesLock.Unlock()

	sql := fmt.Sprintf("DROP TABLE `%s`", tableToDrop.name)
	log.Infof("[ddl] [instance %d] %s", c.caseIndex, sql)
	_, err := c.db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

type ddlTestIndexStrategy int

const (
	ddlTestIndexStrategyBegin ddlTestIndexStrategy = iota
	ddlTestIndexStrategySingleColumnAtBeginning
	ddlTestIndexStrategySingleColumnAtEnd
	ddlTestIndexStrategySingleColumnRandom
	ddlTestIndexStrategyMultipleColumnRandom
	ddlTestIndexStrategyEnd
)

type ddlTestAddIndexConfig struct {
	strategy ddlTestIndexStrategy
}

func (c *testCase) generateAddIndex() error {
	for strategy := ddlTestIndexStrategyBegin + 1; strategy < ddlTestIndexStrategyEnd; strategy++ {
		config := ddlTestAddIndexConfig{
			strategy: strategy,
		}
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.executeAddIndex, config})
	}
	return nil
}

func (c *testCase) executeAddIndex(cfg interface{}) error {
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	config := cfg.(ddlTestAddIndexConfig)

	// build index definition
	index := ddlTestIndex{
		name:      uuid.NewV4().String(),
		signature: "",
		columns:   make([]*ddlTestColumn, 0),
	}

	switch config.strategy {
	case ddlTestIndexStrategySingleColumnAtBeginning:
		index.columns = append(index.columns, table.columns[0])
	case ddlTestIndexStrategySingleColumnAtEnd:
		index.columns = append(index.columns, table.columns[len(table.columns)-1])
	case ddlTestIndexStrategySingleColumnRandom:
		index.columns = append(index.columns, table.columns[rand.Intn(len(table.columns))])
	case ddlTestIndexStrategyMultipleColumnRandom:
		numberOfColumns := rand.Intn(len(table.columns)) + 1
		perm := rand.Perm(numberOfColumns)
		for _, idx := range perm {
			index.columns = append(index.columns, table.columns[idx])
		}
	}

	signature := ""
	for _, col := range index.columns {
		signature += col.name + ","
	}
	index.signature = signature

	// check whether index duplicates
	for _, idx := range table.indexes {
		if idx.signature == index.signature {
			return nil
		}
	}

	// build SQL
	sql := fmt.Sprintf("ALTER TABLE `%s` ADD INDEX `%s` (", table.name, index.name)
	for i, column := range index.columns {
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("`%s`", column.name)
	}
	sql += ")"

	log.Infof("[ddl] [instance %d] %s", c.caseIndex, sql)
	_, err := c.db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}

	table.indexes = append(table.indexes, &index)
	for _, column := range index.columns {
		column.indexReferences++
	}

	return nil
}

func (c *testCase) generateDropIndex() error {
	numberOfIndexToDrop := rand.Intn(10)
	for i := 0; i < numberOfIndexToDrop; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.executeDropIndex, nil})
	}
	return nil
}

func (c *testCase) executeDropIndex(cfg interface{}) error {
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	if len(table.indexes) == 0 {
		return nil
	}

	indexToDropIndex := rand.Intn(len(table.indexes))
	indexToDrop := table.indexes[indexToDropIndex]

	sql := fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`", table.name, indexToDrop.name)

	log.Infof("[ddl] [instance %d] %s", c.caseIndex, sql)
	_, err := c.db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}

	for _, column := range indexToDrop.columns {
		column.indexReferences--
		if column.indexReferences < 0 {
			panic("Unexpected index reference")
		}
	}
	table.indexes = append(table.indexes[:indexToDropIndex], table.indexes[indexToDropIndex+1:]...)

	return nil
}

type ddlTestAddDropColumnStrategy int

const (
	ddlTestAddDropColumnStrategyBegin ddlTestAddDropColumnStrategy = iota
	ddlTestAddDropColumnStrategyAtBeginning
	ddlTestAddDropColumnStrategyAtEnd
	ddlTestAddDropColumnStrategyAtRandom
	ddlTestAddDropColumnStrategyEnd
)

type ddlTestAddDropColumnConfig struct {
	strategy ddlTestAddDropColumnStrategy
}

func (c *testCase) generateAddColumn() error {
	for strategy := ddlTestAddDropColumnStrategyBegin + 1; strategy < ddlTestAddDropColumnStrategyEnd; strategy++ {
		config := ddlTestAddDropColumnConfig{
			strategy: strategy,
		}
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.executeAddColumn, config})
	}
	return nil
}

func (c *testCase) executeAddColumn(cfg interface{}) error {
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	config := cfg.(ddlTestAddDropColumnConfig)

	newColumn := ddlTestColumn{
		name:         uuid.NewV4().String(),
		fieldType:    "int", // TODO: Add more field types and values
		defaultValue: rand.Int31(),
	}

	insertAfterPosition := -1

	// build SQL
	sql := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s", table.name, newColumn.name, newColumn.getDefinition())
	switch config.strategy {
	case ddlTestAddDropColumnStrategyAtBeginning:
		sql += " FIRST"
	case ddlTestAddDropColumnStrategyAtEnd:
		// do nothing
	case ddlTestAddDropColumnStrategyAtRandom:
		insertAfterPosition = rand.Intn(len(table.columns))
		sql += fmt.Sprintf(" AFTER `%s`", table.columns[insertAfterPosition].name)
	}

	log.Infof("[ddl] [instance %d] %s", c.caseIndex, sql)
	_, err := c.db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}

	// update table definitions
	table.lock.Lock()
	newColumn.rows = make([]int32, table.numberOfRows)
	for i := 0; i < table.numberOfRows; i++ {
		newColumn.rows[i] = newColumn.defaultValue
	}
	switch config.strategy {
	case ddlTestAddDropColumnStrategyAtBeginning:
		table.columns = append([]*ddlTestColumn{&newColumn}, table.columns...)
	case ddlTestAddDropColumnStrategyAtEnd:
		table.columns = append(table.columns, &newColumn)
	case ddlTestAddDropColumnStrategyAtRandom:
		table.columns = append(table.columns[:insertAfterPosition+1], append([]*ddlTestColumn{&newColumn}, table.columns[insertAfterPosition+1:]...)...)
	}
	table.lock.Unlock()

	return nil
}

func (c *testCase) generateDropColumn() error {
	for i := 0; i < 5; i++ {
		for strategy := ddlTestAddDropColumnStrategyBegin + 1; strategy < ddlTestAddDropColumnStrategyEnd; strategy++ {
			config := ddlTestAddDropColumnConfig{
				strategy: strategy,
			}
			c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.executeDropColumn, config})
		}
	}
	return nil
}

func (c *testCase) executeDropColumn(cfg interface{}) error {
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	if len(table.columns) <= 1 {
		return nil
	}

	config := cfg.(ddlTestAddDropColumnConfig)
	columnToDropIndex := -1
	switch config.strategy {
	case ddlTestAddDropColumnStrategyAtBeginning:
		columnToDropIndex = 0
	case ddlTestAddDropColumnStrategyAtEnd:
		columnToDropIndex = len(table.columns) - 1
	case ddlTestAddDropColumnStrategyAtRandom:
		columnToDropIndex = rand.Intn(len(table.columns))
	}

	columnToDrop := table.columns[columnToDropIndex]

	// primary key columns cannot be dropped
	if columnToDrop.isPrimaryKey {
		return nil
	}

	// we does not support dropping a column with index
	if columnToDrop.indexReferences > 0 {
		return nil
	}

	columnToDrop.deleted = true

	sql := fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `%s`", table.name, columnToDrop.name)

	log.Infof("[ddl] [instance %d] %s", c.caseIndex, sql)
	_, err := c.db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}

	// update table definitions
	table.lock.Lock()
	table.columns = append(table.columns[:columnToDropIndex], table.columns[columnToDropIndex+1:]...)
	table.lock.Unlock()

	return nil
}
