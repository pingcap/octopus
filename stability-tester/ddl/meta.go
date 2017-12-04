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
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
)

type testCase struct {
	cfg        *DDLCaseConfig
	db         *sql.DB
	caseIndex  int
	ddlOps     []ddlTestOpExecutor
	dmlOps     []ddlTestOpExecutor
	tables     map[string]*ddlTestTable
	tablesLock sync.RWMutex
}

type ddlTestErrorConflict struct {
}

func (err ddlTestErrorConflict) Error() string {
	return "Conflict operation"
}

// pickupRandomTables picks a table randomly. The callee should ensure that
// during this function call the table list is not modified.
//
// Normally the DML op callee should acquire a lock before calling this function
// because the table list may be modified by another parallel DDL op. However
// the DDL op callee doesn't need to acquire a lock because no one will modify the
// table list in parallel ---- DDL ops are executed one by one.
func (c *testCase) pickupRandomTable() *ddlTestTable {
	tableNames := make([]string, 0)
	for name := range c.tables {
		tableNames = append(tableNames, name)
	}
	if len(tableNames) == 0 {
		return nil
	}
	name := tableNames[rand.Intn(len(tableNames))]
	return c.tables[name]
}

type ddlTestTable struct {
	deleted      bool
	name         string
	columns      []*ddlTestColumn
	indexes      []*ddlTestIndex
	numberOfRows int
	lock         sync.RWMutex
}

func (table *ddlTestTable) filterColumns(predicate func(*ddlTestColumn) bool) []*ddlTestColumn {
	retColumns := make([]*ddlTestColumn, 0)
	for index, col := range table.columns {
		if predicate(col) {
			retColumns = append(retColumns, table.columns[index])
		}
	}
	return retColumns
}

func (table *ddlTestTable) predicateAll(col *ddlTestColumn) bool {
	return true
}

func (table *ddlTestTable) predicatePrimaryKey(col *ddlTestColumn) bool {
	return col.isPrimaryKey
}

func (table *ddlTestTable) predicateNonPrimaryKey(col *ddlTestColumn) bool {
	return !col.isPrimaryKey
}

func (table *ddlTestTable) debugPrint(title string) {
	var buffer bytes.Buffer
	table.lock.RLock()
	buffer.WriteString(fmt.Sprintf("======== DEBUG BEGIN for %s ========\n", title))
	buffer.WriteString(fmt.Sprintf("Table Structures of `%s`:\n", table.name))
	for i, column := range table.columns {
		buffer.WriteString(fmt.Sprintf("Column #%d: %s\n", i, column.name))
	}
	buffer.WriteString(fmt.Sprintf("Table Values of `%s`:\n", table.name))
	for i := 0; i < table.numberOfRows; i++ {
		buffer.WriteString("#")
		buffer.WriteString(padRight(fmt.Sprintf("%d", i), " ", 4))
		buffer.WriteString(": ")
		for _, col := range table.columns {
			buffer.WriteString(padLeft(fmt.Sprintf("%d", col.rows[i]), " ", 11))
			buffer.WriteString(", ")
		}
		buffer.WriteString("\n")
	}
	buffer.WriteString("======== DEBUG END ========\n")
	table.lock.RUnlock()
	fmt.Print(buffer.String())
}

type ddlTestColumnDescriptor struct {
	column *ddlTestColumn
	value  int32
}

type ddlTestColumn struct {
	deleted         bool
	name            string
	fieldType       string
	defaultValue    int32
	isPrimaryKey    bool
	rows            []int32
	indexReferences int
}

func (col *ddlTestColumn) getMatchedColumnDescriptor(descriptors []*ddlTestColumnDescriptor) *ddlTestColumnDescriptor {
	for _, d := range descriptors {
		if d.column == col {
			return d
		}
	}
	return nil
}

func (col *ddlTestColumn) getDefinition() string {
	if col.isPrimaryKey {
		return col.fieldType
	}
	return fmt.Sprintf("%s NULL DEFAULT '%d'", col.fieldType, col.defaultValue)
}

type ddlTestIndex struct {
	name      string
	signature string
	columns   []*ddlTestColumn
}
