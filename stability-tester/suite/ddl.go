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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/config"
	"github.com/twinj/uuid"
)

// DDLCase performs DDL operations while running DML operations.
type DDLCase struct {
	cfg        *config.DDLCaseConfig
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

func (c *DDLCase) pickupRandomTable() *ddlTestTable {
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

func padLeft(str, pad string, length int) string {
	padding := strings.Repeat(pad, length)
	str = padding + str
	return str[len(str)-length:]
}

func padRight(str, pad string, length int) string {
	padding := strings.Repeat(pad, length)
	str = str + padding
	return str[0:length]
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

const (
	ddlTestValueNull    int32 = -1
	ddlTestValueInvalid int32 = -99
)

func buildConditionSQL(columnName string, value int32) string {
	sql := fmt.Sprintf("`%s`", columnName)
	if value == ddlTestValueNull {
		sql += " IS NULL"
	} else {
		sql += fmt.Sprintf(" = %d", value)
	}
	return sql
}

type ddlTestOpExecutor struct {
	executeFunc func(interface{}) error
	config      interface{}
}

// NewDDLCase returns a DDLCase.
func NewDDLCase(cfg *config.Config) Case {
	b := &DDLCase{
		cfg:    &cfg.Suite.DDL,
		tables: make(map[string]*ddlTestTable),
		ddlOps: make([]ddlTestOpExecutor, 0),
		dmlOps: make([]ddlTestOpExecutor, 0),
	}
	return b
}

// Initialize does nothing currently
func (c *DDLCase) Initialize(ctx context.Context, db *sql.DB) error {
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

func (c *DDLCase) executeOperations(ops []ddlTestOpExecutor, postOp func() error) error {
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

// Execute implements Case Execute interface.
func (c *DDLCase) Execute(db *sql.DB, testCaseIndex int) error {
	c.caseIndex = testCaseIndex
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
		return err
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
		return err
	})

	if err != nil {
		ddlFailedCounter.Inc()
		log.Errorf("[ddl] [instance %d] ERROR: %s", testCaseIndex, errors.ErrorStack(err))
		os.Exit(1)
		return nil
	}

	log.Infof("[ddl] [instance %d] Round compeleted", testCaseIndex)

	return nil
}

func (c *DDLCase) String() string {
	return "ddl"
}

func init() {
	RegisterSuite("ddl", NewDDLCase)
}

func (c *DDLCase) generateDDLOps() error {
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
	// if err := c.generateRenameTable(); err != nil {
	// 	return errors.Trace(err)
	// }
	return nil
}

func (c *DDLCase) generateDMLOps() error {
	if err := c.generateInsert(); err != nil {
		return errors.Trace(err)
	}
	if err := c.generateUpdate(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *DDLCase) generateAddTable() error {
	c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.executeAddTable, nil})
	return nil
}

func (c *DDLCase) executeAddTable(cfg interface{}) error {
	columnCount := rand.Intn(5) + 5
	tableColumns := make([]*ddlTestColumn, columnCount)
	for i := 0; i < columnCount; i++ {
		column := ddlTestColumn{
			name:         uuid.NewV4().String(),
			fieldType:    "int",
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

func (c *DDLCase) generateDropTable() error {
	c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.executeDropTable, nil})
	return nil
}

func (c *DDLCase) executeDropTable(cfg interface{}) error {
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

// func (c *DDLCase) generateRenameTable() error {
// 	c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.executeRenameTable, nil})
// 	return nil
// }

// // executeRenameTable is implemented as "drop" + "create" for the tester.
// func (c *DDLCase) executeRenameTable(cfg interface{}) error {
// 	// Remove previous table
// 	c.tablesLock.Lock()
// 	tableToRename := c.pickupRandomTable()
// 	if tableToRename == nil {
// 		c.tablesLock.Unlock()
// 		return nil
// 	}
// 	newTable := &ddlTestTable{
// 		name:         uuid.NewV4().String(),
// 		columns:      tableToRename.columns,
// 		indexes:      tableToRename.indexes,
// 		numberOfRows: tableToRename.numberOfRows,
// 	}
// 	delete(c.tables, tableToRename.name)
// 	tableToRename.deleted = true
// 	c.tablesLock.Unlock()

// 	sql := fmt.Sprintf("RENAME TABLE `%s` TO `%s`", tableToRename.name, newTable.name)
//  log.Infof("[ddl] [instance %d] %s", c.caseIndex, sql)
// 	_, err := c.db.Exec(sql)

// 	if err != nil {
// 		return errors.Trace(err)
// 	}

// 	// Add new table
// 	c.tablesLock.Lock()
// 	c.tables[newTable.name] = newTable
// 	c.tablesLock.Unlock()

// 	return nil
// }

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

func (c *DDLCase) generateAddIndex() error {
	for strategy := ddlTestIndexStrategyBegin + 1; strategy < ddlTestIndexStrategyEnd; strategy++ {
		config := ddlTestAddIndexConfig{
			strategy: strategy,
		}
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.executeAddIndex, config})
	}
	return nil
}

func (c *DDLCase) executeAddIndex(cfg interface{}) error {
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	config := cfg.(ddlTestAddIndexConfig)

	if len(table.columns) == 0 {
		return nil
	}

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
		numberOfColumns := rand.Intn(len(table.columns))
		perm := rand.Perm(numberOfColumns)
		for _, idx := range perm {
			index.columns = append(index.columns, table.columns[idx])
		}
	}

	if len(index.columns) == 0 {
		return nil
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

func (c *DDLCase) generateDropIndex() error {
	numberOfIndexToDrop := rand.Intn(10)
	for i := 0; i < numberOfIndexToDrop; i++ {
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.executeDropIndex, nil})
	}
	return nil
}

func (c *DDLCase) executeDropIndex(cfg interface{}) error {
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

func (c *DDLCase) generateAddColumn() error {
	for strategy := ddlTestAddDropColumnStrategyBegin + 1; strategy < ddlTestAddDropColumnStrategyEnd; strategy++ {
		config := ddlTestAddDropColumnConfig{
			strategy: strategy,
		}
		c.ddlOps = append(c.ddlOps, ddlTestOpExecutor{c.executeAddColumn, config})
	}
	return nil
}

func (c *DDLCase) executeAddColumn(cfg interface{}) error {
	table := c.pickupRandomTable()
	if table == nil {
		return nil
	}
	config := cfg.(ddlTestAddDropColumnConfig)

	newColumn := ddlTestColumn{
		name:         uuid.NewV4().String(),
		fieldType:    "int",
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

	// table.debugPrint("AddColumn")

	return nil
}

func (c *DDLCase) generateDropColumn() error {
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

func (c *DDLCase) executeDropColumn(cfg interface{}) error {
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

	// table.debugPrint("DropColumn")

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

func (c *DDLCase) generateInsert() error {
	for i := 0; i < 5; i++ {
		for columnStrategy := ddlTestInsertColumnStrategyBegin + 1; columnStrategy < ddlTestInsertColumnStrategyEnd; columnStrategy++ {
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

func (c *DDLCase) executeInsert(cfg interface{}) error {
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

	// table.debugPrint("Insert")

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

func (c *DDLCase) generateUpdate() error {
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

func (c *DDLCase) buildWhereColumns(whereStrategy ddlTestWhereStrategy, pkColumns, nonPkColumns []*ddlTestColumn, numberOfRows int) []*ddlTestColumnDescriptor {
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

func (c *DDLCase) executeUpdate(cfg interface{}) error {
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

func (c *DDLCase) generateDelete() error {
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

func (c *DDLCase) executeDelete(cfg interface{}) error {
	c.tablesLock.RLock()
	table := c.pickupRandomTable()
	if table == nil {
		c.tablesLock.RUnlock()
		return nil
	}
	table.lock.RLock()
	columns := table.filterColumns(table.predicateAll)
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
			for _, column := range columns {
				column.rows = append(column.rows[:i], column.rows[i+1:]...)
			}
		}
		table.numberOfRows--
	}
	table.lock.Unlock()

	return nil
}

func (c *DDLCase) executeVerifyIntegrity() error {
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

		// table.debugPrint("Verify")

		// var buffer bytes.Buffer
		// buffer.WriteString(fmt.Sprintf("Table data in database for `%s`:\n", table.name))
		// for _, row := range actualRows {
		// 	rowString := ""
		// 	for _, col := range row {
		// 		rowString += fmt.Sprintf("%d,", col)
		// 	}
		// 	buffer.WriteString(fmt.Sprintf("%s\n", rowString))
		// }
		// buffer.WriteString("======== END ========\n")
		// fmt.Printf("%s\n", buffer.String())

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

// parallel run functions in parallel and wait until all of them are completed.
// If one of them returns error, the result is that error.
func parallel(funcs ...func() error) error {
	cr := make(chan error, len(funcs))
	for _, foo := range funcs {
		go func(foo func() error) {
			err := foo()
			cr <- err
		}(foo)
	}
	var err error
	for i := 0; i < len(funcs); i++ {
		r := <-cr
		if r != nil {
			err = r
		}
	}
	return err
}
