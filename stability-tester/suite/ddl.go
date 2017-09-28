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
	"sync"

	"strconv"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/config"
	"github.com/twinj/uuid"
)

const (
	dmlOpUpdate        int = 1 << 0
	dmlOpDelete        int = 1 << 1
	dmlOpMax           int = dmlOpUpdate | dmlOpDelete
	ddlOpAddDropIndex  int = 1 << 0
	ddlOpAddDropColumn int = 1 << 1
	ddlOpRenameColumn  int = 1 << 2
	ddlOpMax           int = ddlOpAddDropIndex | ddlOpAddDropColumn | ddlOpRenameColumn
)

// DDLCase performs DDL operations while running DML operations.
type DDLCase struct {
	cfg *config.DDLCaseConfig
}

type ddlTestCaseContext struct {
	c                   *DDLCase
	db                  *sql.DB
	ddlOpFlags          int
	dmlOpFlags          int
	currentTableID      string
	numberOfBaseColumns int
	numberOfAllColumns  int
	numberOfRows        int
	columnNames         []string
	columnIndexNames    []string
	columnDef           []string
	columnNamesLock     sync.RWMutex
	values              [][]int32
	valuesLock          sync.RWMutex
}

// NewDDLCase returns a DDLCase.
func NewDDLCase(cfg *config.Config) Case {
	b := &DDLCase{
		cfg: &cfg.Suite.DDL,
	}
	return b
}

// Initialize does nothing currently
func (c *DDLCase) Initialize(ctx context.Context, db *sql.DB) error {
	return nil
}

// Execute implements Case Execute interface.
func (c *DDLCase) Execute(db *sql.DB, index int) error {
	for dmlOp := 0; dmlOp <= dmlOpMax; dmlOp++ {
		for ddlOp := 0; ddlOp <= ddlOpMax; ddlOp++ {
			err := c.testDDL(db, dmlOp, ddlOp)
			if err != nil {
				ddlFailedCounter.Inc()
				log.Errorf("[ddl] ERROR: %s", errors.ErrorStack(err))
				return nil
			}
		}
	}
	log.Infof("[ddl] Round compeleted")
	return nil
}

func (c *DDLCase) testDDL(db *sql.DB, dmlOpFlags int, ddlOpFlags int) error {
	ctx := &ddlTestCaseContext{
		c:          c,
		db:         db,
		ddlOpFlags: ddlOpFlags,
		dmlOpFlags: dmlOpFlags,
	}

	ctx.currentTableID = uuid.NewV4().String()

	// Verify table not exist.
	_, err := db.Exec(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", ctx.currentTableID))
	if err == nil {
		return errors.Trace(fmt.Errorf("Expect table does not exist error, but no errors are thrown"))
	}

	// Generate column names
	ctx.numberOfBaseColumns = c.cfg.InitialColumns/2 + rand.Intn(c.cfg.InitialColumns/2) + 1
	ctx.numberOfAllColumns = ctx.numberOfBaseColumns
	for colIdx := 0; colIdx < ctx.numberOfBaseColumns; colIdx++ {
		ctx.columnNames = append(ctx.columnNames, uuid.NewV4().String())
		ctx.columnIndexNames = append(ctx.columnIndexNames, "")
		ctx.columnDef = append(ctx.columnDef, "int")
	}

	// Create table.
	err = createTable(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// Rename table.
	err = renameTableAndVerify(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = parallel(func() error {
		// Add and drop index.
		if ddlOpFlags&ddlOpAddDropIndex > 0 {
			err = addAndDropIndex(ctx)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}, func() error {
		// Insert data.
		err = insertTable(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}, func() error {
		// Rename column.
		if ddlOpFlags&ddlOpRenameColumn > 0 {
			err = renameColumnWithProbability(ctx)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	// Rename table.
	err = renameTableAndVerify(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// Add and drop extra column
	if ddlOpFlags&ddlOpAddDropColumn > 0 {
		err = addAndDropColumn(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}

	err = parallel(func() error {
		// Add and drop index.
		if ddlOpFlags&ddlOpAddDropIndex > 0 {
			err = addAndDropIndex(ctx)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}, func() error {
		// Verify.
		err = verifyDataEquality(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}, func() error {
		// Rename column.
		if ddlOpFlags&ddlOpRenameColumn > 0 {
			err = renameColumnWithProbability(ctx)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	err = parallel(func() error {
		// Add and drop index.
		if ddlOpFlags&ddlOpAddDropIndex > 0 {
			err = addAndDropIndex(ctx)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}, func() error {
		// Update data.
		if dmlOpFlags&dmlOpUpdate > 0 {
			err = updateTable(ctx)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}, func() error {
		// Add and drop extra column
		if ddlOpFlags&ddlOpAddDropColumn > 0 {
			err = addAndDropColumn(ctx)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	// Rename table.
	err = renameTableAndVerify(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = parallel(func() error {
		// Verify.
		err = verifyDataEquality(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}, func() error {
		// Rename column.
		if ddlOpFlags&ddlOpRenameColumn > 0 {
			err = renameColumnWithProbability(ctx)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	err = parallel(func() error {
		// Add and drop index.
		if ddlOpFlags&ddlOpAddDropIndex > 0 {
			err = addAndDropIndex(ctx)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}, func() error {
		// Delete data.
		if dmlOpFlags&dmlOpDelete > 0 {
			err = deleteTable(ctx)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}, func() error {
		// Add and drop extra column
		if ddlOpFlags&ddlOpAddDropColumn > 0 {
			err = addAndDropColumn(ctx)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	// Rename table.
	err = renameTableAndVerify(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// Verify.
	err = verifyDataEquality(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = parallel(func() error {
		// Add and drop index.
		if ddlOpFlags&ddlOpAddDropIndex > 0 {
			err = addAndDropIndex(ctx)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}, func() error {
		return nil
		// Truncate table.
		// err = truncateTable(ctx)
		// if err != nil {
		// 	return errors.Trace(err)
		// }
		// return nil
	}, func() error {
		// Add and drop extra column
		if ddlOpFlags&ddlOpAddDropColumn > 0 {
			err = addAndDropColumn(ctx)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	// Verify truncation.
	// rowsCount := -1
	// err = db.QueryRow(fmt.Sprintf("SELECT count(*) FROM `%s`", ctx.currentTableID)).Scan(&rowsCount)
	// if err != nil {
	// 	return errors.Trace(err)
	// }
	// if rowsCount != 0 {
	// 	return errors.Trace(fmt.Errorf("Unexpected rows count, expect %d rows, got %d rows", 0, rowsCount))
	// }

	// Rename table.
	err = renameTableAndVerify(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// Drop table.
	_, err = db.Exec(fmt.Sprintf("DROP TABLE `%s`", ctx.currentTableID))
	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("[ddl] Dropped table `%s`", ctx.currentTableID)

	// Verify drop.
	_, err = db.Exec(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", ctx.currentTableID))
	if err == nil {
		return errors.Trace(fmt.Errorf("Expect table does not exist error, but no errors are thrown"))
	}
	return nil
}

// createTable creates a new table.
func createTable(ctx *ddlTestCaseContext) error {
	ctx.columnNamesLock.RLock()
	sql := fmt.Sprintf("CREATE TABLE `%s` (", ctx.currentTableID)
	for colIdx := 0; colIdx < ctx.numberOfAllColumns; colIdx++ {
		if colIdx > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("`%s` %s", ctx.columnNames[colIdx], ctx.columnDef[colIdx])
	}
	sql += ")"
	ctx.columnNamesLock.RUnlock()

	_, err := ctx.db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("[ddl] Created table `%s`", ctx.currentTableID)
	return nil
}

// truncateTable truncates a table.
func truncateTable(ctx *ddlTestCaseContext) error {
	_, err := ctx.db.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", ctx.currentTableID))
	if err != nil {
		return errors.Trace(err)
	}
	ctx.valuesLock.Lock()
	ctx.values = ctx.values[:0]
	ctx.numberOfRows = 0
	ctx.valuesLock.Unlock()

	log.Infof("[ddl] Truncated table `%s`", ctx.currentTableID)
	return nil
}

// renameTable renames a table.
func renameTable(ctx *ddlTestCaseContext) (string, error) {
	oldTableID := ctx.currentTableID
	ctx.currentTableID = uuid.NewV4().String()

	strategy := rand.Intn(2)
	sqlStatement := ""
	if strategy == 0 {
		sqlStatement = fmt.Sprintf("RENAME TABLE `%s` TO `%s`", oldTableID, ctx.currentTableID)
	} else if strategy == 1 {
		sqlStatement = fmt.Sprintf("ALTER TABLE `%s` RENAME `%s`", oldTableID, ctx.currentTableID)
	}

	// Perform rename.
	_, err := ctx.db.Exec(sqlStatement)
	if err != nil {
		return "", errors.Trace(err)
	}

	log.Infof("[ddl] Renamed table from `%s` to `%s`", oldTableID, ctx.currentTableID)
	return oldTableID, nil
}

// renameTableAndVerify renames a table and verify whether the old table is not accessible anymore
// and whether the new table is accessible.
func renameTableAndVerify(ctx *ddlTestCaseContext) error {
	oldTableID, err := renameTable(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// Verify old table does not exist.
	_, err = ctx.db.Exec(fmt.Sprintf("SELECT * FROM `%s`", oldTableID))
	if err == nil {
		return errors.Trace(fmt.Errorf("Expect table does not exist error, but no errors are thrown"))
	}

	// Verify new table exists.
	_, err = ctx.db.Exec(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", ctx.currentTableID))
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// insertTable inserts data into the specified table.
func insertTable(ctx *ddlTestCaseContext) error {
	rowsToInsert := int(ctx.c.cfg.InsertRows)/2 + int(rand.Int31n(int32(ctx.c.cfg.InsertRows)/2)) + 1

	for r := 0; r < rowsToInsert; r++ {
		row := []int32{}
		insertStatement := fmt.Sprintf("INSERT INTO `%s` VALUES (", ctx.currentTableID)
		for colIdx := 0; colIdx < ctx.numberOfBaseColumns; colIdx++ {
			row = append(row, rand.Int31())
			if colIdx > 0 {
				insertStatement += ", "
			}
			insertStatement += fmt.Sprintf("%d", row[colIdx])
		}
		insertStatement += ")"
		ctx.valuesLock.Lock()
		ctx.values = append(ctx.values, row)
		ctx.numberOfRows++
		ctx.valuesLock.Unlock()

		_, err := ctx.db.Exec(insertStatement)
		if err != nil {
			return errors.Trace(err)
		}
	}

	log.Infof("[ddl] Inserted %d records into `%s`", rowsToInsert, ctx.currentTableID)
	return nil
}

// updateTable updates data in the specified table.
func updateTable(ctx *ddlTestCaseContext) error {
	rowsToUpdate := int(rand.Int31n(int32(float32(ctx.numberOfRows) * ctx.c.cfg.UpdateRatio)))

	for i := 0; i < rowsToUpdate; i++ {
		ctx.columnNamesLock.RLock()
		ctx.valuesLock.Lock()
		whereCol := rand.Intn(ctx.numberOfBaseColumns)
		updateCol := rand.Intn(ctx.numberOfBaseColumns)
		rowIdx := rand.Int31n(int32(ctx.numberOfRows))
		newValue := rand.Int31()
		sql := fmt.Sprintf("UPDATE `%s` SET `%s` = %d WHERE `%s` = %d",
			ctx.currentTableID,
			ctx.columnNames[updateCol],   // update col name
			newValue,                     // update col value
			ctx.columnNames[whereCol],    // where col name
			ctx.values[rowIdx][whereCol]) // where col value
		ctx.values[rowIdx][updateCol] = newValue
		ctx.valuesLock.Unlock()
		ctx.columnNamesLock.RUnlock()

		_, err := ctx.db.Exec(sql)
		if err != nil {
			return errors.Trace(err)
		}
	}

	log.Infof("[ddl] Updated %d records in `%s`", rowsToUpdate, ctx.currentTableID)
	return nil
}

// deleteTable deletes data from the specified table.
func deleteTable(ctx *ddlTestCaseContext) error {
	rowsToDelete := int(rand.Int31n(int32(float32(ctx.numberOfRows) * ctx.c.cfg.DeleteRatio)))

	for i := 0; i < rowsToDelete; i++ {
		ctx.columnNamesLock.RLock()
		ctx.valuesLock.Lock()
		whereCol := rand.Intn(ctx.numberOfBaseColumns)
		rowIdx := rand.Int31n(int32(ctx.numberOfRows))
		sql := fmt.Sprintf("DELETE FROM `%s` WHERE `%s` = %d",
			ctx.currentTableID,
			ctx.columnNames[whereCol],    // where col name
			ctx.values[rowIdx][whereCol]) // where col value
		ctx.values = append(ctx.values[:rowIdx], ctx.values[rowIdx+1:]...)
		ctx.numberOfRows--
		ctx.valuesLock.Unlock()
		ctx.columnNamesLock.RUnlock()

		_, err := ctx.db.Exec(sql)
		if err != nil {
			return errors.Trace(err)
		}
	}

	log.Infof("[ddl] Deleted %d records in `%s`", rowsToDelete, ctx.currentTableID)
	return nil
}

// verifyDataEquality checks whether all data in the specified table matches the given 2d-array exactly.
func verifyDataEquality(ctx *ddlTestCaseContext) error {
	rows, err := ctx.db.Query(fmt.Sprintf("SELECT * FROM `%s`", ctx.currentTableID))
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	// Read all rows.
	var actualRows [][]int32
	for rows.Next() {
		cols, err1 := rows.Columns()
		if err1 != nil {
			return errors.Trace(err)
		}

		// See https://stackoverflow.com/questions/14477941/read-select-columns-into-string-in-go .
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
				result[i] = -1
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

	ctx.valuesLock.RLock()
	defer ctx.valuesLock.RUnlock()

	if len(actualRows) != ctx.numberOfRows {
		return errors.Trace(fmt.Errorf("Unexpected rows count, expect %d rows, got %d rows", ctx.numberOfRows, len(actualRows)))
	}

	// Compare each row.
	for rowIdx, row := range actualRows {
		if len(row) != len(ctx.values[rowIdx]) {
			return errors.Trace(fmt.Errorf("Unexpected columns count, expect %d columns, got %d columns", len(ctx.values[rowIdx]), len(row)))
		}
		for colIdx, col := range row {
			if col != ctx.values[rowIdx][colIdx] {
				return errors.Trace(fmt.Errorf("Unexpected record value at (row=%d, col=%d), expect %d, got %d", rowIdx, colIdx, ctx.values[rowIdx][colIdx], col))
			}
		}
	}

	return nil
}

// addIndexWithProbability adds index to one of the column randomly with the probability in config.
func addIndexWithProbability(ctx *ddlTestCaseContext) error {
	if rand.Float32() >= ctx.c.cfg.AddIndexProbability {
		return nil
	}

	ctx.columnNamesLock.Lock()
	columnToAddIndex := rand.Intn(ctx.numberOfBaseColumns)
	if len(ctx.columnIndexNames[columnToAddIndex]) > 0 {
		ctx.columnNamesLock.Unlock()
		return nil
	}
	indexName := uuid.NewV4().String()
	columnName := ctx.columnNames[columnToAddIndex]
	sql := fmt.Sprintf("ALTER TABLE `%s` ADD INDEX `%s` (`%s`)",
		ctx.currentTableID,
		indexName,
		columnName)
	ctx.columnIndexNames[columnToAddIndex] = indexName
	_, err := ctx.db.Exec(sql)
	ctx.columnNamesLock.Unlock()

	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("[ddl] Added index `%s` at column `%s` for table `%s`", indexName, columnName, ctx.currentTableID)
	return nil
}

// dropIndexWithProbability drops index from one of the column (which has index) randomly with the probability in config.
func dropIndexWithProbability(ctx *ddlTestCaseContext) error {
	if rand.Float32() >= ctx.c.cfg.DropIndexProbability {
		return nil
	}

	ctx.columnNamesLock.Lock()
	indexColumns := []int{}
	for idx, indexName := range ctx.columnIndexNames {
		if len(indexName) > 0 {
			indexColumns = append(indexColumns, idx)
		}
	}
	if len(indexColumns) == 0 {
		ctx.columnNamesLock.Unlock()
		return nil
	}
	columnToDropIndex := indexColumns[rand.Intn(len(indexColumns))]
	indexName := ctx.columnIndexNames[columnToDropIndex]
	columnName := ctx.columnNames[columnToDropIndex]
	sql := fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`",
		ctx.currentTableID,
		indexName)
	ctx.columnIndexNames[columnToDropIndex] = ""
	_, err := ctx.db.Exec(sql)
	ctx.columnNamesLock.Unlock()

	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("[ddl] Dropped index `%s` at column `%s` for table `%s`", indexName, columnName, ctx.currentTableID)
	return nil
}

func addAndDropIndex(ctx *ddlTestCaseContext) error {
	err := addIndexWithProbability(ctx)
	if err != nil {
		return err
	}
	err = dropIndexWithProbability(ctx)
	if err != nil {
		return err
	}
	return nil
}

// addColumnWithProbability adds an extra column randomly with the probability in config.
func addColumnWithProbability(ctx *ddlTestCaseContext) error {
	if rand.Float32() >= ctx.c.cfg.AddColumnProbability {
		return nil
	}

	ctx.columnNamesLock.Lock()
	ctx.valuesLock.Lock()
	newColumnID := uuid.NewV4().String()
	newColumnDefaultValue := rand.Int31()
	newColumnDef := fmt.Sprintf("int NULL DEFAULT %d", newColumnDefaultValue)
	ctx.columnNames = append(ctx.columnNames, newColumnID)
	ctx.columnIndexNames = append(ctx.columnIndexNames, "")
	ctx.columnDef = append(ctx.columnDef, newColumnDef)
	ctx.numberOfAllColumns++
	for rowIdx, row := range ctx.values {
		ctx.values[rowIdx] = append(row, newColumnDefaultValue)
	}
	ctx.valuesLock.Unlock()
	ctx.columnNamesLock.Unlock()

	sql := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s",
		ctx.currentTableID,
		newColumnID,
		newColumnDef)

	_, err := ctx.db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("[ddl] Added new column `%s` for table `%s`", newColumnID, ctx.currentTableID)
	return nil
}

// dropColumnWithProbability drops an extra column randomly with the probability in config.
func dropColumnWithProbability(ctx *ddlTestCaseContext) error {
	if rand.Float32() >= ctx.c.cfg.DropColumnProbability {
		return nil
	}

	ctx.columnNamesLock.Lock()
	if ctx.numberOfAllColumns == ctx.numberOfBaseColumns {
		ctx.columnNamesLock.Unlock()
		return nil
	}
	ctx.valuesLock.Lock()
	columnIdxToDrop := ctx.numberOfBaseColumns + rand.Intn(ctx.numberOfAllColumns-ctx.numberOfBaseColumns)
	columnName := ctx.columnNames[columnIdxToDrop]
	ctx.columnNames = append(ctx.columnNames[:columnIdxToDrop], ctx.columnNames[columnIdxToDrop+1:]...)
	ctx.columnIndexNames = append(ctx.columnIndexNames[:columnIdxToDrop], ctx.columnIndexNames[columnIdxToDrop+1:]...)
	ctx.columnDef = append(ctx.columnDef[:columnIdxToDrop], ctx.columnDef[columnIdxToDrop+1:]...)
	ctx.numberOfAllColumns--
	for rowIdx, row := range ctx.values {
		ctx.values[rowIdx] = append(row[:columnIdxToDrop], row[columnIdxToDrop+1:]...)
	}
	ctx.valuesLock.Unlock()
	ctx.columnNamesLock.Unlock()

	sql := fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `%s`",
		ctx.currentTableID,
		columnName)

	_, err := ctx.db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("[ddl] Dropped column `%s` from table `%s`", columnName, ctx.currentTableID)
	return nil
}

// renameColumnWithProbability renames a column randomly with the probability in config.
// This operation cannot run in parallel with update, delete and add/drop column.
func renameColumnWithProbability(ctx *ddlTestCaseContext) error {
	if rand.Float32() >= ctx.c.cfg.RenameColumnProbability {
		return nil
	}

	ctx.columnNamesLock.Lock()
	defer ctx.columnNamesLock.Unlock()

	columnIdxToRename := rand.Intn(ctx.numberOfAllColumns)
	columnDef := ctx.columnDef[columnIdxToRename]
	oldColumnID := ctx.columnNames[columnIdxToRename]
	newColumnID := uuid.NewV4().String()
	ctx.columnNames[columnIdxToRename] = newColumnID

	sql := fmt.Sprintf("ALTER TABLE `%s` CHANGE `%s` `%s` %s",
		ctx.currentTableID,
		oldColumnID,
		newColumnID,
		columnDef)

	_, err := ctx.db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("[ddl] Renamed column from `%s` to `%s` in table `%s`", oldColumnID, newColumnID, ctx.currentTableID)
	return nil
}

func addAndDropColumn(ctx *ddlTestCaseContext) error {
	err := addColumnWithProbability(ctx)
	if err != nil {
		return err
	}
	err = dropColumnWithProbability(ctx)
	if err != nil {
		return err
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

func (c *DDLCase) String() string {
	return "ddl"
}

func init() {
	RegisterSuite("ddl", NewDDLCase)
}
