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
	"sync/atomic"

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
	ddlOpModifyColumn  int = 1 << 3
	ddlOpMax           int = ddlOpAddDropIndex | ddlOpAddDropColumn | ddlOpRenameColumn | ddlOpModifyColumn
)

// DDLCase performs DDL operations while running DML operations.
type DDLCase struct {
	cfg        *config.DDLCaseConfig
	insertRows int
	wg         sync.WaitGroup
	stop       int32
}

// NewDDLCase returns a DDLCase.
func NewDDLCase(cfg *config.Config) Case {
	b := &DDLCase{
		cfg:        &cfg.Suite.DDL,
		insertRows: cfg.Suite.DDL.InsertRows,
	}
	return b
}

// Initialize does nothing currently
func (c *DDLCase) Initialize(ctx context.Context, db *sql.DB) error {
	return nil
}

// Execute implements Case Execute interface.
func (c *DDLCase) Execute(db *sql.DB, index int) error {
	if atomic.LoadInt32(&c.stop) != 0 {
		return errors.New("ddl stopped")
	}
	c.wg.Add(1)
	c.createRenameTruncateDropTable(db)
	c.wg.Done()
	return nil
}

// renameTableAndVerify renames a table and verify whether the old table is not accessible anymore
// and whether the new table is accessible.
func (c *DDLCase) renameTableAndVerify(db *sql.DB, tableID string) (newTableID string, err error) {
	oldTableID := tableID

	for strategy := 0; strategy < 2; strategy++ {
		newTableID = uuid.NewV4().String()

		sqlStatement := ""
		if strategy == 0 {
			sqlStatement = fmt.Sprintf("RENAME TABLE `%s` TO `%s`", oldTableID, newTableID)
		} else if strategy == 1 {
			sqlStatement = fmt.Sprintf("ALTER TABLE `%s` RENAME `%s`", oldTableID, newTableID)
		}

		// Perform rename.
		_, err = db.Exec(sqlStatement)
		if err != nil {
			err = errors.Trace(err)
			return
		}

		log.Infof("[ddl] Renamed table from `%s` to `%s`", oldTableID, newTableID)

		// Verify old table does not exist.
		_, err = db.Query(fmt.Sprintf("SELECT * FROM `%s`", oldTableID))
		if err == nil {
			err = errors.Trace(fmt.Errorf("Expect table does not exist error, but no errors are thrown"))
			return
		}

		// Verify new table exists.
		_, err = db.Query(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", newTableID))
		if err != nil {
			err = errors.Trace(err)
			return
		}

		oldTableID = newTableID
	}

	err = nil
	return
}

// verifyDataEquality checks whether all data in a table matches the given 2d-array exactly.
func (c *DDLCase) verifyDataEquality(db *sql.DB, tableID string, expectedRows [][]int32) error {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s`", tableID))
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

	if len(actualRows) != len(expectedRows) {
		return errors.Trace(fmt.Errorf("Unexpected rows count, expect %d rows, got %d rows", len(expectedRows), len(actualRows)))
	}

	// Compare each row.
	for rowIdx, row := range actualRows {
		if len(row) != len(expectedRows[rowIdx]) {
			return errors.Trace(fmt.Errorf("Unexpected columns count, expect %d columns, got %d columns", len(expectedRows[rowIdx]), len(row)))
		}
		for colIdx, col := range row {
			if col != expectedRows[rowIdx][colIdx] {
				return errors.Trace(fmt.Errorf("Unexpected record value at (row=%d, col=%d), expect %d, got %d", rowIdx, colIdx, expectedRows[rowIdx][colIdx], col))
			}
		}
	}

	return nil
}

// addIndexWithProbability adds index to one of the column randomly with the probability in config.
func (c *DDLCase) addIndexWithProbability(db *sql.DB, tableID string, columnNames *[]string, columnIndexNames *[]string) error {
	if rand.Float32() >= c.cfg.AddIndexProbability {
		return nil
	}
	columnToAddIndex := rand.Intn(len(*columnNames))
	if len((*columnIndexNames)[columnToAddIndex]) > 0 {
		return nil
	}
	indexName := uuid.NewV4().String()
	_, err := db.Query(fmt.Sprintf("ALTER TABLE `%s` ADD INDEX `%s` (`%s`)",
		tableID,
		indexName,
		(*columnNames)[columnToAddIndex]))
	if err != nil {
		return errors.Trace(err)
	}
	(*columnIndexNames)[columnToAddIndex] = indexName
	log.Infof("[ddl] Added index %s at column %s for table %s", indexName, (*columnNames)[columnToAddIndex], tableID)
	return nil
}

// dropIndexWithProbability drops index from one of the column (which has index) randomly with the probability in config.
func (c *DDLCase) dropIndexWithProbability(db *sql.DB, tableID string, columnNames *[]string, columnIndexNames *[]string) error {
	if rand.Float32() >= c.cfg.DropIndexProbability {
		return nil
	}
	indexColumns := []int{}
	for idx, indexName := range *columnIndexNames {
		if len(indexName) > 0 {
			indexColumns = append(indexColumns, idx)
		}
	}
	if len(indexColumns) == 0 {
		return nil
	}
	columnToDropIndex := indexColumns[rand.Intn(len(indexColumns))]
	indexName := (*columnIndexNames)[columnToDropIndex]
	_, err := db.Query(fmt.Sprintf("ALTER TABLE `%s` DROP INDEX `%s`",
		tableID,
		indexName))
	if err != nil {
		return errors.Trace(err)
	}
	(*columnIndexNames)[columnToDropIndex] = ""
	log.Infof("[ddl] Dropped index %s at column %s for table %s", indexName, (*columnNames)[columnToDropIndex], tableID)
	return nil
}

func (c *DDLCase) addAndDropIndex(db *sql.DB, tableID string, columnNames *[]string, columnIndexNames *[]string) error {
	err := c.addIndexWithProbability(db, tableID, columnNames, columnIndexNames)
	if err != nil {
		return err
	}
	err = c.dropIndexWithProbability(db, tableID, columnNames, columnIndexNames)
	if err != nil {
		return err
	}
	return nil
}

func (c *DDLCase) createRenameTruncateDropTable(db *sql.DB) {
	if err := func() error {
		for dmlOp := 0; dmlOp <= dmlOpMax; dmlOp++ {
			for ddlOp := 0; ddlOp <= ddlOpMax; ddlOp++ {
				tableID := uuid.NewV4().String()

				// Verify table not exist.
				_, err := db.Query(fmt.Sprintf("SELECT * FROM `%s`", tableID))
				if err == nil {
					return errors.Trace(fmt.Errorf("Expect table does not exist error, but no errors are thrown"))
				}

				// Generate column names
				columns := c.cfg.InitialColumns/2 + rand.Intn(c.cfg.InitialColumns/2) + 1
				columnNames := []string{}
				columnIndexNames := []string{}
				for colIdx := 0; colIdx < columns; colIdx++ {
					columnNames = append(columnNames, uuid.NewV4().String())
					columnIndexNames = append(columnIndexNames, "")
				}

				// Create table.
				createTableStatement := fmt.Sprintf("CREATE TABLE `%s` (", tableID)
				for colIdx := 0; colIdx < columns; colIdx++ {
					if colIdx > 0 {
						createTableStatement += ", "
					}
					createTableStatement += fmt.Sprintf("`%s` int", columnNames[colIdx])
				}
				createTableStatement += ")"
				_, err = db.Exec(createTableStatement)
				if err != nil {
					return errors.Trace(err)
				}

				// Add and drop index.
				if ddlOp&ddlOpAddDropIndex > 0 {
					err = c.addAndDropIndex(db, tableID, &columnNames, &columnIndexNames)
					if err != nil {
						return errors.Trace(err)
					}
				}

				log.Infof("[ddl] Created table `%s`", tableID)

				// Rename table.
				tableID, err = c.renameTableAndVerify(db, tableID)
				if err != nil {
					return errors.Trace(err)
				}

				// Insert data.
				var values [][]int32
				realInsertRows := int(c.cfg.InsertRows)/2 + int(rand.Int31n(int32(c.cfg.InsertRows)/2)) + 1
				for r := 0; r < realInsertRows; r++ {
					row := []int32{}
					insertStatement := fmt.Sprintf("INSERT INTO `%s` VALUES (", tableID)
					for colIdx := 0; colIdx < columns; colIdx++ {
						row = append(row, rand.Int31())
						if colIdx > 0 {
							insertStatement += ", "
						}
						insertStatement += fmt.Sprintf("%d", row[colIdx])
					}
					insertStatement += ")"
					values = append(values, row)
					_, err := db.Exec(insertStatement)
					if err != nil {
						return errors.Trace(err)
					}
				}

				log.Infof("[ddl] Inserted %d records into `%s`", realInsertRows, tableID)

				// Add and drop index.
				if ddlOp&ddlOpAddDropIndex > 0 {
					err = c.addAndDropIndex(db, tableID, &columnNames, &columnIndexNames)
					if err != nil {
						return errors.Trace(err)
					}
				}

				// Rename table.
				tableID, err = c.renameTableAndVerify(db, tableID)
				if err != nil {
					return errors.Trace(err)
				}

				// Verify.
				err = c.verifyDataEquality(db, tableID, values)
				if err != nil {
					return errors.Trace(err)
				}

				// Update data.
				if dmlOp&dmlOpUpdate > 0 {
					for whereCol := 0; whereCol < len(columnNames); whereCol++ {
						for updateCol := 0; updateCol < len(columnNames); updateCol++ {
							rowsToUpdate := int(rand.Int31n(int32(float32(realInsertRows) / float32(len(columnNames)) * c.cfg.UpdateRatio)))
							for i := 0; i < rowsToUpdate; i++ {
								rowIdx := rand.Int31n(int32(len(values)))
								values[rowIdx][updateCol] = rand.Int31()
								_, err := db.Exec(fmt.Sprintf("UPDATE `%s` SET `%s` = %d WHERE `%s` = %d",
									tableID,
									columnNames[updateCol],    // update col name
									values[rowIdx][updateCol], // update col value
									columnNames[whereCol],     // where col name
									values[rowIdx][whereCol])) // where col value
								if err != nil {
									return errors.Trace(err)
								}
							}
							log.Infof("[ddl] Updated %d records in `%s`", rowsToUpdate, tableID)

							// Rename table.
							tableID, err = c.renameTableAndVerify(db, tableID)
							if err != nil {
								return errors.Trace(err)
							}

							// Verify.
							err = c.verifyDataEquality(db, tableID, values)
							if err != nil {
								return errors.Trace(err)
							}
						}
					}
				}

				// Add and drop index.
				if ddlOp&ddlOpAddDropIndex > 0 {
					err = c.addAndDropIndex(db, tableID, &columnNames, &columnIndexNames)
					if err != nil {
						return errors.Trace(err)
					}
				}

				// Delete data.
				if dmlOp&dmlOpDelete > 0 {
					for whereCol := 0; whereCol < len(columnNames); whereCol++ {
						rowsToDelete := int(rand.Int31n(int32(float32(realInsertRows) / float32(len(columnNames)) * c.cfg.DeleteRatio)))
						for i := 0; i < rowsToDelete; i++ {
							rowIdx := rand.Int31n(int32(len(values)))
							_, err := db.Exec(fmt.Sprintf("DELETE FROM `%s` WHERE `%s` = %d",
								tableID,
								columnNames[whereCol],     // where col name
								values[rowIdx][whereCol])) // where col value
							if err != nil {
								return errors.Trace(err)
							}
							values = append(values[:rowIdx], values[rowIdx+1:]...)
						}
						log.Infof("[ddl] Deleted %d records in `%s`", rowsToDelete, tableID)

						// Rename table.
						tableID, err = c.renameTableAndVerify(db, tableID)
						if err != nil {
							return errors.Trace(err)
						}

						// Verify.
						err = c.verifyDataEquality(db, tableID, values)
						if err != nil {
							return errors.Trace(err)
						}
					}
				}

				// Add and drop index.
				if ddlOp&ddlOpAddDropIndex > 0 {
					err = c.addAndDropIndex(db, tableID, &columnNames, &columnIndexNames)
					if err != nil {
						return errors.Trace(err)
					}
				}

				// Rename table.
				tableID, err = c.renameTableAndVerify(db, tableID)
				if err != nil {
					return errors.Trace(err)
				}

				// Truncate table.
				_, err = db.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", tableID))
				if err != nil {
					return errors.Trace(err)
				}
				log.Infof("[ddl] Truncated table `%s`", tableID)

				// Add and drop index.
				if ddlOp&ddlOpAddDropIndex > 0 {
					err = c.addAndDropIndex(db, tableID, &columnNames, &columnIndexNames)
					if err != nil {
						return errors.Trace(err)
					}
				}

				// Verify truncation.
				rowsCount := -1
				err = db.QueryRow(fmt.Sprintf("SELECT count(*) FROM `%s`", tableID)).Scan(&rowsCount)
				if err != nil {
					return errors.Trace(err)
				}
				if rowsCount != 0 {
					return errors.Trace(fmt.Errorf("Unexpected rows count, expect %d rows, got %d rows", 0, rowsCount))
				}

				// Rename table.
				tableID, err = c.renameTableAndVerify(db, tableID)
				if err != nil {
					return errors.Trace(err)
				}

				// Drop table.
				_, err = db.Exec(fmt.Sprintf("DROP TABLE `%s`", tableID))
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl] Dropped table `%s`", tableID)

				// Verify drop.
				_, err = db.Query(fmt.Sprintf("SELECT * FROM `%s`", tableID))
				if err == nil {
					return errors.Trace(fmt.Errorf("Expect table does not exist error, but no errors are thrown"))
				}

			}
		}
		return nil
	}(); err != nil {
		ddlFailedCounter.Inc()
		log.Errorf("[ddl] createRenameTruncateDropTable error: %s", errors.ErrorStack(err))
		return
	}
}

func (c *DDLCase) String() string {
	return "ddl"
}

func init() {
	RegisterSuite("ddl", NewDDLCase)
}
