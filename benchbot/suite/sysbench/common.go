package sysbench

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/ngaut/log"

	. "github.com/pingcap/octopus/benchbot/suite"
)

const (
	sysbenchColumnCSize   = 120
	sysbenchColumnPadSize = 60
	sysbenchTableName     = "sbtest"
	sysbenchTableSchema   = `(
        id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
        k INTEGER UNSIGNED DEFAULT '0' NOT NULL,
        c CHAR(120) DEFAULT '' NOT NULL,
        pad CHAR(60) DEFAULT '' NOT NULL,
        PRIMARY KEY (id)
    )`
)

func prepare(db *sql.DB, tableSize, numTables, numThreads int) ([]string, error) {
	tables := make([]string, 0, numTables)
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("%s_%d", sysbenchTableName, i)
		if err := CreateTable(db, tableName, sysbenchTableSchema); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	wg := sync.WaitGroup{}
	avgRecords := int(math.Ceil(float64(tableSize) / float64(numThreads)))
	for id, records := 0, 0; records < tableSize; id++ {
		wg.Add(1)
		endRecords := records + avgRecords
		endRecords = int(math.Min(float64(endRecords), float64(tableSize)))

		go func(id, records int) {
			worker := newPrepareWorker(id, db, tables, records)
			if err := worker.run(); err != nil {
				log.Errorf("run prepare worker %d error: %s", id, err)
			}
			wg.Done()
		}(id, endRecords-records)

		records = endRecords
	}
	wg.Wait()

	return tables, nil
}

type prepareWorker struct {
	id        int
	db        *sql.DB
	tables    []string
	tableSize int
}

func newPrepareWorker(id int, db *sql.DB, tables []string, tableSize int) *prepareWorker {
	return &prepareWorker{
		id:        id,
		db:        db,
		tables:    tables,
		tableSize: tableSize,
	}
}

func (w *prepareWorker) run() error {
	rander := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
	for _, table := range w.tables {
		stmt := fmt.Sprintf("INSERT INTO `%s` (`k`, `c`, `pad`) VALUES (?, ?, ?)", table)
		for i := 0; i < w.tableSize; i++ {
			columnK := rander.Intn(w.tableSize)
			columnC := RandomAsciiBytes(rander, sysbenchColumnCSize)
			columnPad := RandomAsciiBytes(rander, sysbenchColumnPadSize)
			if _, err := w.db.Exec(stmt, columnK, columnC, columnPad); err != nil {
				return err
			}
		}
	}
	return nil
}
