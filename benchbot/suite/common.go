package suite

import (
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/ngaut/log"
	"golang.org/x/net/context"
)

const (
	asciiStart = int(' ')
	asciiEnd   = int('~')

	fillConcurrency   = 64
	fillStringLen     = 32
	defaultStringSize = 32

	defaultTableName   = "oct_bench"
	defaultTableSchema = `
    (id BIGINT DEFAULT '0' NOT NULL,
     seq INTEGER UNSIGNED DEFAULT '0' NOT NULL,
     name VARCHAR(128) DEFAULT '' NOT NULL,
     KEY idx_id(id),
     KEY idx_seq(seq)) ENGINE=InnoDB DEFAULT CHARSET=utf8`
)

func randomAsciiBytes(size int, r *rand.Rand) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(r.Intn(asciiEnd-asciiStart) + asciiStart)
	}
	return data
}

type dataFill struct {
	id       int
	db       *sql.DB
	rand     *rand.Rand
	startRow int
	endRow   int
	tables   []string
}

func newDataFill(id int, db *sql.DB, tables []string, startRow int, endRow int) *dataFill {
	source := rand.NewSource(int64(time.Now().UnixNano()))
	df := &dataFill{
		id:       id,
		db:       db,
		rand:     rand.New(source),
		tables:   tables,
		startRow: startRow,
		endRow:   endRow,
	}
	return df
}

func (df *dataFill) run() error {
	for _, table := range df.tables {
		// log.Infof("[FILL-%d] filling '%s' (%d, %d) ...", df.id, table, df.startRow, df.endRow)
		if err := df.fill(table); err != nil {
			return err
		}
	}
	return nil
}

func (df *dataFill) fill(table string) (err error) {
	var uid int64
	var name []byte

	rander := df.rand
	db := df.db

	query := fmt.Sprintf("INSERT INTO `%s` (`id`, `seq`, `name`) VALUES (?, ?, ?)", table)
	for r := df.startRow; r < df.endRow; r++ {
		uid = rander.Int63()
		name = randomAsciiBytes(fillStringLen, df.rand)

		_, err = db.Exec(query, uid, r, name)
		if err != nil {
			return
		}
	}

	return
}

func createTable(db *sql.DB, table string) error {
	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` %s", table, defaultTableSchema)); err != nil {
		return err
	}
	return nil
}

func dropTable(db *sql.DB, table string) error {
	if _, err := db.Exec(fmt.Sprintf("DROP TABLE %s", table)); err != nil {
		return err
	}
	return nil
}

func prepare(db *sql.DB, tableCount int, tableRows int) ([]string, error) {
	// TODO :
	//     pass config as arg to define to specify the execution, like `concurrency`. etc.
	// db.SetMaxOpenConns(fillConcurrency)

	log.Infof("create tables ... (%d)", tableCount)

	tables := make([]string, 0, tableCount)
	for i := 0; i < tableCount; i++ {
		tableName := fmt.Sprintf("%s_%d", defaultTableName, i)
		if err := createTable(db, tableName); err != nil {
			return tables, err
		}
		tables = append(tables, tableName)
	}

	log.Infof("fill tables ... (%d x %d)", tableCount, tableRows)

	groupRows := int(math.Ceil(
		float64(tableRows) / float64(fillConcurrency)))

	wg := sync.WaitGroup{}
	for group, start := 0, 0; group < fillConcurrency; group++ {
		end := start + groupRows
		if end > tableRows {
			end = tableRows
		}

		// groupTables := tables[len(tables)-num:]
		worker := newDataFill(group, db, tables, start, end)
		wg.Add(1)
		go func() {
			if err := worker.run(); err != nil {
				log.Errorf("worker prepare failed : %s", err.Error())
			}
			wg.Done()
		}()

		start = end
	}
	wg.Wait()

	sort.Strings(tables)
	return tables, nil
}

//
//
//

type execution func(context.Context, *StatDB, int, int)

func processStatistic(ctx context.Context, db *sql.DB, concurrency, requests int, exec execution) *StatResult {
	stdb := NewStatDB(db)
	{
		avgRequests := int(math.Floor(
			float64(requests) / float64(concurrency))) // ps : divde requests into parts equally

		wg := sync.WaitGroup{}
		for i, remaining := 0, requests; remaining > 0; i++ {
			wg.Add(1)

			reqNum := avgRequests
			if remaining < reqNum {
				reqNum = remaining
			}

			go func(id, requsts int) {
				exec(ctx, stdb, id, requsts)
				wg.Done()
			}(i, reqNum)

			remaining -= avgRequests
		}
		wg.Wait()
	}
	stdb.Close()

	return stdb.Stat()
}
