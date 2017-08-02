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
	asciiStart = int('a')
	asciiEnd   = int('z')

	defaultStringSize = 32

	defaultTableName   = "benchbot"
	defaultTableSchema = `(
        id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
        seq INTEGER UNSIGNED DEFAULT '0' NOT NULL,
        name VARCHAR(128) DEFAULT '' NOT NULL,
        PRIMARY KEY (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8`
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
	tables   []string
	requests int
}

func newDataFill(id int, db *sql.DB, tables []string, requests int) *dataFill {
	source := rand.NewSource(int64(time.Now().UnixNano()))
	df := &dataFill{
		id:       id,
		db:       db,
		rand:     rand.New(source),
		tables:   tables,
		requests: requests,
	}
	return df
}

func (df *dataFill) run() error {
	for _, table := range df.tables {
		if err := df.fill(table); err != nil {
			return err
		}
	}
	return nil
}

func (df *dataFill) fill(table string) error {
	query := fmt.Sprintf("INSERT INTO `%s` (`seq`, `name`) VALUES (1, ?)", table)
	for i := 0; i < df.requests; i++ {
		name := randomAsciiBytes(defaultStringSize, df.rand)
		if _, err := df.db.Exec(query, name); err != nil {
			return err
		}
	}
	return nil
}

func createTable(db *sql.DB, table, schema string) error {
	_, err := db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` %s", table, schema))
	return err
}

func dropTable(db *sql.DB, table string) error {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	return err
}

func prepare(db *sql.DB, tableSize, numTables, numThreads int) ([]string, error) {
	log.Infof("create tables ... (%d)", numTables)

	tables := make([]string, 0, numTables)
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("%s_%d", defaultTableName, i)
		if err := createTable(db, tableName, defaultTableSchema); err != nil {
			return tables, err
		}
		tables = append(tables, tableName)
	}

	log.Infof("fill tables ... (%d x %d)", numTables, tableSize)

	wg := sync.WaitGroup{}
	step := int(math.Ceil(float64(tableSize) / float64(numThreads)))
	for id, requests := 0, tableSize; requests > 0; id++ {
		wg.Add(1)
		go func(id, requests int) {
			worker := newDataFill(id, db, tables, requests)
			if err := worker.run(); err != nil {
				log.Errorf("failed to run worker: %s", err)
			}
			wg.Done()
		}(id, int(math.Min(float64(step), float64(requests))))
		requests -= step
	}
	wg.Wait()

	sort.Strings(tables)
	return tables, nil
}

type execution func(context.Context, *StatDB, int, int)

func processStatistic(exec execution, ctx context.Context, db *sql.DB, numThreads, numRequests int) *StatResult {
	stdb := NewStatDB(db)

	wg := sync.WaitGroup{}
	step := int(math.Ceil(float64(numRequests) / float64(numThreads)))
	for id, requests := 0, numRequests; requests > 0; id++ {
		wg.Add(1)
		go func(id, requests int) {
			exec(ctx, stdb, id, requests)
			wg.Done()
		}(id, int(math.Min(float64(step), float64(requests))))
		requests -= step
	}
	wg.Wait()

	stdb.Close()
	return stdb.Stat()
}
