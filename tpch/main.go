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
//
// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Arjun Narayan
//
// The TPC-H  program is intended to simulate the workload specified by
// the Transaction Processing Council Benchmark TPC-H

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var drop = flag.Bool("drop", false,
	"Drop the existing table and recreate it to start from scratch")
var load = flag.Bool("load", false, "Load data into the database from ")
var dataDir = flag.String("d", "./data/",
	"Source for data files to load from. Data must be generated using the DBGEN utility.")
var insertsPerTransaction = flag.Uint("inserts-per-tx", 100,
	"Number of inserts to batch into a single transaction when loading data")
var queries = flag.String("queries",
	"1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,19,20,21,22", "Queries to run. Use a comma separated list of query numbers.")
var loops = flag.Uint("loops", 1, "Number of times to run the queries (0 = run forever).")
var concurrency = flag.Uint("c", 1, "Number of queries to execute concurrently.")
var maxErrors = flag.Uint64("e", 1, "Number of query errors allowed before aborting (0 = unlimited).")

var user = flag.String("u", "root", "DB user.")
var password = flag.String("p", "", "DB password.")
var host = flag.String("h", "127.0.0.1", "DB host")
var port = flag.Uint("P", 4000, "DB port.")

// Flags for testing this load generator.
var insertLimit = flag.Uint("insert-limit", 0, "Limit number of rows to be inserted from each file "+
	"(0 = unlimited")

func loadFile(dbURL string, datafile string, t table) error {
	seps := strings.SplitN(dbURL, "://", 2)

	// Open connection to server
	db, err := sql.Open("mysql", seps[1])
	if err != nil {
		return err
	}

	filename := fmt.Sprintf("%s/%s", "data", datafile)
	if err := insertTableFromFile(db, filename, t); err != nil {
		return err
	}
	return nil
}

func setupDatabase(dbURL string) (*sql.DB, error) {
	seps := strings.SplitN(dbURL, "://", 2)

	// Open connection to server and create a database.
	db, err := sql.Open("mysql", seps[1])
	if err != nil {
		return nil, err
	}

	return db, nil
}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
	flag.PrintDefaults()
}

// loopQueries runs the given list of queries *loops times. id is used for
// logging. Query errors are atomically added to errorCount, resulting in a
// fatal error if we have more than *maxErrors errors.
func loopQueries(id uint, db *sql.DB, queries []int, wg *sync.WaitGroup, errorCount *uint64) {
	defer wg.Done()
	for i := uint(0); i < *loops || *loops == 0; i++ {
		for _, query := range queries {
			start := time.Now()
			numRows, err := runQuery(db, query)
			elapsed := time.Now().Sub(start)
			if err != nil {
				newErrorCount := atomic.AddUint64(errorCount, 1)
				if newErrorCount < *maxErrors || *maxErrors == 0 {
					log.Printf("[%d] error running query %d", id, query)
				} else {
					log.Fatalf("error running query %s", err)
				}
				continue
			}
			log.Printf("[%d] finished query %d: %d rows returned after %4.2f seconds\n",
				id, query, numRows, elapsed.Seconds())
		}
	}
}

func main() {
	flag.Usage = usage
	flag.Parse()
	dbURL := fmt.Sprintf("tidb://%s:%s@tcp(%s:%d)/tpch", *user, *password, *host, *port)
	if flag.NArg() == 1 {
		dbURL = flag.Arg(0)
	}

	db, err := setupDatabase(dbURL)

	if err != nil {
		fmt.Printf("Setting up database connection failed: %s, continuing assuming database already exists.", err)
	}

	if *drop {
		log.Printf("Dropping the tables")
		if err = dropTables(db); err != nil {
			log.Fatalf("drop tables failed: %s\n", err)
		}
		return
	}

	if *load {
		if err = createTables(db); err != nil {
			log.Fatalf("creating tables and indices failed: %s\n", err)
		}

		files, err := ioutil.ReadDir(fmt.Sprintf("%s", *dataDir))
		if err != nil {
			log.Fatalf("failed to read data directory for loading data: %s", err)
		}

		for _, file := range files {
			t, err := resolveTableTypeFromFileName(file.Name())
			if err != nil {
				log.Fatal(err)
			}
			if err := loadFile(dbURL, file.Name(), t); err != nil {
				log.Fatal(err)
			}
		}

		if err := createIndexes(db); err != nil {
			log.Fatal("failed to create indexes: ", err)
		}
	}

	// Create *concurrency goroutines, each looping over queries in *queries.
	listQueries := strings.Split(*queries, ",")
	var queries []int
	for _, query := range listQueries {
		query = strings.TrimSpace(query)
		queryInt, err := strconv.Atoi(query)
		if err != nil {
			log.Fatalf("error: query %s must be an integer", query)
		}
		queries = append(queries, queryInt)
	}
	var wg sync.WaitGroup
	var errorCount uint64
	for i := uint(0); i < *concurrency; i++ {
		wg.Add(1)
		go loopQueries(i, db, queries, &wg, &errorCount)
	}
	wg.Wait()
}
