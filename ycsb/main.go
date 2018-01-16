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
// The YCSB example program is intended to simulate the workload specified by
// the Yahoo! Cloud Serving Benchmark.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"net/http"
	_ "net/http/pprof"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// SQL statements
const (
	numTableFields = 10
	fieldLength    = 100 // In characters
)

const (
	zipfS    = 0.99
	zipfIMin = 1
)

var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(),
	"Number of concurrent workers sending read/write requests.")
var workload = flag.String("workload", "B", "workload type. Choose from A-F.")
var tolerateErrors = flag.Bool("tolerate-errors", false,
	"Keep running on error. (default false)")
var duration = flag.Duration("duration", 0,
	"The duration to run. If 0, run forever.")
var writeDuration = flag.Duration("write-duration", 0,
	"The duration to perform writes. If 0, write forever.")
var drop = flag.Bool("drop", true,
	"Drop the existing table and recreate it to start from scratch")
var rateLimit = flag.Uint64("rate-limit", 0,
	"Maximum number of operations per second per worker. Set to zero for no rate limit")
var initialLoad = flag.Uint64("initial-load", 10000,
	"Initial number of rows to sequentially insert before beginning Zipfian workload generation")

// 7 days at 5% writes and 30k ops/s
var maxWrites = flag.Uint64("max-writes", 7*24*3600*1500,
	"Maximum number of writes to perform before halting. This is required for accurately generating keys that are uniformly distributed across the keyspace.")

var logFile = flag.String("log-file", "", "log file")
var logLevel = flag.String("L", "info", "log level: info, debug, warn, error, fatal")

var statusAddr = flag.String("status", ":10081", "Ycsb status listening address")
var pushAddr = flag.String("push", "", "Prometheus push address")

var readOnly int32

// ycsbWorker independently issues reads, writes, and scans against the database.
type ycsbWorker struct {
	db Database
	// An RNG used to generate random keys
	zipfR         *ZipfGenerator
	r             *rand.Rand
	readFreq      float32
	writeFreq     float32
	scanFreq      float32
	minNanosPerOp time.Duration
	hashFunc      hash.Hash64
	hashBuf       [8]byte
}

type statistic int

const (
	nonEmptyReads statistic = iota
	emptyReads
	writes
	scans
	writeErrors
	readErrors
	scanErrors
	statsLength
)

var globalStats [statsLength]uint64

type operation int

const (
	writeOp operation = iota
	readOp
	scanOp
)

func newYcsbWorker(db Database, zipfR *ZipfGenerator, workloadFlag string) *ycsbWorker {
	var readFreq, writeFreq, scanFreq float32

	// TODO(arjun): This could be implemented as a token bucket.
	var minNanosPerOp time.Duration
	if *rateLimit != 0 {
		minNanosPerOp = time.Duration(1000000000 / *rateLimit)
	}
	switch workloadFlag {
	case "A", "a":
		readFreq = 0.5
		writeFreq = 0.5
	case "B", "b":
		readFreq = 0.95
		writeFreq = 0.05
	case "C", "c":
		readFreq = 1.0
	case "D", "d":
		readFreq = 0.95
		writeFreq = 0.95
		log.Fatal("Workload D not implemented yet")
		// TODO: workload D (read latest) requires modifying the RNG to
		// skew to the latest keys, so not done yet.
	case "E", "e":
		scanFreq = 0.95
		writeFreq = 0.05
		log.Fatal("Workload E (scans) not implemented yet")
	case "F", "f":
		writeFreq = 1.0
	}
	return &ycsbWorker{
		db:            db,
		r:             rand.New(rand.NewSource(int64(time.Now().UnixNano()))),
		zipfR:         zipfR,
		readFreq:      readFreq,
		writeFreq:     writeFreq,
		scanFreq:      scanFreq,
		minNanosPerOp: minNanosPerOp,
		hashFunc:      fnv.New64(),
	}
}

func (yw *ycsbWorker) hashKey(key uint64) uint64 {
	yw.hashBuf = [8]byte{} // clear hashBuf
	binary.PutUvarint(yw.hashBuf[:], key)
	yw.hashFunc.Reset()
	if _, err := yw.hashFunc.Write(yw.hashBuf[:]); err != nil {
		log.Fatalf("generate hash key failed %v", err)
	}
	// The Go sql driver interface does not support having the high-bit set in
	// uint64 values!
	return yw.hashFunc.Sum64() & math.MaxInt64
}

// Keys are chosen by first drawing from a Zipf distribution and hashing the
// drawn value, so that not all hot keys are close together.
// See YCSB paper section 5.3 for a complete description of how keys are chosen.
func (yw *ycsbWorker) nextReadKey() uint64 {
	var hashedKey uint64
	key := yw.zipfR.Uint64(yw.r.Float64())
	hashedKey = yw.hashKey(key)
	return hashedKey
}

func (yw *ycsbWorker) nextWriteKey() uint64 {
	key := yw.zipfR.IMaxHead()
	hashedKey := yw.hashKey(key)
	return hashedKey
}

// runLoader inserts n rows in parallel across numWorkers, with
// row_id = i*numWorkers + thisWorkerNum for i = 0...(n-1)
func (yw *ycsbWorker) runLoader(n uint64, numWorkers int, thisWorkerNum int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := uint64(thisWorkerNum + zipfIMin); i < n; i += uint64(numWorkers) {
		hashedKey := yw.hashKey(i)
		if err := yw.insertRow(hashedKey, false); err != nil {
			log.Fatalf("error loading row %d: %s", i, err)
			atomic.AddUint64(&globalStats[writeErrors], 1)
		}
	}
}

// runWorker is an infinite loop in which the ycsbWorker reads and writes
// random data into the table in proportion to the op frequencies.
func (yw *ycsbWorker) runWorker(errCh chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		tStart := time.Now()
		switch yw.chooseOp() {
		case readOp:
			if err := yw.readRow(); err != nil {
				atomic.AddUint64(&globalStats[readErrors], 1)
				errCh <- err
			}
		case writeOp:
			if atomic.LoadUint64(&globalStats[writes]) > *maxWrites {
				break
			}
			key := yw.nextWriteKey()
			if err := yw.insertRow(key, true); err != nil {
				errCh <- err
				atomic.AddUint64(&globalStats[writeErrors], 1)
			}
		case scanOp:
			if err := yw.scanRows(); err != nil {
				atomic.AddUint64(&globalStats[scanErrors], 1)
				errCh <- err
			}
		}

		// If we are done faster than the rate limit, wait.
		tElapsed := time.Since(tStart)
		if tElapsed < yw.minNanosPerOp {
			time.Sleep(time.Duration(yw.minNanosPerOp - tElapsed))
		}
	}
}

func (yw *ycsbWorker) insertRow(key uint64, increment bool) error {
	start := time.Now()
	defer func() { cmdDuration.WithLabelValues("insert").Observe(time.Since(start).Seconds()) }()

	fields := make([]string, numTableFields)
	for i := 0; i < len(fields); i++ {
		fields[i] = randString(yw.r, fieldLength)
	}
	if err := yw.db.InsertRow(key, fields); err != nil {
		return err
	}

	if increment {
		if err := yw.zipfR.IncrementIMax(); err != nil {
			return err
		}
	}
	atomic.AddUint64(&globalStats[writes], 1)
	return nil
}

func (yw *ycsbWorker) readRow() error {
	start := time.Now()
	defer func() { cmdDuration.WithLabelValues("read").Observe(time.Since(start).Seconds()) }()

	empty, err := yw.db.ReadRow(yw.nextReadKey())
	if err != nil {
		return err
	}
	if !empty {
		atomic.AddUint64(&globalStats[nonEmptyReads], 1)
		return nil
	}
	atomic.AddUint64(&globalStats[emptyReads], 1)
	return nil
}

func (yw *ycsbWorker) scanRows() error {
	start := time.Now()
	defer cmdDuration.WithLabelValues("scan").Observe(time.Since(start).Seconds())

	atomic.AddUint64(&globalStats[scans], 1)
	return errors.Errorf("not implemented yet")
}

// Choose an operation in proportion to the frequencies.
func (yw *ycsbWorker) chooseOp() operation {
	p := yw.r.Float32()
	if atomic.LoadInt32(&readOnly) == 0 && p <= yw.writeFreq {
		return writeOp
	}
	p -= yw.writeFreq
	// If both scanFreq and readFreq are 0 default to readOp if we've reached
	// this point because readOnly is true.
	if p <= yw.scanFreq {
		return scanOp
	}
	return readOp
}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
	flag.PrintDefaults()
}

func snapshotStats() (s [statsLength]uint64) {
	for i := 0; i < int(statsLength); i++ {
		s[i] = atomic.LoadUint64(&globalStats[i])
	}
	return s
}

func main() {
	flag.Usage = usage
	flag.Parse()

	log.SetHighlighting(false)
	if len(*logFile) > 0 {
		log.SetOutputByName(*logFile)
		log.SetRotateByHour()
	}
	log.SetLevelByString(*logLevel)

	go func() {
		http.Handle("/metrics", prometheus.Handler())
		http.ListenAndServe(*statusAddr, nil)
	}()

	PushMetrics(*pushAddr)

	log.Info("Starting YCSB load generator")

	dbURL := "tidb://root@tcp(127.0.0.1:4000)/"
	if flag.NArg() == 1 {
		dbURL = flag.Arg(0)
	}

	if *concurrency < 1 {
		log.Fatalf("Value of 'concurrency' flag (%d) must be greater than or equal to 1",
			concurrency)
	}

	db, err := SetupDatabase(dbURL)

	if err != nil {
		log.Fatalf("Setting up database failed: %s", err)
	}

	log.Info("Database setup complete. Loading...")

	lastNow := time.Now()
	var lastOpsCount uint64
	var lastStats [statsLength]uint64

	zipfR, err := NewZipfGenerator(zipfIMin, *initialLoad, zipfS)
	if err != nil {
		panic(err)
	}

	workers := make([]*ycsbWorker, *concurrency)
	for i := range workers {
		workers[i] = newYcsbWorker(db.Clone(), zipfR, *workload)
	}

	loadStart := time.Now()
	var wg sync.WaitGroup
	// TODO(peter): Using all of the workers for loading leads to errors with
	// some of the insert statements receiving an EOF. For now, use a single
	// worker.
	for i, n := 0, 1; i < n; i++ {
		wg.Add(1)
		go workers[i].runLoader(*initialLoad, n, i, &wg)
	}
	wg.Wait()
	log.Infof("Loading complete, total time elapsed: %s",
		time.Since(loadStart))

	wg = sync.WaitGroup{}
	errCh := make(chan error)

	var numErr int
	tick := time.Tick(1 * time.Second)
	done := make(chan os.Signal, 3)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		wg.Wait()
		done <- syscall.Signal(0)
	}()

	start := time.Now()
	startOpsCount := globalStats[writes] + globalStats[emptyReads] +
		globalStats[nonEmptyReads] + globalStats[scans]

	for i := range workers {
		wg.Add(1)
		go workers[i].runWorker(errCh, &wg)
	}

	if *duration > 0 {
		go func() {
			time.Sleep(*duration)
			done <- syscall.Signal(0)
		}()
	}

	if *writeDuration > 0 {
		go func() {
			time.Sleep(*writeDuration)
			atomic.StoreInt32(&readOnly, 1)
		}()
	}

	for i := 0; ; {
		select {
		case err := <-errCh:
			numErr++
			if !*tolerateErrors {
				log.Fatal(err)
			} else {
				log.Error(err)
			}
			continue

		case <-tick:
			now := time.Now()
			elapsed := now.Sub(lastNow)

			stats := snapshotStats()
			opsCount := stats[writes] + stats[emptyReads] +
				stats[nonEmptyReads] + stats[scans]
			if i%20 == 0 {
				fmt.Printf("elapsed______ops/sec__reads/empty/errors___writes/errors____scans/errors\n")
			}

			cmdCounter.WithLabelValues("read").Add(float64(stats[nonEmptyReads] - lastStats[nonEmptyReads]))
			cmdCounter.WithLabelValues("empty_read").Add(float64(stats[emptyReads] - lastStats[emptyReads]))
			cmdCounter.WithLabelValues("write").Add(float64(stats[writes] - lastStats[writes]))
			cmdCounter.WithLabelValues("scan").Add(float64(stats[scans] - lastStats[scans]))

			cmdErrorCounter.WithLabelValues("read").Add(float64(stats[readErrors] - lastStats[readErrors]))
			cmdErrorCounter.WithLabelValues("write").Add(float64(stats[writeErrors] - lastStats[writeErrors]))
			cmdErrorCounter.WithLabelValues("scan").Add(float64(stats[scanErrors] - lastStats[scanErrors]))

			fmt.Printf("%7s %12.1f %19s %15s %15s\n",
				time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
				float64(opsCount-lastOpsCount)/elapsed.Seconds(),
				fmt.Sprintf("%d / %d / %d",
					stats[nonEmptyReads]-lastStats[nonEmptyReads],
					stats[emptyReads]-lastStats[emptyReads],
					stats[readErrors]-lastStats[readErrors]),
				fmt.Sprintf("%d / %d",
					stats[writes]-lastStats[writes],
					stats[writeErrors]-lastStats[writeErrors]),
				fmt.Sprintf("%d / %d",
					stats[scans]-lastStats[scans],
					stats[scanErrors]-lastStats[scanErrors]))
			lastStats = stats
			lastOpsCount = opsCount
			lastNow = now
			i++

		case <-done:
			stats := snapshotStats()
			opsCount := stats[writes] + stats[emptyReads] +
				stats[nonEmptyReads] + stats[scans] - startOpsCount
			elapsed := time.Since(start).Seconds()
			fmt.Printf("\nelapsed________ops/sec(total)________errors(total)\n")
			fmt.Printf("%6.1fs %14.1f(%d) %14d\n",
				time.Since(start).Seconds(),
				float64(opsCount)/elapsed, opsCount, numErr)
			return
		}
	}
}
