package main

import (
	// "encoding/binary"
	// "flag"
	"fmt"
	// "math"
	// "math/rand"
	// "os"
	// "os/signal"
	// "runtime"
	// "sync"
	// "sync/atomic"
	// "syscall"
	// "time"
	//
	// "net/http"
	// _ "net/http/pprof"
	//
	// _ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	// "github.com/pingcap/tidb/store/tikv"
	// "github.com/pkg/errors"
	// "github.com/prometheus/client_golang/prometheus"
)

func main() {
	dbURL := "tikv://172.16.20.2:2379"

	db, err := SetupDatabase(dbURL)
	if err != nil {
		log.Fatalf("Setting up database failed: %s", err)
	}

	k1 := Encode(0, 8)
	v1 := "Louis"

	db.InsertKey(k1, v1)

	name, _ := db.ReadKey(k1)
	fmt.Println(name)
}

// var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(),
//     "Number of concurrent workers sending read/write requests.")
// var workload = flag.String("workload", "B", "workload type. Choose from A-F.")
// var tolerateErrors = flag.Bool("tolerate-errors", false,
//     "Keep running on error. (default false)")
// var duration = flag.Duration("duration", 0,
//     "The duration to run. If 0, run forever.")
// var writeDuration = flag.Duration("write-duration", 0,
//     "The duration to perform writes. If 0, write forever.")
// var drop = flag.Bool("drop", true,
//     "Drop the existing table and recreate it to start from scratch")
// var initialLoad = flag.Uint64("initial-load", 10000,
//     "Initial number of rows to sequentially insert before beginning Zipfian workload generation")
//
// // 7 days at 5% writes and 30k ops/s
// var maxWrites = flag.Uint64("max-writes", 7*24*3600*1500,
//     "Maximum number of writes to perform before halting. This is required for accurately generating keys that are uniformly distributed across the keyspace.")
//
// var logFile = flag.String("log-file", "", "log file")
// var logLevel = flag.String("L", "info", "log level: info, debug, warn, error, fatal")
//
// var statusAddr = flag.String("status", ":10081", "Ycsb status listening address")
// var pushAddr = flag.String("push", "", "Prometheus push address")
//
// var readOnly int32
//
// // ycsbWorker independently issues reads, writes, and scans against the database.
// type ycsbWorker struct {
//     db Database
//     // An RNG used to generate random keys
//     zipfR *ZipfGenerator
//     // An RNG used to generate random strings for the values
//     r         *rand.Rand
//     readFreq  float32
//     writeFreq float32
//     scanFreq  float32
// }
//
// type statistic int
//
// const (
//     nonEmptyReads statistic = iota
//     emptyReads
//     writes
//     scans
//     writeErrors
//     readErrors
//     scanErrors
//     statsLength
// )
//
// var globalStats [statsLength]uint64
//
// type operation int
//
// const (
//     writeOp operation = iota
//     readOp
//     scanOp
// )
//
// func newYcsbWorker(db Database, zipfR *ZipfGenerator, workloadFlag string) *ycsbWorker {
//     source := rand.NewSource(int64(time.Now().UnixNano()))
//     var readFreq, writeFreq, scanFreq float32
//
//     switch workloadFlag {
//     case "A", "a":
//         readFreq = 0.5
//         writeFreq = 0.5
//     case "B", "b":
//         readFreq = 0.95
//         writeFreq = 0.05
//     case "C", "c":
//         readFreq = 1.0
//     case "D", "d":
//         readFreq = 0.95
//         writeFreq = 0.95
//         log.Fatal("Workload D not implemented yet")
//         // TODO: workload D (read latest) requires modifying the RNG to
//         // skew to the latest keys, so not done yet.
//     case "E", "e":
//         scanFreq = 0.95
//         writeFreq = 0.05
//         log.Fatal("Workload E (scans) not implemented yet")
//     case "F", "f":
//         writeFreq = 1.0
//     }
//     r := rand.New(source)
//     return &ycsbWorker{
//         db:        db,
//         r:         r,
//         zipfR:     zipfR,
//         readFreq:  readFreq,
//         writeFreq: writeFreq,
//         scanFreq:  scanFreq,
//     }
// }
//
// func (yw *ycsbWorker) nextReadKey() uint64 {
//     var hashedKey uint64
//     key := yw.zipfR.Uint64()
//     hashedKey = yw.hashKey(key)
//     return hashedKey
// }
//
// func (yw *ycsbWorker) nextWriteKey() uint64 {
//     key := yw.zipfR.IMaxHead()
//     hashedKey := yw.hashKey(key)
//     return hashedKey
// }
//
// // runWorker is an infinite loop in which the ycsbWorker reads and writes
// // random data into the table in proportion to the op frequencies.
// func (yw *ycsbWorker) runWorker(errCh chan<- error, wg *sync.WaitGroup) {
//     defer wg.Done()
//
//     for {
//         tStart := time.Now()
//         switch yw.chooseOp() {
//         case readOp:
//             if err := yw.readRow(); err != nil {
//                 atomic.AddUint64(&globalStats[readErrors], 1)
//                 errCh <- err
//             }
//         case writeOp:
//             if atomic.LoadUint64(&globalStats[writes]) > *maxWrites {
//                 break
//             }
//             key := yw.nextWriteKey()
//             if err := yw.insertRow(key, true); err != nil {
//                 errCh <- err
//                 atomic.AddUint64(&globalStats[writeErrors], 1)
//             }
//         case scanOp:
//             if err := yw.scanRows(); err != nil {
//                 atomic.AddUint64(&globalStats[scanErrors], 1)
//                 errCh <- err
//             }
//         }
//     }
// }
//
// var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
//
// // Gnerate a random string of alphabetic characters.
// func (yw *ycsbWorker) randString(length int) string {
//     str := make([]byte, length)
//     for i := range str {
//         str[i] = letters[yw.r.Intn(len(letters))]
//     }
//     return string(str)
// }
//
// func (yw *ycsbWorker) insertRow(key uint64, increment bool) error {
//     fields := make([]string, numTableFields)
//     for i := 0; i < len(fields); i++ {
//         fields[i] = yw.randString(fieldLength)
//     }
//     if err := yw.db.InsertRow(key, fields); err != nil {
//         return err
//     }
//
//     if increment {
//         if err := yw.zipfR.IncrementIMax(); err != nil {
//             return err
//         }
//     }
//     atomic.AddUint64(&globalStats[writes], 1)
//     return nil
// }
//
// func (yw *ycsbWorker) readRow() error {
//     empty, err := yw.db.ReadRow(yw.nextReadKey())
//     if err != nil {
//         return err
//     }
//     if !empty {
//         atomic.AddUint64(&globalStats[nonEmptyReads], 1)
//         return nil
//     }
//     atomic.AddUint64(&globalStats[emptyReads], 1)
//     return nil
// }
//
// func (yw *ycsbWorker) scanRows() error {
//     atomic.AddUint64(&globalStats[scans], 1)
//     return errors.Errorf("not implemented yet")
// }
//
// // Choose an operation in proportion to the frequencies.
// func (yw *ycsbWorker) chooseOp() operation {
//     p := yw.r.Float32()
//     if atomic.LoadInt32(&readOnly) == 0 && p <= yw.writeFreq {
//         return writeOp
//     }
//     p -= yw.writeFreq
//     // If both scanFreq and readFreq are 0 default to readOp if we've reached
//     // this point because readOnly is true.
//     if p <= yw.scanFreq {
//         return scanOp
//     }
//     return readOp
// }
//
// var usage = func() {
//     fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
//     fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
//     flag.PrintDefaults()
// }
//
// func snapshotStats() (s [statsLength]uint64) {
//     for i := 0; i < int(statsLength); i++ {
//         s[i] = atomic.LoadUint64(&globalStats[i])
//     }
//     return s
// }
//
// func main() {
//     flag.Usage = usage
//     flag.Parse()
//
//     tikv.MaxConnectionCount = 128
//
//     log.SetHighlighting(false)
//     if len(*logFile) > 0 {
//         log.SetOutputByName(*logFile)
//         log.SetRotateByHour()
//     }
//     log.SetLevelByString(*logLevel)
//
//     go func() {
//         http.ListenAndServe(*statusAddr, nil)
//         http.Handle("/metrics", prometheus.Handler())
//     }()
//
//     PushMetrics(*pushAddr)
//
//     log.Info("Starting YCSB load generator")
//
//     dbURL := "tidb://root@tcp(127.0.0.1:4000)/"
//     if flag.NArg() == 1 {
//         dbURL = flag.Arg(0)
//     }
//
//     if *concurrency < 1 {
//         log.Fatalf("Value of 'concurrency' flag (%d) must be greater than or equal to 1",
//             concurrency)
//     }
//
//     db, err := SetupDatabase(dbURL)
//
//     if err != nil {
//         log.Fatalf("Setting up database failed: %s", err)
//     }
//
//     log.Info("Database setup complete.")
//
//     lastNow := time.Now()
//     var lastOpsCount uint64
//     var lastStats [statsLength]uint64
//
//     workers := make([]*ycsbWorker, *concurrency)
//     for i := range workers {
//         workers[i] = newYcsbWorker(db.Clone(), zipfR, *workload)
//     }
//
//     wg = sync.WaitGroup{}
//     errCh := make(chan error)
//
//     var numErr int
//     tick := time.Tick(1 * time.Second)
//     done := make(chan os.Signal, 3)
//     signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
//
//     go func() {
//         wg.Wait()
//         done <- syscall.Signal(0)
//     }()
//
//     start := time.Now()
//     startOpsCount := globalStats[writes] + globalStats[emptyReads] +
//         globalStats[nonEmptyReads] + globalStats[scans]
//
//     for i := range workers {
//         wg.Add(1)
//         go workers[i].runWorker(errCh, &wg)
//     }
//
//     if *duration > 0 {
//         go func() {
//             time.Sleep(*duration)
//             done <- syscall.Signal(0)
//         }()
//     }
//
//     if *writeDuration > 0 {
//         go func() {
//             time.Sleep(*writeDuration)
//             atomic.StoreInt32(&readOnly, 1)
//         }()
//     }
//
//     for i := 0; ; {
//         select {
//         case err := <-errCh:
//             numErr++
//             if !*tolerateErrors {
//                 log.Fatal(err)
//             } else {
//                 log.Error(err)
//             }
//             continue
//
//         case <-tick:
//             now := time.Now()
//             elapsed := now.Sub(lastNow)
//
//             stats := snapshotStats()
//             opsCount := stats[writes] + stats[emptyReads] +
//                 stats[nonEmptyReads] + stats[scans]
//             if i%20 == 0 {
//                 fmt.Printf("elapsed______ops/sec__reads/empty/errors___writes/errors____scans/errors\n")
//             }
//
//             cmdCounter.WithLabelValues("read").Add(float64(stats[nonEmptyReads] - lastStats[nonEmptyReads]))
//             cmdCounter.WithLabelValues("empty_read").Add(float64(stats[emptyReads] - lastStats[emptyReads]))
//             cmdCounter.WithLabelValues("write").Add(float64(stats[writes] - lastStats[writes]))
//             cmdCounter.WithLabelValues("scan").Add(float64(stats[scans] - lastStats[scans]))
//
//             cmdErrorCounter.WithLabelValues("read").Add(float64(stats[readErrors] - lastStats[readErrors]))
//             cmdErrorCounter.WithLabelValues("write").Add(float64(stats[writeErrors] - lastStats[writeErrors]))
//             cmdErrorCounter.WithLabelValues("scan").Add(float64(stats[scanErrors] - lastStats[scanErrors]))
//
//             fmt.Printf("%7s %12.1f %19s %15s %15s\n",
//                 time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
//                 float64(opsCount-lastOpsCount)/elapsed.Seconds(),
//                 fmt.Sprintf("%d / %d / %d",
//                     stats[nonEmptyReads]-lastStats[nonEmptyReads],
//                     stats[emptyReads]-lastStats[emptyReads],
//                     stats[readErrors]-lastStats[readErrors]),
//                 fmt.Sprintf("%d / %d",
//                     stats[writes]-lastStats[writes],
//                     stats[writeErrors]-lastStats[writeErrors]),
//                 fmt.Sprintf("%d / %d",
//                     stats[scans]-lastStats[scans],
//                     stats[scanErrors]-lastStats[scanErrors]))
//             lastStats = stats
//             lastOpsCount = opsCount
//             lastNow = now
//             i++
//
//         case <-done:
//             stats := snapshotStats()
//             opsCount := stats[writes] + stats[emptyReads] +
//                 stats[nonEmptyReads] + stats[scans] - startOpsCount
//             elapsed := time.Since(start).Seconds()
//             fmt.Printf("\nelapsed________ops/sec(total)________errors(total)\n")
//             fmt.Printf("%6.1fs %14.1f(%d) %14d\n",
//                 time.Since(start).Seconds(),
//                 float64(opsCount)/elapsed, opsCount, numErr)
//             return
//         }
//     }
// }
