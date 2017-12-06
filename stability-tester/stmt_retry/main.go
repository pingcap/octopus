package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/util"
)

var (
	logFile    = flag.String("log-file", "", "log file")
	logLevel   = flag.String("L", "info", "log level: info, debug, warn, error, faltal")
	user       = flag.String("user", "root", "db username")
	password   = flag.String("password", "", "user password")
	pds        = flag.String("pds", "", "separated by \",\"")
	tidbs      = flag.String("tidbs", "", "separated by \",\"")
	tikvs      = flag.String("tikvs", "", "separated by \",\"")
	lbService  = flag.String("lb-service", "", "lb")
	metricAddr = flag.String("metric-addr", "", "metric address")
	dbName     = flag.String("db", "test", "database name")

	defaultConcurrent      = 2
	defaultRunStmsCount    = 1024 * 128
	defaultRunTimeInterval = time.Hour
)

func main() {
	flag.Parse()
	util.InitLog(*logFile, *logLevel)

	ctx, cancel := context.WithCancel(context.Background())
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <-sc
		log.Infof("Got signal [%d] to exist.", sig)
		cancel()
		os.Exit(0)
	}()

	run(ctx)
}

func run(ctx context.Context) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s)/%s", *user, *password, *lbService, *dbName)
	db, err := util.OpenDB(dbDSN, defaultConcurrent)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("DROP TABLE IF EXISTS mobike")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS mobike (id INT PRIMARY KEY, txn INT, status INT, UNIQUE KEY txn_idx(txn))")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("INSERT test.mobike values (1, 100, 1)")
	if err != nil {
		log.Fatal(err)
	}

	var (
		pc     = 0
		cctx   context.Context
		cancel context.CancelFunc
	)
	for {
		cctx, cancel = context.WithCancel(ctx)
		if pc%2 == 0 {
			go runCases(cctx, db)
		}
		pc++

		select {
		case <-ctx.Done():
			cancel()
			return
		case <-time.After(defaultRunTimeInterval):
			cancel()
		}
	}
}

func runCases(ctx context.Context, db *sql.DB) {
	var stmts = []string{
		"UPDATE mobike SET status=1, txn=100 WHERE id=1",
		"UPDATE mobike SET status=2, txn=100 WHERE txn=200",
		"UPDATE mobike SET status=2, txn=200 WHERE txn=100",
	}

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		go func(idx int) {

			wg.Add(1)
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				txn, err := db.Begin()
				if err != nil {
					log.Errorf("begin transaction %v", err)
					continue
				}
				perm := rand.Perm(len(stmts))
				for _, idx := range perm {
					query := stmts[idx]
					_, err = txn.Exec(stmts[idx])
					if err != nil {
						log.Errorf("stmt: %v, err: %v, perm: %v", query, err, perm)
						checkTable(db)
					}

				}
				if err != nil {
					txn.Rollback()
					continue
				}
				err = txn.Commit()
				if err != nil {
					log.Errorf("err %v thread %d, perm %v", err, idx, perm)
					checkTable(db)
				}

				log.Infof("thread %d, perm %v", idx, perm)
			}
		}(i)
	}

	wg.Wait()
}

func checkTable(db *sql.DB) {
	_, err := db.Exec("admin check table mobike")
	if err != nil && strings.Contains(err.Error(), "1105") {
		log.Fatalf("data nonconsistent %v", err)
	}
}
