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
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/pingcap/octopus/stability-tester/merge_test/util"
	log "github.com/sirupsen/logrus"
)

var (
	host     = flag.String("host", "127.0.0.1", "DB host")
	port     = flag.Int("P", 4000, "DB port")
	user     = flag.String("user", "root", "DB user")
	password = flag.String("pass", "", "DB password")
	pd       = flag.String("pd", "http://127.0.0.1:2379", "pd endpoint")
	//test     = flag.String("test", "12345678", "test cases")
)

const deleteBatch = 100

func main() {
	flag.Parse()

	util.PdAddr = *pd

	rand.Seed(time.Now().UnixNano())

	db, err := util.InitDB(*user, *password, *host, *port)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
	defer db.Close()

	// log.Info("check create table...")
	// err = checkCreateTable(db)
	// if err != nil {
	// 	log.Fatal(errors.ErrorStack(err))
	// }

	// log.Info("check split table region mannualy...")
	// err = checkSplitTableRegion(db)
	// if err != nil {
	// 	log.Fatal(errors.ErrorStack(err))
	// }

	// log.Info("check drop table...")
	// err = checkDropTable(db)
	// if err != nil {
	// 	log.Fatal(errors.ErrorStack(err))
	// }

	// log.Info("check insert and delete all rows of table ...")
	// err = checkInsertAndDeleteAll(db)
	// if err != nil {
	// 	log.Fatal(errors.ErrorStack(err))
	// }

	// log.Info("check insert and delete partial rows of table ...")
	// err = checkInsertAndDeletePartial(db)
	// if err != nil {
	// 	log.Fatal(errors.ErrorStack(err))
	// }

	// log.Info("check shuffle...")
	// err = checkShuffle(db)
	// if err != nil {
	// 	log.Fatal(errors.ErrorStack(err))
	// }

	// log.Info("check speed...")
	// err = checkSpeed(db)
	// if err != nil {
	// 	log.Fatal(errors.ErrorStack(err))
	// }

	log.Info("check correctness...")
	err = checkCorrectness(db)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	log.Info("pass merge test...")
}

func checkCreateTable(db *util.DB) error {
	initCount, err := util.GetRegionCount()
	if err != nil {
		return errors.Trace(err)
	}

	err = db.CreateTable(1)
	if err != nil {
		return errors.Trace(err)
	}

	time.Sleep(60 * time.Second)

	nowCount, err := util.GetRegionCount()
	if nowCount != initCount+1 {
		return fmt.Errorf("initCount is %v, after create a table region count is %v", initCount, nowCount)
	}

	err = db.CreateTable(1)
	if err != nil {
		return errors.Trace(err)
	}

	time.Sleep(60 * time.Second)

	nowCount, err = util.GetRegionCount()
	if nowCount != initCount+2 {
		return fmt.Errorf("initCount is %v, after create two tables region count is %v", initCount, nowCount)
	}

	return nil
}

func checkSplitTableRegion(db *util.DB) error {
	// for merge use
	err := db.CreateTable(1)
	if err != nil {
		return errors.Trace(err)
	}

	initCount, err := util.GetRegionCount()
	if err != nil {
		return errors.Trace(err)
	}

	tableRegionInfo, err := util.CreateNewRegion(db)
	if err != nil {
		return errors.Trace(err)
	}

	leftID, rightID, err := util.SplitRegion(tableRegionInfo)
	if err != nil {
		return errors.Trace(err)
	}
	if !(tableRegionInfo.ID != leftID || tableRegionInfo.ID != rightID) {
		return fmt.Errorf("unexpected region ids after split")
	}

	// wait regions to merge
	time.Sleep(60 * time.Second)

	nowRegions, err := util.GetRegions()
	if err != nil {
		return errors.Trace(err)
	}
	nowCount := len(nowRegions)

	if nowCount != initCount+1 {
		return fmt.Errorf("initCount is %v, after split table and merge table, region count is %v", initCount, nowCount)
	}
	if util.IsRegionIDExist(nowRegions, leftID) && util.IsRegionIDExist(nowRegions, rightID) {
		return fmt.Errorf("unexpected region ids after merge")
	}

	return nil
}

func cleanTableData(db *util.DB, fn func([]*util.RegionInfo) error) error {
	// for merge use
	err := db.CreateTable(1)
	if err != nil {
		return errors.Trace(err)
	}

	tableRegion, err := util.CreateNewRegion(db)
	if err != nil {
		return errors.Trace(err)
	}

	err = db.InsertUntilSplit(tableRegion.ID)
	if err != nil {
		return errors.Trace(err)
	}

	time.Sleep(time.Second * 60)

	tableRegions, err := util.FindNewSplitRegions(tableRegion)
	if err != nil {
		return errors.Trace(err)
	}

	err = fn(tableRegions)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func checkDropTable(db *util.DB) error {
	callback := func(regions []*util.RegionInfo) error {
		for _, region := range regions {
			nowRegion, err := util.GetRegionInfo(region.ID)
			if err != nil {
				return errors.Trace(err)
			}
			log.Infof("[region %d] approximate size is %d before drop", nowRegion.ID, nowRegion.ApproximateSize)
		}

		err := db.DropTable()
		if err != nil {
			return errors.Trace(err)
		}

		time.Sleep(time.Second * 60)

		for _, region := range regions {
			nowRegion, err := util.GetRegionInfo(region.ID)
			if err != nil {
				return errors.Trace(err)
			}
			log.Infof("[region %d] approximate size is %d after drop", nowRegion.ID, nowRegion.ApproximateSize)
		}
		log.Info("------------------------------------------------")

		// wait to merge
		err = db.InsertUntilMerge(regions)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}

	return cleanTableData(db, callback)
}

func checkInsertAndDeleteAll(db *util.DB) error {
	callback := func(regions []*util.RegionInfo) error {
		for _, region := range regions {
			nowRegion, err := util.GetRegionInfo(region.ID)
			if err != nil {
				return errors.Trace(err)
			}
			log.Infof("[region %d] approximate size is %d before delete", nowRegion.ID, nowRegion.ApproximateSize)
		}

		err := db.DeleteAll()
		if err != nil {
			return errors.Trace(err)
		}

		time.Sleep(time.Second * 60)

		for _, region := range regions {
			nowRegion, err := util.GetRegionInfo(region.ID)
			if err != nil {
				return errors.Trace(err)
			}
			log.Infof("[region %d] approximate size is %d after delete", nowRegion.ID, nowRegion.ApproximateSize)
		}
		log.Info("------------------------------------------------")

		// wait to merge
		err = db.InsertUntilMerge(regions)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}

	return cleanTableData(db, callback)
}

func checkInsertAndDeletePartial(db *util.DB) error {
	callback := func(regions []*util.RegionInfo) error {
		for _, region := range regions {
			nowRegion, err := util.GetRegionInfo(region.ID)
			if err != nil {
				return errors.Trace(err)
			}
			log.Infof("[region %d] approximate size is %d before delete", nowRegion.ID, nowRegion.ApproximateSize)
		}

		err := db.DeletePartial()
		if err != nil {
			return errors.Trace(err)
		}

		time.Sleep(time.Second * 60)

		for _, region := range regions {
			nowRegion, err := util.GetRegionInfo(region.ID)
			if err != nil {
				return errors.Trace(err)
			}
			log.Infof("[region %d] approximate size is %d after delete", nowRegion.ID, nowRegion.ApproximateSize)
		}
		log.Info("------------------------------------------------")

		// wait to merge
		err = db.InsertUntilTimeout(regions, 30*time.Minute)
		if err != nil {
			return errors.Trace(err)
		}

		return nil
	}

	return cleanTableData(db, callback)
}

func checkShuffle(db *util.DB) error {
	err := db.CreateTable(1)
	if err != nil {
		return errors.Trace(err)
	}

	tableRegion, err := util.CreateNewRegion(db)
	if err != nil {
		return errors.Trace(err)
	}

	err = db.InsertUntilSplit(tableRegion.ID)
	if err != nil {
		return errors.Trace(err)
	}

	tableRegions, err := util.FindNewSplitRegions(tableRegion)
	if err != nil {
		return errors.Trace(err)
	}

	err = util.StartShuffleRegions()
	if err != nil {
		return errors.Trace(err)
	}
	for retry := 0; retry < 5; retry++ {
		ok := false
		for _, region := range tableRegions {
			nowRegion, err := util.GetRegionInfo(region.ID)
			if err != nil {
				return errors.Trace(err)
			}

			if !util.IsSameLocation(region.Peers, nowRegion.Peers) {
				err = util.StopShuffleRegions()
				if err != nil {
					return errors.Trace(err)
				}
				ok = true
				break
			}
		}

		if ok {
			break
		}
		// wait region peers to move
		time.Sleep(60 * time.Second)
	}

	err = db.DropTable()
	if err != nil {
		return errors.Trace(err)
	}

	// wait to merge
	err = db.InsertUntilMerge(tableRegions)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func checkSpeed(db *util.DB) error {
	var wg sync.WaitGroup

	for i := 1; i <= 10; i++ {
		tableRegion, err := util.CreateNewRegion(db)
		if err != nil {
			continue
		}

		tableID := util.GetTableID(tableRegion)

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(time.Minute * 5)
			cancel()
		}()

		db.InsertIntoNewTable(ctx, 200)

		time.Sleep(time.Second * 60)

		tableRegions, err := util.FindNewSplitRegions(tableRegion)
		if err != nil {
			return errors.Trace(err)
		}

		err = db.DropTable()
		if err != nil {
			return errors.Trace(err)
		}

		begin := time.Now()

		go func() {
			wg.Add(1)
			defer wg.Done()

			for {
				time.Sleep(time.Second * 60)

				prevCount := len(tableRegions)
				for i := len(tableRegions) - 1; i >= 0; i-- {
					region := tableRegions[i]
					nowRegion, err := util.GetRegionInfo(region.ID)
					if err != nil {
						log.Fatal(err)
					}

					if nowRegion.ID == 0 {
						tableRegions = append(tableRegions[:i], tableRegions[i+1:]...)
						continue
					}

					tableRegions[i] = nowRegion
				}
				if len(tableRegions) <= 1 {
					log.Infof("table %d has merged all regions into one, %v seconds after drop table", tableID, time.Since(begin).Seconds())
					return
				}

				var err error
				tableRegions, err = util.CheckRegionsTableID(tableID, tableRegions)
				if err != nil {
					log.Fatal(err)
				}

				if len(tableRegions) != prevCount {
					log.Infof("table %d has %d regions, %v seconds after drop table", tableID, len(tableRegions), time.Since(begin).Seconds())
				}
			}
		}()
	}

	err := db.CreateTable(1)
	if err != nil {
		return errors.Trace(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		wg.Wait()
		cancel()
	}()

	db.InsertIntoNewTable(ctx, 200)

	return nil
}

const (
	accountNum        = 10000
	initialBalance    = 1000
	insertBatchSize   = 100
	insertConcurrency = 100
	maxTransfer       = 100
	systemAccountID   = 0
)

var remark = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXVZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXVZlkjsanksqiszndqpijdslnnq"

func checkCorrectness(db *util.DB) error {
	initRegions, err := util.GetRegions()
	if err != nil {
		return errors.Trace(err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS bank2_accounts (
			id INT,
			balance INT NOT NULL,
			name VARCHAR(32),
			remark VARCHAR(128),
			PRIMARY KEY (id),
			UNIQUE INDEX byName (name)
		  );
		  
		  CREATE TABLE IF NOT EXISTS bank2_transaction (
			id INT,
			booking_date TIMESTAMP DEFAULT NOW(),
			txn_date TIMESTAMP DEFAULT NOW(),
			txn_ref VARCHAR(32),
			PRIMARY KEY (id),
			UNIQUE INDEX byTxnRef (txn_ref)
		  );
		  
		  CREATE TABLE IF NOT EXISTS bank2_transaction_leg (
			id INT AUTO_INCREMENT,
			account_id INT,
			amount INT NOT NULL,
			running_balance INT NOT NULL,
			txn_id INT,
			PRIMARY KEY (id)
		  );
		  
		  TRUNCATE TABLE bank2_accounts;
		  TRUNCATE TABLE bank2_transaction;
		  TRUNCATE TABLE bank2_transaction_leg;`)
	if err != nil {
		return errors.Trace(err)
	}

	time.Sleep(time.Second * 60)

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	rand.Seed(time.Now().UnixNano())
	defer cancel()
	go func() {
		var monitorRegions []uint64
		for {
			time.Sleep(time.Second * time.Duration(rand.Intn(60)))

			select {
			case <-ctx.Done():
				return
			default:
			}
			nowRegions, err := util.GetRegions()
			if err != nil {
				log.Fatal(err)
			}

			for i := len(monitorRegions) - 1; i >= 0; i-- {
				if !util.IsRegionIDExist(nowRegions, monitorRegions[i]) {
					log.Infof("region %v has been merged")
					monitorRegions = append(monitorRegions[:i], monitorRegions[i+1:]...)
					continue
				}
			}

			newRegions, err := util.GetNewTableRegions(initRegions, nowRegions)
			if err != nil {
				log.Fatal(err)
			}

			source := newRegions[rand.Intn(len(newRegions))]
			target := util.FindAdjacentRegion(newRegions, source)

			err = util.MergeRegion(source, target)
			log.Infof("try to add operator merge region %v into region %v", source.ID, target.ID)
			if err != nil {
				log.Warning(err)
			} else {
				monitorRegions = append(monitorRegions, source.ID)
			}
		}
	}()

	var wg sync.WaitGroup
	type Job struct {
		begin, end int
	}
	ch := make(chan Job)
	maxLen := len(remark)
	for i := 0; i < insertConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range ch {
				select {
				case <-ctx.Done():
					return
				default:
				}
				start := time.Now()
				args := make([]string, 0, insertBatchSize)
				for i := job.begin; i < job.end; i++ {
					args = append(args, fmt.Sprintf(`(%d, %d, "account %d", "%s")`, i, initialBalance, i, remark[:rand.Intn(maxLen)]))
				}

				query := fmt.Sprintf("INSERT IGNORE INTO bank2_accounts (id, balance, name, remark) VALUES %s", strings.Join(args, ","))
				err := util.RunWithRetry(ctx, 200, 5*time.Second, func() error {
					_, err := db.Exec(query)
					return err
				})
				if err != nil {
					log.Fatalf("exec %s err %s", query, err)
				}
				log.Infof("insert %d accounts, takes %s", job.end-job.begin, time.Since(start))
			}
		}()
	}

	var begin, end int
	for begin = 1; begin <= accountNum; begin = end {
		end = begin + insertBatchSize
		if end > accountNum {
			end = accountNum + 1
		}
		select {
		case <-ctx.Done():
			return nil
		case ch <- Job{begin: begin, end: end}:
		}
	}
	close(ch)
	wg.Wait()

	select {
	case <-ctx.Done():
		log.Warn("initialize is cancel")
		return nil
	default:
	}

	query := fmt.Sprintf(`INSERT IGNORE INTO bank2_accounts (id, balance, name) VALUES (%d, %d, "system account")`, systemAccountID, int64(accountNum*initialBalance))
	err = util.RunWithRetry(ctx, 100, 3*time.Second, func() error {
		_, err := db.Exec(query)
		return err
	})
	if err != nil {
		log.Fatalf("insert system account err: %v", err)
	}

	startVerify(ctx, db)
	return nil
}

func startVerify(ctx context.Context, db *util.DB) {
	verify(db)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
				verify(db)
			}
		}
	}()
}

func verify(db *util.DB) {
	tx, err := db.Begin()
	if err != nil {
		log.Warning(err)
	}
	defer tx.Rollback()

	var tso uint64
	if err = tx.QueryRow("SELECT @@tidb_current_ts").Scan(&tso); err == nil {
		log.Infof("SELECT SUM(balance) to verify use tso %d", tso)
	}

	var total int64
	err = tx.QueryRow("SELECT SUM(balance) AS total FROM bank2_accounts").Scan(&total)
	if err != nil {
		log.Warning(err)
	}

	expectTotal := (int64(accountNum) * initialBalance) * 2
	if total != expectTotal {
		log.Fatalf("[bank2] bank2_accounts total should be %d, but got %d", expectTotal, total)
	}
}
