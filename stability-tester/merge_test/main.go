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
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"github.com/pingcap/octopus/stability-tester/merge_test/util"
)

var (
	host     = flag.String("host", "127.0.0.1", "DB host")
	port     = flag.Int("P", 4000, "DB port")
	user     = flag.String("user", "root", "DB user")
	password = flag.String("pass", "", "DB password")
	pd       = flag.String("pd", "http://127.0.0.1:2379", "pd endpoint")
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

	log.Info("check create table...")
	err = checkCreateTable(db)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	log.Info("check split table region mannualy...")
	err = checkSplitTableRegion(db)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	log.Info("check drop table...")
	err = checkDropTable(db)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	log.Info("check insert and delete all rows of table ...")
	err = checkInsertAndDeleteAll(db)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	log.Info("check insert and delete partial rows of table ...")
	err = checkInsertAndDeletePartial(db)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	log.Info("check shuffle...")
	err = checkShuffle(db)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	log.Info("check speed...")
	err = checkSpeed(db)
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
