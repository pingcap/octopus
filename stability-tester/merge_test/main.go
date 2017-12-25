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
	"flag"
	"fmt"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-test/merge_test/util"
)

var (
	host     = flag.String("host", "127.0.0.1", "DB host")
	port     = flag.Int("P", 4000, "DB port")
	user     = flag.String("user", "root", "DB user")
	password = flag.String("pass", "", "DB password")
	pd       = flag.String("pd", "http://127.0.0.1:2379", "pd endpoint")
	tikv     = flag.String("tikv", "127.0.0.1:20160", "tikv endpoint")
)

const deleteBatch = 100

func main() {
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	db, err := util.InitDB(*user, *password, *host, *port)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
	defer db.Close()

	util.PdAddr = *pd
	err = util.InitTikvClient(*tikv)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	defer util.Clean()

	log.Infof("check create table...")
	err = checkCreateTable(db)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	log.Infof("check split table region mannualy...")
	err = checkSplitTableRegion(db)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	log.Infof("check drop table...")
	err = checkDropTable(db)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	log.Infof("check insert and delete table region...")
	err = checkInsertAndDelete(db)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	log.Infof("check shuffle...")
	err = checkShuffle(db)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	log.Infof("pass merge test...")
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

func cleanTableData(db *util.DB, fn func(*util.RegionInfo, *util.RegionInfo) error) error {
	// for merge use
	err := db.CreateTable(1)
	if err != nil {
		return errors.Trace(err)
	}

	tableRegion, err := util.CreateNewRegion(db)
	if err != nil {
		return errors.Trace(err)
	}

	err = db.Insert(tableRegion.ID)
	if err != nil {
		return errors.Trace(err)
	}

	//time.Sleep(time.Second * 60)

	nowRegions, err := util.GetRegions()
	if err != nil {
		return errors.Trace(err)
	}
	newRegion, err := util.FindNewSplitRegion(nowRegions, tableRegion)
	if err != nil {
		return errors.Trace(err)
	}
	tableRegion, err = util.GetRegionInfo(tableRegion.ID)
	if err != nil {
		return errors.Trace(err)
	}

	err = fn(newRegion, tableRegion)
	if err != nil {
		return errors.Trace(err)
	}

	// wait to merge
	err = db.InsertUntilMerge(tableRegion, newRegion)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func checkDropTable(db *util.DB) error {
	callback := func(oneRegion, otherRegion *util.RegionInfo) error {
		log.Infof("[region %d] one region's approximate size is %d before drop", oneRegion.ID, oneRegion.ApproximateSize)
		log.Infof("[region %d] other region's approximate size is %d before drop", otherRegion.ID, otherRegion.ApproximateSize)

		err := db.DropTable()
		if err != nil {
			return errors.Trace(err)
		}

		oneRegion, err = util.GetRegionInfo(oneRegion.ID)
		if err != nil {
			return errors.Trace(err)
		}
		otherRegion, err = util.GetRegionInfo(otherRegion.ID)
		if err != nil {
			return errors.Trace(err)
		}

		log.Infof("[region %d] one region's approximate size is %d after drop", oneRegion.ID, oneRegion.ApproximateSize)
		log.Infof("[region %d] other region's approximate size is %d after drop", otherRegion.ID, otherRegion.ApproximateSize)
		return nil
	}

	return cleanTableData(db, callback)
}

func checkInsertAndDelete(db *util.DB) error {
	callback := func(oneRegion, otherRegion *util.RegionInfo) error {
		log.Infof("[region %d] one region's approximate size is %d before delete", oneRegion.ID, oneRegion.ApproximateSize)
		log.Infof("[region %d] other region's approximate size is %d before delete", otherRegion.ID, otherRegion.ApproximateSize)

		err := db.DeleteAll()
		if err != nil {
			return errors.Trace(err)
		}

		oneRegion, err = util.GetRegionInfo(oneRegion.ID)
		if err != nil {
			return errors.Trace(err)
		}
		otherRegion, err = util.GetRegionInfo(otherRegion.ID)
		if err != nil {
			return errors.Trace(err)
		}

		log.Infof("[region %d] one region's approximate size is %d after delete", oneRegion.ID, oneRegion.ApproximateSize)
		log.Infof("[region %d] other region's approximate size is %d after delete", otherRegion.ID, otherRegion.ApproximateSize)
		return nil
	}

	return cleanTableData(db, callback)
}

func checkShuffle(db *util.DB) error {
	err := db.CreateTable(10)
	if err != nil {
		return errors.Trace(err)
	}

	oneRegion, err := util.CreateNewRegion(db)
	if err != nil {
		return errors.Trace(err)
	}

	err = db.Insert(oneRegion.ID)
	if err != nil {
		return errors.Trace(err)
	}

	nowRegions, err := util.GetRegions()
	if err != nil {
		return errors.Trace(err)
	}

	otherRegion, err := util.FindNewSplitRegion(nowRegions, oneRegion)
	if err != nil {
		return errors.Trace(err)
	}

	err = util.StartShuffleRegions()
	if err != nil {
		return errors.Trace(err)
	}
	for retry := 0; retry < 5; retry++ {

		oneRegion, err = util.GetRegionInfo(oneRegion.ID)
		if err != nil {
			return errors.Trace(err)
		}
		otherRegion, err = util.GetRegionInfo(otherRegion.ID)
		if err != nil {
			return errors.Trace(err)
		}

		if !util.IsSameLocation(oneRegion.Peers, otherRegion.Peers) {
			err = util.StopShuffleRegions()
			if err != nil {
				return errors.Trace(err)
			}
			break
		}

		// wait region peers to move
		time.Sleep(120 * time.Second)
	}

	err = db.DropTable()
	if err != nil {
		return errors.Trace(err)
	}

	// wait to merge
	err = db.InsertUntilMerge(oneRegion, otherRegion)
	if err != nil {
		return errors.Trace(err)
	}

	nowRegions, err = util.GetRegions()
	if err != nil {
		return errors.Trace(err)
	}

	if util.IsRegionIDExist(nowRegions, oneRegion.ID) {
		nowRegion, err := util.GetRegionInfo(oneRegion.ID)
		if err != nil {
			return errors.Trace(err)
		}
		if !util.IsSameLocation(oneRegion.Peers, nowRegion.Peers) {
			return fmt.Errorf("unexpected peer location for region %d after merge", oneRegion.ID)
		}
	} else if util.IsRegionIDExist(nowRegions, otherRegion.ID) {
		nowRegion, err := util.GetRegionInfo(otherRegion.ID)
		if err != nil {
			return errors.Trace(err)
		}
		if !util.IsSameLocation(otherRegion.Peers, nowRegion.Peers) {
			return fmt.Errorf("unexpected peer location for region %d after merge", otherRegion.ID)
		}
	}

	return nil
}
