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

package util

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

var (
	createSQL = `DROP TABLE IF EXISTS merge_test%d;
	CREATE TABLE merge_test%d (
		id bigint,
		flag bigint,
		timestamp bigint,
		reserved1 text,
		reserved2 text,
		reserved3 text,
		reserved4 text,
		reserved5 text,
		reserved6 text,
		reserved7 text,
		reserved8 text,
		reserved9 text,
		reserved10 text,
		reserved11 text,
		reserved12 text,
		reserved13 text,
		reserved14 text,
		reserved15 text,
		reserved16 text,
		reserved17 text,
		reserved18 text,
		reserved19 text,
		reserved20 text,
		reserved21 text,
		reserved22 text,
		reserved23 text,
		reserved24 text,
		reserved25 text,
		reserved26 text,
		reserved27 text,
		reserved28 text,
		reserved29 text,
		reserved30 text
	);`
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

const concurrency = 500

type DB struct {
	*sql.DB
	id   int
	flag int
}

func InitDB(user, password, host string, port int) (*DB, error) {
	var err error
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/test", user, password, host, port)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db.SetMaxIdleConns(concurrency + 1)
	return &DB{DB: db, id: 1, flag: 1}, nil
}

func (db *DB) InsertUntilSplitToRegions(tableID int64, n int) error {
	ctx, cancel := context.WithCancel(context.Background())
	wg := db.Insert(ctx, db.id-1, 100)
	defer wg.Wait()
	defer cancel()

	for {
		regions, err := GetRegions()
		if err != nil {
			return errors.Trace(err)
		}

		find := 0
		for _, region := range regions {
			if GetTableID(region) == tableID {
				find++
			}
		}

		log.Infof("find %d regions for table %d", find, tableID)
		if find >= n {
			break
		}

		time.Sleep(time.Second * 60)
	}
	log.Info("------------------------------------------------")

	return nil
}

func (db *DB) WaitUntilMerge(tableRegions []*RegionInfo) error {
	tableID := GetTableID(tableRegions[0])
	for _, region := range tableRegions {
		if GetTableID(region) != tableID {
			return fmt.Errorf("unexpected table regions")
		}
	}

	for {
		time.Sleep(time.Second * 60)

		isMerge := false
		for i := len(tableRegions) - 1; i >= 0; i-- {
			region := tableRegions[i]
			nowRegion, err := GetRegionInfo(region.ID)
			if err != nil {
				return errors.Trace(err)
			}

			if nowRegion.ID == 0 {
				isMerge = true
				tableRegions = append(tableRegions[:i], tableRegions[i+1:]...)
				log.Infof("region %d was merged", region.ID)
				continue
			}

			if GetTableID(nowRegion) != tableID {
				return fmt.Errorf("region %d's tableID has changed", region.ID)
			}

			log.Infof("[region %d] approximate size is %d", nowRegion.ID, nowRegion.ApproximateSize)
			tableRegions[i] = nowRegion
		}
		log.Info("------------------------------------------------")
		if isMerge && len(tableRegions) <= 1 {
			return nil
		}

		var err error
		tableRegions, err = CheckRegionsTableID(tableID, tableRegions)
		if err != nil {
			return errors.Trace(err)
		}
	}
}

func (db *DB) WaitUntilTimeout(tableRegions []*RegionInfo, timeout time.Duration) error {
	tableID := GetTableID(tableRegions[0])
	for _, region := range tableRegions {
		if GetTableID(region) != tableID {
			return fmt.Errorf("unexpected table regions")
		}
	}

	for _, region := range tableRegions {
		nowRegion, err := GetRegionInfo(region.ID)
		if err != nil {
			return errors.Trace(err)
		}
		log.Infof("[region %d] approximate size is %d", nowRegion.ID, nowRegion.ApproximateSize)
	}
	log.Info("------------------------------------------------")

	time.Sleep(timeout)

	for _, region := range tableRegions {
		nowRegion, err := GetRegionInfo(region.ID)
		if err != nil {
			return errors.Trace(err)
		}

		if nowRegion.ID == 0 {
			log.Infof("[region %d] was merged", region.ID)
			continue
		}
		log.Infof("[region %d] approximate size is %d", nowRegion.ID, nowRegion.ApproximateSize)
	}
	log.Info("------------------------------------------------")

	return nil
}

func (db *DB) InsertIntoNewTable(ctx context.Context, batchSize int) {
	wg := db.Insert(ctx, db.id-1, batchSize)
	wg.Wait()
}

func (db *DB) Insert(ctx context.Context, tableId int, batchSize int) *sync.WaitGroup {
	var wg sync.WaitGroup
	db.flag = 1
	for i := 0; i < concurrency; i++ {
		go func(ctx context.Context) {
			wg.Add(1)
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				err := db.insertBatch(tableId, batchSize)
				if err != nil {
					log.Fatal(err)
				}
				db.flag++
			}
		}(ctx)
	}

	return &wg
}

func (db *DB) insertBatch(tableId int, batchSize int) error {
	now := time.Now().Unix()
	table := fmt.Sprintf("merge_test%d", tableId)

	values := make([]string, batchSize)
	for i := 0; i < batchSize; i++ {
		values[i] = fmt.Sprintf(`(%d,%d,%d`, rand.Int31n(100000), db.flag, now)
		for j := 1; j <= 30; j++ {
			values[i] += fmt.Sprintf(",\"%s\"", randSeq(255))
		}
		values[i] += ")"
	}

	sql := fmt.Sprintf(`INSERT INTO %s VALUES %s;`, table, strings.Join(values, ","))
	_, err := db.Exec(sql)
	return errors.Trace(err)
}

func (db *DB) deleteBatch() error {
	sql := fmt.Sprintf(`DELETE FROM merge_test%d WHERE flag=%d;`, db.id-1, db.flag)
	_, err := db.Exec(sql)
	return errors.Trace(err)
}

func (db *DB) DeleteAll() error {
	err := db.AddDeleteIndex()
	if err != nil {
		return errors.Trace(err)
	}
	for db.flag > 1 {
		db.flag--
		if err := db.deleteBatch(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (db *DB) DeletePartial() error {
	err := db.AddDeleteIndex()
	if err != nil {
		return errors.Trace(err)
	}
	for db.flag > 1 {
		db.flag -= 2
		if err := db.deleteBatch(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (db *DB) DropTable() error {
	sql := fmt.Sprintf(`DROP TABLE merge_test%d;`, db.id-1)
	_, err := db.Exec(sql)

	return errors.Trace(err)
}

func (db *DB) TruncateTable() error {
	sql := fmt.Sprintf(`TRUNCATE TABLE merge_test%d`, db.id-1)
	_, err := db.Exec(sql)

	return errors.Trace(err)
}

func (db *DB) CreateTable() (int64, error) {
	query := fmt.Sprintf(createSQL, db.id, db.id)
	db.id++
	_, err := db.Exec(query)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return GetTableIDByName(fmt.Sprintf("merge_test%d", db.id-1))
}

func (db *DB) AddIndex() error {
	sql := fmt.Sprintf(`ALTER TABLE merge_test%d ADD INDEX idx (reserved1);`, db.id-1)
	_, err := db.Exec(sql)

	return errors.Trace(err)
}

func (db *DB) AddDeleteIndex() error {
	sql := fmt.Sprintf(`ALTER TABLE merge_test%d ADD INDEX deleteIdx (flag);`, db.id-1)
	_, err := db.Exec(sql)

	return errors.Trace(err)
}

func (db *DB) DropIndex() error {
	sql := fmt.Sprintf(`DROP INDEX idx on merge_test%d;`, db.id-1)
	_, err := db.Exec(sql)

	return errors.Trace(err)
}
