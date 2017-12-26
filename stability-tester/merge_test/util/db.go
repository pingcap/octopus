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
	"github.com/ngaut/log"
)

var (
	createSQL = `CREATE TABLE IF NOT EXISTS table%d (
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
	id   = 1
	flag = 1
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
}

func InitDB(user, password, host string, port int) (*DB, error) {
	var err error
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db.SetMaxIdleConns(concurrency + 1)
	_, err = db.Exec("drop database if exists merge_test;")
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, err = db.Exec("create database merge_test; use merge_test;")
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &DB{DB: db}, nil
}

func (db *DB) Insert(ctx context.Context) error {
	var wg sync.WaitGroup
	flag = 1
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

				err := db.InsertBatch(id-1, 100)
				if err != nil {
					log.Fatal(err)
				}
				flag++
			}
		}(ctx)
	}

	wg.Wait()
	return nil
}

func (db *DB) InsertUntilSplit(regionID uint64) error {
	time.Sleep(time.Second * 10)
	originRegion, err := GetRegionInfo(regionID)
	if err != nil {
		return errors.Trace(err)
	}

	flag = 1
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	go func(ctx context.Context) {
		wg.Add(1)
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			err := db.InsertBatch(id-1, 100)
			if err != nil {
				log.Fatal(err)
			}
			flag++
		}
	}(ctx)

	for {
		nowRegion, err := GetRegionInfo(regionID)
		if err != nil {
			return errors.Trace(err)
		}

		log.Infof("[region %d] approximate size is : %d", regionID, nowRegion.ApproximateSize)

		if nowRegion.StartKey != originRegion.StartKey || nowRegion.EndKey != originRegion.EndKey {
			log.Infof("[region %d] region split", regionID)
			cancel()
			break
		}

		time.Sleep(time.Second * 60)
	}
	wg.Wait()
	return nil
}

func (db *DB) InsertUntilMerge(tableRegions []*RegionInfo) error {
	ctx, cancel := context.WithCancel(context.Background())

	tableID := GetTableID(tableRegions[0])
	for _, region := range tableRegions {
		if GetTableID(region) != tableID {
			return fmt.Errorf("unexpected table regions")
		}
	}

	var wg sync.WaitGroup
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

				err := db.InsertBatch(id-2, 20)
				if err != nil {
					log.Fatal(err)
				}

			}
		}(ctx)
	}
	defer wg.Wait()
	defer cancel()

	for {
		time.Sleep(time.Second * 60)

		isMerge, isChange := false, false
		for i, region := range tableRegions {
			nowRegion, err := GetRegionInfo(region.ID)
			if err != nil {
				return errors.Trace(err)
			}

			if nowRegion.ID == 0 {
				isMerge = true
				log.Infof("region %d was merged", region.ID)
			}

			if GetTableID(nowRegion) != tableID {
				return fmt.Errorf("region %d's tableID has changed", region.ID)
			}

			if nowRegion.StartKey != region.StartKey || nowRegion.EndKey != region.EndKey {
				isChange = true
				log.Infof("region %d's key range has changed", region.ID)
			}

			log.Infof("[region %d] approximate size is %d", nowRegion.ID, nowRegion.ApproximateSize)
			tableRegions[i] = nowRegion
		}

		if !isMerge && isChange {
			return fmt.Errorf("region change unexpectedly when insert")
		} else if isMerge && isChange {
			return nil
		}

		if err := checkRegionsTableID(tableID, tableRegions); err != nil {
			return errors.Trace(err)
		}
	}
}

func (db *DB) InsertBatch(tableId int, batchSize int) error {
	now := time.Now().Unix()
	table := fmt.Sprintf("table%d", tableId)

	values := make([]string, batchSize)
	for i := 0; i < batchSize; i++ {
		values[i] = fmt.Sprintf(`(%d,%d,%d`, rand.Int31n(100000), flag, now)
		for j := 1; j <= 30; j++ {
			values[i] += fmt.Sprintf(",\"%s\"", randSeq(255))
		}
		values[i] += ")"
	}

	sql := fmt.Sprintf(`use merge_test; INSERT INTO %s VALUES %s;`, table, strings.Join(values, ","))
	_, err := db.Exec(sql)
	return errors.Trace(err)
}

func (db *DB) deleteBatch() error {
	flag--
	sql := fmt.Sprintf(`use merge_test; DELETE FROM table%d WHERE flag=%d;`, id-1, flag)
	_, err := db.Exec(sql)
	return errors.Trace(err)
}

func (db *DB) DeleteAll() error {
	for flag != 1 {
		if err := db.deleteBatch(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (db *DB) DropTable() error {
	sql := fmt.Sprintf(`use merge_test; DROP TABLE table%d;`, id-1)
	_, err := db.Exec(sql)

	return errors.Trace(err)
}

func (db *DB) CreateTable(count int) error {
	for i := 0; i < count; i++ {
		query := fmt.Sprintf(createSQL, id)
		id++
		_, err := db.Exec(query)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
