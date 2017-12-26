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

func (db *DB) Insert(regionID uint64) error {
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

func (db *DB) InsertUntilMerge(oneRegion, otherRegion *RegionInfo) error {
	ctx, cancel := context.WithCancel(context.Background())

	var tableID int64
	oneKey := Key{}
	fmt.Sscanf("\""+oneRegion.StartKey+"\"", "%q", &oneKey)
	otherKey := Key{}
	fmt.Sscanf("\""+otherRegion.StartKey+"\"", "%q", &otherKey)
	if oneKey.TableID() != otherKey.TableID() {
		return fmt.Errorf("unexpected table id for two regions")
	} else {
		tableID = oneKey.TableID()
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

		one, err := GetRegionInfo(oneRegion.ID)
		if err != nil {
			return errors.Trace(err)
		}
		other, err := GetRegionInfo(otherRegion.ID)
		if err != nil {
			return errors.Trace(err)
		}

		x := one.ID != 0
		y := other.ID != 0
		if x && !y {
			log.Infof("region %d merge into region %d", other.ID, one.ID)
			return nil
		} else if !x && y {
			log.Infof("region %d merge into region %d", one.ID, other.ID)
			return nil
		}

		log.Infof("[region %d] one region's approximate size is %d", one.ID, one.ApproximateSize)
		log.Infof("[region %d] other region's approximate size is %d", other.ID, other.ApproximateSize)

		if one.StartKey != oneRegion.StartKey || one.EndKey != oneRegion.EndKey {
			return fmt.Errorf("region %d's key range has changed", one.ID)
		} else if other.StartKey != otherRegion.StartKey || other.EndKey != otherRegion.EndKey {
			return fmt.Errorf("region %d's key range has changed", other.ID)
		}

		if err = checkRegionsTableID(tableID, one.ID, other.ID); err != nil {
			return errors.Trace(err)
		}
		oneRegion, otherRegion = one, other
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
