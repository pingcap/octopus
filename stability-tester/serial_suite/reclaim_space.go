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

package serial_suite

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/octopus/stability-tester/config"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"golang.org/x/net/context"
)

// ReclaimSpaceCase simulates a case which creates tables,
// inserts data into tables and drops tables repeadedly.
type ReclaimSpaceCase struct {
	cfg          *config.ReclaimSpaceCaseConfig
	host         string
	port         int
	user         string
	password     string
	pd           string
	db           *sql.DB
	store        kv.Storage
	pendingTasks []kv.KeyRange
	maxTaskTS    uint64
}

// NewReclaimSpaceCase creates a ReclaimSpaceCase.
func NewReclaimSpaceCase(cfg *config.Config) Case {
	return &ReclaimSpaceCase{
		cfg:      &cfg.SerialSuite.ReclaimSpace,
		host:     cfg.Host,
		port:     cfg.Port,
		user:     cfg.User,
		password: cfg.Password,
		pd:       cfg.PD,
	}
}

// Initialize creates the tables.
func (c *ReclaimSpaceCase) Initialize() error {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/test", c.user, c.password, c.host, c.port)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return errors.Trace(err)
	}
	c.db = db
	tidb.RegisterStore("tikv", tikv.Driver{})
	store, err := tidb.NewStore(fmt.Sprintf("tikv://%s?disableGC=true", c.pd))
	if err != nil {
		return errors.Trace(err)
	}
	c.store = store
	return nil
}

func (c *ReclaimSpaceCase) String() string {
	return "reclaim_space"
}

// Execute implements Case Execute interface.
func (c *ReclaimSpaceCase) Execute(ctx context.Context) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				log.Info("[reclaim space] createTable")
				if err := c.createTable(); err != nil {
					log.Errorf("[reclaim space] fail to create tables %v", err)
				}

				log.Info("[reclaim space] insertTable")
				if err := c.insertTable(); err != nil {
					log.Errorf("[reclaim space] insert err %v", err)
				}

				log.Info("[reclaim space] dropTable")
				if err := c.dropTable(); err != nil {
					log.Errorf("[reclaim space] fail to drop tables %v", err)
				}

				log.Info("[reclaim space] checkTableExist")
				if err := c.checkTableExist(); err != nil {
					log.Errorf("[reclaim space] fail to check table %v", err)
				}

				log.Info("[reclaim space] checkIndexExist")
				if err := c.checkIndexExist(); err != nil {
					log.Errorf("[reclaim space] fail to check index %v", err)
				}

				log.Info("[reclaim space] wait drop")
				if err := c.checkIfDrop(); err != nil {
					log.Errorf("[reclaim space] fail to drop tables in kv %v", err)
				}

				log.Infof("[reclaim space] loop finish")
			}
		}
	}()
	return nil
}

func (c *ReclaimSpaceCase) createTable() error {
	createSQL := `CREATE TABLE IF NOT EXISTS table%d (
        game_id int(11) DEFAULT NULL,
        server_id varchar(16) DEFAULT NULL,
        operator_id int(11) DEFAULT NULL,
        channel_id varchar(32) DEFAULT NULL,
        ad_info varchar(32) DEFAULT NULL,
        phone_type varchar(32) DEFAULT NULL,
        os_system varchar(32) DEFAULT NULL,
        resolution varchar(32) DEFAULT NULL,
        net_type varchar(32) DEFAULT NULL,
        client_ip varchar(16) DEFAULT NULL,
        account varchar(64) DEFAULT NULL,
        user_id varchar(32) DEFAULT NULL,
        reg_time int(11) DEFAULT NULL,
        paid_user int(11) DEFAULT NULL,
        vip int(11) DEFAULT NULL,
        role_guid bigint(30) DEFAULT NULL,
        role_name varchar(32) DEFAULT NULL,
        class int(11) DEFAULT NULL,
        gender int(11) DEFAULT NULL,
        level int(11) DEFAULT NULL,
        gm_level int(11) DEFAULT NULL,
        gold bigint(30) DEFAULT NULL,
        diamond bigint(30) DEFAULT NULL,
        binding_diamond bigint(30) DEFAULT NULL,
        guild_id bigint(30) DEFAULT NULL,
        timestamp int(11) DEFAULT NULL,
        summary varchar(128) DEFAULT NULL,
        reserved_28 varchar(32) DEFAULT NULL,
        reserved_29 varchar(32) DEFAULT NULL,
        reserved_30 varchar(32) DEFAULT NULL,
        reserved_31 varchar(32) DEFAULT NULL,
        reserved_32 varchar(32) DEFAULT NULL,
        reserved_33 varchar(32) DEFAULT NULL,
        reserved_34 varchar(32) DEFAULT NULL,
        reserved_35 varchar(32) DEFAULT NULL,
        reserved_36 varchar(32) DEFAULT NULL,
        reserved_37 varchar(32) DEFAULT NULL,
        reserved_38 varchar(32) DEFAULT NULL,
        reserved_39 varchar(32) DEFAULT NULL,
        reserved_40 varchar(32) DEFAULT NULL,
        reserved_41 varchar(32) DEFAULT NULL,
        reserved_42 varchar(32) DEFAULT NULL,
        reserved_43 varchar(32) DEFAULT NULL,
        reserved_44 varchar(32) DEFAULT NULL,
        reserved_45 varchar(32) DEFAULT NULL,
        reserved_46 varchar(32) DEFAULT NULL,
        reserved_47 varchar(32) DEFAULT NULL,
        reserved_48 varchar(32) DEFAULT NULL,
        reserved_49 varchar(32) DEFAULT NULL,
        reserved_50 int(11) DEFAULT NULL,
        KEY index_1 (timestamp),
        KEY index_2 (server_id,timestamp),
        KEY index_3 (server_id,role_guid,timestamp) ) ENGINE=InnoDB;`

	for i := 1; i <= c.cfg.TableCount; i++ {
		q := fmt.Sprintf(createSQL, i)

		if _, err := c.db.Exec(q); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *ReclaimSpaceCase) insertTable() error {
	var wg sync.WaitGroup
	var e error
	wg.Add(c.cfg.Concurrency)
	for i := 0; i < c.cfg.Concurrency; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < c.cfg.Items; j++ {
				if err := c.insert(); err != nil {
					e = errors.Trace(err)
				}
			}
		}()
	}
	wg.Wait()
	if e != nil {
		return e
	}
	return nil
}

func (c *ReclaimSpaceCase) insert() error {
	valueFormat := `(147,%s,2017,NULL,
    NULL,NULL,NULL,NULL,NULL,NULL,'fengqing3',
    'fengqing3',1489818789,0,1,%d,
    '贝蒂姬恩',1,0,100,0,10000000,9000000,9000000,0,
    %d,NULL,'1489995689','1489818791',
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
    NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1)`
	now := time.Now().Unix()
	table := fmt.Sprintf("table%d", rand.Intn(c.cfg.TableCount)+1)

	values := make([]string, c.cfg.BatchSize)
	for i := 0; i < c.cfg.BatchSize; i++ {
		serverID := fmt.Sprintf("%d", rand.Int31n(10000))
		roleGUID := rand.Int63()

		values[i] = fmt.Sprintf(valueFormat, serverID, roleGUID, now)
	}

	q := fmt.Sprintf(`INSERT INTO %s VALUES %s`, table, strings.Join(values, ","))
	if _, err := c.db.Exec(q); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *ReclaimSpaceCase) loadPendingTasks() error {
	c.maxTaskTS = 0
	c.pendingTasks = nil

	q := fmt.Sprintf(`SELECT start_key, end_key, ts FROM MYSQL.GC_DELETE_RANGE`)
	rows, err := c.db.Query(q)
	if err != nil {
		return errors.Trace(err)
	}
	for rows.Next() {
		var ts uint64
		var startKey, endKey string
		if err := rows.Scan(&startKey, &endKey, &ts); err != nil {
			return errors.Trace(err)
		}
		start, err := hex.DecodeString(startKey)
		if err != nil {
			return errors.Trace(err)
		}
		end, err := hex.DecodeString(endKey)
		if err != nil {
			return errors.Trace(err)
		}
		if c.maxTaskTS < ts {
			c.maxTaskTS = ts
		}
		c.pendingTasks = append(c.pendingTasks, kv.KeyRange{
			StartKey: start,
			EndKey:   end,
		})
	}
	if err := rows.Err(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *ReclaimSpaceCase) dropTable() error {
	for i := 1; i <= c.cfg.TableCount; i++ {
		q := fmt.Sprintf(`DROP TABLE IF EXISTS table%d`, i)
		if _, err := c.db.Exec(q); err != nil {
			return errors.Trace(err)
		}
	}
	err := c.loadPendingTasks()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *ReclaimSpaceCase) isRangeDropped() (bool, error) {
	rows, err := c.db.Query(fmt.Sprintf(`SELECT * FROM MYSQL.GC_DELETE_RANGE WHERE ts <= %v limit 1`, c.maxTaskTS))
	if err != nil {
		return false, errors.Trace(err)
	}
	defer rows.Close()
	return !rows.Next(), nil
}

func (c *ReclaimSpaceCase) checkIfDrop() error {
	// Wait until ranges are dropped.
	for {
		ok, err := c.isRangeDropped()
		if ok {
			break
		}
		if err != nil {
			log.Warnf("[reclaim space] check drop err: %v", err)
		}
		log.Infof("[reclaim space] still not dropped")
		time.Sleep(time.Duration(c.cfg.SleepTime) * time.Second)
	}

	// All records should be gone.
	for _, keyrange := range c.pendingTasks {
		sk := keyrange.StartKey
		ek := keyrange.EndKey
		err := kv.RunInNewTxn(c.store, false, func(txn kv.Transaction) error {
			it, err := txn.Seek(sk)
			if err != nil {
				return errors.Trace(err)
			}
			if it.Valid() && it.Key().Cmp(ek) != 1 {
				log.Fatalf("[reclaim space] table not dropped from kv store, start: %q, end: %q", sk, ek)
			}
			return nil
		})
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (c *ReclaimSpaceCase) checkTableExist() error {
	q := fmt.Sprintf(`SHOW TABLES`)
	results, err := c.db.Exec(q)
	if err != nil {
		return errors.Trace(err)
	}
	num, err := results.RowsAffected()
	if err != nil {
		return errors.Trace(err)
	}
	if num != 0 {
		panic("[reclaim space] table still exists")
	}
	return nil
}

func (c *ReclaimSpaceCase) checkIndexExist() error {
	q := fmt.Sprintf(`SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = 'test'`)
	results, err := c.db.Exec(q)
	if err != nil {
		return errors.Trace(err)
	}
	num, err := results.RowsAffected()
	if err != nil {
		return errors.Trace(err)
	}
	if num != 0 {
		panic("[reclaim space] index still exists")
	}
	return nil
}

func init() {
	RegisterSuite("reclaim_space", NewReclaimSpaceCase)
}
