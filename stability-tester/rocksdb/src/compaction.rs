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

use tempdir::TempDir;
use config::{new_rocksdb, ALL_CFS};

use rocksdb::*;

use basic::{do_check_exist, make_key, make_val};

fn do_check_count(db: &DB, expected: u64) {
    for cf in ALL_CFS {
        let mut actual = 0;
        let cf_handle = db.cf_handle(cf).unwrap();
        let mut iter = db.iter_cf(cf_handle);
        iter.seek(SeekKey::Start);
        while iter.valid() {
            iter.next();
            actual += 1;
        }
        assert_eq!(actual, expected);
    }
}

fn do_check_kv(db: &DB, k_start: u64, k_end: u64, val: &[u8]) {
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in k_start..k_end {
            assert_eq!(
                db.get_cf(cf_handle, make_key(i).as_bytes())
                    .unwrap()
                    .unwrap(),
                val
            );
        }
    }
}

fn test_rocksdb_compaction_in_memtable() {
    let path = TempDir::new("_rocksdb_compaction_in_memtable").unwrap();
    let db = new_rocksdb(path.path().to_str().unwrap()).unwrap();

    // the memtable size is 128MB, after insertion there should be 4 memtables
    let total = 15;
    let val = &[0; 8 << 20];
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for _ in 0..4 {
            for i in 0..total {
                db.put_cf(cf_handle, make_key(i).as_bytes(), val).unwrap();
            }
        }
    }
    db.compact_range(None, None);
    do_check_count(&db, total);
    do_check_kv(&db, 0, total, val);
}

fn test_rocksdb_compaction_cross_level_case1() {
    let path = TempDir::new("_rocksdb_compaction_cross_level_case1").unwrap();
    let db = new_rocksdb(path.path().to_str().unwrap()).unwrap();

    let total = 32;
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for _ in 0..10 {
            for i in 0..total {
                db.put_cf(cf_handle, make_key(i).as_bytes(), make_val(i).as_bytes()).unwrap();
            }
            db.flush(true).unwrap();
        }
    }
    db.compact_range(None, None);
    do_check_count(&db, total);
    do_check_exist(&db, 0, total, 0);
}

fn test_rocksdb_compaction_cross_level_case2() {
    let path = TempDir::new("_rocksdb_compaction_cross_level_case2").unwrap();
    let db = new_rocksdb(path.path().to_str().unwrap()).unwrap();

    let total = 32;
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for _ in 0..10 {
            for i in 0..total {
                db.put_cf(cf_handle, make_key(i).as_bytes(), make_val(i).as_bytes()).unwrap();
            }
            db.flush(true).unwrap();
        }
    }
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in 0..(total / 2) {
            db.delete_cf(cf_handle, make_key(i).as_bytes()).unwrap();
        }
    }
    db.compact_range(None, None);
    do_check_count(&db, total / 2);
    do_check_exist(&db, total / 2, total, total / 2);
}

pub fn test_rocksdb_compaction() {
    test_rocksdb_compaction_in_memtable();
    test_rocksdb_compaction_cross_level_case1();
    test_rocksdb_compaction_cross_level_case2();
}
