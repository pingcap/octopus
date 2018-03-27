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

use basic::{do_check_exist, do_check_non_exist, do_insert, make_key, make_val};

fn do_delete_range(db: &DB, k_start: u64, k_end: u64) {
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        db.delete_range_cf(
            cf_handle,
            make_key(k_start).as_bytes(),
            make_key(k_end).as_bytes(),
        ).unwrap();
    }
}

fn test_rocksdb_delete_range_in_memtable() {
    let path = TempDir::new("_rocksdb_delete_range_in_memtable").unwrap();
    let db = new_rocksdb(path.path().to_str().unwrap()).unwrap();
    let beg = 11111;
    let end = 99999;
    do_insert(&db, beg, end, beg);
    do_delete_range(&db, beg, end / 2);
    do_check_non_exist(&db, beg, end / 2);
    do_check_exist(&db, end / 2, end, end / 2);
}

fn test_rocksdb_delete_range_for_level0() {
    let path = TempDir::new("_rocksdb_delete_range_for_level0").unwrap();
    let db = new_rocksdb(path.path().to_str().unwrap()).unwrap();
    let beg = 11;
    let end = 44;
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        let mut i = beg;
        while i < end {
            for _ in 0..10 {
                db.put_cf(cf_handle, make_key(i).as_bytes(), make_val(i).as_bytes()).unwrap();
                i += 1;
            }
            db.flush(true).unwrap();
        }
    }
    do_delete_range(&db, beg + 10, end - 10);
    do_check_exist(&db, beg, beg + 10, beg);
    do_check_exist(&db, end - 10, end, end - 10);
    do_check_non_exist(&db, beg + 10, end - 10);
}

fn test_rocksdb_delete_cross_level() {
    let path = TempDir::new("_rocksdb_delete_range_cross_level").unwrap();
    let db = new_rocksdb(path.path().to_str().unwrap()).unwrap();
    let beg = 11;
    let end = 100;
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        let mut i = beg;
        // write to level 0 and level 1
        while i < end {
            for _ in 0..10 {
                db.put_cf(cf_handle, make_key(i).as_bytes(), make_val(i).as_bytes()).unwrap();
                i += 1;
            }
            db.flush(true).unwrap();
        }
        // write to memtable
        for j in beg..end {
            db.put_cf(cf_handle, make_key(j).as_bytes(), make_val(j).as_bytes()).unwrap();
        }
    }
    do_delete_range(&db, beg + 10, end - 10);
    do_check_exist(&db, beg, beg + 10, beg);
    do_check_exist(&db, end - 10, end, end - 10);
    do_check_non_exist(&db, beg + 10, end - 10);
}

pub fn test_rocksdb_delete_range() {
    test_rocksdb_delete_range_in_memtable();
    test_rocksdb_delete_range_for_level0();
    test_rocksdb_delete_cross_level();
}
