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

pub const NUM: u64 = 100000;

pub fn make_key(key: u64) -> String {
    let key = format!("kkkkkkkk{}00000000", key);
    key
}

pub fn make_val(val: u64) -> String {
    let val = format!("vvvvvvvv{}00000000", val);
    val
}

pub fn do_insert(db: &DB, k_start: u64, k_end: u64, v_start: u64) {
    for cf in ALL_CFS {
        let mut v = v_start;
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in k_start..k_end {
            db.put_cf(cf_handle, make_key(i).as_bytes(), make_val(v).as_bytes())
                .unwrap();
            v += 1;
        }
    }
}

pub fn do_delete(db: &DB, k_start: u64, k_end: u64) {
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in k_start..k_end {
            db.delete_cf(cf_handle, make_key(i).as_bytes()).unwrap();
        }
    }
}

pub fn do_check_exist(db: &DB, k_start: u64, k_end: u64, v_start: u64) {
    for cf in ALL_CFS {
        let mut v = v_start;
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in k_start..k_end {
            assert_eq!(
                db.get_cf(cf_handle, make_key(i).as_bytes())
                    .unwrap()
                    .unwrap(),
                make_val(v).as_bytes()
            );
            v += 1;
        }
    }
}

pub fn do_check_non_exist(db: &DB, k_start: u64, k_end: u64) {
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in k_start..k_end {
            assert!(
                db.get_cf(cf_handle, make_key(i).as_bytes())
                    .unwrap()
                    .is_none()
            );
        }
    }
}

pub fn varify_db_equal(db1: &DB, db2: &DB) {
    for cf in ALL_CFS {
        let cf_handle1 = db1.cf_handle(cf).unwrap();
        let cf_handle2 = db2.cf_handle(cf).unwrap();
        let mut iter1 = db1.iter_cf(cf_handle1);
        let mut iter2 = db2.iter_cf(cf_handle2);
        iter1.seek(SeekKey::Start);
        iter2.seek(SeekKey::Start);
        while iter1.valid() && iter2.valid() {
            assert_eq!(iter1.key(), iter2.key());
            assert_eq!(iter1.value(), iter2.value());
            iter1.next();
            iter2.next();
        }
        assert_eq!(iter1.valid(), iter2.valid());
    }
}

fn test_rocksdb_insert() {
    let path = TempDir::new("_rocksdb_insert").unwrap();
    let db = new_rocksdb(path.path().to_str().unwrap()).unwrap();
    do_insert(&db, 0, NUM, 0);
    do_check_exist(&db, 0, NUM, 0);
}

fn test_rocksdb_update() {
    let path = TempDir::new("_rocksdb_update").unwrap();
    let db = new_rocksdb(path.path().to_str().unwrap()).unwrap();
    do_insert(&db, 0, NUM, 0);
    do_insert(&db, 0, NUM, NUM);
    do_check_exist(&db, 0, NUM, NUM);
}

fn test_rocksdb_delete() {
    let path = TempDir::new("_rocksdb_delete").unwrap();
    let db = new_rocksdb(path.path().to_str().unwrap()).unwrap();
    do_insert(&db, 0, NUM, 0);
    do_delete(&db, 0, NUM / 2);
    do_check_non_exist(&db, 0, NUM / 2);
    do_check_exist(&db, NUM / 2, NUM, NUM / 2);
}

pub fn test_rocksdb_basic() {
    test_rocksdb_insert();
    test_rocksdb_delete();
    test_rocksdb_update();
}
