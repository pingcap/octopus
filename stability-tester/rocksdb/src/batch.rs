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

use basic::{make_key, make_val, NUM, do_check_exist, do_check_non_exist};

fn do_batch_insert(db: &DB, batch: &WriteBatch, k_start: u64, k_end: u64, v_start: u64) {
    for cf in ALL_CFS {
        let mut v = v_start;
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in k_start..k_end {
            batch.put_cf(cf_handle, make_key(i).as_bytes(), make_val(v).as_bytes())
                .unwrap();
            v += 1;
        }
    }
}

fn do_batch_delete(db: &DB, batch: &WriteBatch, k_start: u64, k_end: u64) {
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in k_start..k_end {
            batch.delete_cf(cf_handle, make_key(i).as_bytes()).unwrap();
        }
    }
}

fn test_rocksdb_writebatch_insert() {
    let path = TempDir::new("_rocksdb_writebatch_insert").unwrap();
    let db = new_rocksdb(path.path().to_str().unwrap()).unwrap();
    let batch = WriteBatch::new();
    do_batch_insert(&db, &batch, 0, NUM, 0);
    db.write(batch).unwrap();
    do_check_exist(&db, 0, NUM, 0);
}

fn test_rocksdb_writebatch_update() {
    let path = TempDir::new("_rocksdb_writebatch_update").unwrap();
    let db = new_rocksdb(path.path().to_str().unwrap()).unwrap();
    let batch = WriteBatch::new();
    do_batch_insert(&db, &batch, 0, NUM, 0);
    do_batch_insert(&db, &batch, 0, NUM, NUM);
    db.write(batch).unwrap();
    do_check_exist(&db, 0, NUM, NUM);
}

fn test_rocksdb_writebatch_delete() {
    let path = TempDir::new("_rocksdb_writebatch_delete").unwrap();
    let db = new_rocksdb(path.path().to_str().unwrap()).unwrap();
    let batch = WriteBatch::new();
    do_batch_insert(&db, &batch, 0, NUM, 0);
    do_batch_delete(&db, &batch, 0, NUM / 2);
    db.write(batch).unwrap();
    do_check_non_exist(&db, 0, NUM / 2);
    do_check_exist(&db, NUM / 2, NUM, NUM / 2);
}

pub fn test_rocksdb_writebatch() {
    test_rocksdb_writebatch_insert();
    test_rocksdb_writebatch_update();
    test_rocksdb_writebatch_delete();
}
