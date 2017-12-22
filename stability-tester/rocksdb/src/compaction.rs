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

fn test_rocksdb_compaction_case1() {
    let path = TempDir::new("_rocksdb_compact").unwrap();
    let db = new_rocksdb(path.path().to_str().unwrap()).unwrap();

    let total = 10;
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        for i in 0..total {
            db.put_cf(cf_handle, make_key(i).as_bytes(), make_val(i).as_bytes()).unwrap();
            db.flush(true).unwrap();
        }
    }
    db.compact_range(None, None);
    do_check_count(&db, total);
    do_check_exist(&db, 0, total, 0);
}

pub fn test_rocksdb_compaction() {
    test_rocksdb_compaction_case1();
}
