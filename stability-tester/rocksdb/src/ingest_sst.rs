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

use std::fs;
use tempdir::TempDir;
use config::{new_rocksdb, ALL_CFS};

use rocksdb::*;

use basic::{do_check_exist, make_key, make_val, varify_db_equal};

fn do_gen_sst(opt: ColumnFamilyOptions, cf: &CFHandle, path: &str, start: u64, end: u64) {
    let _ = fs::remove_file(path);
    let env_opt = EnvOptions::new();
    let mut writer = SstFileWriter::new_cf(env_opt, opt, cf);
    writer.open(path).unwrap();
    for i in start..end {
        writer
            .put(make_key(i).as_bytes(), make_val(i).as_bytes())
            .unwrap();
    }
    writer.finish().unwrap();
}

fn gen_sst_from_db(opt: ColumnFamilyOptions, cf: &CFHandle, path: &str, db: &DB) {
    let _ = fs::remove_file(path);
    let env_opt = EnvOptions::new();
    let mut writer = SstFileWriter::new_cf(env_opt, opt, cf);
    writer.open(path).unwrap();
    let mut iter = db.iter_cf(cf);
    iter.seek(SeekKey::Start);
    while iter.valid() {
        writer.put(iter.key(), iter.value()).unwrap();
        iter.next();
    }
    writer.finish().unwrap();
}

fn test_rocksdb_ingest_sst_basic() {
    let path = TempDir::new("_rocksdb_ingest_sst_basic").unwrap();
    let db = new_rocksdb(path.path().to_str().unwrap()).unwrap();

    let sst_dir = TempDir::new("_rocksdb_sst_file_basic").unwrap();
    let start = 11111;
    let end = 99999;
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        let file_path = sst_dir.path().join(cf);
        do_gen_sst(
            db.get_options(),
            cf_handle,
            file_path.to_str().unwrap(),
            start,
            end,
        );
        db.ingest_external_file_cf(
            cf_handle,
            &IngestExternalFileOptions::new(),
            &[file_path.to_str().unwrap()],
        ).unwrap();
    }
    do_check_exist(&db, start, end, start);
}

fn test_rocksdb_ingest_sst_from_db() {
    let path = TempDir::new("_rocksdb_ingest_sst_from_db").unwrap();
    let db = new_rocksdb(path.path().to_str().unwrap()).unwrap();

    let start = 11111;
    let end = 99999;
    let sst_dir = TempDir::new("_rocksdb_sst_file_from_db").unwrap();
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        let file_path = sst_dir.path().join(cf);
        do_gen_sst(
            db.get_options(),
            cf_handle,
            file_path.to_str().unwrap(),
            start,
            end,
        );
        db.ingest_external_file_cf(
            cf_handle,
            &IngestExternalFileOptions::new(),
            &[file_path.to_str().unwrap()],
        ).unwrap();
    }

    let dst_path = TempDir::new("_rocksdb_ingest_sst_dst_from_db").unwrap();
    let dst_db = new_rocksdb(dst_path.path().to_str().unwrap()).unwrap();
    let dst_sst_dir = TempDir::new("_rocksdb_sst_file_dst_from_db").unwrap();
    for cf in ALL_CFS {
        let cf_handle = db.cf_handle(cf).unwrap();
        let file_path = dst_sst_dir.path().join(cf);
        gen_sst_from_db(
            db.get_options(),
            cf_handle,
            file_path.to_str().unwrap(),
            &db,
        );
        let dst_cf_handle = dst_db.cf_handle(cf).unwrap();
        dst_db
            .ingest_external_file_cf(
                dst_cf_handle,
                &IngestExternalFileOptions::new(),
                &[file_path.to_str().unwrap()],
            )
            .unwrap();
    }
    varify_db_equal(&db, &dst_db);
}

pub fn test_rocksdb_ingest_sst() {
    test_rocksdb_ingest_sst_basic();
    test_rocksdb_ingest_sst_from_db();
}
