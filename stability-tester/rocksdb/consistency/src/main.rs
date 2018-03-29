// Copyright 2018 PingCAP, Inc.
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

#[macro_use]
extern crate log;
extern crate rand;
extern crate rocksdb;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate tempdir;
extern crate toml;
mod config;
mod util;

use std::fs;
use std::thread;
use std::sync::{mpsc, Arc};
use std::collections::{HashSet, BTreeMap};
use std::time;

use tempdir::TempDir;
use config::{new_rocksdb, ALL_CFS, CF_DEFAULT, CF_WRITE};
use rand::Rng;
use rocksdb::*;

const BATCH_SIZE: usize = 256;
const INGEST_SIZE: usize = 1024;
const UNIT: usize = 1000_000;

fn get_cf_handle(db: &DB, len: usize) -> &CFHandle {
    if len > 64 {
        db.cf_handle(CF_DEFAULT).unwrap()
    } else {
        db.cf_handle(CF_WRITE).unwrap()
    }
}

// the suffix stands for timestamp
fn make_key(key: u64) -> Vec<u8> {
    format!("{}00000000", key).as_bytes().to_owned()
}

fn make_val(val: u64) -> Vec<u8> {
    let even = if val % 2 == 0 { true } else { false };
    let mut val = val.to_string();
    // if even, we generate big value(> 64 bytes) to put them in default column family
    // else the value will be put in write column family
    if even {
        for _ in 0..10 {
            val.push_str("0123456789");
        }
    }
    format!("{}00000000", val).as_bytes().to_owned()
}

fn do_insert(db: &DB, key: u64) {
    let k = make_key(key);
    let v = make_val(key);
    let cf_handle = get_cf_handle(&db, v.len());
    db.put_cf(cf_handle, &k, &v).unwrap();
}

fn do_delete(db: &DB, key: u64) {
    let k = make_key(key);
    let v = make_val(key);
    let cf_handle = get_cf_handle(&db, v.len());
    db.delete_cf(cf_handle, &k).unwrap();
}

fn do_write_batch(db: &DB, key: &Vec<(u64, bool)>) {
    let batch = WriteBatch::new();
    for &(e, insert) in key {
        let k = make_key(e);
        let v = make_val(e);
        let cf_handle = get_cf_handle(&db, v.len());
        if insert {
            batch.put_cf(cf_handle, &k, &v).unwrap();
        } else {
            batch.delete_cf(cf_handle, &k).unwrap();
        }
    }
    db.write(batch).unwrap();
}

fn do_ingest(db: &DB, keys: &Vec<u64>) {
    let sst_dir = TempDir::new("rocksdb_consistency_test").unwrap();
    let default_path = sst_dir.path().join(CF_DEFAULT);
    let default_cf_handle = db.cf_handle(CF_DEFAULT).unwrap();
    let write_path = sst_dir.path().join(CF_WRITE);
    let write_cf_handle = db.cf_handle(CF_WRITE).unwrap();
    let opt = db.get_options();
    let env_opt = EnvOptions::new();
    let mut default_writer = SstFileWriter::new_cf(env_opt, opt.clone(), default_cf_handle);
    let env_opt = EnvOptions::new();
    let mut write_writer = SstFileWriter::new_cf(env_opt, opt, write_cf_handle);
    let d_path = default_path.to_str().unwrap();
    let w_path = write_path.to_str().unwrap();
    default_writer.open(d_path).unwrap();
    write_writer.open(w_path).unwrap();

    let mut map = BTreeMap::new();
    for &e in keys {
        map.insert(make_key(e), make_val(e));
    }
    let (mut default, mut write) = (false, false);
    for (k, v) in map {
        if v.len() > 64 {
            default_writer.put(&k, &v).unwrap();
            default = true;
        } else {
            write_writer.put(&k, &v).unwrap();
            write = true;
        }
    }

    if default {
        default_writer.finish().unwrap();
        db.ingest_external_file_cf(
            default_cf_handle,
            &IngestExternalFileOptions::new(),
            &[d_path],
        ).unwrap();
    }
    if write {
        write_writer.finish().unwrap();
        db.ingest_external_file_cf(
            write_cf_handle,
            &IngestExternalFileOptions::new(),
            &[w_path],
        ).unwrap();
    }
}

fn do_delete_range(db: &DB, delete_range: &Vec<(u64, u64)>) {
    for &(s, e) in delete_range {
        let s_key = make_key(s);
        let e_key = make_key(e);
        let default_cf_handle = db.cf_handle(CF_DEFAULT).unwrap();
        let write_cf_handle = db.cf_handle(CF_WRITE).unwrap();
        db.delete_range_cf(default_cf_handle, &s_key, &e_key)
            .unwrap();
        db.delete_range_cf(write_cf_handle, &s_key, &e_key).unwrap();
    }
}

fn run(db: Arc<DB>, mut insert: Vec<u64>, mut delete: Vec<u64>, delete_range: Vec<(u64, u64)>) {
    let mut rng = rand::thread_rng();
    let to_delete = delete.clone();
    let mut to_batch: Vec<(u64, bool)> = Vec::new(); // <key, insert_or_not>
    let mut to_ingest = Vec::new();
    rng.shuffle(&mut insert);
    rng.shuffle(&mut delete);
    loop {
        if insert.is_empty() && delete.is_empty() {
            break;
        }
        let key;
        if delete.is_empty() {
            key = insert.pop().unwrap();
        } else if insert.is_empty() || rng.gen::<bool>() {
            key = delete.pop().unwrap();
        } else {
            key = insert.pop().unwrap();
        }

        match rng.gen::<u32>() % 10 {
            0 | 1 | 2 | 3 | 4 | 5 => do_insert(&db, key),
            6 | 7 => {
                if to_batch.len() == BATCH_SIZE {
                    do_write_batch(&db, &to_batch);
                    to_batch.clear();
                }
                to_batch.push((key, true));
            }
            8 | 9 => {
                if to_ingest.len() == INGEST_SIZE {
                    do_ingest(&db, &to_ingest);
                    to_ingest.clear();
                }
                to_ingest.push(key);
            }
            _ => assert!(false),
        }
    }

    if to_batch.len() > 0 {
        do_write_batch(&db, &to_batch);
        to_batch.clear();
    }
    if to_ingest.len() > 0 {
        do_ingest(&db, &to_ingest);
    }

    for key in to_delete {
        match rng.gen::<u32>() % 10 {
            0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 => do_delete(&db, key),
            8 | 9 => {
                if to_batch.len() == BATCH_SIZE {
                    do_write_batch(&db, &to_batch);
                    to_batch.clear();
                }
                to_batch.push((key, false));
            }
            _ => assert!(false),
        }
    }
    if to_batch.len() > 0 {
        do_write_batch(&db, &to_batch);
    }

    do_delete_range(&db, &delete_range);
}

struct DBs {
  dbs: Vec<Arc<DB>>,
}

impl DBs {
  pub fn new(count: usize) -> DBs {
      let mut dbs = Vec::new();
      for i in 0..count {
          let path = format!("/tmp/rocksdb_{}", i);
          let _ = fs::remove_dir_all(path.as_str());
          let db = new_rocksdb(path.as_str()).unwrap();
          dbs.push(Arc::new(db));
      }
      DBs {
          dbs: dbs,
      }
  }

  pub fn get_db(&self, i: usize) -> Arc<DB> {
      Arc::clone(&self.dbs[i])
  }

  pub fn verify(&self) {
      for cf in ALL_CFS {
          info!("start verify cf {:?}", cf);
          let mut iters = Vec::new();
          for db in &self.dbs {
              let handle = db.cf_handle(cf).unwrap();
              let mut iter = db.iter_cf(handle);
              iter.seek(SeekKey::Start);
              iters.push(iter);
          }

          let mut flag = iters.pop().unwrap();
          loop {
              let mut all_valid = true;
              if !flag.valid() {
                  all_valid = false;
              }
              for iter in &iters {
                  if !iter.valid() {
                      all_valid = false;
                  }
              }
              if !all_valid {
                  break;
              }
              for iter in iters.iter_mut() {
                  assert_eq!(flag.key(), iter.key());
                  assert_eq!(flag.value(), iter.value());
                  iter.next();
              }
              flag.next();
          }

          for iter in &iters {
              assert_eq!(flag.valid(), iter.valid());
          }

          // do a compaction after every column family verification
          for db in &self.dbs {
              let handle = db.cf_handle(cf).unwrap();
              db.compact_range_cf(handle, None, None);
          }
          info!("verify cf {:?} success", cf);
      }
  }
}

fn run_test() {
    let mut range = 3;
    let count = 5;
    let dbs = DBs::new(count);
    let mut rng = rand::thread_rng();
    loop {
        let mut keys = HashSet::with_capacity(UNIT);
        for _ in 0..UNIT {
            keys.insert(rng.gen::<u64>());
        }
        let keys: Vec<u64> = keys.iter().map(|&x| x).collect();
        let mut delete_range = Vec::new();
        for _ in 0..range {
            loop {
                let s = rng.gen::<u64>();
                let e = rng.gen::<u64>();
                if s.to_string() < e.to_string() {
                    delete_range.push((s, e));
                    break;
                }
            }
        }

        let (sx, rx) = mpsc::channel();
        for i in 0..count {
            let db = dbs.get_db(i);
            let insert = keys[0..UNIT / 2].to_vec().clone();
            let delete = keys[UNIT / 2..].to_vec().clone();
            let d_range = delete_range.clone();
            let sender = sx.clone();
            thread::spawn(move || {
                run(db, insert, delete, d_range);
                sender.send(0).unwrap();
            });
        }
        for _ in 0..count {
            rx.recv().unwrap();
        }

        let time = time::Instant::now();
        dbs.verify();
        // if verification time is more than 3 hours, we should delete more data
        if time.elapsed().as_secs() >= 3 * 60 * 60 {
            range = 5;
        } else {
            range = 3;
        }
    }
}

fn main() {
    run_test();
}
