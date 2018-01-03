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

extern crate rocksdb;
extern crate serde;
extern crate tempdir;
extern crate toml;
#[macro_use]
extern crate serde_derive;

mod util;
mod config;
mod basic;
mod ingest_sst;
mod delete_range;
mod compaction;
mod batch;

fn main() {
  loop {
      basic::test_rocksdb_basic();
      ingest_sst::test_rocksdb_ingest_sst();
      delete_range::test_rocksdb_delete_range();
      compaction::test_rocksdb_compaction();
      batch::test_rocksdb_writebatch();
  }
}
