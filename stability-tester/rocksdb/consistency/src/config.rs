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

extern crate sys_info;

use std::usize;
use std::fs;
use std::path::Path;
use rocksdb::*;

use util::{self, compression_type_level_serde, FixedSuffixSliceTransform, NoopSliceTransform,
           ReadableDuration, ReadableSize, GB, KB, MB};

pub fn db_exist(path: &str) -> bool {
    let path = Path::new(path);
    if !path.exists() || !path.is_dir() {
        return false;
    }

    // If path is not an empty directory, we say db exists. If path is not an empty directory
    // but db has not been created, DB::list_column_families will failed and we can cleanup
    // the directory by this indication.
    fs::read_dir(&path).unwrap().next().is_some()
}

// `cfs_diff' Returns a Vec of cf which is in `a' but not in `b'.
pub fn cfs_diff<'a>(a: &[&'a str], b: &[&str]) -> Vec<&'a str> {
    a.iter()
        .filter(|x| b.iter().find(|y| y == x).is_none())
        .map(|x| *x)
        .collect()
}

pub struct CFOptions<'a> {
    pub cf: &'a str,
    pub options: ColumnFamilyOptions,
}

impl<'a> CFOptions<'a> {
    pub fn new(cf: &'a str, options: ColumnFamilyOptions) -> CFOptions<'a> {
        CFOptions {
            cf: cf,
            options: options,
        }
    }
}

pub type CfName = &'static str;
pub const CF_DEFAULT: CfName = "default";
pub const CF_LOCK: CfName = "lock";
pub const CF_WRITE: CfName = "write";
pub const CF_RAFT: CfName = "raft";

pub const ALL_CFS: &'static [CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT];

const LOCKCF_MIN_MEM: usize = 256 * MB as usize;
const LOCKCF_MAX_MEM: usize = GB as usize;
const RAFT_MIN_MEM: usize = 256 * MB as usize;
const RAFT_MAX_MEM: usize = 2 * GB as usize;

fn memory_mb_for_cf(is_raft_db: bool, cf: &str) -> usize {
    let total_mem = sys_info::mem_info().unwrap().total * KB;
    let (ratio, min, max) = match (is_raft_db, cf) {
        (true, CF_DEFAULT) => (0.02, RAFT_MIN_MEM, RAFT_MAX_MEM),
        (false, CF_DEFAULT) => (0.25, 0, usize::MAX),
        (false, CF_LOCK) => (0.02, LOCKCF_MIN_MEM, LOCKCF_MAX_MEM),
        (false, CF_WRITE) => (0.15, 0, usize::MAX),
        _ => unreachable!(),
    };
    let mut size = (total_mem as f64 * ratio) as usize;
    if size < min {
        size = min;
    } else if size > max {
        size = max;
    }
    size / MB as usize
}

macro_rules! cf_config {
    ($name:ident) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $name {
            pub block_size: ReadableSize,
            pub block_cache_size: ReadableSize,
            pub disable_block_cache: bool,
            pub cache_index_and_filter_blocks: bool,
            pub pin_l0_filter_and_index_blocks: bool,
            pub use_bloom_filter: bool,
            pub whole_key_filtering: bool,
            pub bloom_filter_bits_per_key: i32,
            pub block_based_bloom_filter: bool,
            pub read_amp_bytes_per_bit: u32,
            #[serde(with = "compression_type_level_serde")]
            pub compression_per_level: [DBCompressionType; 7],
            pub write_buffer_size: ReadableSize,
            pub max_write_buffer_number: i32,
            pub min_write_buffer_number_to_merge: i32,
            pub max_bytes_for_level_base: ReadableSize,
            pub target_file_size_base: ReadableSize,
            pub level0_file_num_compaction_trigger: i32,
            pub level0_slowdown_writes_trigger: i32,
            pub level0_stop_writes_trigger: i32,
            pub max_compaction_bytes: ReadableSize,
            #[serde(with = "util::compaction_pri_serde")]
            pub compaction_pri: CompactionPriority,
            pub dynamic_level_bytes: bool,
            pub num_levels: i32,
            pub max_bytes_for_level_multiplier: i32,
        }
    }
}

macro_rules! build_cf_opt {
    ($opt:ident) => {{
        let mut block_base_opts = BlockBasedOptions::new();
        block_base_opts.set_block_size($opt.block_size.0 as usize);
        block_base_opts.set_no_block_cache($opt.disable_block_cache);
        block_base_opts.set_lru_cache($opt.block_cache_size.0 as usize, -1, 0, 0.0);
        block_base_opts.set_cache_index_and_filter_blocks($opt.cache_index_and_filter_blocks);
        block_base_opts.set_pin_l0_filter_and_index_blocks_in_cache(
            $opt.pin_l0_filter_and_index_blocks);
        if $opt.use_bloom_filter {
            block_base_opts.set_bloom_filter($opt.bloom_filter_bits_per_key,
                                             $opt.block_based_bloom_filter);
            block_base_opts.set_whole_key_filtering($opt.whole_key_filtering);
        }
        block_base_opts.set_read_amp_bytes_per_bit($opt.read_amp_bytes_per_bit);
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_block_based_table_factory(&block_base_opts);
        cf_opts.compression_per_level(&$opt.compression_per_level);
        cf_opts.set_write_buffer_size($opt.write_buffer_size.0);
        cf_opts.set_max_write_buffer_number($opt.max_write_buffer_number);
        cf_opts.set_min_write_buffer_number_to_merge($opt.min_write_buffer_number_to_merge);
        cf_opts.set_max_bytes_for_level_base($opt.max_bytes_for_level_base.0);
        cf_opts.set_target_file_size_base($opt.target_file_size_base.0);
        cf_opts.set_level_zero_file_num_compaction_trigger($opt.level0_file_num_compaction_trigger);
        cf_opts.set_level_zero_slowdown_writes_trigger($opt.level0_slowdown_writes_trigger);
        cf_opts.set_level_zero_stop_writes_trigger($opt.level0_stop_writes_trigger);
        cf_opts.set_max_compaction_bytes($opt.max_compaction_bytes.0);
        cf_opts.compaction_priority($opt.compaction_pri);
        cf_opts.set_level_compaction_dynamic_level_bytes($opt.dynamic_level_bytes);
        cf_opts.set_max_bytes_for_level_multiplier($opt.max_bytes_for_level_multiplier);

        cf_opts
    }};
}

cf_config!(DefaultCfConfig);

impl Default for DefaultCfConfig {
    fn default() -> DefaultCfConfig {
        DefaultCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(false, CF_DEFAULT) as u64),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: false,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
        }
    }
}

impl DefaultCfConfig {
    pub fn build_opt(&self) -> ColumnFamilyOptions {
        let cf_opts = build_cf_opt!(self);
        cf_opts
    }
}

cf_config!(WriteCfConfig);

impl Default for WriteCfConfig {
    fn default() -> WriteCfConfig {
        WriteCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(false, CF_WRITE) as u64),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            whole_key_filtering: false,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: false,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
        }
    }
}

impl WriteCfConfig {
    pub fn build_opt(&self) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self);
        // Prefix extractor(trim the timestamp at tail) for write cf.
        let e = Box::new(FixedSuffixSliceTransform::new(8));
        cf_opts
            .set_prefix_extractor("FixedSuffixSliceTransform", e)
            .unwrap();
        // Create prefix bloom filter for memtable.
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        cf_opts
    }
}

cf_config!(LockCfConfig);

impl Default for LockCfConfig {
    fn default() -> LockCfConfig {
        LockCfConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(false, CF_LOCK) as u64),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [DBCompressionType::No; 7],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(128),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 1,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: false,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
        }
    }
}

impl LockCfConfig {
    pub fn build_opt(&self) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self);
        let f = Box::new(NoopSliceTransform);
        cf_opts
            .set_prefix_extractor("NoopSliceTransform", f)
            .unwrap();
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        cf_opts
    }
}

cf_config!(RaftCfConfig);

impl Default for RaftCfConfig {
    fn default() -> RaftCfConfig {
        RaftCfConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: ReadableSize::mb(128),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [DBCompressionType::No; 7],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(128),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 1,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: false,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
        }
    }
}

impl RaftCfConfig {
    pub fn build_opt(&self) -> ColumnFamilyOptions {
        let mut cf_opts = build_cf_opt!(self);
        let f = Box::new(NoopSliceTransform);
        cf_opts
            .set_prefix_extractor("NoopSliceTransform", f)
            .unwrap();
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        cf_opts
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct DbConfig {
    #[serde(with = "util::recovery_mode_serde")] pub wal_recovery_mode: DBRecoveryMode,
    pub wal_dir: String,
    pub wal_ttl_seconds: u64,
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: ReadableSize,
    pub max_background_jobs: i32,
    pub max_manifest_file_size: ReadableSize,
    pub create_if_missing: bool,
    pub max_open_files: i32,
    pub enable_statistics: bool,
    pub stats_dump_period: ReadableDuration,
    pub compaction_readahead_size: ReadableSize,
    pub info_log_max_size: ReadableSize,
    pub info_log_roll_time: ReadableDuration,
    pub info_log_dir: String,
    pub rate_bytes_per_sec: ReadableSize,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    pub max_sub_compactions: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    pub use_direct_io_for_flush_and_compaction: bool,
    pub enable_pipelined_write: bool,
    pub defaultcf: DefaultCfConfig,
    pub writecf: WriteCfConfig,
    pub lockcf: LockCfConfig,
    pub raftcf: RaftCfConfig,
}

impl Default for DbConfig {
    fn default() -> DbConfig {
        DbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs: 6,
            max_manifest_file_size: ReadableSize::mb(20),
            create_if_missing: true,
            max_open_files: 40960,
            enable_statistics: true,
            stats_dump_period: ReadableDuration::minutes(10),
            compaction_readahead_size: ReadableSize::kb(0),
            info_log_max_size: ReadableSize::kb(0),
            info_log_roll_time: ReadableDuration::secs(0),
            info_log_dir: "".to_owned(),
            rate_bytes_per_sec: ReadableSize::kb(0),
            bytes_per_sync: ReadableSize::mb(0),
            wal_bytes_per_sync: ReadableSize::kb(512),
            max_sub_compactions: 1,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_compaction: false,
            enable_pipelined_write: true,
            defaultcf: DefaultCfConfig::default(),
            writecf: WriteCfConfig::default(),
            lockcf: LockCfConfig::default(),
            raftcf: RaftCfConfig::default(),
        }
    }
}

cf_config!(RaftDefaultCfConfig);

impl Default for RaftDefaultCfConfig {
    fn default() -> RaftDefaultCfConfig {
        RaftDefaultCfConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: ReadableSize::mb(memory_mb_for_cf(true, CF_DEFAULT) as u64),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_level_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            max_compaction_bytes: ReadableSize::gb(2),
            compaction_pri: CompactionPriority::ByCompensatedSize,
            dynamic_level_bytes: false,
            num_levels: 7,
            max_bytes_for_level_multiplier: 10,
        }
    }
}

fn check_and_open(
    path: &str,
    mut db_opt: DBOptions,
    cfs_opts: Vec<CFOptions>,
) -> Result<DB, String> {
    // If db not exist, create it.
    if !db_exist(path) {
        db_opt.create_if_missing(true);

        let mut cfs_v = vec![];
        let mut cf_opts_v = vec![];
        if let Some(x) = cfs_opts.iter().find(|x| x.cf == CF_DEFAULT) {
            cfs_v.push(x.cf);
            cf_opts_v.push(x.options.clone());
        }
        let cfds = cfs_v.into_iter().zip(cf_opts_v).collect();
        let mut db = DB::open_cf(db_opt, path, cfds)?;
        for x in cfs_opts {
            if x.cf == CF_DEFAULT {
                continue;
            }
            db.create_cf((x.cf, x.options))?;
        }

        return Ok(db);
    }

    db_opt.create_if_missing(false);

    // List all column families in current db.
    let cfs_list = DB::list_column_families(&db_opt, path)?;
    let existed: Vec<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
    let needed: Vec<&str> = cfs_opts.iter().map(|x| x.cf).collect();

    // If all column families are exist, just open db.
    if existed == needed {
        let mut cfs_v = vec![];
        let mut cfs_opts_v = vec![];
        for x in cfs_opts {
            cfs_v.push(x.cf);
            cfs_opts_v.push(x.options);
        }

        let cfds = cfs_v.into_iter().zip(cfs_opts_v).collect();
        return DB::open_cf(db_opt, path, cfds);
    }

    // Open db.
    let mut cfs_v: Vec<&str> = Vec::new();
    let mut cfs_opts_v: Vec<ColumnFamilyOptions> = Vec::new();
    for cf in &existed {
        cfs_v.push(cf);
        match cfs_opts.iter().find(|x| x.cf == *cf) {
            Some(x) => {
                cfs_opts_v.push(x.options.clone());
            }
            None => {
                cfs_opts_v.push(ColumnFamilyOptions::new());
            }
        }
    }
    let cfds = cfs_v.into_iter().zip(cfs_opts_v).collect();
    let mut db = DB::open_cf(db_opt, path, cfds).unwrap();

    // Drop discarded column families.
    //    for cf in existed.iter().filter(|x| needed.iter().find(|y| y == x).is_none()) {
    for cf in cfs_diff(&existed, &needed) {
        // Never drop default column families.
        if cf != CF_DEFAULT {
            db.drop_cf(cf)?;
        }
    }

    // Create needed column families not existed yet.
    for cf in cfs_diff(&needed, &existed) {
        db.create_cf((
            cf,
            cfs_opts
                .iter()
                .find(|x| x.cf == cf)
                .unwrap()
                .options
                .clone(),
        ))?;
    }
    Ok(db)
}

pub fn new_rocksdb(path: &str) -> Result<DB, String> {
    let mut cfs_opts = Vec::with_capacity(ALL_CFS.len());
    let cfg_rocksdb = DbConfig::default();
    for cf in ALL_CFS {
        let cf_opt = match *cf {
            CF_DEFAULT => CFOptions::new(CF_DEFAULT, cfg_rocksdb.defaultcf.build_opt()),
            CF_LOCK => CFOptions::new(CF_LOCK, cfg_rocksdb.lockcf.build_opt()),
            CF_WRITE => CFOptions::new(CF_WRITE, cfg_rocksdb.writecf.build_opt()),
            CF_RAFT => CFOptions::new(CF_RAFT, cfg_rocksdb.raftcf.build_opt()),
            _ => CFOptions::new(*cf, ColumnFamilyOptions::new()),
        };
        cfs_opts.push(cf_opt);
    }
    let mut db_opts = DBOptions::new();
    db_opts.enable_statistics(true);
    check_and_open(path, db_opts, cfs_opts)
}
