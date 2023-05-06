//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::hash_map::RandomState;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use common_arrow::parquet::metadata::FileMetaData;
use common_arrow::parquet::metadata::ThriftFileMetaData;
use common_ast::ast::DatabaseEngine::Default;
use common_base::base::tokio;
use common_cache::Cache;
use common_expression::types::Int32Type;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::Column;
use common_expression::ColumnId;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::statistics::gen_columns_statistics;
use common_storages_fuse::statistics::STATS_STRING_PREFIX_LEN;
use common_storages_fuse::FuseStorageFormat;
use opendal::Operator;
use storages_common_cache::InMemoryCacheBuilder;
use storages_common_cache::InMemoryItemCacheHolder;
use storages_common_index::filters::FilterBuilder;
use storages_common_index::filters::Xor8Builder;
use storages_common_index::filters::Xor8Filter;
use storages_common_index::BloomIndexMeta;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ColumnMeta;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::Compression;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::SingleColumnMeta;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::Versioned;
use sysinfo::get_current_pid;
use sysinfo::ProcessExt;
use sysinfo::System;
use sysinfo::SystemExt;
use uuid::Uuid;

use crate::storages::fuse::block_writer::BlockWriter;

// NOTE:
//
// usage of memory is observed at *process* level, please do not combine them into
// one test function.
//
// by default, these cases are ignored (in CI).
//
// please run the following two cases individually (in different process)

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_index_meta_cache_size_file_meta_data() -> common_exception::Result<()> {
    let thrift_file_meta = setup().await?;

    let cache_number = 300_000;

    let meta: FileMetaData = FileMetaData::try_from_thrift(thrift_file_meta)?;

    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();
    let scenario = "FileMetaData";

    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let cache = InMemoryCacheBuilder::new_item_cache::<FileMetaData>(cache_number as u64);

    populate_cache(&cache, meta, cache_number);
    show_memory_usage(scenario, base_memory_usage, cache_number);

    drop(cache);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_index_meta_cache_size_bloom_meta() -> common_exception::Result<()> {
    let thrift_file_meta = setup().await?;

    let cache_number = 300_000;

    let bloom_index_meta = BloomIndexMeta::try_from(thrift_file_meta)?;

    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();

    let scenario = "BloomIndexMeta(mini)";
    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let cache = InMemoryCacheBuilder::new_item_cache::<BloomIndexMeta>(cache_number as u64);
    populate_cache(&cache, bloom_index_meta, cache_number);
    show_memory_usage("BloomIndexMeta(Mini)", base_memory_usage, cache_number);

    drop(cache);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_index_meta_cache_size_bloom_filter() -> common_exception::Result<()> {
    let cache_number = 500_000;
    let num_rows = 200_000;

    // it is "dense" data, all values are distinct
    let keys: Vec<u64> = (0..num_rows).map(|i| i).collect();
    let mut builder = Xor8Builder::create();
    builder.add_keys(&keys);
    let base_filter = builder.build()?;

    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();

    let scenario = "BloomFilterIndex";
    eprintln!(
        "scenario {}, pid {}, base memory {}, rows per block {}, all distinct values",
        scenario, pid, base_memory_usage, num_rows
    );

    let cache = InMemoryCacheBuilder::new_item_cache::<Xor8Filter>(cache_number as u64);
    {
        let mut c = cache.write();
        for _ in 0..cache_number {
            let uuid = Uuid::new_v4();

            let filter = Xor8Filter {
                filter: xorfilter::Xor8 {
                    hash_builder: base_filter.filter.hash_builder.clone(),
                    seed: base_filter.filter.seed,
                    num_keys: base_filter.filter.num_keys,
                    block_length: base_filter.filter.block_length,
                    finger_prints: std::sync::Arc::new(
                        base_filter.filter.finger_prints.as_ref().clone(),
                    ),
                },
            };
            (*c).put(format!("{}", uuid.simple()), std::sync::Arc::new(filter));
        }
    }
    show_memory_usage("BloomFilterIndex", base_memory_usage, cache_number);

    drop(cache);

    Ok(())
}

// cargo test --test it storages::fuse::bloom_index_meta_size::test_random_location_memory_size --no-fail-fast -- --ignored --exact -Z unstable-options --show-output
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_random_location_memory_size() -> common_exception::Result<()> {
    // generate random location of Type Location
    let location_gen = TableMetaLocationGenerator::with_prefix("/root".to_string());

    let num_segments = 5_000_000;
    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();
    let scenario = format!("{} segment locations", num_segments);

    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let mut locations: HashSet<Location, RandomState> = HashSet::new();
    for _ in 0..num_segments {
        let segment_path = location_gen.gen_segment_info_location();
        let segment_location = (segment_path, SegmentInfo::VERSION);
        locations.insert(segment_location);
    }

    show_memory_usage(&scenario, base_memory_usage, num_segments);

    drop(locations);

    Ok(())
}

//#[tokio::test(flavor = "multi_thread")]
#[test]
#[ignore]
fn test_seg_deser() {
    let col_meta = ColumnMeta::Parquet(SingleColumnMeta {
        offset: 0,
        len: 0,
        num_values: 0,
    });

    let col_stat = ColumnStatistics {
        min: Scalar::String(
            String::from_utf8(vec![b'a'; STATS_STRING_PREFIX_LEN])
                .unwrap()
                .into_bytes()
                .into(),
        ),
        max: Scalar::String(
            String::from_utf8(vec![b'a'; STATS_STRING_PREFIX_LEN])
                .unwrap()
                .into_bytes()
                .into(),
        ),
        null_count: 0,
        in_memory_size: 0,
        distinct_of_values: None,
    };

    let number_col_stat = ColumnStatistics {
        min: Scalar::Number(NumberScalar::Int32(0)),
        max: Scalar::Number(NumberScalar::Int32(0)),
        null_count: 0,
        in_memory_size: 0,
        distinct_of_values: None,
    };

    // 20 string columns, 5 number columns
    let num_string_columns = 20;
    let num_number_columns = 5;
    let col_metas = (0..num_string_columns + num_number_columns)
        .map(|id| (id as ColumnId, col_meta.clone()))
        .collect::<HashMap<_, _>>();

    assert_eq!(num_number_columns + num_string_columns, col_metas.len());

    let mut col_stats = (0..num_string_columns)
        .map(|id| (id as ColumnId, col_stat.clone()))
        .collect::<HashMap<_, _>>();
    for idx in num_string_columns..num_string_columns + num_number_columns {
        col_stats.insert(idx as ColumnId, number_col_stat.clone());
    }
    assert_eq!(num_number_columns + num_string_columns, col_stats.len());

    let location_gen = TableMetaLocationGenerator::with_prefix("/root/12345/67890".to_owned());

    let (block_location, block_uuid) = location_gen.gen_block_location();
    let block_meta = BlockMeta {
        row_count: 0,
        block_size: 0,
        file_size: 0,
        col_stats: col_stats.clone(),
        // col_stats: HashMap::new(),
        col_metas,
        // col_metas: HashMap::new(),
        cluster_stats: None,
        location: block_location,
        // location: ("".to_owned(), 0),
        bloom_filter_index_location: Some(location_gen.block_bloom_index_location(&block_uuid)),
        // bloom_filter_index_location: Some(("".to_owned(), 0)),
        bloom_filter_index_size: 0,
        compression: Compression::Lz4,
    };

    let num_blocks = 1000;

    let block_metas = (0..num_blocks)
        .map(|_| Arc::new(block_meta.clone()))
        .collect::<Vec<_>>();

    let statistics = Statistics {
        row_count: 0,
        block_count: 0,
        perfect_block_count: 0,
        uncompressed_byte_size: 0,
        compressed_byte_size: 0,
        index_size: 0,
        col_stats: col_stats.clone(),
    };

    let segment_info = SegmentInfo::new(block_metas, statistics.clone());
    let bytes = segment_info.to_bytes().unwrap();
    let being = Instant::now();
    for _ in 0..10000 {
        let _segment = SegmentInfo::from_bytes_new(&bytes).unwrap();
    }
    let elapsed = being.elapsed();
    eprintln!(
        "{}.{:03} seconds",
        elapsed.as_secs(),
        elapsed.subsec_millis()
    );
    eprintln!(" {} ms per item", elapsed.as_millis() / 10000);
}

use common_expression::types::decimal::DecimalScalar;
use indexmap::IndexMap;

//#[derive(Debug, Clone)]
// pub enum ScalarNew {
//    Null,
//    EmptyArray,
//    EmptyMap,
//    Number(NumberScalar),
//    //    Decimal(DecimalScalar),
//    Timestamp(i64),
//    Date(i32),
//    Boolean(bool),
//    String(Vec<u8>),
//    //    Array(Column),
//    //    Map(Column),
//    //    Bitmap(Vec<u8>),
//    //    Tuple(Vec<ScalarNew>),
//    Variant(Vec<u8>),
//}
//#[derive(Debug, Clone)]
// pub struct ColumnStatisticsNew {
//    pub min: ScalarNew,
//    pub max: ScalarNew,
//    pub null_count: u64,
//    pub in_memory_size: u64,
//    pub distinct_of_values: Option<u64>,
//}
// type ColStatsMap = IndexMap<ColumnId, ColumnStatistics, RandomState>;
//#[tokio::test(flavor = "multi_thread")]
//#[ignore]
// async fn test_col_stats_size() -> common_exception::Result<()> {
//    // use fnv::FnvBuildHasher;
//    // use fxhash::FxBuildHasher;
//
//    let fields = (0..23)
//        .map(|_| TableField::new("id", TableDataType::Number(NumberDataType::Int32)))
//        .collect::<Vec<_>>();
//
//    let num_fields = fields.len();
//    let schema = TableSchemaRefExt::create(fields);
//
//    eprintln!("size of scalar {}", std::mem::size_of::<Scalar>());
//    eprintln!("size of scalarNew {}", std::mem::size_of::<ScalarNew>());
//    eprintln!(
//        "size of ColumnStatisticsNew {}",
//        std::mem::size_of::<ColumnStatisticsNew>()
//    );
//    eprintln!(
//        "size of ColumnStatistics {}",
//        std::mem::size_of::<ColumnStatistics>()
//    );
//
//    eprintln!(
//        "size of SingleColumnMeta {}",
//        std::mem::size_of::<SingleColumnMeta>()
//    );
//    let col_stat = ColumnStatistics {
//        min: Scalar::Null,
//        max: Scalar::Null,
//        null_count: 0,
//        in_memory_size: 0,
//        distinct_of_values: None,
//    };
//
//    let mut col_stats: ColStatsMap = (0..schema.fields().len())
//        .map(|id| (id as ColumnId, col_stat.clone()))
//        .collect();
//
//    col_stats.shrink_to_fit();
//
//    let cache_number = 5_000_000;
//    let sys = System::new_all();
//    let pid = get_current_pid().unwrap();
//    let process = sys.process(pid).unwrap();
//    let base_memory_usage = process.memory();
//    let scenario = format!("{} ColumnStats, {} columns ", cache_number, num_fields);
//
//    eprintln!(
//        "scenario {}, pid {}, base memory {}",
//        scenario, pid, base_memory_usage
//    );
//
//    //     let cache = InMemoryCacheBuilder::new_item_cache::<BTreeMap<ColumnId, ColumnStatisticsNew>>(
//    let cache = InMemoryCacheBuilder::new_item_cache::<ColStatsMap>(cache_number as u64);
//    {
//        let mut c = cache.write();
//        for i in 0..cache_number {
//            let uuid = Uuid::new_v4();
//            let mut n = col_stats.clone();
//            n.shrink_to_fit();
//            (*c).put(format!("{}", uuid.simple()), std::sync::Arc::new(n));
//        }
//    }
//
//    show_memory_usage("SegmentInfoCache", base_memory_usage, cache_number);
//
//    Ok(())
//}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_col_meta_size() -> common_exception::Result<()> {
    // use fnv::FnvBuildHasher;
    // use fxhash::FxBuildHasher;

    let fields = (0..23)
        .map(|_| TableField::new("id", TableDataType::Number(NumberDataType::Int32)))
        .collect::<Vec<_>>();

    let num_fields = fields.len();
    let schema = TableSchemaRefExt::create(fields);

    let col_meta = ColumnMeta::Parquet(SingleColumnMeta {
        offset: 0,
        len: 0,
        num_values: 0,
    });

    eprintln!("size of ColumnMeta {}", std::mem::size_of::<ColumnMeta>());
    eprintln!(
        "size of SingleColumnMeta {}",
        std::mem::size_of::<SingleColumnMeta>()
    );

    // let col_metas: HashMap<ColumnId, ColumnMeta> = (0..schema.fields().len())
    // let col_metas: IndexMap<ColumnId, ColumnMeta> = (0..schema.fields().len())
    //    .map(|id| (id as ColumnId, col_meta.clone()))
    //    .collect();
    let mut col_metas = Vec::with_capacity(schema.fields().len());
    for i in 0..schema.fields().len() {
        col_metas.push((i, col_meta.clone()));
    }

    let cache_number = 5_000_000;
    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();
    let scenario = format!("{} ColumnMetas, {} columns ", cache_number, num_fields);

    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let cache =
        //InMemoryCacheBuilder::new_item_cache::<HashMap<ColumnId, ColumnMeta>>(cache_number as u64);
         //    InMemoryCacheBuilder::new_item_cache::<IndexMap<ColumnId, ColumnMeta>>(cache_number as u64);
     InMemoryCacheBuilder::new_item_cache::<Vec<(usize, ColumnMeta)>>(cache_number as u64);
    {
        let mut c = cache.write();
        for i in 0..cache_number {
            let uuid = Uuid::new_v4();
            (*c).put(
                format!("{}", uuid.simple()),
                std::sync::Arc::new(col_metas.clone()),
            );
        }
    }

    show_memory_usage("ColumnMeta", base_memory_usage, cache_number);

    Ok(())
}

use smallvec::SmallVec;
pub mod shrink {
    use enum_as_inner::EnumAsInner;
    use serde::Deserialize;
    use serde::Serialize;
    use storages_common_table_meta::meta::ClusterStatistics;
    use storages_common_table_meta::meta::FormatVersion;

    use super::*;
    #[derive(Debug, Clone, EnumAsInner, PartialEq, Eq, Serialize, Deserialize)]
    pub enum ScalarNew {
        Null,
        EmptyArray,
        EmptyMap,
        Number(NumberScalar),
        Decimal(DecimalScalar),
        Timestamp(i64),
        Date(i32),
        Boolean(bool),
        String(Vec<u8>),
        // String(SmallVec<[u8; STATS_STRING_PREFIX_LEN]>),
        //    Array(Column),
        //    Map(Column),
        //    Bitmap(Vec<u8>),
        //    Tuple(Vec<ScalarNew>),
        // Variant(Vec<u8>),
    }
    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
    pub struct ColumnStatisticsNew {
        pub min: ScalarNew,
        pub max: ScalarNew,
        pub null_count: u64,
        pub in_memory_size: u64,
        pub distinct_of_values: Option<u64>,
    }

    #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Default)]
    pub struct StatisticsNew {
        pub row_count: u64,
        pub block_count: u64,
        pub perfect_block_count: u64,

        pub uncompressed_byte_size: u64,
        pub compressed_byte_size: u64,
        pub index_size: u64,

        pub col_stats: HashMap<ColumnId, ColumnStatisticsNew>,
    }

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
    pub struct BlockMetaNew {
        pub row_count: u64,
        pub block_size: u64,
        pub file_size: u64,
        pub col_stats: HashMap<ColumnId, ColumnStatisticsNew>,
        pub col_metas: HashMap<ColumnId, ColumnMeta>,
        pub cluster_stats: Option<ClusterStatistics>,
        /// location of data block
        pub location: Location,
        /// location of bloom filter index
        pub bloom_filter_index_location: Option<Location>,

        #[serde(default)]
        pub bloom_filter_index_size: u64,
        pub compression: Compression,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    pub struct SegmentInfoNew {
        pub format_version: FormatVersion,
        /// blocks belong to this segment
        pub blocks: Vec<Arc<BlockMetaNew>>,
        /// summary statistics
        pub summary: StatisticsNew,
    }

    impl SegmentInfoNew {
        pub fn new(blocks: Vec<Arc<BlockMetaNew>>, summary: StatisticsNew) -> Self {
            Self {
                format_version: SegmentInfo::VERSION,
                blocks,
                summary,
            }
        }
    }
}

type SegmentInfoTest = SegmentInfo;
type BlockMetaTest = BlockMeta;
type ScalarTest = Scalar;
type ColumnStatisticsTest = ColumnStatistics;
type StatisticsTest = Statistics;

// type SegmentInfoTest = shrink::SegmentInfoNew;
// type BlockMetaTest = shrink::BlockMetaNew;
// type ScalarTest = shrink::ScalarNew;
// type ColumnStatisticsTest = shrink::ColumnStatisticsNew;
// type StatisticsTest = shrink::StatisticsNew;

// cargo test --test it storages::fuse::bloom_index_meta_size::test_random_location_memory_size --no-fail-fast -- --ignored --exact -Z unstable-options --show-output
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_segment_info_size() -> common_exception::Result<()> {
    // let fields = (0..23)
    //    .map(|_| TableField::new("id", TableDataType::Number(NumberDataType::Int32)))
    //    .collect::<Vec<_>>();

    // let schema = TableSchemaRefExt::create(fields);

    // let mut columns = vec![];
    // for _ in 0..schema.fields().len() {
    //    // values do not matter
    //    let column = Int32Type::from_data(vec![1]);
    //    columns.push(column)
    //}

    let col_meta = ColumnMeta::Parquet(SingleColumnMeta {
        offset: 0,
        len: 0,
        num_values: 0,
    });

    eprintln!(">>>> size of {}", std::mem::size_of::<ScalarTest>());
    let col_stat = ColumnStatisticsTest {
        min: ScalarTest::String(
            String::from_utf8(vec![b'a'; STATS_STRING_PREFIX_LEN])?
                .into_bytes()
                .into(),
        ),
        max: ScalarTest::String(
            String::from_utf8(vec![b'a'; STATS_STRING_PREFIX_LEN])?
                .into_bytes()
                .into(),
        ),
        null_count: 0,
        in_memory_size: 0,
        distinct_of_values: None,
    };

    let number_col_stat = ColumnStatisticsTest {
        min: ScalarTest::Number(NumberScalar::Int32(0)),
        max: ScalarTest::Number(NumberScalar::Int32(0)),
        null_count: 0,
        in_memory_size: 0,
        distinct_of_values: None,
    };

    // 20 string columns, 5 number columns
    let num_string_columns = 20;
    let num_number_columns = 5;
    let col_metas = (0..num_string_columns + num_number_columns)
        .map(|id| (id as ColumnId, col_meta.clone()))
        .collect::<HashMap<_, _>>();

    assert_eq!(num_number_columns + num_string_columns, col_metas.len());

    let mut col_stats = (0..num_string_columns)
        .map(|id| (id as ColumnId, col_stat.clone()))
        .collect::<HashMap<_, _>>();
    for idx in num_string_columns..num_string_columns + num_number_columns {
        col_stats.insert(idx as ColumnId, number_col_stat.clone());
    }
    assert_eq!(num_number_columns + num_string_columns, col_stats.len());

    let location_gen = TableMetaLocationGenerator::with_prefix("/root/12345/67890".to_owned());

    let (block_location, block_uuid) = location_gen.gen_block_location();
    let block_meta = BlockMetaTest {
        row_count: 0,
        block_size: 0,
        file_size: 0,
        col_stats: col_stats.clone(),
        // col_stats: HashMap::new(),
        col_metas,
        // col_metas: HashMap::new(),
        cluster_stats: None,
        location: block_location,
        // location: ("".to_owned(), 0),
        bloom_filter_index_location: Some(location_gen.block_bloom_index_location(&block_uuid)),
        // bloom_filter_index_location: Some(("".to_owned(), 0)),
        bloom_filter_index_size: 0,
        compression: Compression::Lz4,
    };

    let num_blocks = 1000;

    let block_metas = (0..num_blocks)
        .map(|_| Arc::new(block_meta.clone()))
        .collect::<Vec<_>>();

    let statistics = StatisticsTest {
        row_count: 0,
        block_count: 0,
        perfect_block_count: 0,
        uncompressed_byte_size: 0,
        compressed_byte_size: 0,
        index_size: 0,
        col_stats: col_stats.clone(),
    };

    let segment_info = SegmentInfoTest::new(block_metas, statistics.clone());

    let cache_number = 3000;
    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();
    let scenario = format!(
        "{} SegmentInfo, {} block per seg ",
        cache_number, num_blocks
    );

    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let cache = InMemoryCacheBuilder::new_item_cache::<SegmentInfoTest>(cache_number as u64);
    {
        let mut c = cache.write();
        for i in 0..cache_number {
            let uuid = Uuid::new_v4();
            let mut block_metas = segment_info
                .blocks
                .iter()
                .map(|b: &Arc<BlockMetaTest>| Arc::new(b.as_ref().clone()))
                .collect::<Vec<_>>();
            block_metas.shrink_to_fit();
            let mut statistics = statistics.clone();
            statistics.col_stats.shrink_to_fit();
            let segment_info = SegmentInfoTest::new(block_metas, statistics);
            (*c).put(
                format!("{}", uuid.simple()),
                // format!("{}", i),
                std::sync::Arc::new(segment_info),
            );
        }
    }
    show_memory_usage("SegmentInfoCache", base_memory_usage, cache_number);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_segment_info_bytes_size() -> common_exception::Result<()> {
    let col_meta = ColumnMeta::Parquet(SingleColumnMeta {
        offset: 0,
        len: 0,
        num_values: 0,
    });

    eprintln!(">>>> size of {}", std::mem::size_of::<ScalarTest>());
    let col_stat = ColumnStatisticsTest {
        min: ScalarTest::String(
            String::from_utf8(vec![b'a'; STATS_STRING_PREFIX_LEN])?
                .into_bytes()
                .into(),
        ),
        max: ScalarTest::String(
            String::from_utf8(vec![b'a'; STATS_STRING_PREFIX_LEN])?
                .into_bytes()
                .into(),
        ),
        null_count: 0,
        in_memory_size: 0,
        distinct_of_values: None,
    };

    let number_col_stat = ColumnStatisticsTest {
        min: ScalarTest::Number(NumberScalar::Int32(0)),
        max: ScalarTest::Number(NumberScalar::Int32(0)),
        null_count: 0,
        in_memory_size: 0,
        distinct_of_values: None,
    };

    // 20 string columns, 5 number columns
    let num_string_columns = 20;
    let num_number_columns = 5;
    let col_metas = (0..num_string_columns + num_number_columns)
        .map(|id| (id as ColumnId, col_meta.clone()))
        .collect::<HashMap<_, _>>();

    assert_eq!(num_number_columns + num_string_columns, col_metas.len());

    let mut col_stats = (0..num_string_columns)
        .map(|id| (id as ColumnId, col_stat.clone()))
        .collect::<HashMap<_, _>>();
    for idx in num_string_columns..num_string_columns + num_number_columns {
        col_stats.insert(idx as ColumnId, number_col_stat.clone());
    }
    assert_eq!(num_number_columns + num_string_columns, col_stats.len());

    let location_gen = TableMetaLocationGenerator::with_prefix("/root/12345/67890".to_owned());

    let (block_location, block_uuid) = location_gen.gen_block_location();
    let block_meta = BlockMetaTest {
        row_count: 0,
        block_size: 0,
        file_size: 0,
        col_stats: col_stats.clone(),
        // col_stats: HashMap::new(),
        col_metas,
        // col_metas: HashMap::new(),
        cluster_stats: None,
        location: block_location,
        // location: ("".to_owned(), 0),
        bloom_filter_index_location: Some(location_gen.block_bloom_index_location(&block_uuid)),
        // bloom_filter_index_location: Some(("".to_owned(), 0)),
        bloom_filter_index_size: 0,
        compression: Compression::Lz4,
    };

    let num_blocks = 1000;

    let block_metas = (0..num_blocks)
        .map(|_| Arc::new(block_meta.clone()))
        .collect::<Vec<_>>();

    let statistics = StatisticsTest {
        row_count: 0,
        block_count: 0,
        perfect_block_count: 0,
        uncompressed_byte_size: 0,
        compressed_byte_size: 0,
        index_size: 0,
        col_stats: col_stats.clone(),
    };

    let segment_info = SegmentInfoTest::new(block_metas, statistics.clone());

    let cache_number = 3000;
    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();
    let scenario = format!(
        "{} SegmentInfo, {} block per seg ",
        cache_number, num_blocks
    );

    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let cache = InMemoryCacheBuilder::new_item_cache::<Vec<u8>>(cache_number as u64);
    {
        let mut c = cache.write();
        for i in 0..cache_number {
            let uuid = Uuid::new_v4();
            let mut block_metas = segment_info
                .blocks
                .iter()
                .map(|b: &Arc<BlockMetaTest>| Arc::new(b.as_ref().clone()))
                .collect::<Vec<_>>();
            block_metas.shrink_to_fit();
            let mut statistics = statistics.clone();
            statistics.col_stats.shrink_to_fit();
            let segment_info = SegmentInfoTest::new(block_metas, statistics);
            (*c).put(
                format!("{}", uuid.simple()),
                // format!("{}", i),
                // std::sync::Arc::new(segment_info.to_bytes()?),
                std::sync::Arc::new(serde_json::to_vec(&segment_info)?),
            );
        }
    }
    show_memory_usage("SegmentInfoCache", base_memory_usage, cache_number);

    Ok(())
}

fn populate_cache<T>(cache: &InMemoryItemCacheHolder<T>, item: T, num_cache: usize)
where T: Clone {
    let mut c = cache.write();
    for _ in 0..num_cache {
        let uuid = Uuid::new_v4();
        (*c).put(
            format!("{}", uuid.simple()),
            std::sync::Arc::new(item.clone()),
        );
    }
}

async fn setup() -> common_exception::Result<ThriftFileMetaData> {
    let fields = (0..23)
        .map(|_| TableField::new("id", TableDataType::Number(NumberDataType::Int32)))
        .collect::<Vec<_>>();

    let schema = TableSchemaRefExt::create(fields);

    let mut columns = vec![];
    for _ in 0..schema.fields().len() {
        // values do not matter
        let column = Int32Type::from_data(vec![1]);
        columns.push(column)
    }

    let block = DataBlock::new_from_columns(columns);
    let operator = Operator::new(opendal::services::Memory::default())?.finish();
    let loc_generator = TableMetaLocationGenerator::with_prefix("/".to_owned());
    let col_stats = gen_columns_statistics(&block, None, &schema)?;
    let block_writer = BlockWriter::new(&operator, &loc_generator);
    let (_block_meta, thrift_file_meta) = block_writer
        .write(FuseStorageFormat::Parquet, &schema, block, col_stats, None)
        .await?;

    Ok(thrift_file_meta.unwrap())
}

fn show_memory_usage(case: &str, base_memory_usage: u64, num_cache_items: usize) {
    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    {
        let memory_after = process.memory();
        let delta = memory_after - base_memory_usage;
        let delta_gb = (delta as f64) / 1024.0 / 1024.0 / 1024.0;
        eprintln!(
            "
            cache item type : {},
            number of cached items {},
            mem usage(B):{:+},
            mem usage(GB){:+}
            ",
            case, num_cache_items, delta, delta_gb
        );
    }
}
