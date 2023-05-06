// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#[macro_use]
extern crate criterion;

use std::collections::HashMap;
use std::sync::Arc;

use common_expression::types::NumberScalar;
use common_expression::ColumnId;
use common_expression::Scalar;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::statistics::STATS_STRING_PREFIX_LEN;
use criterion::black_box;
use criterion::Criterion;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ColumnMeta;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::Compression;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::SingleColumnMeta;
use storages_common_table_meta::meta::Statistics;

fn bench(c: &mut Criterion) {
    // let mut group = c.benchmark_group("segment_deser");

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

    c.bench_function("deser_segment_info", |b| {
        b.iter(|| {
            let _segment = SegmentInfo::from_bytes_new(black_box(&bytes)).unwrap();
        })
    });
}

fn bench_segment_v2(c: &mut Criterion) {
    // let mut group = c.benchmark_group("segment_deser");

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
    let bytes = serde_json::to_vec(&segment_info).unwrap();

    c.bench_function("deser_json_segment_info", |b| {
        b.iter(|| {
            // let _segment = SegmentInfo::from_bytes_new(black_box(&bytes)).unwrap();
            let _segment: SegmentInfo = serde_json::from_slice((&bytes)).unwrap();
        })
    });
}

criterion_group!(benches, bench, bench_segment_v2);
criterion_main!(benches);
