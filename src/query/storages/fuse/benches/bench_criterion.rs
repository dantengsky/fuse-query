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

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use bytes::Bytes;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_native::read::NativeColumnsReader;
use databend_common_storages_fuse::io::serialize_block;
use databend_common_storages_fuse::io::WriteSettings;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_storages_common_table_meta::table::TableCompression;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

// 与 bench.rs 中相同的 NUM_ROWS 常量
const NUM_ROWS: usize = 6002732;

/// 从 Parquet 文件读取数据块和表架构
fn read_parquet_file() -> (DataBlock, TableSchema) {
    // 246M    /tmp/tpch_1/lineitem.parquet/
    // generate by duckdb:
    // CALL dbgen(sf=1)
    // EXPORT DATABASE '/tmp/tpch_1/' (FORMAT PARQUET)
    // let file = "/tmp/tpch_1/lineitem.parquet";
    let file = "/data2/zhaobr/databend-workshop/databend_tpch/tpch_1000/lineitem/100.parquet";
    let file = std::fs::File::open(file).unwrap();

    // Create a sync parquet reader with batch_size.
    // batch_size is the number of rows to read up to buffer once from pages, defaults to 1024
    let mut parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(usize::MAX)
        .build()
        .unwrap();

    let batch = parquet_reader.next().unwrap();
    let batch = batch.unwrap();
    let schema: TableSchema = batch.schema().as_ref().try_into().unwrap();
    let data_schema = DataSchema::from(&schema);
    let (block, _) = DataBlock::from_record_batch(&data_schema, &batch).unwrap();
    (block, schema)
}

/// 反序列化 Parquet 数据
fn deser_parquet_impl(a: Bytes) {
    let reader = ParquetRecordBatchReaderBuilder::try_new(a.clone())
        .unwrap()
        .with_batch_size(8192)
        .build()
        .unwrap();
    let batch: Vec<Result<RecordBatch, arrow_schema::ArrowError>> = reader.collect();
    let batch = batch.into_iter().map(|r| r.unwrap()).collect::<Vec<_>>();
    let num_rows: usize = batch.iter().map(|b| b.num_rows()).sum();
    assert_eq!(num_rows, NUM_ROWS);
}

/// 准备格式化文件
fn prepare_format_file(
    storage_format: FuseStorageFormat,
    compression: TableCompression,
    enable_encoding: bool,
) -> (Bytes, TableSchemaRef) {
    let (datablock, schema) = read_parquet_file();
    // write the block into temp memory buffers
    let max_page_size = 8192;
    let block_per_seg = 1000;

    let write_settings = WriteSettings {
        storage_format,
        table_compression: compression,
        max_page_size,
        block_per_seg,
        enable_encoding,
    };
    let schema = Arc::new(schema);
    let mut buffer = Vec::new();
    let _ = serialize_block(&write_settings, &schema, datablock, &mut buffer).unwrap();

    (buffer.into(), schema)
}

/// Parquet 反序列化基准测试（无编码）
fn bench_parquet_deser_no_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("parquet_deser_no_encoding");

    for compression in [TableCompression::LZ4, TableCompression::Zstd] {
        let (data, _) = prepare_format_file(FuseStorageFormat::Parquet, compression, false);

        // 设置吞吐量为输入数据的大小
        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{:?}", compression)),
            &data,
            |b, data| {
                b.iter(|| deser_parquet_impl(data.clone()));
            },
        );
    }
    group.finish();
}

/// Parquet 反序列化基准测试（有编码）
fn bench_parquet_deser_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("parquet_deser_encoding");
    // group.measurement_time(Duration::from_secs(3));

    for compression in [TableCompression::LZ4, TableCompression::Zstd] {
        let (data, _) = prepare_format_file(FuseStorageFormat::Parquet, compression, true);

        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{:?}", compression)),
            &data,
            |b, data| {
                b.iter(|| deser_parquet_impl(data.clone()));
            },
        );
    }
    group.finish();
}

/// Native 反序列化基准测试
fn bench_native_deser(c: &mut Criterion) {
    let mut group = c.benchmark_group("native_deser");
    // group.measurement_time(Duration::from_secs(3));

    for compression in [TableCompression::LZ4, TableCompression::Zstd] {
        let (data, schema) = prepare_format_file(FuseStorageFormat::Native, compression, false);

        group.throughput(Throughput::Bytes(data.len() as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{:?}", compression)),
            &(data.clone(), schema.clone()),
            |b, (data, schema)| {
                b.iter(|| {
                    let mut seek_a = std::io::Cursor::new(data.clone());
                    let metas =
                        databend_common_native::read::reader::read_meta(&mut seek_a).unwrap();

                    let reader = NativeColumnsReader::new().unwrap();
                    let mut columns = Vec::with_capacity(schema.fields().len());

                    for (meta, f) in metas.iter().zip(schema.fields().iter()) {
                        let bs = data
                            .slice(meta.offset as usize..(meta.offset + meta.total_len()) as usize);
                        let col = reader
                            .batch_read_column(vec![bs.as_ref()], f.data_type.clone(), vec![meta
                                .pages
                                .clone()])
                            .unwrap();

                        columns.push(col);
                    }
                    let datablock = DataBlock::new_from_columns(columns);
                    assert_eq!(datablock.num_rows(), NUM_ROWS);
                    criterion::black_box(datablock)
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_parquet_deser_no_encoding,
    bench_parquet_deser_encoding,
    bench_native_deser
);
criterion_main!(benches);
