//  Copyright 2021 Datafuse Labs.
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
//

use std::sync::Arc;

use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::arrow::io::parquet::write::*;
use common_arrow::parquet::encoding::Encoding;
use common_base::TrySpawn;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use opendal::Operator;
use parquet_format_async_temp::FileMetaData;

use crate::sessions::QueryContext;

pub async fn write_block(
    ctx: Arc<QueryContext>,
    arrow_schema: &ArrowSchema,
    block: DataBlock,
    data_accessor: Operator,
    location: &str,
) -> Result<(u64, FileMetaData)> {
    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Lz4, // let's begin with lz4
        version: Version::V2,
    };
    let batch = Chunk::try_from(block)?;
    let encodings: Vec<_> = arrow_schema
        .fields
        .iter()
        .map(|f| col_encoding(&f.data_type))
        .collect();

    let iter = vec![Ok(batch)];
    let row_groups = RowGroupIterator::try_new(iter.into_iter(), arrow_schema, options, encodings)?;

    // PutObject in S3 need to know the content-length in advance
    // multipart upload may intimidate this, but let's fit things together first
    // see issue #xxx

    // we need a configuration of block size threshold here
    let mut buf = Vec::with_capacity(100 * 1024 * 1024);

    let result =
        common_arrow::write_parquet_file(&mut buf, row_groups, arrow_schema.clone(), options)
            .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;

    // hot fix; use IO thread to do the write

    let loc = location.to_owned();
    let fut = async move {
        data_accessor
            .object(loc.as_str())
            .writer()
            .write_bytes(buf)
            .await
            .map_err(|e| ErrorCode::DalTransportError(e.to_string()))
    };

    let _ = ctx
        .get_storage_runtime()
        .try_spawn(fut)?
        .await
        .map_err(|e| ErrorCode::DalTransportError(format!("io task failure. {}", e.to_string())))?;

    Ok(result)
}

fn col_encoding(_data_type: &ArrowDataType) -> Encoding {
    // Although encoding does work, parquet2 has not implemented decoding of DeltaLengthByteArray yet, we fallback to Plain
    // From parquet2: Decoding "DeltaLengthByteArray"-encoded required V2 pages is not yet implemented for Binary.
    //
    //match data_type {
    //    ArrowDataType::Binary
    //    | ArrowDataType::LargeBinary
    //    | ArrowDataType::Utf8
    //    | ArrowDataType::LargeUtf8 => Encoding::DeltaLengthByteArray,
    //    _ => Encoding::Plain,
    //}
    Encoding::Plain
}
