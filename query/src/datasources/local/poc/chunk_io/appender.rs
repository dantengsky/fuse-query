// Copyright 2020 Datafuse Labs.
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
//

use std::convert::TryFrom;
use std::io::Cursor;
use std::iter::repeat;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::io::parquet::read::*;
use common_arrow::arrow::io::parquet::write::*;
use common_arrow::arrow::record_batch::RecordBatch;
use common_arrow::arrow_flight::utils::flight_data_to_arrow_batch;
use common_arrow::arrow_flight::FlightData;
use common_dal::DataAccessor;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_flights::storage_api_impl::AppendResult;
use common_store_api::BlockStream;
use futures::StreamExt;
use uuid::Uuid;

pub(crate) struct Appender<T> {
    data_accessor: T,
}

impl<T> Appender<T>
where T: DataAccessor
{
    /// Assumes
    /// - upstream caller has properly batched data
    pub async fn append_data(
        &self,
        schema: Schema,
        path: String,
        mut stream: BlockStream,
    ) -> Result<AppendResult> {
        while let Some(block) = stream.next().await {
            let (rows, cols, wire_bytes) =
                (block.num_rows(), block.num_columns(), block.memory_size());
            let part_uuid = Uuid::new_v4().to_simple().to_string() + ".parquet";
            let location = format!("{}/{}", path, part_uuid);
            let buffer = write_in_memory(block)?;

            // get statistics, and yes, it is silly to read it again
            let mut cursor = Cursor::new(buffer);
            let parquet_meta = read_metadata(&mut cursor)?;

            let rows = parquet_meta.num_rows;
            for x in parquet_meta.row_groups {
                x.columns().iter().for_each(|item| {
                    let compressed_size = item.compressed_size();
                    let uncompressed_size = item.uncompressed_size();
                    let num_values = item.num_values();
                    let meta = item.data_page_offset();
                    let st = item.statistics()??;
                    let ty = item.type_();
                    let py = st.physical_type();
                    py.
                })
            }

            self.data_accessor.put(&location, buffer).await?;
        }
        Ok(result)
    }
}

pub(crate) fn write_in_memory(arrow_schema: &Schema, block: DataBlock) -> Result<Vec<u8>> {
    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Uncompressed,
        version: Version::V2,
    };
    let encodings: Vec<_> = repeat(Encoding::Plain).take(block.num_columns()).collect();
    let memory_size = block.memory_size();
    let batch = RecordBatch::try_from(block)?;
    let iter = vec![Ok(batch)];
    let row_groups = RowGroupIterator::try_new(iter.into_iter(), arrow_schema, options, encodings)?;
    let writer = Vec::with_capacity(memory_size);
    let mut cursor = Cursor::new(writer);
    let parquet_schema = row_groups.parquet_schema().clone();
    write_file(
        &mut cursor,
        row_groups,
        &arrow_schema,
        parquet_schema,
        options,
        None,
    )?;

    Ok(cursor.into_inner())
}
