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
use std::io::Cursor;

use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::read::read_metadata;
use common_arrow::arrow::io::parquet::write::write_file;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::arrow::io::parquet::write::*;
use common_arrow::arrow::record_batch::RecordBatch;
use common_arrow::parquet::statistics::serialize_statistics;
use common_dal::DataAccessor;
use common_datablocks::DataBlock;
use common_exception::Result;
use futures::StreamExt;
use uuid::Uuid;

use crate::datasources::local::poc::fuse_table::FuseTable;
use crate::datasources::local::poc::types::statistics::BlockInfo;
use crate::datasources::local::poc::types::statistics::ColStats;

pub type BlockStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = DataBlock> + Sync + Send + 'static>>;

pub struct StatsAccumulator {}

impl StatsAccumulator {}
impl<T> FuseTable<T>
where T: DataAccessor + Send + Sync
{
    pub async fn append_blocks(
        &self,
        arrow_schema: ArrowSchema,
        mut stream: BlockStream,
    ) -> Result<()> {
        //TODO base patch
        let prefix = "tbl_id/tbl_name"; // a hint
        let mut blocks = vec![];

        while let Some(block) = stream.next().await {
            // 1. At present, for each block, we create a parquet
            let buffer = self.write_in_memory(&arrow_schema, block)?;
            let file_byte_size = buffer.len() as u64;

            // 2. extract statistics
            // TODO : it is silly to read it again
            let mut cursor = Cursor::new(buffer);
            let parquet_meta = read_metadata(&mut cursor)?;
            let row_count = parquet_meta.num_rows as u64;

            // We arrange exactly one row group, and one page insides the parquet file
            let rg_meta = &parquet_meta.row_groups[0];

            // todo check this transmute
            let compressed_size = rg_meta.compressed_size() as u64;
            let total_byte_size = rg_meta.total_byte_size() as u64;

            let col_stats = rg_meta
                .columns()
                .iter()
                .map(|item| {
                    let col_id = item.descriptor().type_().get_basic_info().id().unwrap();
                    let compressed_size = item.compressed_size();
                    let uncompressed_size = item.uncompressed_size();
                    //let _num_values = item.num_values();
                    // TODO error handling
                    let st = item.statistics().unwrap().unwrap();
                    let ref_s = st.as_ref();
                    let s = serialize_statistics(ref_s);
                    let distinct_count = s.distinct_count;
                    let null_count = s.null_count.unwrap();
                    let min = s.min_value.unwrap();
                    let max = s.max_value.unwrap();
                    (col_id, ColStats {
                        min,
                        max,
                        null_count: null_count as u64,
                        distinct_count: distinct_count.map(|v| v as u64),
                        uncompressed_size: uncompressed_size as u64,
                        compressed_size: compressed_size as u64,
                    })
                })
                .collect();

            let part_uuid = Uuid::new_v4().to_simple().to_string() + ".parquet";
            let location = format!("{}/{}", prefix, part_uuid);

            let block_info = BlockInfo {
                location: location.clone(),
                file_byte_size,
                compressed_size,
                total_byte_size,
                row_count,
                col_stats,
            };

            blocks.push(block_info);

            // write to storage
            self.data_accessor
                .put(&location, cursor.into_inner())
                .await?;
        }

        Ok(())
    }

    pub(crate) fn write_in_memory(
        &self,
        arrow_schema: &ArrowSchema,
        block: DataBlock,
    ) -> Result<Vec<u8>> {
        let options = WriteOptions {
            write_statistics: true,
            compression: Compression::Uncompressed,
            version: Version::V2,
        };
        use std::iter::repeat;
        let encodings: Vec<_> = repeat(Encoding::Plain).take(block.num_columns()).collect();

        let memory_size = block.memory_size();
        let batch = RecordBatch::try_from(block)?;
        let iter = vec![Ok(batch)];
        let row_groups =
            RowGroupIterator::try_new(iter.into_iter(), arrow_schema, options, encodings)?;
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
}
