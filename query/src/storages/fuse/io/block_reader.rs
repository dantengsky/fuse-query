// Copyright 2021 Datafuse Labs.
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

use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::read::read_columns_many_async;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_base::tokio::sync::Semaphore;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;
use common_tracing::tracing::debug_span;
use common_tracing::tracing::Instrument;
use futures::future::BoxFuture;
use opendal::Operator;

use super::parallel_async_reader::ParallelAsyncReader;
use crate::sessions::QueryContext;
use crate::storages::fuse::io::meta_readers::BlockMetaReader;

pub struct BlockReader {
    data_accessor: Operator,
    path: String,
    block_schema: DataSchemaRef,
    arrow_table_schema: ArrowSchema,
    projection: Vec<usize>,
    file_len: u64,
    metadata_reader: BlockMetaReader,
    ctx: Arc<QueryContext>,
}

impl BlockReader {
    pub fn new(
        data_accessor: Operator,
        path: String,
        table_schema: DataSchemaRef,
        projection: Vec<usize>,
        file_len: u64,
        reader: BlockMetaReader,
        ctx: Arc<QueryContext>,
    ) -> Self {
        let block_schema = Arc::new(table_schema.project(projection.clone()));
        let arrow_table_schema = table_schema.to_arrow();
        Self {
            data_accessor,
            path,
            block_schema,
            arrow_table_schema,
            projection,
            file_len,
            metadata_reader: reader,
            ctx,
        }
    }

    pub async fn read(self: &Arc<Self>) -> Result<DataBlock> {
        let exec = self.ctx.get_storage_executor();
        let reader = self.clone();
        let r = exec
            .spawn(async move {
                let block = reader.do_read().await?;
                Ok(block)
            })
            .await
            .map_err(|e| ErrorCode::LogicalError(format!("{}", e.to_string())))?;
        r
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn do_read(&self) -> Result<DataBlock> {
        let block_meta = &self.metadata_reader.read(self.path.as_str()).await?;
        let metadata = block_meta.inner();

        // FUSE uses exact one "row group"
        let num_row_groups = metadata.row_groups.len();
        let row_group = if num_row_groups != 1 {
            return Err(ErrorCode::LogicalError(format!(
                "invalid parquet file, expect exact one row group insides, but got {}",
                num_row_groups
            )));
        } else {
            &metadata.row_groups[0]
        };

        let arrow_fields = &self.arrow_table_schema.fields;
        let stream_len = self.file_len;
        let parquet_fields = metadata.schema().fields();

        // read_columns_many_async use field name to filter columns
        let fields_to_read = self
            .projection
            .clone()
            .into_iter()
            .map(|idx| {
                let origin = arrow_fields[idx].clone();
                Field {
                    name: parquet_fields[idx].name().to_string(),
                    ..origin
                }
            })
            .collect();

        // todo(youngsofun): make this a config
        let semaphore = Arc::new(Semaphore::new(2));
        let factory = || {
            let data_accessor = self.data_accessor.clone();
            let path = self.path.clone();
            let semaphore = semaphore.clone();
            Box::pin(async move {
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                Ok(ParallelAsyncReader::new(
                    permit,
                    data_accessor
                        .object(path.as_str())
                        .limited_reader(stream_len),
                ))
            }) as BoxFuture<_>
        };

        let column_chunks = read_columns_many_async(factory, row_group, fields_to_read, None)
            .instrument(debug_span!("block_reader_read_columns").or_current())
            .await
            .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;

        let mut chunks =
            RowGroupDeserializer::new(column_chunks, row_group.num_rows() as usize, None);

        // expect exact one chunk
        let chunk = match chunks.next() {
            None => return Err(ErrorCode::ParquetError("fail to get a chunk")),
            Some(chunk) => chunk.map_err(|e| ErrorCode::ParquetError(e.to_string()))?,
        };

        let block = DataBlock::from_chunk(&self.block_schema, &chunk)?;
        Ok(block)
    }
}
