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

use std::collections::HashMap;
use std::sync::Arc;

use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_arrow::parquet::metadata::RowGroupMetaData;
use common_arrow::parquet::read::BasicDecompressor;
use common_arrow::parquet::read::PageIterator;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;
use common_tracing::tracing::debug;
use common_tracing::tracing::debug_span;
use common_tracing::tracing::Instrument;
use common_tracing::tracing::Span;
use futures::future::try_join_all;
use futures::AsyncReadExt;
use opendal::Operator;

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

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read(&self) -> Result<DataBlock> {
        let block_meta = self.metadata_reader.read(self.path.as_str()).await?;
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
        //let stream_len = self.file_len;
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

        let num_rows = row_group.num_rows();
        let column_chunks = self
            .read_columns(
                self.data_accessor.clone(),
                self.path.clone(),
                //block_meta,
                &block_meta.inner().row_groups[0],
                fields_to_read,
            )
            .instrument(debug_span!("block_reader_read_columns").or_current())
            .await
            .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;

        let mut chunks = RowGroupDeserializer::new(column_chunks, num_rows as usize, None);

        // expect exact one chunk
        let chunk = match chunks.next() {
            None => return Err(ErrorCode::ParquetError("fail to get a chunk")),
            Some(chunk) => chunk.map_err(|e| ErrorCode::ParquetError(e.to_string()))?,
        };

        let block = DataBlock::from_chunk(&self.block_schema, &chunk)?;
        Ok(block)
    }

    pub async fn read_columns<'a>(
        &self,
        data_accessor: Operator,
        path: String,
        row_group: &RowGroupMetaData,
        fields: Vec<Field>,
    ) -> Result<Vec<ArrayIter<'a>>> {
        let exec = self.ctx.get_storage_executor();
        let columns = row_group.columns();
        let col_map = columns
            .iter()
            .map(|col| (col.descriptor().path_in_schema()[0].clone(), col))
            .collect::<HashMap<String, &ColumnChunkMetaData>>();
        let mut futs = vec![];
        let mut col_meta = vec![];
        let res;
        {
            let _span_read_cols = debug_span!("issue_read_cols");
            debug!("issue readings | Begin");
            for field in &fields {
                if let Some(meta) = col_map.get(field.name.as_str()) {
                    let (start, len) = meta.byte_range();
                    let mut reader = data_accessor.object(path.as_str()).range_reader(start, len);
                    let mut chunk = vec![0; len as usize];
                    debug!("read_exact, offset {}, len {}", start, len);
                    let current = Span::current();
                    let fut = async move {
                        reader
                            .read_exact(&mut chunk)
                            .instrument(
                                debug_span!(parent: current, "read_exact_col_chunk").or_current(),
                            )
                            .await?;
                        Ok::<_, ErrorCode>(chunk)
                    };
                    // spawn io tasks
                    let fut = exec.spawn(fut);
                    futs.push(fut);
                    col_meta.push(meta);
                }
            }
            debug!("issue readings | End");
            res = try_join_all(futs)
                .instrument(debug_span!("join_all_read_cols").or_current())
                .await
                .map_err(|e| ErrorCode::LogicalError(e.to_string()))?;
        }

        let mut result = vec![];
        {
            let _span_build_array_iter = debug_span!("build_array_iter");
            let chunk_size = row_group.num_rows() as usize;
            for (i, chunk) in res.into_iter().enumerate() {
                let chunk = chunk?;
                let col_meta = col_meta[i];
                let field = fields[i].clone();
                let pages = PageIterator::new(
                    std::io::Cursor::new(chunk),
                    col_meta.num_values(),
                    col_meta.compression(),
                    col_meta.descriptor().clone(),
                    Arc::new(|_, _| true),
                    vec![],
                );

                let l = BasicDecompressor::new(pages, vec![]);
                let r = col_meta.descriptor().type_();
                let c = column_iter_to_arrays(vec![l], vec![r], field.clone(), chunk_size)?;
                result.push(c)
            }
        }
        Ok(result)
    }
}
