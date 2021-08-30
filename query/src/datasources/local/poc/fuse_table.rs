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

use std::any::Any;
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
use common_datavalues::arrays::ArrayAgg;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_flights::storage_api_impl::ReadAction;
use common_flights::MetaApi;
use common_planners::InsertIntoPlan;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::TruncateTablePlan;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;
use uuid::Uuid;

use crate::catalogs::Table;
use crate::sessions::DatafuseQueryContextRef;

pub struct FuseTable<T> {
    pub(crate) data_accessor: T,
}

#[async_trait::async_trait]
impl<T> Table for FuseTable<T>
where T: DataAccessor + Send + Sync
{
    fn name(&self) -> &str {
        todo!()
    }

    fn engine(&self) -> &str {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn schema(&self) -> common_exception::Result<DataSchemaRef> {
        todo!()
    }

    fn is_local(&self) -> bool {
        todo!()
    }

    fn read_plan(
        &self,
        ctx: DatafuseQueryContextRef,
        scan: &ScanPlan,
        partitions: usize,
    ) -> Result<ReadDataSourcePlan> {
        todo!()
        //let expression = &scan.push_downs.filters;
        //let partitioning_def = scan.table_schema.
        //index_util::partitioning_expr(expression, schema.partitioning_def());
        //let index_pointer = schema.index_pointer();
        //let tbl_index = index_util::open(index_pointer)?;
        //let parts = tbl_index.apply(expr);
        //Ok(self.read_datasource_paln(parts))
    }

    async fn read(
        &self,
        ctx: DatafuseQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let progress_callback = ctx.progress_callback();
        let plan = source_plan.clone();
        // TODO config
        let bite = 1;
        let iter = std::iter::from_fn(move || match ctx.try_get_partitions(bite) {
            Err(e) => {
                log::warn!(
                    "error while getting next partitions from context, {}",
                    e.to_string()
                );
                // TODO is it correct to ignore this?
                None
            }
            Ok(parts) if parts.is_empty() => None,
            Ok(parts) => Some(parts.clone()),
        });
        todo!()

        //        let schema = source_plan.schema.clone();
        //        let parts = futures::stream::iter(iter);
        //        let streams = parts.then(move |parts| {
        //            let schema = schema.clone();
        //            async move {
        //                let r = self.read_partition(schema, &parts).await;
        //                r.unwrap_or_else(|e| {
        //                    Box::pin(futures::stream::once(async move {
        //                        Err(common_exception::ErrorCode::CannotReadFile(format!(
        //                            "get partition failure. partition [{:?}], error {}",
        //                            &parts, e
        //                        )))
        //                    }))
        //                })
        //            }
        //        });
        //
        //        let stream = ProgressStream::try_create(Box::pin(streams.flatten()), progress_callback?)?;
        //        Ok(Box::pin(stream))
    }

    async fn append_data(
        &self,
        ctx: DatafuseQueryContextRef,
        insert_plan: InsertIntoPlan,
    ) -> Result<()> {
        // 1. take out input stream from plan
        let mut block_stream = {
            match insert_plan.input_stream.lock().take() {
                Some(s) => s,
                None => return Err(ErrorCode::EmptyData("input stream consumed")),
            }
        };

        // 2. Append blocks to storage
        //
        let arrow_schema = insert_plan.schema.to_arrow();
        let append_results = self.append_blocks(arrow_schema, block_stream).await?;

        // 3. commit
        // let commit_message = to_commit_msg(append_results);

        Ok(())
    }

    async fn truncate(
        &self,
        _ctx: DatafuseQueryContextRef,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        todo!()
    }
}

pub type BlockStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = DataBlock> + Sync + Send + 'static>>;

/*
impl<T> FuseTable<T>
where T: DataAccessor + Send + Sync
{
    pub async fn append_blocks(
        &self,
        arrow_schema: ArrowSchema,
        mut stream: BlockStream,
    ) -> Result<()> {
        //TODO base patch
        let path = "";
        while let Some(block) = stream.next().await {
            let (rows, cols, wire_bytes) =
                (block.num_rows(), block.num_columns(), block.memory_size());

            let part_uuid = Uuid::new_v4().to_simple().to_string() + ".parquet";
            let location = format!("{}/{}", path, part_uuid);
            let buffer = self.write_in_memory(&arrow_schema, block)?;

            // TODO : it is silly to read it again
            let mut cursor = Cursor::new(buffer);
            let parquet_meta = read_metadata(&mut cursor)?;
            //let row_group_meta = parquet_meta.row_groups[0];
            let ord = parquet_meta.column_orders.unwrap()[1];

            let rows = parquet_meta.num_rows;
            for x in parquet_meta.row_groups {
                let rg_compressed_size = x.compressed_size();
                let rg_total_bytes_size = x.total_byte_size();
                x.columns().iter().for_each(|item| {
                    let compressed_size = item.compressed_size();
                    let uncompressed_size = item.uncompressed_size();
                    let num_values = item.num_values();
                    if let Some(Ok(st)) = item.statistics() {
                        let ref_s = st.as_ref();
                        let s = serialize_statistics(ref_s);
                        let distinct_count = s.distinct_count;
                        let min = s.min_value;
                        let max = s.max_value;
                        let null_count = s.null_count;
                    }
                    //let ty = item.type_();
                    //let py = st.physical_type();
                })
            }

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
*/
