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
use std::num::ParseIntError;
use std::sync::Arc;

use common_arrow::arrow::io::parquet::read::RecordReader;
use common_arrow::arrow_flight::utils::flight_data_from_arrow_batch;
use common_arrow::parquet::read;
use common_dal::DataAccessor;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::InsertIntoPlan;
use common_planners::Part;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_runtime::tokio::task;
use common_streams::ParquetStream;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use crossbeam::channel::bounded;
use crossbeam::channel::Receiver;
use crossbeam::channel::Sender;
use futures::AsyncReadExt;
use futures::FutureExt;
use futures::StreamExt;

use crate::catalogs::Table;
use crate::datasources::fuse_table::constants::META_KEY_SNAPSHOT_OBJ_LOC;
use crate::datasources::fuse_table::constants::META_KEY_SNAPSHOT_OBJ_SIZE;
use crate::datasources::fuse_table::io::snapshot_reader::read_table_snapshot;
use crate::datasources::fuse_table::types::table_snapshot::BlockMeta;
use crate::datasources::fuse_table::types::table_snapshot::TableSnapshot;
use crate::datasources::fuse_table::util::index_tools;
use crate::sessions::DatafuseQueryContextRef;

pub struct FuseTable<T> {
    pub(crate) data_accessor: T,
}

pub(crate) async fn read_part(_part: Part) -> SendableDataBlockStream {
    todo!()
}

#[async_trait::async_trait]
impl<T> Table for FuseTable<T>
where T: DataAccessor + Send + Sync + Clone + 'static
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
        _partitions: usize,
    ) -> Result<ReadDataSourcePlan> {
        // primary func: partition/cluster pruning

        let tbl_snapshot = self.table_snapshot(&ctx)?;
        if let Some(snapshot) = tbl_snapshot {
            let block_metas = index_tools::filter(&snapshot, &scan.push_downs);
            let (statistics, parts) = self.to_partitions(&block_metas);
            let plan = ReadDataSourcePlan {
                db: scan.schema_name.clone(),
                table: self.name().to_string(),
                table_id: scan.table_id,
                table_version: scan.table_version,
                schema: self.schema()?.clone(),
                parts,
                statistics,
                description: "".to_string(),
                scan_plan: Arc::new(scan.clone()),
                remote: true,
            };
            Ok(plan)
        } else {
            self.empty_read_source_plan(scan)
        }
    }

    async fn read(
        &self,
        ctx: DatafuseQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        // primary functionalities:
        // 1. read part data
        // 2. col pruning
        let _projection = source_plan.scan_plan.projected_schema.clone();

        let progress_callback = ctx.progress_callback();
        let iter = std::iter::from_fn(move || match ctx.try_get_partitions(1) {
            Err(_) => None,
            Ok(parts) if parts.is_empty() => None,
            Ok(parts) => Some(parts),
        })
        .flatten();
        let parts = futures::stream::iter(iter);
        let streams = parts.then(read_part);
        let stream = ProgressStream::try_create(Box::pin(streams.flatten()), progress_callback?)?;
        Ok(Box::pin(stream))
        //todo!()
    }

    async fn append_data(
        &self,
        _ctx: DatafuseQueryContextRef,
        insert_plan: InsertIntoPlan,
    ) -> Result<()> {
        // 1. take out input stream from plan
        let block_stream = {
            match insert_plan.input_stream.lock().take() {
                Some(s) => s,
                None => return Err(ErrorCode::EmptyData("input stream consumed")),
            }
        };

        // 2. Append blocks to storage
        //
        let arrow_schema = insert_plan.schema.to_arrow();
        let _append_results = self.append_blocks(arrow_schema, block_stream).await?;

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

impl<T> FuseTable<T>
where T: DataAccessor + Send + Sync + Clone + 'static
{
    fn table_snapshot(&self, ctx: &DatafuseQueryContextRef) -> Result<Option<TableSnapshot>> {
        let schema = self.schema()?;
        if let Some(loc) = schema.meta().get(META_KEY_SNAPSHOT_OBJ_LOC) {
            let len: u64 = schema
                .meta()
                .get(META_KEY_SNAPSHOT_OBJ_SIZE)
                .ok_or_else(|| {
                    ErrorCode::IllegalSchema("metadata of snapshot info size not found")
                })?
                .parse()
                .map_err(|e: ParseIntError| {
                    ErrorCode::IllegalSchema(format!(
                        "invalid meta key snapshot object size: {}",
                        e.to_string()
                    ))
                })?;

            let r = read_table_snapshot(self.data_accessor.clone(), &ctx, loc, len)?;
            Ok(Some(r))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn empty_read_source_plan(&self, scan: &ScanPlan) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            db: scan.schema_name.clone(),
            table: self.name().to_string(),
            table_id: scan.table_id,
            table_version: scan.table_version,
            schema: self.schema()?.clone(),
            parts: vec![],
            statistics: Statistics::default(),
            description: "".to_string(),
            scan_plan: Arc::new(scan.clone()),
            remote: true,
        })
    }

    pub(crate) fn to_partitions(&self, blocs: &[BlockMeta]) -> (Statistics, Partitions) {
        todo!()
    }
}
