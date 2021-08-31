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
use std::sync::Arc;

use common_dal::DataAccessor;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::InsertIntoPlan;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;
use futures::AsyncReadExt;

use crate::catalogs::Table;
use crate::datasources::local::poc::index_util;
use crate::datasources::local::poc::types::statistics::TableSnapshot;
use crate::sessions::DatafuseQueryContextRef;

pub struct FuseTable<T> {
    pub(crate) data_accessor: T,
}

const META_KEY_SNAPSHOT_OBJ_LOC: &'static str = "snapshot_location";
const META_KEY_SNAPSHOT_OBJ_SIZE: &'static str = "snapshot_size";

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
        let schema = self.schema()?;
        let segment_location = schema
            .meta()
            .get(META_KEY_SNAPSHOT_OBJ_LOC)
            .ok_or_else(|| {
                ErrorCode::IllegalSchema("metadata of snapshot info location not found")
            })?;

        let segment_size: u64 = schema
            .meta()
            .get(META_KEY_SNAPSHOT_OBJ_SIZE)
            .ok_or_else(|| ErrorCode::IllegalSchema("metadata of snapshot info size not found"))?
            .parse()?;

        let (tx, rx) = std::sync::mpsc::channel();

        let da = self.data_accessor.clone();
        let loc = segment_location.clone();
        {
            ctx.execute_task(async move {
                let input_stream = da.get_input_stream(&loc, Some(segment_size));
                match input_stream.await {
                    Ok(mut input) => {
                        let mut buffer = vec![];
                        // TODO send this error
                        input.read_to_end(&mut buffer).await?;
                        let _ = tx.send(Ok(buffer));
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                    }
                }
                Ok::<(), ErrorCode>(())
            })?;
        }

        let res = rx.recv().map_err(ErrorCode::from_std_error)?;

        let snapshot = serde_json::from_slice::<TableSnapshot>(&res?)?;

        let parts = index_util::filter(&snapshot, &scan.push_downs);
        let statistics = Statistics::default();

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
    }

    async fn read(
        &self,
        ctx: DatafuseQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let _progress_callback = ctx.progress_callback();
        let _plan = source_plan.clone();
        // TODO config
        let bite = 1;
        let _iter = std::iter::from_fn(move || match ctx.try_get_partitions(bite) {
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
