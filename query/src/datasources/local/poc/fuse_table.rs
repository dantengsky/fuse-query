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

use common_datavalues::DataSchemaRef;
use common_flights::storage_api_impl::ReadAction;
use common_planners::InsertIntoPlan;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::TruncateTablePlan;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use crate::datasources::Table;
use crate::sessions::DatafuseQueryContextRef;

struct FuseTable {}

impl Table for FuseTable {
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
    ) -> common_exception::Result<ReadDataSourcePlan> {
        //
        let expression = &scan.push_downs.filters;

        let partitioning_def = scan.table_schema.
        index_util::partitioning_expr(expression, schema.partitioning_def());
        let index_pointer = schema.index_pointer();
        let tbl_index = index_util::open(index_pointer)?;
        let parts = tbl_index.apply(expr);
        Ok(self.read_datasource_paln(parts))
    }

    async fn read(
        &self,
        ctx: DatafuseQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> common_exception::Result<SendableDataBlockStream> {
        let progress_callback = ctx.progress_callback();
        let plan = source_plan.clone();
        let iter = std::iter::from_fn(move || match ctx.try_get_partitions(1) {
            Err(_) => None, // TODO error handling
            Ok(parts) if parts.is_empty() => None,
            Ok(parts) => {
                let plan = plan.clone();
                Some(ReadAction {
                    part: parts[0].clone(),
                    push_down: PlanNode::ReadSource(plan),
                })
            }
        });

        let schema = source_plan.schema.clone();
        let parts = futures::stream::iter(iter);
        let streams = parts.then(move |parts| {
            let schema = schema.clone();
            async move {
                let r = self.read_partition(schema, &parts).await;
                r.unwrap_or_else(|e| {
                    Box::pin(futures::stream::once(async move {
                        Err(ErrorCode::CannotReadFile(format!(
                            "get partition failure. partition [{:?}], error {}",
                            &parts, e
                        )))
                    }))
                })
            }
        });

        let stream = ProgressStream::try_create(Box::pin(streams.flatten()), progress_callback?)?;
        Ok(Box::pin(stream))
    }

    async fn append_data(
        &self,
        _ctx: DatafuseQueryContextRef,
        _insert_plan: InsertIntoPlan,
    ) -> common_exception::Result<()> {
        todo!()
    }

    async fn truncate(
        &self,
        _ctx: DatafuseQueryContextRef,
        _truncate_plan: TruncateTablePlan,
    ) -> common_exception::Result<()> {
        todo!()
    }
}
