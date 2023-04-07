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

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use common_base::runtime::Runtime;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::Pipeline;
use opendal::ops::OpRead;
use opendal::raw::Accessor;
use opendal::raw::AccessorInfo;
use opendal::raw::RpRead;
use opendal::Builder;
use opendal::Error;
use opendal::ErrorKind;
use opendal::Operator;
use opendal::Scheme;
use storages_common_index::Index;
use storages_common_index::RangeIndex;

use crate::fuse_lazy_part::FuseLazyPartInfo;
use crate::io::BlockReader;
use crate::operations::fuse_source::build_fuse_source_pipeline;
use crate::FuseTable;

impl FuseTable {
    pub fn create_block_reader(
        &self,
        projection: Projection,
        query_internal_columns: bool,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Arc<BlockReader>> {
        let table_schema = self.table_info.schema();
        BlockReader::create(
            self.operator.clone(),
            table_schema,
            projection,
            ctx,
            query_internal_columns,
        )
    }

    // Build the block reader.
    fn build_block_reader(
        &self,
        plan: &DataSourcePlan,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Arc<BlockReader>> {
        self.create_block_reader(
            PushDownInfo::projection_of_push_downs(&self.table_info.schema(), &plan.push_downs),
            plan.query_internal_columns,
            ctx,
        )
    }

    fn adjust_io_request(&self, ctx: &Arc<dyn TableContext>) -> Result<usize> {
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;

        if !self.operator.info().can_blocking() {
            Ok(std::cmp::max(max_threads, max_io_requests))
        } else {
            // For blocking fs, we don't want this to be too large
            Ok(std::cmp::min(max_threads, max_io_requests).clamp(1, 48))
        }
    }

    #[inline]
    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let mut lazy_init_segments = Vec::with_capacity(plan.parts.len());

        for part in &plan.parts.partitions {
            if let Some(lazy_part_info) = part.as_any().downcast_ref::<FuseLazyPartInfo>() {
                lazy_init_segments.push(lazy_part_info.segment_location.clone());
            }
        }

        if !lazy_init_segments.is_empty() {
            let table = self.clone();
            let table_info = self.table_info.clone();
            let push_downs = plan.push_downs.clone();
            let query_ctx = ctx.clone();
            let dal = self.operator.clone();
            let plan = plan.clone();

            // TODO: need refactor
            pipeline.set_on_init(move || {
                let table = table.clone();
                let table_info = table_info.clone();
                let ctx = query_ctx.clone();
                let dal = dal.clone();
                let push_downs = push_downs.clone();
                let lazy_init_segments = lazy_init_segments.clone();

                let partitions = Runtime::with_worker_threads(2, None)?.block_on(async move {
                    // if query from distribute query node, need to init segment id at first
                    let segment_id_map = if plan.query_internal_columns {
                        let snapshot = table.read_table_snapshot().await?;
                        if let Some(snapshot) = snapshot {
                            let segment_count = snapshot.segments.len();
                            let mut segment_id_map = HashMap::new();
                            for (i, segment_loc) in snapshot.segments.iter().enumerate() {
                                segment_id_map
                                    .insert(segment_loc.0.to_string(), segment_count - i - 1);
                            }
                            Some(segment_id_map)
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    eprintln!("reading data & pruning");
                    let faked_dal = Operator::new(FakedBuilder {})?.finish();
                    eprintln!("using faked dal");
                    let (_statistics, partitions) = table
                        .prune_snapshot_blocks(
                            ctx,
                            faked_dal,
                            push_downs,
                            table_info,
                            lazy_init_segments,
                            0,
                            segment_id_map,
                        )
                        .await?;

                    Result::<_, ErrorCode>::Ok(partitions)
                })?;

                query_ctx.set_partitions(partitions)?;
                Ok(())
            });
        }

        let block_reader = self.build_block_reader(plan, ctx.clone())?;
        let max_io_requests = self.adjust_io_request(&ctx)?;

        let topk = plan.push_downs.as_ref().and_then(|x| {
            x.top_k(
                plan.schema().as_ref(),
                self.cluster_key_str(),
                RangeIndex::supported_type,
            )
        });

        build_fuse_source_pipeline(
            ctx,
            pipeline,
            self.storage_format,
            block_reader,
            plan,
            topk,
            max_io_requests,
        )
    }
}

#[derive(Debug)]
struct FakeAccessor;

#[derive(Default)]
struct FakedBuilder;

use async_trait::async_trait;
#[async_trait]
impl Accessor for FakeAccessor {
    type Reader = FakedRead;
    type BlockingReader = ();
    type Writer = ();
    type BlockingWriter = ();
    type Pager = ();
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        AccessorInfo::default()
    }

    async fn read(&self, path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
        let (_, _) = (path, args);

        let rp_read = RpRead::new(100);
        Ok((rp_read, FakedRead {}))
    }
}

impl Builder for FakedBuilder {
    const SCHEME: Scheme = Scheme::Fs;
    type Accessor = FakeAccessor;

    fn from_map(map: HashMap<String, String>) -> Self {
        FakedBuilder {}
    }

    fn build(&mut self) -> opendal::Result<Self::Accessor> {
        Ok(FakeAccessor {})
    }
}

struct FakedRead;

use bytes::Bytes;
use opendal::raw::oio::Read;

impl Read for FakedRead {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<opendal::Result<usize>> {
        let (_, _) = (cx, buf);
        eprintln!("faked reader : poll read");

        // unimplemented!("poll_read is required to be implemented for oio::Read")
        Poll::Ready(Err(opendal::Error::new(
            ErrorKind::Unexpected,
            "output reader doesn't support seeking",
        )))
    }

    fn poll_seek(&mut self, cx: &mut Context<'_>, pos: io::SeekFrom) -> Poll<opendal::Result<u64>> {
        let (_, _) = (cx, pos);

        eprintln!("faked reader : poll seek");
        Poll::Ready(Err(opendal::Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support seeking",
        )))
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<opendal::Result<Bytes>>> {
        let _ = cx;

        eprintln!("faked reader : poll next");
        Poll::Ready(Some(Err(Error::new(
            ErrorKind::Unsupported,
            "output reader doesn't support streaming",
        ))))
    }
}
