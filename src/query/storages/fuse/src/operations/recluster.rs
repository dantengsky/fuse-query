// Copyright 2021 Datafuse Labs
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

use databend_common_base::runtime::TrySpawn;
use databend_common_base::GLOBAL_TASK;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_sql::BloomIndexColumns;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use log::warn;
use opendal::Operator;

use crate::operations::ReclusterMutator;
use crate::pruning::create_segment_location_vector;
use crate::pruning::PruningContext;
use crate::pruning::SegmentPruner;
use crate::FuseTable;
use crate::SegmentLocation;
use crate::DEFAULT_AVG_DEPTH_THRESHOLD;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD;

impl FuseTable {
    /// The flow of Pipeline is as follows:
    // ┌──────────┐     ┌───────────────┐     ┌─────────┐
    // │FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┐
    // └──────────┘     └───────────────┘     └─────────┘    │
    // ┌──────────┐     ┌───────────────┐     ┌─────────┐    │     ┌──────────────┐     ┌─────────┐
    // │FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┤────►│MultiSortMerge├────►│Resize(N)├───┐
    // └──────────┘     └───────────────┘     └─────────┘    │     └──────────────┘     └─────────┘   │
    // ┌──────────┐     ┌───────────────┐     ┌─────────┐    │                                        │
    // │FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┘                                        │
    // └──────────┘     └───────────────┘     └─────────┘                                             │
    // ┌──────────────────────────────────────────────────────────────────────────────────────────────┘
    // │         ┌──────────────┐
    // │    ┌───►│SerializeBlock├───┐
    // │    │    └──────────────┘   │
    // │    │    ┌──────────────┐   │    ┌─────────┐    ┌────────────────┐     ┌─────────────┐     ┌──────────┐
    // └───►│───►│SerializeBlock├───┤───►│Resize(1)├───►│SerializeSegment├────►│ReclusterAggr├────►│CommitSink│
    //      │    └──────────────┘   │    └─────────┘    └────────────────┘     └─────────────┘     └──────────┘
    //      │    ┌──────────────┐   │
    //      └───►│SerializeBlock├───┘
    //           └──────────────┘
    #[async_backtrace::framed]
    pub async fn build_recluster_mutator(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        limit: Option<usize>,
    ) -> Result<Option<ReclusterMutator>> {
        // Status.
        {
            let status = "recluster: begin to run recluster";
            ctx.set_status_info(status);
            log::info!("{}", status);
        }

        if self.cluster_key_meta.is_none() {
            return Ok(None);
        }

        let snapshot_opt = self.read_table_snapshot().await?;
        let snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no recluster.
            return Ok(None);
        };

        let settings = ctx.get_settings();
        let mut max_tasks = 1;
        let cluster = ctx.get_cluster();
        if !cluster.is_empty() && settings.get_enable_distributed_recluster()? {
            max_tasks = cluster.nodes.len();
        }

        let schema = self.schema_with_stream();
        let default_cluster_key_id = self.cluster_key_meta.clone().unwrap().0;
        let block_thresholds = self.get_block_thresholds();
        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);
        let avg_depth_threshold = self.get_option(
            FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD,
            DEFAULT_AVG_DEPTH_THRESHOLD,
        );
        let block_count = snapshot.summary.block_count;
        let threshold = (block_count as f64 * avg_depth_threshold)
            .max(1.0)
            .min(64.0);
        let mut mutator = ReclusterMutator::try_create(
            ctx.clone(),
            snapshot.clone(),
            schema,
            threshold,
            block_thresholds,
            default_cluster_key_id,
            max_tasks,
        )?;

        let segment_locations = snapshot.segments.clone();
        let segment_locations = create_segment_location_vector(segment_locations, None);

        let max_threads = settings.get_max_threads()? as usize;
        let limit = limit.unwrap_or(1000);

        'F: for chunk in segment_locations.chunks(limit) {
            // read segments.
            let compact_segments = Self::segment_pruning(
                &ctx,
                self.schema_with_stream(),
                self.get_operator(),
                &push_downs,
                chunk.to_vec(),
            )
            .await?;
            if compact_segments.is_empty() {
                continue;
            }

            // select the segments with the highest depth.
            let selected_segs = ReclusterMutator::select_segments(
                &compact_segments,
                block_per_seg,
                max_threads * 2,
                default_cluster_key_id,
            )?;
            // select the blocks with the highest depth.
            if selected_segs.is_empty() {
                for compact_segment in compact_segments.into_iter() {
                    if !ReclusterMutator::segment_can_recluster(
                        &compact_segment.1.summary,
                        block_per_seg,
                        default_cluster_key_id,
                    ) {
                        continue;
                    }

                    if mutator.target_select(vec![compact_segment]).await? {
                        log::info!("Number of segments scheduled for recluster: 1");
                        break 'F;
                    }
                }
            } else {
                let mut selected_segments = Vec::with_capacity(selected_segs.len());
                selected_segs.into_iter().for_each(|i| {
                    selected_segments.push(compact_segments[i].clone());
                });

                let num_selected = selected_segments.len();
                if mutator.target_select(selected_segments).await? {
                    log::info!(
                        "Number of segments scheduled for recluster: {}",
                        num_selected
                    );
                    break;
                }
            }
        }

        Ok(Some(mutator))
    }

    pub async fn segment_pruning(
        ctx: &Arc<dyn TableContext>,
        schema: TableSchemaRef,
        dal: Operator,
        push_down: &Option<PushDownInfo>,
        mut segment_locs: Vec<SegmentLocation>,
    ) -> Result<Vec<(SegmentLocation, Arc<CompactSegmentInfo>)>> {
        let max_concurrency = {
            let max_threads = ctx.get_settings().get_max_threads()? as usize;
            let v = std::cmp::max(max_threads, 10);
            if v > max_threads {
                warn!(
                    "max_threads setting is too low {}, increased to {}",
                    max_threads, v
                )
            }
            v
        };

        // Only use push_down here.
        let pruning_ctx = PruningContext::try_create(
            ctx,
            dal,
            schema.clone(),
            push_down,
            None,
            vec![],
            BloomIndexColumns::None,
            max_concurrency,
        )?;

        let segment_pruner = SegmentPruner::create(pruning_ctx.clone(), schema)?;
        let mut remain = segment_locs.len() % max_concurrency;
        let batch_size = segment_locs.len() / max_concurrency;
        let mut works = Vec::with_capacity(max_concurrency);

        while !segment_locs.is_empty() {
            let gap_size = std::cmp::min(1, remain);
            let batch_size = batch_size + gap_size;
            remain -= gap_size;

            let batch = segment_locs.drain(0..batch_size).collect::<Vec<_>>();
            works.push(pruning_ctx.pruning_runtime.spawn(GLOBAL_TASK, {
                let segment_pruner = segment_pruner.clone();

                async move {
                    let pruned_segments = segment_pruner.pruning(batch).await?;
                    Result::<_, ErrorCode>::Ok(pruned_segments)
                }
            }));
        }

        match futures::future::try_join_all(works).await {
            Err(e) => Err(ErrorCode::StorageOther(format!(
                "segment pruning failure, {}",
                e
            ))),
            Ok(workers) => {
                let mut metas = vec![];
                for worker in workers {
                    let res = worker?;
                    metas.extend(res);
                }
                Ok(metas)
            }
        }
    }
}
