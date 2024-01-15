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

use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Partitions;
use common_exception::ErrorCode;
use common_exception::Result;
use common_settings::ReplaceIntoShuffleStrategy;
use common_sql::executor::physical_plans::CompactSource;
use common_sql::executor::physical_plans::CopyIntoTable;
use common_sql::executor::physical_plans::CopyIntoTableSource;
use common_sql::executor::physical_plans::DeleteSource;
use common_sql::executor::physical_plans::QuerySource;
use common_sql::executor::physical_plans::ReclusterSource;
use common_sql::executor::physical_plans::ReclusterTask;
use common_sql::executor::physical_plans::ReplaceDeduplicate;
use common_sql::executor::physical_plans::ReplaceInto;
use common_sql::executor::physical_plans::TableScan;
use common_storages_fuse::TableContext;
use storages_common_table_meta::meta::BlockSlotDescription;
use storages_common_table_meta::meta::Location;

use crate::api::DataExchange;
use crate::schedulers::Fragmenter;
use crate::schedulers::QueryFragmentAction;
use crate::schedulers::QueryFragmentActions;
use crate::schedulers::QueryFragmentsActions;
use crate::sessions::QueryContext;
use crate::sql::executor::PhysicalPlan;
use crate::sql::executor::PhysicalPlanReplacer;

/// Type of plan fragment
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FragmentType {
    /// Root fragment of a query plan
    Root,

    /// Intermediate fragment of a query plan,
    /// doesn't contain any `TableScan` operator.
    Intermediate,

    /// Leaf fragment of a query plan, which contains
    /// a `TableScan` operator.
    Source,
    /// Leaf fragment of a delete plan, which contains
    /// a `DeleteSource` operator.
    DeleteLeaf,
    /// Intermediate fragment of a replace into plan, which contains a `ReplaceInto` operator.
    ReplaceInto,
    Compact,
    Recluster,
}

#[derive(Clone)]
pub struct PlanFragment {
    pub plan: PhysicalPlan,
    pub fragment_type: FragmentType,
    pub fragment_id: usize,
    pub exchange: Option<DataExchange>,
    pub query_id: String,

    // The fragments to ask data from.
    pub source_fragments: Vec<PlanFragment>,
}

impl PlanFragment {
    pub fn get_actions(
        &self,
        ctx: Arc<QueryContext>,
        actions: &mut QueryFragmentsActions,
    ) -> Result<()> {
        for input in self.source_fragments.iter() {
            input.get_actions(ctx.clone(), actions)?;
        }

        let mut fragment_actions = QueryFragmentActions::create(self.fragment_id);

        match &self.fragment_type {
            FragmentType::Root => {
                let action = QueryFragmentAction::create(
                    Fragmenter::get_local_executor(ctx),
                    self.plan.clone(),
                );
                fragment_actions.add_action(action);
            }
            FragmentType::Intermediate => {
                if self
                    .source_fragments
                    .iter()
                    .any(|fragment| matches!(&fragment.exchange, Some(DataExchange::Merge(_))))
                {
                    // If this is a intermediate fragment with merge input,
                    // we will only send it to coordinator node.
                    let action = QueryFragmentAction::create(
                        Fragmenter::get_local_executor(ctx),
                        self.plan.clone(),
                    );
                    fragment_actions.add_action(action);
                } else {
                    // Otherwise distribute the fragment to all the executors.
                    for executor in Fragmenter::get_executors(ctx) {
                        let action = QueryFragmentAction::create(executor, self.plan.clone());
                        fragment_actions.add_action(action);
                    }
                }
            }
            FragmentType::Source => {
                // Redistribute partitions
                self.redistribute_source_fragment(ctx, &mut fragment_actions)?;
            }
            FragmentType::DeleteLeaf => {
                self.redistribute_delete_leaf(ctx, &mut fragment_actions)?;
            }
            FragmentType::ReplaceInto => {
                // Redistribute partitions
                self.redistribute_replace_into(ctx, &mut fragment_actions)?;
            }
            FragmentType::Compact => {
                self.redistribute_compact(ctx, &mut fragment_actions)?;
            }
            FragmentType::Recluster => {
                self.redistribute_recluster(ctx, &mut fragment_actions)?;
            }
        }

        if let Some(ref exchange) = self.exchange {
            fragment_actions.set_exchange(exchange.clone());
        }
        actions.add_fragment_actions(fragment_actions)
    }

    /// Redistribute partitions of current source fragment to executors.
    fn redistribute_source_fragment(
        &self,
        ctx: Arc<QueryContext>,
        fragment_actions: &mut QueryFragmentActions,
    ) -> Result<()> {
        if self.fragment_type != FragmentType::Source {
            return Err(ErrorCode::Internal(
                "Cannot redistribute a non-source fragment".to_string(),
            ));
        }

        let read_source = self.get_read_source()?;

        let executors = Fragmenter::get_executors(ctx);
        // Redistribute partitions of ReadDataSourcePlan.
        let partitions = &read_source.parts;
        let partition_reshuffle = partitions.reshuffle(executors)?;

        for (executor, parts) in partition_reshuffle.iter() {
            let mut new_read_source = read_source.clone();
            new_read_source.parts = parts.clone();
            let mut plan = self.plan.clone();

            // Replace `ReadDataSourcePlan` with rewritten one and generate new fragment for it.
            let mut replace_read_source = ReplaceReadSource {
                source: new_read_source,
            };
            plan = replace_read_source.replace(&plan)?;

            fragment_actions
                .add_action(QueryFragmentAction::create(executor.clone(), plan.clone()));
        }

        Ok(())
    }

    fn redistribute_delete_leaf(
        &self,
        ctx: Arc<QueryContext>,
        fragment_actions: &mut QueryFragmentActions,
    ) -> Result<()> {
        let plan = match &self.plan {
            PhysicalPlan::ExchangeSink(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let plan = match plan.input.as_ref() {
            PhysicalPlan::DeleteSource(plan) => plan,
            _ => unreachable!("logic error"),
        };

        let partitions: &Partitions = &plan.parts;
        let executors = Fragmenter::get_executors(ctx);

        let partition_reshuffle = partitions.reshuffle(executors)?;

        for (executor, parts) in partition_reshuffle.into_iter() {
            let mut plan = self.plan.clone();

            let mut replace_delete_source = ReplaceDeleteSource { partitions: parts };
            plan = replace_delete_source.replace(&plan)?;

            fragment_actions.add_action(QueryFragmentAction::create(executor, plan));
        }

        Ok(())
    }

    fn redistribute_replace_into(
        &self,
        ctx: Arc<QueryContext>,
        fragment_actions: &mut QueryFragmentActions,
    ) -> Result<()> {
        let plan = match &self.plan {
            PhysicalPlan::ExchangeSink(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let plan = match plan.input.as_ref() {
            PhysicalPlan::ReplaceInto(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let partitions = &plan.segments;
        let executors = Fragmenter::get_executors(ctx.clone());
        let local_id = ctx.get_cluster().local_id.clone();
        match ctx.get_settings().get_replace_into_shuffle_strategy()? {
            ReplaceIntoShuffleStrategy::SegmentLevelShuffling => {
                let partition_reshuffle = Self::reshuffle(executors, partitions.clone())?;
                for (executor, parts) in partition_reshuffle.into_iter() {
                    let mut plan = self.plan.clone();
                    let need_insert = executor == local_id;

                    let mut replace_replace_into = ReplaceReplaceInto {
                        partitions: parts,
                        slot: None,
                        need_insert,
                    };
                    plan = replace_replace_into.replace(&plan)?;

                    fragment_actions.add_action(QueryFragmentAction::create(executor, plan));
                }
            }
            ReplaceIntoShuffleStrategy::BlockLevelShuffling => {
                let num_slots = executors.len();
                // assign all the segment locations to each one of the executors,
                // but for each segment, one executor only need to take part of the blocks
                for (executor_idx, executor) in executors.into_iter().enumerate() {
                    let mut plan = self.plan.clone();
                    let need_insert = executor == local_id;
                    let mut replace_replace_into = ReplaceReplaceInto {
                        partitions: partitions.clone(),
                        slot: Some(BlockSlotDescription {
                            num_slots,
                            slot: executor_idx as u32,
                        }),
                        need_insert,
                    };
                    plan = replace_replace_into.replace(&plan)?;

                    fragment_actions.add_action(QueryFragmentAction::create(executor, plan));
                }
            }
        }
        Ok(())
    }

    fn redistribute_compact(
        &self,
        ctx: Arc<QueryContext>,
        fragment_actions: &mut QueryFragmentActions,
    ) -> Result<()> {
        let exchange_sink = match &self.plan {
            PhysicalPlan::ExchangeSink(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let compact_block = match exchange_sink.input.as_ref() {
            PhysicalPlan::CompactSource(plan) => plan,
            _ => unreachable!("logic error"),
        };

        let partitions: &Partitions = &compact_block.parts;
        let executors = Fragmenter::get_executors(ctx);

        let partition_reshuffle = partitions.reshuffle(executors)?;

        for (executor, parts) in partition_reshuffle.into_iter() {
            let mut plan = self.plan.clone();

            let mut replace_compact_source = ReplaceCompactBlock { partitions: parts };
            plan = replace_compact_source.replace(&plan)?;

            fragment_actions.add_action(QueryFragmentAction::create(executor, plan));
        }

        Ok(())
    }

    fn redistribute_recluster(
        &self,
        ctx: Arc<QueryContext>,
        fragment_actions: &mut QueryFragmentActions,
    ) -> Result<()> {
        let exchange_sink = match &self.plan {
            PhysicalPlan::ExchangeSink(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let recluster = match exchange_sink.input.as_ref() {
            PhysicalPlan::ReclusterSource(plan) => plan,
            _ => unreachable!("logic error"),
        };

        let tasks = recluster.tasks.clone();
        let executors = Fragmenter::get_executors(ctx);
        if tasks.len() > executors.len() {
            return Err(ErrorCode::Internal(format!(
                "Cannot recluster {} tasks to {} executors",
                tasks.len(),
                executors.len()
            )));
        }

        let task_reshuffle = Self::reshuffle(executors, tasks)?;
        for (executor, tasks) in task_reshuffle.into_iter() {
            let mut plan = self.plan.clone();
            let mut replace_recluster = ReplaceReclusterSource { tasks };
            plan = replace_recluster.replace(&plan)?;
            fragment_actions.add_action(QueryFragmentAction::create(executor, plan));
        }

        Ok(())
    }

    fn reshuffle<T: Clone>(
        executors: Vec<String>,
        partitions: Vec<T>,
    ) -> Result<HashMap<String, Vec<T>>> {
        let num_parts = partitions.len();
        let num_executors = executors.len();
        let mut executors_sorted = executors;
        executors_sorted.sort();

        let mut parts = partitions
            .into_iter()
            .enumerate()
            .map(|(idx, p)| (idx % num_executors, p))
            .collect::<Vec<_>>();
        parts.sort_by(|a, b| a.0.cmp(&b.0));
        let partitions: Vec<_> = parts.into_iter().map(|x| x.1).collect();

        // parts_per_executor = num_parts / num_executors
        // remain = num_parts % num_executors
        // part distribution:
        //   executor number      | Part number of each executor
        // ------------------------------------------------------
        // num_executors - remain |   parts_per_executor
        //     remain             |   parts_per_executor + 1
        let mut executor_part = HashMap::default();
        for (idx, executor) in executors_sorted.into_iter().enumerate() {
            let begin = num_parts * idx / num_executors;
            let end = num_parts * (idx + 1) / num_executors;
            let parts = if begin == end {
                // reach here only when num_executors > num_parts
                vec![]
            } else {
                partitions[begin..end].to_vec()
            };
            executor_part.insert(executor, parts);
        }

        Ok(executor_part)
    }

    fn get_read_source(&self) -> Result<DataSourcePlan> {
        if self.fragment_type != FragmentType::Source {
            return Err(ErrorCode::Internal(
                "Cannot get read source from a non-source fragment".to_string(),
            ));
        }

        let mut source = vec![];

        let mut collect_read_source = |plan: &PhysicalPlan| match plan {
            PhysicalPlan::TableScan(scan) => source.push(*scan.source.clone()),
            PhysicalPlan::CopyIntoTable(copy) => {
                if let Some(stage) = copy.source.as_stage().cloned() {
                    source.push(*stage);
                }
            }
            _ => {}
        };

        PhysicalPlan::traverse(
            &self.plan,
            &mut |_| true,
            &mut collect_read_source,
            &mut |_| {},
        );

        if source.len() != 1 {
            Err(ErrorCode::Internal(
                "Invalid source fragment with multiple table scan".to_string(),
            ))
        } else {
            Ok(source.remove(0))
        }
    }
}

pub struct ReplaceReadSource {
    pub source: DataSourcePlan,
}

impl PhysicalPlanReplacer for ReplaceReadSource {
    fn replace_table_scan(&mut self, plan: &TableScan) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::TableScan(TableScan {
            plan_id: plan.plan_id,
            source: Box::new(self.source.clone()),
            name_mapping: plan.name_mapping.clone(),
            table_index: plan.table_index,
            stat_info: plan.stat_info.clone(),
            internal_column: plan.internal_column.clone(),
        }))
    }

    fn replace_copy_into_table(&mut self, plan: &CopyIntoTable) -> Result<PhysicalPlan> {
        match &plan.source {
            CopyIntoTableSource::Query(query_ctx) => {
                let input = self.replace(&query_ctx.plan)?;
                Ok(PhysicalPlan::CopyIntoTable(Box::new(CopyIntoTable {
                    source: CopyIntoTableSource::Query(Box::new(QuerySource {
                        plan: input,
                        ..*query_ctx.clone()
                    })),
                    ..plan.clone()
                })))
            }
            CopyIntoTableSource::Stage(_) => {
                Ok(PhysicalPlan::CopyIntoTable(Box::new(CopyIntoTable {
                    source: CopyIntoTableSource::Stage(Box::new(self.source.clone())),
                    ..plan.clone()
                })))
            }
        }
    }
}

struct ReplaceReclusterSource {
    pub tasks: Vec<ReclusterTask>,
}

impl PhysicalPlanReplacer for ReplaceReclusterSource {
    fn replace_recluster_source(&mut self, plan: &ReclusterSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::ReclusterSource(Box::new(ReclusterSource {
            tasks: self.tasks.clone(),
            ..plan.clone()
        })))
    }
}

struct ReplaceCompactBlock {
    pub partitions: Partitions,
}

impl PhysicalPlanReplacer for ReplaceCompactBlock {
    fn replace_compact_source(&mut self, plan: &CompactSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::CompactSource(Box::new(CompactSource {
            parts: self.partitions.clone(),
            ..plan.clone()
        })))
    }
}

struct ReplaceDeleteSource {
    pub partitions: Partitions,
}

impl PhysicalPlanReplacer for ReplaceDeleteSource {
    fn replace_delete_source(&mut self, plan: &DeleteSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::DeleteSource(Box::new(DeleteSource {
            parts: self.partitions.clone(),
            ..plan.clone()
        })))
    }
}

struct ReplaceReplaceInto {
    pub partitions: Vec<(usize, Location)>,
    // for standalone mode, slot is None
    pub slot: Option<BlockSlotDescription>,
    pub need_insert: bool,
}

impl PhysicalPlanReplacer for ReplaceReplaceInto {
    fn replace_replace_into(&mut self, plan: &ReplaceInto) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ReplaceInto(Box::new(ReplaceInto {
            input: Box::new(input),
            need_insert: self.need_insert,
            segments: self.partitions.clone(),
            block_slots: self.slot.clone(),
            ..plan.clone()
        })))
    }

    fn replace_deduplicate(&mut self, plan: &ReplaceDeduplicate) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ReplaceDeduplicate(Box::new(
            ReplaceDeduplicate {
                input: Box::new(input),
                need_insert: self.need_insert,
                table_is_empty: self.partitions.is_empty(),
                ..plan.clone()
            },
        )))
    }
}
