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

use common_catalog::plan::DataSourcePlan;
use common_exception::Result;
use common_expression::DataSchemaRef;
use enum_as_inner::EnumAsInner;

use super::physical_plans::MergeIntoAddRowNumber;
use crate::executor::physical_plans::AggregateExpand;
use crate::executor::physical_plans::AggregateFinal;
use crate::executor::physical_plans::AggregatePartial;
use crate::executor::physical_plans::CommitSink;
use crate::executor::physical_plans::CompactSource;
use crate::executor::physical_plans::ConstantTableScan;
use crate::executor::physical_plans::CopyIntoTable;
use crate::executor::physical_plans::CteScan;
use crate::executor::physical_plans::DeleteSource;
use crate::executor::physical_plans::DistributedInsertSelect;
use crate::executor::physical_plans::EvalScalar;
use crate::executor::physical_plans::Exchange;
use crate::executor::physical_plans::ExchangeSink;
use crate::executor::physical_plans::ExchangeSource;
use crate::executor::physical_plans::Filter;
use crate::executor::physical_plans::HashJoin;
use crate::executor::physical_plans::Lambda;
use crate::executor::physical_plans::Limit;
use crate::executor::physical_plans::MaterializedCte;
use crate::executor::physical_plans::MergeInto;
use crate::executor::physical_plans::MergeIntoAppendNotMatched;
use crate::executor::physical_plans::MergeIntoSource;
use crate::executor::physical_plans::Project;
use crate::executor::physical_plans::ProjectSet;
use crate::executor::physical_plans::RangeJoin;
use crate::executor::physical_plans::ReclusterSink;
use crate::executor::physical_plans::ReclusterSource;
use crate::executor::physical_plans::ReplaceAsyncSourcer;
use crate::executor::physical_plans::ReplaceDeduplicate;
use crate::executor::physical_plans::ReplaceInto;
use crate::executor::physical_plans::RowFetch;
use crate::executor::physical_plans::RuntimeFilterSource;
use crate::executor::physical_plans::Sort;
use crate::executor::physical_plans::TableScan;
use crate::executor::physical_plans::Udf;
use crate::executor::physical_plans::UnionAll;
use crate::executor::physical_plans::Window;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, EnumAsInner)]
pub enum PhysicalPlan {
    /// Query
    TableScan(TableScan),
    Filter(Filter),
    Project(Project),
    EvalScalar(EvalScalar),
    ProjectSet(ProjectSet),
    AggregateExpand(AggregateExpand),
    AggregatePartial(AggregatePartial),
    AggregateFinal(AggregateFinal),
    Window(Window),
    Lambda(Lambda),
    Sort(Sort),
    Limit(Limit),
    RowFetch(RowFetch),
    HashJoin(HashJoin),
    RangeJoin(RangeJoin),
    Exchange(Exchange),
    UnionAll(UnionAll),
    RuntimeFilterSource(RuntimeFilterSource),
    CteScan(CteScan),
    MaterializedCte(MaterializedCte),
    ConstantTableScan(ConstantTableScan),
    Udf(Udf),

    /// For insert into ... select ... in cluster
    DistributedInsertSelect(Box<DistributedInsertSelect>),

    /// Synthesized by fragmented
    ExchangeSource(ExchangeSource),
    ExchangeSink(ExchangeSink),

    /// Delete
    DeleteSource(Box<DeleteSource>),

    /// Copy into table
    CopyIntoTable(Box<CopyIntoTable>),

    /// Replace
    ReplaceAsyncSourcer(ReplaceAsyncSourcer),
    ReplaceDeduplicate(Box<ReplaceDeduplicate>),
    ReplaceInto(Box<ReplaceInto>),

    /// MergeInto
    MergeIntoSource(MergeIntoSource),
    MergeInto(Box<MergeInto>),
    MergeIntoAppendNotMatched(Box<MergeIntoAppendNotMatched>),
    MergeIntoAddRowNumber(Box<MergeIntoAddRowNumber>),

    /// Compact
    CompactSource(Box<CompactSource>),

    /// Commit
    CommitSink(Box<CommitSink>),

    /// Recluster
    ReclusterSource(Box<ReclusterSource>),
    ReclusterSink(Box<ReclusterSink>),
}

impl PhysicalPlan {
    /// Get the id of the plan node
    pub fn get_id(&self) -> u32 {
        match self {
            PhysicalPlan::TableScan(v) => v.plan_id,
            PhysicalPlan::Filter(v) => v.plan_id,
            PhysicalPlan::Project(v) => v.plan_id,
            PhysicalPlan::EvalScalar(v) => v.plan_id,
            PhysicalPlan::ProjectSet(v) => v.plan_id,
            PhysicalPlan::AggregateExpand(v) => v.plan_id,
            PhysicalPlan::AggregatePartial(v) => v.plan_id,
            PhysicalPlan::AggregateFinal(v) => v.plan_id,
            PhysicalPlan::Window(v) => v.plan_id,
            PhysicalPlan::Lambda(v) => v.plan_id,
            PhysicalPlan::Sort(v) => v.plan_id,
            PhysicalPlan::Limit(v) => v.plan_id,
            PhysicalPlan::RowFetch(v) => v.plan_id,
            PhysicalPlan::HashJoin(v) => v.plan_id,
            PhysicalPlan::RangeJoin(v) => v.plan_id,
            PhysicalPlan::Exchange(v) => v.plan_id,
            PhysicalPlan::UnionAll(v) => v.plan_id,
            PhysicalPlan::RuntimeFilterSource(v) => v.plan_id,
            PhysicalPlan::DistributedInsertSelect(v) => v.plan_id,
            PhysicalPlan::ExchangeSource(v) => v.plan_id,
            PhysicalPlan::ExchangeSink(v) => v.plan_id,
            PhysicalPlan::CteScan(v) => v.plan_id,
            PhysicalPlan::MaterializedCte(v) => v.plan_id,
            PhysicalPlan::ConstantTableScan(v) => v.plan_id,
            PhysicalPlan::Udf(v) => v.plan_id,
            PhysicalPlan::DeleteSource(_)
            | PhysicalPlan::MergeInto(_)
            | PhysicalPlan::MergeIntoAddRowNumber(_)
            | PhysicalPlan::MergeIntoSource(_)
            | PhysicalPlan::MergeIntoAppendNotMatched(_)
            | PhysicalPlan::CommitSink(_)
            | PhysicalPlan::CopyIntoTable(_)
            | PhysicalPlan::ReplaceAsyncSourcer(_)
            | PhysicalPlan::ReplaceDeduplicate(_)
            | PhysicalPlan::ReplaceInto(_)
            | PhysicalPlan::CompactSource(_)
            | PhysicalPlan::ReclusterSource(_)
            | PhysicalPlan::ReclusterSink(_) => {
                unreachable!()
            }
        }
    }

    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        match self {
            PhysicalPlan::TableScan(plan) => plan.output_schema(),
            PhysicalPlan::Filter(plan) => plan.output_schema(),
            PhysicalPlan::Project(plan) => plan.output_schema(),
            PhysicalPlan::EvalScalar(plan) => plan.output_schema(),
            PhysicalPlan::AggregateExpand(plan) => plan.output_schema(),
            PhysicalPlan::AggregatePartial(plan) => plan.output_schema(),
            PhysicalPlan::AggregateFinal(plan) => plan.output_schema(),
            PhysicalPlan::Window(plan) => plan.output_schema(),
            PhysicalPlan::Lambda(plan) => plan.output_schema(),
            PhysicalPlan::Sort(plan) => plan.output_schema(),
            PhysicalPlan::Limit(plan) => plan.output_schema(),
            PhysicalPlan::RowFetch(plan) => plan.output_schema(),
            PhysicalPlan::HashJoin(plan) => plan.output_schema(),
            PhysicalPlan::Exchange(plan) => plan.output_schema(),
            PhysicalPlan::ExchangeSource(plan) => plan.output_schema(),
            PhysicalPlan::ExchangeSink(plan) => plan.output_schema(),
            PhysicalPlan::UnionAll(plan) => plan.output_schema(),
            PhysicalPlan::ProjectSet(plan) => plan.output_schema(),
            PhysicalPlan::RuntimeFilterSource(plan) => plan.output_schema(),
            PhysicalPlan::RangeJoin(plan) => plan.output_schema(),
            PhysicalPlan::CopyIntoTable(plan) => plan.output_schema(),
            PhysicalPlan::CteScan(plan) => plan.output_schema(),
            PhysicalPlan::MaterializedCte(plan) => plan.output_schema(),
            PhysicalPlan::ConstantTableScan(plan) => plan.output_schema(),
            PhysicalPlan::Udf(plan) => plan.output_schema(),
            PhysicalPlan::MergeIntoSource(plan) => plan.input.output_schema(),
            PhysicalPlan::MergeInto(plan) => Ok(plan.output_schema.clone()),
            PhysicalPlan::MergeIntoAddRowNumber(plan) => plan.output_schema(),
            PhysicalPlan::ReplaceAsyncSourcer(_)
            | PhysicalPlan::ReplaceDeduplicate(_)
            | PhysicalPlan::ReplaceInto(_)
            | PhysicalPlan::MergeIntoAppendNotMatched(_)
            | PhysicalPlan::CompactSource(_)
            | PhysicalPlan::CommitSink(_)
            | PhysicalPlan::DistributedInsertSelect(_)
            | PhysicalPlan::DeleteSource(_)
            | PhysicalPlan::ReclusterSource(_)
            | PhysicalPlan::ReclusterSink(_) => Ok(DataSchemaRef::default()),
        }
    }

    pub fn name(&self) -> String {
        match self {
            PhysicalPlan::TableScan(_) => "TableScan".to_string(),
            PhysicalPlan::Filter(_) => "Filter".to_string(),
            PhysicalPlan::Project(_) => "Project".to_string(),
            PhysicalPlan::EvalScalar(_) => "EvalScalar".to_string(),
            PhysicalPlan::AggregateExpand(_) => "AggregateExpand".to_string(),
            PhysicalPlan::AggregatePartial(_) => "AggregatePartial".to_string(),
            PhysicalPlan::AggregateFinal(_) => "AggregateFinal".to_string(),
            PhysicalPlan::Window(_) => "Window".to_string(),
            PhysicalPlan::Lambda(_) => "Lambda".to_string(),
            PhysicalPlan::Sort(_) => "Sort".to_string(),
            PhysicalPlan::Limit(_) => "Limit".to_string(),
            PhysicalPlan::RowFetch(_) => "RowFetch".to_string(),
            PhysicalPlan::HashJoin(_) => "HashJoin".to_string(),
            PhysicalPlan::Exchange(_) => "Exchange".to_string(),
            PhysicalPlan::UnionAll(_) => "UnionAll".to_string(),
            PhysicalPlan::DistributedInsertSelect(_) => "DistributedInsertSelect".to_string(),
            PhysicalPlan::ExchangeSource(_) => "Exchange Source".to_string(),
            PhysicalPlan::ExchangeSink(_) => "Exchange Sink".to_string(),
            PhysicalPlan::ProjectSet(_) => "Unnest".to_string(),
            PhysicalPlan::RuntimeFilterSource(_) => "RuntimeFilterSource".to_string(),
            PhysicalPlan::CompactSource(_) => "CompactBlock".to_string(),
            PhysicalPlan::DeleteSource(_) => "DeleteSource".to_string(),
            PhysicalPlan::CommitSink(_) => "CommitSink".to_string(),
            PhysicalPlan::RangeJoin(_) => "RangeJoin".to_string(),
            PhysicalPlan::CopyIntoTable(_) => "CopyIntoTable".to_string(),
            PhysicalPlan::ReplaceAsyncSourcer(_) => "ReplaceAsyncSourcer".to_string(),
            PhysicalPlan::ReplaceDeduplicate(_) => "ReplaceDeduplicate".to_string(),
            PhysicalPlan::ReplaceInto(_) => "Replace".to_string(),
            PhysicalPlan::MergeInto(_) => "MergeInto".to_string(),
            PhysicalPlan::MergeIntoSource(_) => "MergeIntoSource".to_string(),
            PhysicalPlan::MergeIntoAppendNotMatched(_) => "MergeIntoAppendNotMatched".to_string(),
            PhysicalPlan::CteScan(_) => "PhysicalCteScan".to_string(),
            PhysicalPlan::MaterializedCte(_) => "PhysicalMaterializedCte".to_string(),
            PhysicalPlan::ConstantTableScan(_) => "PhysicalConstantTableScan".to_string(),
            PhysicalPlan::MergeIntoAddRowNumber(_) => "AddRowNumber".to_string(),
            PhysicalPlan::ReclusterSource(_) => "ReclusterSource".to_string(),
            PhysicalPlan::ReclusterSink(_) => "ReclusterSink".to_string(),
            PhysicalPlan::Udf(_) => "Udf".to_string(),
        }
    }

    pub fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        match self {
            PhysicalPlan::TableScan(_)
            | PhysicalPlan::CteScan(_)
            | PhysicalPlan::ConstantTableScan(_)
            | PhysicalPlan::ExchangeSource(_)
            | PhysicalPlan::CompactSource(_)
            | PhysicalPlan::DeleteSource(_)
            | PhysicalPlan::CopyIntoTable(_)
            | PhysicalPlan::ReplaceAsyncSourcer(_)
            | PhysicalPlan::ReclusterSource(_) => Box::new(std::iter::empty()),
            PhysicalPlan::Filter(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Project(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::EvalScalar(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregateExpand(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregatePartial(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregateFinal(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Window(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Lambda(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Sort(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Limit(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::RowFetch(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::HashJoin(plan) => Box::new(
                std::iter::once(plan.probe.as_ref()).chain(std::iter::once(plan.build.as_ref())),
            ),
            PhysicalPlan::Exchange(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ExchangeSink(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::UnionAll(plan) => Box::new(
                std::iter::once(plan.left.as_ref()).chain(std::iter::once(plan.right.as_ref())),
            ),
            PhysicalPlan::DistributedInsertSelect(plan) => {
                Box::new(std::iter::once(plan.input.as_ref()))
            }
            PhysicalPlan::CommitSink(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ProjectSet(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::RuntimeFilterSource(plan) => Box::new(
                std::iter::once(plan.left_side.as_ref())
                    .chain(std::iter::once(plan.right_side.as_ref())),
            ),
            PhysicalPlan::RangeJoin(plan) => Box::new(
                std::iter::once(plan.left.as_ref()).chain(std::iter::once(plan.right.as_ref())),
            ),
            PhysicalPlan::ReplaceDeduplicate(plan) => {
                Box::new(std::iter::once(plan.input.as_ref()))
            }
            PhysicalPlan::ReplaceInto(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::MergeInto(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::MergeIntoAddRowNumber(plan) => {
                Box::new(std::iter::once(plan.input.as_ref()))
            }
            PhysicalPlan::MergeIntoSource(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::MergeIntoAppendNotMatched(plan) => {
                Box::new(std::iter::once(plan.input.as_ref()))
            }
            PhysicalPlan::MaterializedCte(plan) => Box::new(
                std::iter::once(plan.left.as_ref()).chain(std::iter::once(plan.right.as_ref())),
            ),
            PhysicalPlan::ReclusterSink(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Udf(plan) => Box::new(std::iter::once(plan.input.as_ref())),
        }
    }

    /// Used to find data source info in a non-aggregation and single-table query plan.
    pub fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        match self {
            PhysicalPlan::TableScan(scan) => Some(&scan.source),
            PhysicalPlan::Filter(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Project(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::EvalScalar(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Window(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Lambda(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Sort(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Limit(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Exchange(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::ExchangeSink(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::DistributedInsertSelect(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::ProjectSet(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::RowFetch(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Udf(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::RuntimeFilterSource(_)
            | PhysicalPlan::UnionAll(_)
            | PhysicalPlan::ExchangeSource(_)
            | PhysicalPlan::HashJoin(_)
            | PhysicalPlan::RangeJoin(_)
            | PhysicalPlan::MaterializedCte(_)
            | PhysicalPlan::AggregateExpand(_)
            | PhysicalPlan::AggregateFinal(_)
            | PhysicalPlan::AggregatePartial(_)
            | PhysicalPlan::CompactSource(_)
            | PhysicalPlan::DeleteSource(_)
            | PhysicalPlan::CommitSink(_)
            | PhysicalPlan::CopyIntoTable(_)
            | PhysicalPlan::ReplaceAsyncSourcer(_)
            | PhysicalPlan::ReplaceDeduplicate(_)
            | PhysicalPlan::ReplaceInto(_)
            | PhysicalPlan::MergeInto(_)
            | PhysicalPlan::MergeIntoAddRowNumber(_)
            | PhysicalPlan::MergeIntoAppendNotMatched(_)
            | PhysicalPlan::MergeIntoSource(_)
            | PhysicalPlan::ConstantTableScan(_)
            | PhysicalPlan::CteScan(_)
            | PhysicalPlan::ReclusterSource(_)
            | PhysicalPlan::ReclusterSink(_) => None,
        }
    }

    pub fn is_distributed_plan(&self) -> bool {
        self.children().any(|child| child.is_distributed_plan())
            || matches!(
                self,
                Self::ExchangeSource(_) | Self::ExchangeSink(_) | Self::Exchange(_)
            )
    }
}
