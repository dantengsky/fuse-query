// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datavalues::DataSchemaRef;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::planners::{
    AggregatePlan, EmptyPlan, ExplainPlan, FilterPlan, LimitPlan, PlanBuilder, ProjectionPlan,
    ReadDataSourcePlan, ScanPlan, SelectPlan,
};

#[derive(Clone)]
pub enum PlanNode {
    Empty(EmptyPlan),
    Projection(ProjectionPlan),
    Aggregate(AggregatePlan),
    Filter(FilterPlan),
    Limit(LimitPlan),
    Scan(ScanPlan),
    ReadSource(ReadDataSourcePlan),
    Explain(ExplainPlan),
    Select(SelectPlan),
}

impl PlanNode {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> DataSchemaRef {
        match self {
            PlanNode::Empty(v) => v.schema(),
            PlanNode::Scan(v) => v.schema(),
            PlanNode::Projection(v) => v.schema(),
            PlanNode::Aggregate(v) => v.schema(),
            PlanNode::Filter(v) => v.schema(),
            PlanNode::Limit(v) => v.schema(),
            PlanNode::ReadSource(v) => v.schema(),
            PlanNode::Select(v) => v.plan.schema(),
            PlanNode::Explain(_) => unimplemented!(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            PlanNode::Empty(_) => "EmptyPlan",
            PlanNode::Scan(_) => "ScanPlan",
            PlanNode::Projection(_) => "ProjectionPlan",
            PlanNode::Aggregate(_) => "AggregatePlan",
            PlanNode::Filter(_) => "FilterPlan",
            PlanNode::Limit(_) => "LimitPlan",
            PlanNode::ReadSource(_) => "ReadSourcePlan",
            PlanNode::Explain(_) => "ExplainPlan",
            PlanNode::Select(_) => "SelectPlan",
        }
    }

    fn to_array(&self, with_parent: bool) -> FuseQueryResult<Vec<PlanNode>> {
        let max_depth = 128;
        let mut depth = 0;
        let mut result = vec![];
        let mut plan = self.clone();

        loop {
            if depth > max_depth {
                return Err(FuseQueryError::Plan(format!(
                    "PlanNode depth more than {}",
                    max_depth
                )));
            }

            match plan {
                PlanNode::Aggregate(v) => {
                    result.push(PlanNode::Aggregate(v.clone()));
                    plan = v.input.as_ref().clone();
                    depth += 1;
                }
                PlanNode::Projection(v) => {
                    result.push(PlanNode::Projection(v.clone()));
                    plan = v.input.as_ref().clone();
                    depth += 1;
                }
                PlanNode::Filter(v) => {
                    result.push(PlanNode::Filter(v.clone()));
                    plan = v.input.as_ref().clone();
                    depth += 1;
                }
                PlanNode::Limit(v) => {
                    result.push(PlanNode::Limit(v.clone()));
                    plan = v.input.as_ref().clone();
                    depth += 1;
                }
                PlanNode::Select(v) => {
                    if with_parent {
                        result.push(PlanNode::Select(v.clone()));
                    }
                    plan = v.plan.as_ref().clone();
                    depth += 1;
                }
                PlanNode::Explain(v) => {
                    if with_parent {
                        result.push(PlanNode::Explain(v.clone()));
                    }
                    plan = v.plan.as_ref().clone();
                    depth += 1;
                }

                // Return.
                PlanNode::Empty(_) => {
                    break;
                }
                PlanNode::Scan(v) => {
                    result.push(PlanNode::Scan(v));
                    break;
                }
                PlanNode::ReadSource(v) => {
                    result.push(PlanNode::ReadSource(v));
                    break;
                }
            }
        }
        result.reverse();
        Ok(result)
    }

    pub fn children_to_plans(&self) -> FuseQueryResult<Vec<PlanNode>> {
        self.to_array(false)
    }

    pub fn node_to_plans(&self) -> FuseQueryResult<Vec<PlanNode>> {
        self.to_array(true)
    }

    pub fn plans_to_node(array: &[PlanNode]) -> FuseQueryResult<PlanNode> {
        let mut builder = PlanBuilder::empty(false);
        for plan in array {
            match plan {
                PlanNode::Projection(v) => {
                    builder = builder.project(v.expr.clone())?;
                }
                PlanNode::Aggregate(v) => {
                    builder = builder.aggregate(v.group_expr.clone(), v.aggr_expr.clone())?;
                }
                PlanNode::Filter(v) => {
                    builder = builder.filter(v.predicate.clone())?;
                }
                PlanNode::Limit(v) => {
                    builder = builder.limit(v.n)?;
                }
                PlanNode::ReadSource(v) => {
                    builder = PlanBuilder::from(&PlanNode::ReadSource(v.clone()))
                }
                PlanNode::Explain(_v) => {
                    builder = builder.explain()?;
                }
                PlanNode::Select(_v) => {
                    builder = builder.select()?;
                }
                PlanNode::Empty(_) => {}
                PlanNode::Scan(_) => {}
            }
        }
        builder.build()
    }
}
