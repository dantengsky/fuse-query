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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;

use crate::ColumnSet;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::SortItem;

/// TopN operator: the fusion of `Limit` and `Sort` produced by
/// `RulePushDownLimitSort` when `limit + offset` is within the
/// push-down threshold.
///
/// Semantics: the top `limit` rows ordered by `items`, after skipping
/// `offset` rows. The candidate capacity of the partial stage is
/// `limit + offset`; `offset` is only applied at the final stage.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopN {
    pub items: Vec<SortItem>,
    pub limit: usize,
    pub offset: usize,

    /// Lazy columns absorbed from the `Limit` operator, used to build a
    /// `RowFetch` above the final TopN stage.
    pub lazy_columns: ColumnSet,

    /// Distributed marker, mirroring `Sort::after_exchange`:
    /// - `None`: single-node plan.
    /// - `Some(false)`: partial stage below the exchange.
    /// - `Some(true)`: final stage above the exchange.
    pub after_exchange: Option<bool>,
}

impl TopN {
    pub fn used_columns(&self) -> ColumnSet {
        self.items.iter().map(|item| item.index).collect()
    }

    /// The candidate capacity of the partial stage.
    pub fn candidate_count(&self) -> usize {
        self.limit.saturating_add(self.offset)
    }

    pub fn without_lazy_columns(&self) -> TopN {
        TopN {
            lazy_columns: Default::default(),
            ..self.clone()
        }
    }
}

impl Operator for TopN {
    fn rel_op(&self) -> RelOp {
        RelOp::TopN
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();
        required.distribution = Distribution::Serial;
        Ok(required)
    }

    fn compute_required_prop_children(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        Ok(vec![vec![RequiredProperty {
            distribution: Distribution::Serial,
        }]])
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        Ok(Arc::new(RelationalProperty {
            output_columns: input_prop.output_columns.clone(),
            outer_columns: input_prop.outer_columns.clone(),
            used_columns: input_prop.used_columns.clone(),
            orderings: self.items.clone(),
            partition_orderings: None,
        }))
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let stat_info = rel_expr.derive_cardinality_child(0)?;
        let partial = self.after_exchange == Some(false);
        let output_rows = if partial {
            self.candidate_count()
        } else {
            self.limit
        };
        let precise_cardinality = if output_rows == 0 {
            Some(0)
        } else {
            stat_info.statistics.precise_cardinality.map(|rows| {
                if partial {
                    rows.min(self.candidate_count() as u64)
                } else {
                    rows.saturating_sub(self.offset as u64)
                        .min(self.limit as u64)
                }
            })
        };
        // Only the final stage consumes the offset. Keep the conservative
        // input estimate separate from the point estimate: a skewed join
        // below TopN must not turn an underestimated build into a broadcast.
        let bound_rows = |rows: f64| {
            let rows = if partial {
                rows
            } else {
                (rows - self.offset as f64).max(0.0)
            };
            rows.min(output_rows as f64)
        };
        let cardinality = precise_cardinality
            .map(|rows| rows as f64)
            .unwrap_or_else(|| bound_rows(stat_info.cardinality));
        let max_cardinality = precise_cardinality
            .map(|rows| rows as f64)
            .unwrap_or_else(|| bound_rows(stat_info.max_cardinality.max(stat_info.cardinality)));

        Ok(Arc::new(StatInfo {
            cardinality,
            max_cardinality,
            statistics: Statistics {
                precise_cardinality,
                column_stats: Default::default(),
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::ir::SExpr;
    use crate::plans::DummyTableScan;

    fn top_n_stats(
        after_exchange: Option<bool>,
        limit: usize,
        offset: usize,
        cardinality: f64,
        max_cardinality: f64,
        precise_cardinality: Option<u64>,
    ) -> Result<Arc<StatInfo>> {
        let input = SExpr::create(
            DummyTableScan::default(),
            vec![],
            None,
            None,
            Some(Arc::new(StatInfo {
                cardinality,
                max_cardinality,
                statistics: Statistics {
                    precise_cardinality,
                    ..Default::default()
                },
            })),
        );
        let expr = SExpr::create_unary(
            TopN {
                items: vec![],
                limit,
                offset,
                lazy_columns: Default::default(),
                after_exchange,
            },
            input,
        );
        RelExpr::with_s_expr(&expr).derive_cardinality()
    }

    #[test]
    fn top_n_stats_bound_partial_candidates_and_final_offset() -> Result<()> {
        let partial = top_n_stats(Some(false), 10, 5, 8.0, 1000.0, None)?;
        assert_eq!(partial.cardinality, 8.0);
        assert_eq!(partial.max_cardinality, 15.0);
        for stage in [None, Some(true)] {
            let final_stat = top_n_stats(stage, 10, 5, 8.0, 1000.0, None)?;
            assert_eq!(final_stat.cardinality, 3.0);
            assert_eq!(final_stat.max_cardinality, 10.0);
            assert_eq!(final_stat.statistics.precise_cardinality, None);
        }
        Ok(())
    }

    #[test]
    fn top_n_stats_preserve_precise_empty_without_promoting_estimates() -> Result<()> {
        for stage in [None, Some(false), Some(true)] {
            let empty = top_n_stats(stage, 10, 5, 0.0, 0.0, Some(0))?;
            assert_eq!(empty.cardinality, 0.0);
            assert_eq!(empty.max_cardinality, 0.0);
            assert_eq!(empty.statistics.precise_cardinality, Some(0));

            let estimated = top_n_stats(stage, 10, 5, 0.0, 1000.0, None)?;
            assert_eq!(estimated.statistics.precise_cardinality, None);
            assert!(estimated.max_cardinality > 0.0);
        }
        for stage in [None, Some(true)] {
            let exhausted = top_n_stats(stage, 10, 5, 4.0, 4.0, Some(4))?;
            assert_eq!(exhausted.cardinality, 0.0);
            assert_eq!(exhausted.max_cardinality, 0.0);
            assert_eq!(exhausted.statistics.precise_cardinality, Some(0));
            let zero_limit = top_n_stats(stage, 0, 5, 100.0, 1000.0, None)?;
            assert_eq!(zero_limit.max_cardinality, 0.0);
            assert_eq!(zero_limit.statistics.precise_cardinality, Some(0));
        }
        let partial = top_n_stats(Some(false), 10, 5, 8.0, 8.0, Some(8))?;
        assert_eq!(partial.statistics.precise_cardinality, Some(8));
        let saturated = top_n_stats(Some(false), usize::MAX, 5, 8.0, 8.0, Some(8))?;
        assert_eq!(saturated.max_cardinality, 8.0);
        Ok(())
    }
}
