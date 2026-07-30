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

use std::cmp::Ordering;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::stat_distribution::StatCardinality;
use databend_common_expression::stat_distribution::StatCount;
use databend_common_expression::stat_distribution::StatEstimate;

use crate::ColumnSet;
use crate::ScalarExpr;
use crate::Symbol;
use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::ColumnStatSet;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::PhysicalProperty;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::EvalScalar;
use crate::plans::Operator;
use crate::plans::RelOp;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct UnionAll {
    // We'll cast the output of union to the expected data type by the cast expr at runtime.
    // Left of union, output idx and the expected data type
    pub left_outputs: Vec<(Symbol, Option<ScalarExpr>)>,
    // Right of union, output idx and the expected data type
    pub right_outputs: Vec<(Symbol, Option<ScalarExpr>)>,
    // Recursive cte scan names
    // For example: `with recursive t as (select 1 as x union all select m.x+f.x from t as m, t as f where m.x < 3) select * from t`
    // The `cte_scan_names` are `m` and `f`
    pub cte_scan_names: Vec<String>,
    pub logical_recursive_cte_id: Option<u32>,
    pub output_indexes: Vec<Symbol>,
}

impl UnionAll {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for (idx, _) in &self.left_outputs {
            used_columns.insert(*idx);
        }
        for (idx, _) in &self.right_outputs {
            used_columns.insert(*idx);
        }
        Ok(used_columns)
    }

    pub fn derive_union_stats(
        &self,
        left_stat_info: Arc<StatInfo>,
        right_stat_info: Arc<StatInfo>,
    ) -> Result<Arc<StatInfo>> {
        let cardinality = left_stat_info.cardinality + right_stat_info.cardinality;

        let precise_cardinality = left_stat_info
            .statistics
            .precise_cardinality
            .zip(right_stat_info.statistics.precise_cardinality)
            .map(|(left, right)| left + right);
        let left_cardinality = left_stat_info
            .statistics
            .precise_cardinality
            .map(StatCardinality::exact)
            .unwrap_or_else(|| StatCardinality::estimate(left_stat_info.cardinality));
        let right_cardinality = right_stat_info
            .statistics
            .precise_cardinality
            .map(StatCardinality::exact)
            .unwrap_or_else(|| StatCardinality::estimate(right_stat_info.cardinality));

        debug_assert_eq!(self.left_outputs.len(), self.right_outputs.len());
        debug_assert_eq!(self.left_outputs.len(), self.output_indexes.len());

        let column_stats = self
            .left_outputs
            .iter()
            .zip(&self.right_outputs)
            .zip(self.output_indexes.iter().copied())
            .map(
                |(((left_output, left_expr), (right_output, right_expr)), output)| {
                    let left = match left_expr.as_ref() {
                        Some(expr) => EvalScalar::derive_item_stat(
                            expr,
                            &left_stat_info.statistics,
                            left_cardinality,
                        )?,
                        None => left_stat_info
                            .statistics
                            .column_stats
                            .get(left_output)
                            .cloned(),
                    };
                    let right = match right_expr.as_ref() {
                        Some(expr) => EvalScalar::derive_item_stat(
                            expr,
                            &right_stat_info.statistics,
                            right_cardinality,
                        )?,
                        None => right_stat_info
                            .statistics
                            .column_stats
                            .get(right_output)
                            .cloned(),
                    };

                    let (left, right) = match (left, right) {
                        (Some(left), Some(right)) => (left, right),
                        (Some(left), None)
                            if right_stat_info.statistics.precise_cardinality == Some(0) =>
                        {
                            return Ok(Some((output, left)));
                        }
                        (None, Some(right))
                            if left_stat_info.statistics.precise_cardinality == Some(0) =>
                        {
                            return Ok(Some((output, right)));
                        }
                        _ => return Ok(None),
                    };
                    let min = if left.min.compare(&right.min)? == Ordering::Less {
                        left.min.clone()
                    } else {
                        right.min.clone()
                    };
                    let max = if left.max.compare(&right.max)? == Ordering::Greater {
                        left.max.clone()
                    } else {
                        right.max.clone()
                    };
                    Ok(Some((output, ColumnStat {
                        min,
                        max,
                        ndv: Self::merge_ndv(&left, &right),
                        null_count: StatCount::sum(left.null_count, right.null_count),
                        histogram: None,
                    })))
                },
            )
            .filter_map(Result::transpose)
            .collect::<Result<ColumnStatSet>>()?;

        Ok(Arc::new(StatInfo {
            cardinality,
            max_cardinality: cardinality
                .max(left_stat_info.max_cardinality)
                .max(right_stat_info.max_cardinality),
            statistics: Statistics {
                precise_cardinality,
                column_stats,
            },
        }))
    }

    fn merge_ndv(left: &ColumnStat, right: &ColumnStat) -> StatEstimate {
        let lower = left.ndv.lower.max(right.ndv.lower);
        let upper = left.ndv.upper + right.ndv.upper;
        StatEstimate::new(
            lower,
            left.ndv
                .expected
                .max(right.ndv.expected)
                .clamp(lower, upper),
            upper,
        )
    }
}

impl Operator for UnionAll {
    fn rel_op(&self) -> RelOp {
        RelOp::UnionAll
    }

    fn arity(&self) -> usize {
        2
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let left_prop = rel_expr.derive_relational_prop_child(0)?;
        let right_prop = rel_expr.derive_relational_prop_child(1)?;

        // Derive output columns
        let output_columns = self.output_indexes.iter().cloned().collect();
        // Derive outer columns
        let mut outer_columns = left_prop.outer_columns.clone();
        outer_columns = outer_columns
            .union(&right_prop.outer_columns)
            .cloned()
            .collect();

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(left_prop.used_columns.clone());
        used_columns.extend(right_prop.used_columns.clone());

        Ok(Arc::new(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            orderings: vec![],
            partition_orderings: None,
        }))
    }

    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        let left_physical_prop = rel_expr.derive_physical_prop_child(0)?;
        let right_physical_prop = rel_expr.derive_physical_prop_child(1)?;

        if left_physical_prop.distribution == Distribution::Serial
            || right_physical_prop.distribution == Distribution::Serial
        {
            return Ok(PhysicalProperty {
                distribution: Distribution::Serial,
            });
        }

        Ok(PhysicalProperty {
            distribution: Distribution::Random,
        })
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let left_stat_info = rel_expr.derive_cardinality_child(0)?;
        let right_stat_info = rel_expr.derive_cardinality_child(1)?;
        self.derive_union_stats(left_stat_info, right_stat_info)
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let required = required.clone();
        let left_physical_prop = rel_expr.derive_physical_prop_child(0)?;
        let right_physical_prop = rel_expr.derive_physical_prop_child(1)?;
        if left_physical_prop.distribution == Distribution::Serial
            || right_physical_prop.distribution == Distribution::Serial
            || required.distribution == Distribution::Serial
        {
            Ok(RequiredProperty {
                distribution: Distribution::Serial,
            })
        } else {
            Ok(RequiredProperty {
                distribution: Distribution::Random,
            })
        }
    }

    fn compute_required_prop_children(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        // (Any, Any)
        let mut children_required = vec![vec![
            RequiredProperty {
                distribution: Distribution::Any,
            },
            RequiredProperty {
                distribution: Distribution::Any,
            },
        ]];

        // (Serial, Serial)
        children_required.push(vec![
            RequiredProperty {
                distribution: Distribution::Serial,
            },
            RequiredProperty {
                distribution: Distribution::Serial,
            },
        ]);

        Ok(children_required)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_expression::stat_distribution::StatEstimate;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_statistics::Datum;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Visibility;
    use crate::optimizer::ir::Statistics;
    use crate::plans::BoundColumnRef;
    use crate::plans::CastExpr;
    use crate::plans::Join;
    use crate::plans::JoinEquiCondition;
    use crate::plans::JoinType;

    fn column(index: usize, data_type: DataType) -> ScalarExpr {
        BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                format!("c{index}"),
                Symbol::new(index),
                Box::new(data_type),
                Visibility::Visible,
            )
            .build(),
        }
        .into()
    }

    fn stat_info(index: usize, cardinality: f64) -> Arc<StatInfo> {
        Arc::new(StatInfo {
            cardinality,
            max_cardinality: cardinality,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats: HashMap::from([(Symbol::new(index), ColumnStat {
                    min: Datum::Int(1),
                    max: Datum::Int(cardinality as i64),
                    ndv: StatEstimate::exact(cardinality),
                    null_count: StatCount::exact(0),
                    histogram: None,
                })]),
            },
        })
    }

    fn single_column_union(left: usize, right: usize, output: usize) -> UnionAll {
        UnionAll {
            left_outputs: vec![(Symbol::new(left), None)],
            right_outputs: vec![(Symbol::new(right), None)],
            cte_scan_names: vec![],
            logical_recursive_cte_id: None,
            output_indexes: vec![Symbol::new(output)],
        }
    }

    #[test]
    fn test_nonempty_branch_without_key_stats_keeps_union_key_unknown() -> Result<()> {
        let left = stat_info(0, 3.0);
        let right = Arc::new(StatInfo {
            cardinality: 4.0,
            max_cardinality: 4.0,
            statistics: Statistics::default(),
        });

        let union = single_column_union(0, 1, 2).derive_union_stats(left, right)?;

        assert!(!union.statistics.column_stats.contains_key(&Symbol::new(2)));
        Ok(())
    }

    #[test]
    fn test_exact_empty_branch_preserves_other_union_key_stats() -> Result<()> {
        let left = stat_info(0, 3.0);
        let right = Arc::new(StatInfo {
            cardinality: 0.0,
            max_cardinality: 0.0,
            statistics: Statistics {
                precise_cardinality: Some(0),
                column_stats: HashMap::new(),
            },
        });

        let union = single_column_union(0, 1, 2).derive_union_stats(left, right)?;
        let stat = &union.statistics.column_stats[&Symbol::new(2)];

        assert_eq!(stat.ndv, StatEstimate::exact(3.0));
        assert_eq!(stat.min, Datum::Int(1));
        assert_eq!(stat.max, Datum::Int(3));
        Ok(())
    }

    #[test]
    fn test_cast_outer_union_chain_keeps_small_join_build() -> Result<()> {
        let int = DataType::Number(NumberDataType::Int64);
        let nullable_int = int.clone().wrap_nullable();
        let source = stat_info(0, 550_000_000.0);
        let cast = ScalarExpr::CastExpr(CastExpr {
            span: None,
            is_try: false,
            argument: Box::new(column(0, int)),
            target_type: Box::new(nullable_int.clone()),
        });
        let cast_stat = EvalScalar::derive_item_stat(
            &cast,
            &source.statistics,
            StatCardinality::estimate(source.cardinality),
        )?
        .expect("lossless nullable cast should preserve key statistics");
        let large = Arc::new(StatInfo {
            cardinality: source.cardinality,
            max_cardinality: source.max_cardinality,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats: HashMap::from([(Symbol::new(1), cast_stat)]),
            },
        });

        let outer = |small_index, cardinality| {
            Join {
                equi_conditions: vec![JoinEquiCondition::new(
                    column(1, nullable_int.clone()),
                    column(small_index, nullable_int.clone()),
                    false,
                )],
                join_type: JoinType::Right,
                ..Default::default()
            }
            .derive_join_stats(large.clone(), stat_info(small_index, cardinality))
        };
        let left = outer(2, 3.0)?;
        let right = outer(3, 4.0)?;
        assert_eq!(left.max_cardinality, 3.0);
        assert_eq!(right.max_cardinality, 4.0);

        let union = single_column_union(2, 3, 4).derive_union_stats(left, right)?;
        assert_eq!(union.cardinality, 7.0);
        assert_eq!(union.max_cardinality, 7.0);
        assert!(union.statistics.column_stats.contains_key(&Symbol::new(4)));

        let parent = Join {
            equi_conditions: vec![JoinEquiCondition::new(
                column(1, nullable_int.clone()),
                column(4, nullable_int),
                false,
            )],
            join_type: JoinType::Inner,
            ..Default::default()
        }
        .derive_join_stats(large, union)?;
        assert_eq!(parent.cardinality, 7.0);
        assert_eq!(parent.max_cardinality, 7.0);
        assert!(!parent.cardinality_is_severely_underestimated());
        Ok(())
    }
}
