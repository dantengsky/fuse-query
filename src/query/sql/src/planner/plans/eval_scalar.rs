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

use databend_common_exception::Result;
use databend_common_expression::Domain;
use databend_common_expression::FunctionContext;
use databend_common_expression::StatEvaluator;
use databend_common_expression::stat_distribution::OwnedDistribution;
use databend_common_expression::stat_distribution::ReturnStat;
use databend_common_expression::stat_distribution::StatCardinality;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::ColumnBinding;
use crate::ColumnBindingBuilder;
use crate::ColumnSet;
use crate::Symbol;
use crate::Visibility;
use crate::optimizer::ir::ColumnStat;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::BoundColumnRef;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;

/// Evaluate scalar expression
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EvalScalar {
    pub items: Vec<ScalarItem>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ScalarItem {
    pub scalar: ScalarExpr,
    // The index of the derived column in metadata
    pub index: Symbol,
}

impl ScalarItem {
    pub fn column_binding(&self, name: String) -> Result<ColumnBinding> {
        Ok(ColumnBindingBuilder::new(
            name,
            self.index,
            Box::new(self.scalar.data_type()?),
            Visibility::Visible,
        )
        .build())
    }

    pub fn bound_column_expr(&self, name: String) -> Result<ScalarExpr> {
        if let ScalarExpr::BoundColumnRef(_) = &self.scalar {
            return Ok(self.scalar.clone());
        }

        let column_binding = self.column_binding(name)?;
        Ok(BoundColumnRef {
            span: None,
            column: column_binding,
        }
        .into())
    }
}

impl EvalScalar {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for item in self.items.iter() {
            used_columns.insert(item.index);
            used_columns.extend(item.scalar.used_columns());
        }
        Ok(used_columns)
    }

    pub(crate) fn derive_item_stat(
        scalar: &ScalarExpr,
        input_statistics: &Statistics,
        cardinality: StatCardinality,
    ) -> Result<Option<ColumnStat>> {
        let expr = scalar.as_symbol_expr()?;
        let column_refs = expr.column_refs();
        let mut input_stats = HashMap::with_capacity(column_refs.len());
        for (index, data_type) in column_refs {
            let Some(column_stat) = input_statistics.column_stats.get(&index) else {
                return Ok(None);
            };
            let Ok(arg_stat) = column_stat.to_arg_stat(&data_type) else {
                return Ok(None);
            };
            input_stats.insert(index, arg_stat);
        }

        let Some(stat) = StatEvaluator::run(
            &expr,
            &FunctionContext::default(),
            &BUILTIN_FUNCTIONS,
            cardinality,
            &input_stats,
        )?
        else {
            return Ok(None);
        };
        Ok(Self::column_stat_from_return_stat(stat.into_owned()))
    }

    fn column_stat_from_return_stat(stat: ReturnStat) -> Option<ColumnStat> {
        let value_domain = match &stat.domain {
            Domain::Nullable(domain) => domain.value.as_deref()?,
            domain => domain,
        };
        let (min, max) = value_domain.to_minmax();
        let min = min.to_datum()?;
        let max = max.to_datum()?;
        let histogram = match stat.distribution {
            OwnedDistribution::Histogram(histogram) => Some(histogram),
            OwnedDistribution::Unknown | OwnedDistribution::Boolean(_) => None,
        };
        Some(ColumnStat {
            min,
            max,
            ndv: stat.ndv,
            null_count: stat.null_count,
            histogram,
        })
    }
}

impl Operator for EvalScalar {
    fn rel_op(&self) -> RelOp {
        RelOp::EvalScalar
    }

    fn scalar_expr_iter(&self) -> Box<dyn Iterator<Item = &ScalarExpr> + '_> {
        Box::new(self.items.iter().map(|expr| &expr.scalar))
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        // Derive output columns
        let mut output_columns = input_prop.output_columns.clone();
        for item in self.items.iter() {
            output_columns.insert(item.index);
        }

        // Derive outer columns
        let mut outer_columns = input_prop
            .outer_columns
            .difference(&input_prop.output_columns)
            .cloned()
            .collect::<ColumnSet>();
        for item in self.items.iter() {
            let used_columns = item.scalar.used_columns();
            let outer = used_columns.difference(&input_prop.output_columns).cloned();
            outer_columns.extend(outer);
        }

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(input_prop.used_columns.clone());

        // Derive orderings
        let orderings = input_prop.orderings.clone();
        let partition_orderings = input_prop.partition_orderings.clone();

        Ok(Arc::new(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            orderings,
            partition_orderings,
        }))
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let input = rel_expr.derive_cardinality_child(0)?;
        if self.items.iter().all(|item| {
            matches!(
                &item.scalar,
                ScalarExpr::BoundColumnRef(column) if column.column.index == item.index
            )
        }) {
            return Ok(input);
        }

        let cardinality = input
            .statistics
            .precise_cardinality
            .map(StatCardinality::exact)
            .unwrap_or_else(|| StatCardinality::estimate(input.cardinality));
        let defined_columns = self
            .items
            .iter()
            .map(|item| item.index)
            .collect::<ColumnSet>();
        // Note: `RuleMergeEvalScalar` on this branch concatenates items without
        // deduplicating them, so `items` may contain repeated indexes. Repeated
        // items carry the same scalar, hence the same derived statistics, and
        // collecting into `column_stats` below keeps a single entry per index.

        let item_column_stats = self
            .items
            .iter()
            .map(|item| {
                let stat = if let ScalarExpr::BoundColumnRef(column) = &item.scalar {
                    input
                        .statistics
                        .column_stats
                        .get(&column.column.index)
                        .cloned()
                } else {
                    Self::derive_item_stat(&item.scalar, &input.statistics, cardinality)?
                };
                Ok(stat.map(|stat| (item.index, stat)))
            })
            .collect::<Result<Vec<_>>>()?;
        let column_stats = item_column_stats
            .into_iter()
            .flatten()
            .chain(
                input
                    .statistics
                    .column_stats
                    .iter()
                    .filter_map(|(index, stat)| {
                        (!defined_columns.contains(index)).then_some((*index, stat.clone()))
                    }),
            )
            .collect();

        Ok(Arc::new(StatInfo {
            cardinality: input.cardinality,
            statistics: Statistics {
                precise_cardinality: input.statistics.precise_cardinality,
                column_stats,
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::stat_distribution::StatCount;
    use databend_common_expression::stat_distribution::StatEstimate;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_statistics::Datum;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Visibility;
    use crate::optimizer::ir::SExpr;
    use crate::plans::ConstantTableScan;
    use crate::plans::RelOperator;

    fn int64() -> DataType {
        DataType::Number(NumberDataType::Int64)
    }

    fn column(index: usize) -> ScalarExpr {
        BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                format!("c{index}"),
                Symbol::new(index),
                Box::new(int64()),
                Visibility::Visible,
            )
            .build(),
        }
        .into()
    }

    /// A leaf whose derived statistics are known, so `derive_stats` has a
    /// concrete input to work from without needing a real table.
    fn leaf_with_stats(index: usize, cardinality: usize) -> SExpr {
        let scan = ConstantTableScan::new_empty_scan(
            Arc::new(databend_common_expression::DataSchema::empty()),
            ColumnSet::new(),
        );
        let expr = SExpr::create_leaf(Arc::new(RelOperator::ConstantTableScan(scan)));
        let stat = Arc::new(StatInfo {
            cardinality: cardinality as f64,
            statistics: Statistics {
                precise_cardinality: Some(cardinality as u64),
                column_stats: HashMap::from([(Symbol::new(index), ColumnStat {
                    min: Datum::Int(1),
                    max: Datum::Int(cardinality as i64),
                    ndv: StatEstimate::exact(cardinality as f64),
                    null_count: StatCount::exact(0),
                    histogram: None,
                })]),
            },
        });
        expr.stat_info.set(stat).expect("stat_info unset");
        expr
    }

    /// `RuleMergeEvalScalar` on this branch concatenates the up and down items
    /// without deduplicating them, so a merged `EvalScalar` can hold the same
    /// index twice. Upstream asserts this cannot happen; here it can, and
    /// deriving statistics must still succeed.
    #[test]
    fn test_derive_stats_tolerates_duplicated_items() -> Result<()> {
        let input = leaf_with_stats(0, 3);
        let eval = EvalScalar {
            items: vec![
                ScalarItem {
                    scalar: column(0),
                    index: Symbol::new(0),
                },
                ScalarItem {
                    scalar: column(0),
                    index: Symbol::new(0),
                },
                ScalarItem {
                    scalar: plus_one(0),
                    index: Symbol::new(1),
                },
            ],
        };
        let expr = SExpr::create_unary(Arc::new(RelOperator::EvalScalar(eval.clone())), input);

        let stat = eval.derive_stats(&RelExpr::with_s_expr(&expr))?;

        // The repeated index collapses to a single entry carrying the stats of
        // the scalar it repeats, and the derived column is added alongside it.
        let derived = &stat.statistics.column_stats[&Symbol::new(0)];
        assert_eq!(derived.min, Datum::Int(1));
        assert_eq!(derived.max, Datum::Int(3));
        assert_eq!(stat.cardinality, 3.0);
        Ok(())
    }

    /// An `EvalScalar` that only re-projects its input unchanged should hand
    /// back the input statistics untouched.
    #[test]
    fn test_derive_stats_passes_through_identity_projection() -> Result<()> {
        let input = leaf_with_stats(0, 5);
        let eval = EvalScalar {
            items: vec![ScalarItem {
                scalar: column(0),
                index: Symbol::new(0),
            }],
        };
        let expr = SExpr::create_unary(Arc::new(RelOperator::EvalScalar(eval.clone())), input);

        let stat = eval.derive_stats(&RelExpr::with_s_expr(&expr))?;

        assert_eq!(stat.cardinality, 5.0);
        assert_eq!(stat.statistics.precise_cardinality, Some(5));
        assert!(stat.statistics.column_stats.contains_key(&Symbol::new(0)));
        Ok(())
    }

    /// A derived column whose input statistics are missing must simply be
    /// absent from the output rather than inventing a value.
    #[test]
    fn test_derive_stats_omits_items_without_input_stats() -> Result<()> {
        // Statistics exist for column 0 only; the item reads column 7.
        let input = leaf_with_stats(0, 4);
        let eval = EvalScalar {
            items: vec![ScalarItem {
                scalar: plus_one(7),
                index: Symbol::new(9),
            }],
        };
        let expr = SExpr::create_unary(Arc::new(RelOperator::EvalScalar(eval.clone())), input);

        let stat = eval.derive_stats(&RelExpr::with_s_expr(&expr))?;

        assert!(!stat.statistics.column_stats.contains_key(&Symbol::new(9)));
        // Pass-through columns of the input are still preserved.
        assert!(stat.statistics.column_stats.contains_key(&Symbol::new(0)));
        Ok(())
    }

    fn plus_one(index: usize) -> ScalarExpr {
        ScalarExpr::FunctionCall(crate::plans::FunctionCall {
            span: None,
            func_name: "plus".to_string(),
            params: vec![],
            arguments: vec![
                column(index),
                ScalarExpr::ConstantExpr(crate::plans::ConstantExpr {
                    span: None,
                    value: databend_common_expression::Scalar::Number(
                        databend_common_expression::types::number::NumberScalar::Int64(1),
                    ),
                }),
            ],
        })
    }
}
