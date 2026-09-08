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

use databend_common_exception::ErrorCode;
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
        // Validate before the identity fast path as well. Duplicate definitions
        // are invalid even when both items simply forward the same input column.
        // Reuse the set below instead of silently collapsing duplicate statistics.
        let mut defined_columns = ColumnSet::new();
        for item in &self.items {
            if !defined_columns.insert(item.index) {
                return Err(ErrorCode::Internal(format!(
                    "Invalid EvalScalar: duplicate output index {}",
                    item.index
                )));
            }
        }

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
            max_cardinality: input.max_cardinality,
            statistics: Statistics {
                precise_cardinality: input.statistics.precise_cardinality,
                column_stats,
            },
        }))
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DataType;

    use super::*;
    use crate::optimizer::ir::SExpr;
    use crate::plans::ConstantExpr;
    use crate::plans::DummyTableScan;

    fn constant(index: usize, value: bool) -> ScalarItem {
        ScalarItem {
            index: Symbol::new(index),
            scalar: ConstantExpr {
                span: None,
                value: Scalar::Boolean(value),
            }
            .into(),
        }
    }

    fn identity(index: usize) -> ScalarItem {
        ScalarItem {
            index: Symbol::new(index),
            scalar: BoundColumnRef {
                span: None,
                column: ColumnBindingBuilder::new(
                    format!("c{index}"),
                    Symbol::new(index),
                    Box::new(DataType::Boolean),
                    Visibility::Visible,
                )
                .build(),
            }
            .into(),
        }
    }

    fn input() -> SExpr {
        SExpr::create_unary(
            EvalScalar {
                items: vec![constant(2, true), constant(3, false)],
            },
            SExpr::create_leaf(DummyTableScan::default()),
        )
    }

    #[test]
    fn eval_scalar_output_index_rejects_duplicates() {
        // These checks must return errors with debug assertions both enabled
        // and disabled; neither silent map deduplication nor a panic is valid.
        for items in [
            vec![identity(2), identity(2)],
            vec![constant(2, true), constant(2, false)],
            vec![identity(2), constant(2, false)],
            vec![identity(2), identity(3), identity(2)],
        ] {
            let expression = SExpr::create_unary(EvalScalar { items }, input());
            let error = RelExpr::with_s_expr(&expression)
                .derive_cardinality()
                .unwrap_err();
            assert_eq!(error.code(), ErrorCode::Internal("").code());
            assert!(error.message().contains("duplicate output index 2"));
        }
    }

    #[test]
    fn eval_scalar_output_index_preserves_identity_fast_path() -> Result<()> {
        for items in [vec![], vec![identity(2)], vec![identity(3), identity(2)]] {
            let child = input();
            let expected = RelExpr::with_s_expr(&child).derive_cardinality()?;
            let expression = SExpr::create_unary(EvalScalar { items }, child);
            let actual = RelExpr::with_s_expr(&expression).derive_cardinality()?;
            assert!(Arc::ptr_eq(&expected, &actual));
        }
        Ok(())
    }

    #[test]
    fn eval_scalar_output_index_allows_shadowing_in_different_nodes() -> Result<()> {
        // Defining #2 once in each of two different layers is legal. Only
        // duplicate definitions within the same EvalScalar are rejected.
        let expression = SExpr::create_unary(
            EvalScalar {
                items: vec![constant(2, false), constant(4, true)],
            },
            input(),
        );
        let actual = RelExpr::with_s_expr(&expression).derive_cardinality()?;
        assert_eq!(actual.cardinality, 1.0);
        assert!(actual.statistics.column_stats.contains_key(&Symbol::new(3)));
        assert!(actual.statistics.column_stats.contains_key(&Symbol::new(4)));
        Ok(())
    }
}
