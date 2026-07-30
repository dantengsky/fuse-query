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

use databend_common_exception::Result;
use databend_common_expression::Scalar;

use crate::Metadata;
use crate::MetadataRef;
use crate::Symbol;
use crate::optimizer::ir::SExpr;
use crate::plans::ConstantExpr;
use crate::plans::EvalScalar;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;

/// Convert an outer-join exclusion filter to the corresponding anti join when
/// the null-tested expression is a regular equi-key on the null-extended side.
pub fn outer_join_to_anti_join(s_expr: &SExpr, metadata: MetadataRef) -> Result<Option<SExpr>> {
    let filter = s_expr.plan().as_filter().unwrap();
    let join_expr = s_expr.unary_child();
    let join = join_expr.plan().as_join().unwrap();
    let anti_join_type = match join.join_type {
        JoinType::Left => JoinType::LeftAnti,
        JoinType::Right => JoinType::RightAnti,
        _ => return Ok(None),
    };

    let null_extended_prop = match join.join_type {
        JoinType::Left => join_expr.right_child().derive_relational_prop()?,
        JoinType::Right => join_expr.left_child().derive_relational_prop()?,
        _ => unreachable!(),
    };
    let metadata = metadata.read();

    let null_extended_expr = match join.join_type {
        JoinType::Left => join_expr.right_child(),
        JoinType::Right => join_expr.left_child(),
        _ => unreachable!(),
    };
    let mut predicate_index = None;
    for (index, predicate) in filter.predicates.iter().enumerate() {
        let Some(null_tested_expr) = null_tested_expr(predicate) else {
            continue;
        };

        let is_regular_equi_key = join.equi_conditions.iter().any(|condition| {
            !condition.is_null_equal
                && match join.join_type {
                    JoinType::Left => condition.right == *null_tested_expr,
                    JoinType::Right => condition.left == *null_tested_expr,
                    _ => unreachable!(),
                }
        });
        if is_regular_equi_key {
            predicate_index = Some(index);
            break;
        }

        // A non-nullable source expression can only become NULL through outer
        // join null extension, so testing it for NULL also selects unmatched
        // rows exactly. Metadata retains the pre-join type while the binding
        // above the outer join is nullable.
        let ScalarExpr::BoundColumnRef(column) = null_tested_expr else {
            continue;
        };
        if null_extended_prop
            .output_columns
            .contains(&column.column.index)
            && subtree_output_is_non_null(null_extended_expr, column.column.index, &metadata)?
        {
            predicate_index = Some(index);
            break;
        }
    }
    let Some(predicate_index) = predicate_index else {
        return Ok(None);
    };

    // An anti join only produces its preserved side. Recreate the columns that
    // the outer join used to null-extend so operators above keep the same schema.
    let null_items = null_extended_prop
        .output_columns
        .iter()
        .map(|index| ScalarItem {
            scalar: ScalarExpr::TypedConstantExpr(
                ConstantExpr {
                    span: None,
                    value: Scalar::Null,
                },
                metadata.column(*index).data_type().wrap_nullable(),
            ),
            index: *index,
        })
        .collect();
    drop(metadata);

    let result = SExpr::create_binary(
        Join {
            join_type: anti_join_type,
            ..join.clone()
        },
        join_expr.left_child_arc(),
        join_expr.right_child_arc(),
    )
    .build_unary(EvalScalar { items: null_items });

    Ok(Some(if filter.predicates.len() > 1 {
        let mut filter = filter.clone();
        filter.predicates.remove(predicate_index);
        result.build_unary(filter)
    } else {
        result
    }))
}

fn subtree_output_is_non_null(s_expr: &SExpr, output: Symbol, metadata: &Metadata) -> Result<bool> {
    match s_expr.plan() {
        RelOperator::EvalScalar(eval) => {
            if let Some(item) = eval.items.iter().find(|item| item.index == output) {
                return scalar_is_non_null(&item.scalar, s_expr.unary_child(), metadata);
            }
        }
        RelOperator::UnionAll(union) => {
            let Some(position) = union
                .output_indexes
                .iter()
                .position(|index| *index == output)
            else {
                return Ok(false);
            };
            let (left_output, left_expr) = &union.left_outputs[position];
            let (right_output, right_expr) = &union.right_outputs[position];
            let left_non_null = match left_expr {
                Some(expr) => scalar_is_non_null(expr, s_expr.left_child(), metadata)?,
                None => subtree_output_is_non_null(s_expr.left_child(), *left_output, metadata)?,
            };
            if !left_non_null {
                return Ok(false);
            }
            return match right_expr {
                Some(expr) => scalar_is_non_null(expr, s_expr.right_child(), metadata),
                None => subtree_output_is_non_null(s_expr.right_child(), *right_output, metadata),
            };
        }
        // A nested outer join may null-extend a source column even when its
        // metadata type is non-nullable. Stop here instead of treating that
        // source type as proof about the nested join's output.
        RelOperator::Join(_) => return Ok(false),
        _ => {}
    }

    if s_expr.children().count() == 1
        && s_expr
            .unary_child()
            .derive_relational_prop()?
            .output_columns
            .contains(&output)
    {
        return subtree_output_is_non_null(s_expr.unary_child(), output, metadata);
    }

    Ok(!metadata.column(output).data_type().is_nullable_or_null())
}

fn scalar_is_non_null(scalar: &ScalarExpr, input: &SExpr, metadata: &Metadata) -> Result<bool> {
    match scalar {
        ScalarExpr::BoundColumnRef(column) => {
            subtree_output_is_non_null(input, column.column.index, metadata)
        }
        ScalarExpr::ConstantExpr(constant) | ScalarExpr::TypedConstantExpr(constant, _) => {
            Ok(!matches!(constant.value, Scalar::Null))
        }
        ScalarExpr::CastExpr(cast) if !cast.is_try => {
            scalar_is_non_null(&cast.argument, input, metadata)
        }
        _ => Ok(!scalar.data_type()?.is_nullable_or_null()),
    }
}

fn null_tested_expr(predicate: &ScalarExpr) -> Option<&ScalarExpr> {
    if let ScalarExpr::FunctionCall(not) = predicate
        && not.func_name == "not"
        && let [ScalarExpr::FunctionCall(is_not_null)] = not.arguments.as_slice()
        && is_not_null.func_name == "is_not_null"
        && let [expr] = is_not_null.arguments.as_slice()
    {
        Some(expr)
    } else {
        None
    }
}
