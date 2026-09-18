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

use databend_common_exception::Result;

use crate::optimizer::ir::MAX_CARDINALITY_UNDERESTIMATION_RATIO;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOp;
use crate::plans::RelOperator;

fn contains_recursive_cte(expr: &SExpr) -> bool {
    matches!(expr.plan(), RelOperator::RecursiveCteScan(_))
        || expr.children().any(contains_recursive_cte)
}

/// Cardinality used to order a hash build candidate against the alternative
/// build input.
///
/// Expected rows drive the choice. A severely underestimated input is ordered
/// by its risk bound instead, but only while that bound also dwarfs the
/// alternative build: shielding a cheap build from a possibly huge one is
/// worthwhile, whereas preferring an input that is already expected to be
/// large over a small expected input with a comparable worst case is not.
fn build_cardinality(candidate: &StatInfo, alternative: &StatInfo) -> f64 {
    if candidate.cardinality_is_severely_underestimated()
        && candidate.risk_cardinality()
            > alternative.risk_cardinality() * MAX_CARDINALITY_UNDERESTIMATION_RATIO
    {
        candidate.risk_cardinality()
    } else {
        candidate.cardinality
    }
}

fn should_commute(join_type: JoinType, left: &StatInfo, right: &StatInfo) -> bool {
    let left_build_cardinality = build_cardinality(left, right);
    let right_build_cardinality = build_cardinality(right, left);
    if left_build_cardinality < right_build_cardinality
        || (left_build_cardinality == right_build_cardinality
            && left.cardinality < right.cardinality)
    {
        return matches!(
            join_type,
            JoinType::Inner
                | JoinType::Cross
                | JoinType::Left
                | JoinType::Right
                | JoinType::LeftSingle
                | JoinType::RightSingle
                | JoinType::LeftSemi
                | JoinType::RightSemi
                | JoinType::LeftAnti
                | JoinType::RightAnti
                | JoinType::LeftMark
                | JoinType::RightMark
        );
    }

    if left_build_cardinality != right_build_cardinality || left.cardinality != right.cardinality {
        return false;
    }

    if left.cardinality == 0.0 && matches!(join_type, JoinType::Left | JoinType::Right) {
        let left_proven_empty = left.statistics.precise_cardinality == Some(0);
        let right_proven_empty = right.statistics.precise_cardinality == Some(0);
        if left_proven_empty != right_proven_empty {
            // The right child is the hash-build side. Prefer the input that is
            // known to be empty over one whose zero cardinality is only estimated.
            return left_proven_empty;
        }
    }

    matches!(
        join_type,
        JoinType::Right | JoinType::RightSingle | JoinType::RightSemi | JoinType::RightAnti
    )
}

/// Rule to apply commutativity of join operator.
/// Since we will always use the right child as build side, this
/// rule will help us measure which child is the better one.
pub struct RuleCommuteJoin {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleCommuteJoin {
    pub fn new() -> Self {
        Self {
            id: RuleID::CommuteJoin,

            // LogicalJoin
            // | \
            // *  *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Join,
                children: vec![Matcher::Leaf, Matcher::Leaf],
            }],
        }
    }
}

impl Rule for RuleCommuteJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let mut join: Join = s_expr.plan().clone().try_into()?;

        if join.build_side_cache_info.is_some() {
            return Ok(());
        }

        let left_child = s_expr.child(0)?;
        let right_child = s_expr.child(1)?;

        if join.join_type == JoinType::Cross
            && (contains_recursive_cte(left_child) || contains_recursive_cte(right_child))
        {
            return Ok(());
        }

        let left_rel_expr = RelExpr::with_s_expr(left_child);
        let right_rel_expr = RelExpr::with_s_expr(right_child);
        let left_stat = left_rel_expr.derive_cardinality()?;
        let right_stat = right_rel_expr.derive_cardinality()?;
        let need_commute = should_commute(join.join_type, &left_stat, &right_stat);
        if need_commute {
            // Swap the join conditions side
            for condition in join.equi_conditions.iter_mut() {
                (condition.left, condition.right) =
                    (condition.right.clone(), condition.left.clone());
            }
            join.join_type = join.join_type.opposite();
            join.single_to_inner = join.single_to_inner.map(|join_type| join_type.opposite());
            let mut result = SExpr::create_binary(
                Arc::new(join.into()),
                Arc::new(right_child.clone()),
                Arc::new(left_child.clone()),
            );
            result.set_applied_rule(&self.id);
            state.add_result(result);
        }
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RuleCommuteJoin {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;

    use super::*;
    use crate::optimizer::ir::Statistics;
    use crate::plans::ConstantExpr;
    use crate::plans::DummyTableScan;
    use crate::plans::Filter;
    use crate::plans::MutationSource;
    use crate::plans::ScalarExpr;

    fn empty_stat(precise: bool) -> StatInfo {
        StatInfo {
            cardinality: 0.0,
            max_cardinality: 0.0,
            stale_range_statistics: false,
            statistics: Statistics {
                precise_cardinality: precise.then_some(0),
                column_stats: Default::default(),
            },
        }
    }

    fn estimated_stat(cardinality: f64) -> StatInfo {
        StatInfo {
            cardinality,
            max_cardinality: cardinality,
            stale_range_statistics: false,
            statistics: Statistics::default(),
        }
    }

    #[test]
    fn test_commute_join_prefers_safer_build_cardinality() {
        let mut left = estimated_stat(1_000.0);
        left.max_cardinality = 1_000.0;
        let mut underestimated_right = estimated_stat(10.0);
        underestimated_right.max_cardinality = 200_000_000.0;

        assert!(should_commute(
            JoinType::Inner,
            &left,
            &underestimated_right
        ));
        assert!(!should_commute(
            JoinType::Inner,
            &underestimated_right,
            &left
        ));
    }

    #[test]
    fn test_commute_join_preserves_cardinality_order_without_severe_underestimate() {
        let mut selective_left = estimated_stat(1_000_000.0);
        selective_left.max_cardinality = 1_000_000_000.0;
        let right = estimated_stat(100_000_000.0);

        assert!(should_commute(JoinType::Inner, &selective_left, &right));
        assert!(!should_commute(JoinType::Inner, &right, &selective_left));
    }

    #[test]
    fn test_severe_underestimate_yields_to_comparably_large_certain_build() {
        // A stale time window estimates one row with a risk bound of a fifth of
        // its source, while the other outer-join input is expected to hold more
        // than a hundred million rows. Building the large input is certain to be
        // expensive, so the small expected input stays the build side.
        let mut stale_window = estimated_stat(1.0);
        stale_window.max_cardinality = 272_222_196.2;
        let large_certain = estimated_stat(172_046_190.0);

        assert!(should_commute(
            JoinType::Left,
            &stale_window,
            &large_certain
        ));
        assert!(!should_commute(
            JoinType::Right,
            &large_certain,
            &stale_window
        ));

        // A cheap certain build is still shielded from the possibly huge input.
        let cheap_certain = estimated_stat(100_000.0);
        assert!(!should_commute(
            JoinType::Left,
            &stale_window,
            &cheap_certain
        ));
        assert!(should_commute(
            JoinType::Right,
            &cheap_certain,
            &stale_window
        ));
    }

    #[test]
    fn test_outer_join_prefers_stale_window_over_large_dimension_build() {
        // Model a large append-heavy table after applying a narrow range predicate
        // to stale statistics: expected rows drive build-side ordering, while the
        // separate risk bound remains available to the distribution planner.
        let mut stale_window = estimated_stat(80_000_000.0);
        stale_window.max_cardinality = 40_000_000_000.0;
        let account_dimension = estimated_stat(225_000_000.0);

        assert!(should_commute(
            JoinType::Left,
            &stale_window,
            &account_dimension
        ));
        assert!(!should_commute(
            JoinType::Right,
            &account_dimension,
            &stale_window
        ));
    }

    fn proven_empty_expr() -> SExpr {
        SExpr::create_unary(
            Filter {
                predicates: vec![ScalarExpr::ConstantExpr(ConstantExpr {
                    span: None,
                    value: Scalar::Boolean(false),
                })],
            },
            SExpr::create_leaf(DummyTableScan::default()),
        )
    }

    #[test]
    fn test_outer_join_zero_tie_prefers_proven_empty_build_side() {
        let proven_empty = empty_stat(true);
        let estimated_empty = empty_stat(false);

        assert!(should_commute(
            JoinType::Left,
            &proven_empty,
            &estimated_empty
        ));
        assert!(!should_commute(
            JoinType::Left,
            &estimated_empty,
            &proven_empty
        ));
        assert!(should_commute(
            JoinType::Right,
            &proven_empty,
            &estimated_empty
        ));
        assert!(!should_commute(
            JoinType::Right,
            &estimated_empty,
            &proven_empty
        ));
    }

    #[test]
    fn test_outer_join_zero_tie_preserves_existing_canonicalization() {
        let left = empty_stat(false);
        let right = empty_stat(false);

        assert!(!should_commute(JoinType::Left, &left, &right));
        assert!(should_commute(JoinType::Right, &left, &right));
    }

    #[test]
    fn test_outer_join_builds_smaller_nonzero_input() {
        let selective_preserved_input = estimated_stat(1.0);
        let stale_range_input = estimated_stat(20.0);

        assert!(should_commute(
            JoinType::Left,
            &selective_preserved_input,
            &stale_range_input
        ));
        assert!(!should_commute(
            JoinType::Left,
            &stale_range_input,
            &selective_preserved_input
        ));
    }

    #[test]
    fn test_commute_join_builds_proven_empty_input() -> Result<()> {
        let join = Join {
            join_type: JoinType::Left,
            ..Default::default()
        };
        let expr = SExpr::create_binary(
            join,
            proven_empty_expr(),
            SExpr::create_leaf(MutationSource::default()),
        );
        let mut state = TransformResult::new();

        RuleCommuteJoin::new().apply(&expr, &mut state)?;

        assert_eq!(state.results().len(), 1);
        let result_join: Join = state.results()[0].plan().clone().try_into()?;
        assert_eq!(result_join.join_type, JoinType::Right);
        assert_eq!(
            RelExpr::with_s_expr(state.results()[0].child(1)?)
                .derive_cardinality()?
                .statistics
                .precise_cardinality,
            Some(0)
        );
        Ok(())
    }

    #[test]
    fn test_commute_join_keeps_proven_empty_build_input() -> Result<()> {
        let join = Join {
            join_type: JoinType::Left,
            ..Default::default()
        };
        let expr = SExpr::create_binary(
            join,
            SExpr::create_leaf(MutationSource::default()),
            proven_empty_expr(),
        );
        let mut state = TransformResult::new();

        RuleCommuteJoin::new().apply(&expr, &mut state)?;

        assert!(state.results().is_empty());
        Ok(())
    }
}
