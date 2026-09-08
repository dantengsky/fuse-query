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

use std::collections::HashSet;

use databend_common_exception::Result;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

async fn write_optimized_case(file: &mut impl std::io::Write, case: &SqlTestCase) -> Result<()> {
    let ctx = setup_context(case).await?;
    ctx.set_cluster_node_num(1);

    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;

    assert_unique_eval_scalar_indexes(&optimized_plan, case.name);

    write_case_header(file, case)?;
    writeln!(file, "raw_plan:")?;
    writeln!(file, "{}", raw_plan.format_indent(Default::default())?)?;
    writeln!(file, "optimized_plan:")?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(Default::default())?
    )?;
    writeln!(file)?;

    Ok(())
}

/// Decorrelation re-emits correlated columns as `outer.*` projections carrying their resolved
/// (derived) index, while dropping original items by their pre-resolution index. When a resolved
/// index collided with an original item's index, both survived and the `EvalScalar` ended up with
/// two items writing one output index.
///
/// An `EvalScalar` defines exactly one value per output index, so assert that invariant directly on
/// the plan tree rather than relying on the golden text. This also holds in release builds, where
/// the equivalent `debug_assert` in `EvalScalar::derive_stats` compiles out.
fn assert_unique_eval_scalar_indexes(plan: &Plan, case_name: &str) {
    let Plan::Query { s_expr, .. } = plan else {
        panic!("case {case_name} should optimize into a query plan");
    };

    let mut stack = vec![s_expr.as_ref()];
    let mut eval_scalars_seen = 0usize;
    while let Some(expr) = stack.pop() {
        if let RelOperator::EvalScalar(eval_scalar) = expr.plan() {
            eval_scalars_seen += 1;
            let input_columns = expr
                .unary_child()
                .derive_relational_prop()
                .unwrap()
                .output_columns
                .clone();
            let mut indexes = HashSet::with_capacity(eval_scalar.items.len());
            for item in &eval_scalar.items {
                assert!(
                    item.scalar.used_columns().is_subset(&input_columns),
                    "case {case_name}: EvalScalar references columns outside its input: {:?}",
                    item
                );
                assert!(
                    indexes.insert(item.index),
                    "case {}: EvalScalar defines output index {} more than once, in items {:?}",
                    case_name,
                    item.index,
                    eval_scalar
                        .items
                        .iter()
                        .map(|item| item.index)
                        .collect::<Vec<_>>(),
                );
            }
        }
        stack.extend(expr.children());
    }

    // Guard against the walk silently passing because it never reached an EvalScalar.
    assert!(
        eval_scalars_seen > 0,
        "case {case_name} should contain at least one EvalScalar"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_computed_correlated_alias_variants() -> Result<()> {
    let statements = [
        (
            "arithmetic_alias_in_nested_join",
            "SELECT * FROM (VALUES (1,1),(2,2)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT t2.a+1 AS a FROM (VALUES (1,1),(2,2)) t2(a,b) JOIN (VALUES (1)) t3(c) ON t2.a=t1.a WHERE t2.b=t1.b) s WHERE s.a=t1.a)",
        ),
        (
            "computed_second_alias",
            "SELECT * FROM (VALUES (1,1),(2,2)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT t2.a AS a, t2.b+1 AS b FROM (VALUES (1,1),(2,2)) t2(a,b) JOIN (VALUES (1)) t3(c) ON t2.a=t1.a WHERE t2.b=t1.b) s WHERE s.a=t1.a AND s.b=t1.b)",
        ),
        (
            "two_correlations_share_one_inner_column",
            "SELECT * FROM (VALUES (1,1),(2,2)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT t2.a AS a FROM (VALUES (1,1),(2,2)) t2(a,b) WHERE t2.a=t1.a AND t2.a=t1.b) s WHERE s.a=t1.a)",
        ),
        (
            "computed_alias_under_limit",
            "SELECT * FROM (VALUES (1,1),(2,2)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT t2.a+1 AS a FROM (VALUES (1,1),(2,2)) t2(a,b) WHERE t2.b=t1.b LIMIT 1) s WHERE s.a=t1.a)",
        ),
        (
            "correlation_in_outer_join_on",
            "SELECT * FROM (VALUES (1,1),(2,2)) t1(a,b) WHERE NOT EXISTS (SELECT 1 FROM (SELECT t2.a AS a FROM (VALUES (1,1),(2,2)) t2(a,b) LEFT JOIN (VALUES (1)) t3(c) ON t2.a=t1.a WHERE t2.b=t1.b) s WHERE s.a=t1.a)",
        ),
    ];
    let mut file = open_golden_file("optimizer", "decorrelate_computed_aliases.txt")?;
    for (name, sql) in statements {
        let case = SqlTestCase {
            name,
            description: "Preserve definitions while resolving correlated aliases.",
            setup_sqls: &[],
            sql,
        };
        write_optimized_case(&mut file, &case).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_decorrelate_correlated_alias_regressions() -> Result<()> {
    let mut file = open_golden_file("optimizer", "decorrelate_correlated_aliases.txt")?;

    let cases = [
        SqlTestCase {
            name: "nested_filter_alias_reaches_limit_rewrite",
            description: "Filter-derived correlated aliases must remain visible while rewriting a deeper correlated LIMIT subtree.",
            setup_sqls: &[],
            sql: r#"
        SELECT *
        FROM (VALUES (1, 1)) AS t1(a, b)
        WHERE EXISTS (
            SELECT 1
            FROM (
                SELECT t2.a
                FROM (VALUES (1, 1)) AS t2(a, b)
                WHERE t2.b = t1.b
                LIMIT 1
            ) AS s
            WHERE s.a = t1.a
        )
    "#,
        },
        SqlTestCase {
            name: "nested_filter_alias_survives_deeper_join_rewrite",
            description: "A deeper join remap must override a stale filter-local alias instead of collapsing the correlated predicate into a self-equality.",
            setup_sqls: &[],
            sql: r#"
        SELECT *
        FROM (VALUES (1, 1)) AS t1(a, b)
        WHERE EXISTS (
            SELECT 1
            FROM (
                SELECT t2.a
                FROM (VALUES (1, 1)) AS t2(a, b)
                JOIN (VALUES (1)) AS t3(c)
                  ON t2.a = t1.a
                WHERE t2.b = t1.b
            ) AS s
            WHERE s.a = t1.a
        )
    "#,
        },
    ];

    for case in &cases {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}
