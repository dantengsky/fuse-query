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

use std::io::Write;

use databend_common_exception::Result;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

async fn write_optimized_case(file: &mut impl std::io::Write, case: &SqlTestCase) -> Result<()> {
    let ctx = setup_context(case).await?;
    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;

    write_case_header(file, case)?;
    writeln!(file, "raw_plan:")?;
    let raw_plan = raw_plan.format_indent(Default::default())?;
    writeln!(file, "{}", raw_plan.trim_end())?;
    writeln!(file, "optimized_plan:")?;
    let optimized_plan = optimized_plan.format_indent(Default::default())?;
    writeln!(file, "{}", optimized_plan.trim_end())?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_outer_join_to_anti_optimizer_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "outer_join_to_anti.txt")?;
    let cases = [
        SqlTestCase {
            name: "regular_right_key_becomes_left_anti",
            description: "A NULL filter on a regular right equi-key should become a left anti join.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE r.k IS NULL",
        },
        SqlTestCase {
            name: "right_outputs_are_reconstructed_as_nulls",
            description: "Right outputs should remain available as typed NULL expressions.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.k, r.payload
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE r.k IS NULL",
        },
        SqlTestCase {
            name: "remaining_filter_is_preserved",
            description: "An unrelated predicate on the preserved side should remain after rewriting.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE r.k IS NULL AND l.keep > 0",
        },
        SqlTestCase {
            name: "right_outer_exclusion_becomes_anti",
            description: "The symmetric right outer exclusion should become an anti join.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.keep, r.k
FROM outer_to_anti_left AS l
RIGHT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE l.k IS NULL",
        },
        SqlTestCase {
            name: "nullable_payload_test_keeps_outer_join",
            description: "A nullable non-key payload may be NULL on matched rows and is not an anti-join signal.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.payload
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE r.payload IS NULL",
        },
        SqlTestCase {
            name: "non_nullable_derived_payload_becomes_anti",
            description: "A non-nullable derived payload can only be NULL because the outer join found no match.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k
FROM outer_to_anti_left AS l
LEFT JOIN (
    SELECT k, 'first' AS marker FROM outer_to_anti_right
    UNION ALL
    SELECT k, 'second' AS marker FROM outer_to_anti_right
) AS r ON l.k = r.k
WHERE r.marker IS NULL",
        },
        SqlTestCase {
            name: "nullable_union_branch_keeps_outer_join",
            description: "All Union branches must prove a derived payload non-null before the rewrite is safe.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k
FROM outer_to_anti_left AS l
LEFT JOIN (
    SELECT k, payload AS marker FROM outer_to_anti_right
    UNION ALL
    SELECT k, 1 AS marker FROM outer_to_anti_right
) AS r ON l.k = r.k
WHERE r.marker IS NULL",
        },
        SqlTestCase {
            name: "nested_outer_join_output_keeps_outer_join",
            description: "A nested outer join may null-extend a source column declared NOT NULL.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE, NON_NULL_TABLE],
            sql: "SELECT l.k
FROM outer_to_anti_left AS l
LEFT JOIN (
    SELECT r.k, n.marker
    FROM outer_to_anti_right AS r
    LEFT JOIN outer_to_anti_non_null AS n ON r.k = n.k
) AS d ON l.k = d.k
WHERE d.marker IS NULL",
        },
        SqlTestCase {
            name: "null_masking_right_equi_expression_is_not_anti",
            description: "A right equi-key expression that maps NULL to a value must not become a left anti join.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k = IF(r.k IS NULL, 0, r.k)
WHERE IF(r.k IS NULL, 0, r.k) IS NULL",
        },
        SqlTestCase {
            name: "null_masking_left_equi_expression_is_not_anti",
            description: "A left equi-key expression that maps NULL to a value must not become a right anti join.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT r.k
FROM outer_to_anti_left AS l
RIGHT JOIN outer_to_anti_right AS r ON IF(l.k IS NULL, 0, l.k) = r.k
WHERE IF(l.k IS NULL, 0, l.k) IS NULL",
        },
        SqlTestCase {
            name: "null_safe_condition_keeps_outer_join",
            description: "A null-equal join key must not be rewritten as an anti join.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.k
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k IS NOT DISTINCT FROM r.k
WHERE r.k IS NULL",
        },
    ];

    for (index, case) in cases.iter().enumerate() {
        if index > 0 {
            writeln!(file)?;
        }
        write_optimized_case(&mut file, case).await?;
    }
    Ok(())
}

const LEFT_TABLE: &str = "CREATE TABLE outer_to_anti_left
(
    k INTEGER,
    item INTEGER,
    keep INTEGER
)";

const RIGHT_TABLE: &str = "CREATE TABLE outer_to_anti_right
(
    k INTEGER,
    item INTEGER,
    payload INTEGER
)";

const NON_NULL_TABLE: &str = "CREATE TABLE outer_to_anti_non_null
(
    k INTEGER,
    marker INTEGER NOT NULL
)";
