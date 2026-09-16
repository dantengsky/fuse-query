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

use std::collections::BTreeSet;
use std::sync::Arc;

use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::number::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::plans::JoinType;
use databend_query::interpreters::InterpreterFactory;
use databend_query::physical_plans::ConstantTableScan;
use databend_query::physical_plans::HashJoin;
use databend_query::physical_plans::PhysicalPlan;
use databend_query::physical_plans::PhysicalPlanMeta;
use databend_query::physical_plans::PhysicalRuntimeFilters;
use databend_query::pipelines::processors::HashJoinDesc;
use databend_query::pipelines::processors::transforms::HashJoinFactory;
use databend_query::pipelines::processors::transforms::Join;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextSettings;
use databend_query::sql::Planner;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn outer_join_keeps_matched_grouping_sets_nulls() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let database = fixture.default_db_name();
    for sql in [
        format!("CREATE TABLE {database}.grouping_outer_l(k INT NOT NULL)"),
        format!("CREATE TABLE {database}.grouping_outer_r(marker INT NOT NULL)"),
        format!("INSERT INTO {database}.grouping_outer_l VALUES (1), (2)"),
        format!("INSERT INTO {database}.grouping_outer_r VALUES (7)"),
    ] {
        fixture.execute_command(&sql).await?;
    }

    for grouping in [
        "GROUPING SETS ((marker), ())",
        "ROLLUP(marker)",
        "CUBE(marker)",
    ] {
        let query = format!(
            "SELECT l.k, r.marker FROM {database}.grouping_outer_l l LEFT JOIN \
             (SELECT marker, count(*) AS n FROM {database}.grouping_outer_r GROUP BY {grouping}) r \
             ON l.k = r.n WHERE r.marker IS NULL ORDER BY l.k"
        );
        let blocks: Vec<DataBlock> = fixture.execute_query(&query).await?.try_collect().await?;
        let block = DataBlock::concat(&blocks)?;
        assert_eq!(block.num_rows(), 2, "{grouping}");
        assert_eq!(
            block.get_by_offset(0).value().index(0).unwrap().to_string(),
            "1"
        );
        assert_eq!(
            block.get_by_offset(0).value().index(1).unwrap().to_string(),
            "2"
        );
        assert!(block.get_by_offset(1).value().index(0).unwrap().is_null());
        assert!(block.get_by_offset(1).value().index(1).unwrap().is_null());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn correlated_computed_alias_preserves_results() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    for (projection, join_condition, expected) in [
        ("t2.a + 1", "t2.a = t1.a", 0),
        ("t2.a + 1", "t2.a + 1 = t1.a", 1),
        ("cast(t2.a as bigint) + 1", "t2.a + 1 = t1.a", 1),
        ("case when t2.a = 1 then 2 else 3 end", "t2.a + 1 = t1.a", 1),
    ] {
        let sql = format!(
            "SELECT count(*) FROM (VALUES (1,1),(2,1)) t1(a,b) WHERE EXISTS (SELECT 1 FROM (SELECT {projection} AS a FROM (VALUES (1,1),(2,1)) t2(a,b) JOIN (VALUES (1)) t3(c) ON {join_condition} WHERE t2.b=t1.b) s WHERE s.a=t1.a)"
        );
        let blocks: Vec<DataBlock> = fixture.execute_query(&sql).await?.try_collect().await?;
        let block = DataBlock::concat(&blocks)?;
        assert_eq!(
            block.get_by_offset(0).value().index(0).unwrap().to_string(),
            expected.to_string(),
            "{sql}"
        );
    }
    Ok(())
}

fn constant_plan() -> PhysicalPlan {
    PhysicalPlan::new(ConstantTableScan {
        meta: PhysicalPlanMeta::new("ConstantTableScan"),
        values: vec![],
        num_rows: 0,
        output_schema: Arc::new(DataSchema::empty()),
    })
}

fn hash_join_plan(join_type: JoinType, probe_key: &Expr, build_key: &Expr) -> HashJoin {
    HashJoin {
        meta: PhysicalPlanMeta::new("HashJoin"),
        projections: BTreeSet::new(),
        probe_projections: BTreeSet::new(),
        build_projections: BTreeSet::new(),
        build: constant_plan(),
        probe: constant_plan(),
        build_keys: vec![build_key.as_remote_expr()],
        probe_keys: vec![probe_key.as_remote_expr()],
        is_null_equal: vec![false],
        non_equi_conditions: vec![],
        join_type,
        marker_index: None,
        from_correlated_subquery: false,
        probe_to_build: vec![],
        output_schema: Arc::new(DataSchema::empty()),
        need_hold_hash_table: false,
        stat_info: None,
        single_to_inner: None,
        build_side_cache_info: None,
        runtime_filter: PhysicalRuntimeFilters::default(),
        broadcast_id: None,
        nested_loop_filter: None,
    }
}

fn int64_key(value: i64) -> Expr {
    Expr::constant(
        Scalar::Number(NumberScalar::Int64(value)),
        Some(DataType::Number(NumberDataType::Int64)),
    )
}

fn int64_factory(
    ctx: Arc<QueryContext>,
    physical_join: &HashJoin,
) -> anyhow::Result<Arc<HashJoinFactory>> {
    let desc = Arc::new(HashJoinDesc::create(physical_join)?);
    let method =
        DataBlock::choose_hash_method_with_types(&[DataType::Number(NumberDataType::Int64)])?;
    Ok(HashJoinFactory::create(
        ctx,
        FunctionContext::default(),
        method,
        desc,
    ))
}

#[tokio::test(flavor = "multi_thread")]
async fn right_outer_join_skips_probe_keys_when_build_is_empty() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let function_ctx = FunctionContext::default();

    let invalid_probe_key = check_function(
        None,
        "to_int64",
        &[],
        &[Expr::constant(Scalar::String("invalid".to_string()), None)],
        &BUILTIN_FUNCTIONS,
    )?;
    let build_key = int64_key(0);

    let physical_join = hash_join_plan(JoinType::Right, &invalid_probe_key, &build_key);
    let desc = Arc::new(HashJoinDesc::create(&physical_join)?);
    let method =
        DataBlock::choose_hash_method_with_types(&[DataType::Number(NumberDataType::Int64)])?;
    let factory = HashJoinFactory::create(ctx, function_ctx, method, desc);
    let mut join = factory.create_memory_join(JoinType::Right, 0)?;

    join.add_block(None)?;
    while join.final_build()?.is_some() {}

    // The invalid cast proves that the actual probe-key evaluator is not reached.
    let mut stream = join.probe_block(DataBlock::new(vec![], 1))?;
    assert!(stream.next()?.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn right_outer_join_completes_without_probe_key_error_when_build_is_empty()
-> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings()
        .set_setting("enable_experimental_new_join".to_string(), "1".to_string())?;
    ctx.get_settings()
        .set_setting("enable_join_runtime_filter".to_string(), "0".to_string())?;

    // This pins that the query completes empty rather than raising the probe-key cast error.
    // Note it does not by itself prove the probe pipeline was stopped: the key is evaluated
    // inside the join's own probe_block (EXPLAIN shows it under `probe keys:`, with no
    // EvalScalar in the probe pipeline), and probe_block returns early on an empty build
    // before evaluating keys. The pipeline-level skip is asserted against execution metrics
    // in mode/standalone/empty_build_join_short_circuit.test.
    let query = r#"
        SELECT probe.number
        FROM numbers(10) AS probe
        RIGHT JOIN (
            SELECT number
            FROM numbers(1)
            WHERE number > 10
        ) AS build
            ON to_int64(concat('invalid-', to_string(probe.number))) = build.number
    "#;
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(query).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let blocks: Vec<DataBlock> = interpreter.execute(ctx).await?.try_collect().await?;

    assert_eq!(blocks.iter().map(DataBlock::num_rows).sum::<usize>(), 0);
    Ok(())
}

/// `can_skip_probe` is a join-type predicate: it must cover exactly the join types whose output
/// is empty once the build side is empty. Getting this wrong is either a silent full probe scan
/// (missing type) or a wrong result (extra type), so pin every type the new join dispatches.
#[tokio::test(flavor = "multi_thread")]
async fn can_skip_probe_covers_join_types_emptied_by_empty_build() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    // Left/LeftAnti still emit their preserved probe rows, so they must keep probing.
    let cases = [
        (JoinType::Inner, true),
        (JoinType::LeftSemi, true),
        (JoinType::Right, true),
        (JoinType::RightSemi, true),
        (JoinType::RightAnti, true),
        (JoinType::Left, false),
        (JoinType::LeftAnti, false),
    ];

    for (join_type, expected) in cases {
        let physical_join = hash_join_plan(join_type, &int64_key(1), &int64_key(0));
        let factory = int64_factory(ctx.clone(), &physical_join)?;
        let join = factory.create_hybrid_join(join_type, 0)?;
        assert_eq!(
            join.can_skip_probe(),
            expected,
            "unexpected can_skip_probe for {join_type:?}"
        );
    }
    Ok(())
}

/// The emptiness signal must not depend on runtime filters existing. `PhysicalRuntimeFilters` is
/// empty here, so no runtime-filter builder is created and the builders' own row counter stays 0;
/// reporting that as `build_rows` would make `build_side_empty()` true for a non-empty build side
/// and skip the probe, producing wrong results rather than merely slow ones.
#[tokio::test(flavor = "multi_thread")]
async fn build_runtime_filter_reports_logical_build_rows_without_runtime_filters()
-> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let physical_join = hash_join_plan(JoinType::LeftSemi, &int64_key(1), &int64_key(0));
    assert!(
        physical_join.runtime_filter.filters.is_empty(),
        "this test only covers the no-runtime-filter path"
    );

    let factory = int64_factory(ctx, &physical_join)?;
    let mut join = factory.create_hybrid_join(JoinType::LeftSemi, 0)?;

    join.add_block(Some(DataBlock::new(vec![], 3)))?;
    join.add_block(Some(DataBlock::new(vec![], 4)))?;
    join.add_block(None)?;
    while join.final_build()?.is_some() {}

    assert_eq!(join.build_runtime_filter()?.build_rows, 7);
    Ok(())
}
