// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use aggregating_index::get_agg_index_handler;
use chrono::Utc;
use common_base::base::tokio;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::block_debug::assert_two_blocks_sorted_eq_with_name;
use common_expression::DataBlock;
use common_expression::SendableDataBlockStream;
use common_meta_app::schema::CreateIndexReq;
use common_meta_app::schema::IndexMeta;
use common_meta_app::schema::IndexNameIdent;
use common_meta_app::schema::IndexType;
use common_sql::plans::Plan;
use common_sql::Planner;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::TestFixture;
use enterprise_query::test_kits::context::EESetup;
use futures_util::TryStreamExt;

async fn plan_sql(ctx: Arc<QueryContext>, sql: &str) -> Result<Plan> {
    let mut planner = Planner::new(ctx);
    let (plan, _) = planner.plan_sql(sql).await?;

    Ok(plan)
}

async fn execute_sql(ctx: Arc<QueryContext>, sql: &str) -> Result<SendableDataBlockStream> {
    let plan = plan_sql(ctx.clone(), sql).await?;
    execute_plan(ctx, &plan).await
}

async fn execute_plan(ctx: Arc<QueryContext>, plan: &Plan) -> Result<SendableDataBlockStream> {
    let interpreter = InterpreterFactory::get(ctx.clone(), plan).await?;
    interpreter.execute(ctx).await
}

async fn create_index(
    ctx: Arc<QueryContext>,
    index_name: &str,
    query: &str,
    sync_creation: bool,
) -> Result<u64> {
    let sql = format!("CREATE AGGREGATING INDEX {index_name} AS {query}");

    let plan = plan_sql(ctx.clone(), &sql).await?;

    if let Plan::CreateIndex(plan) = plan {
        let catalog = ctx.get_catalog("default").await?;
        let create_index_req = CreateIndexReq {
            if_not_exists: plan.if_not_exists,
            name_ident: IndexNameIdent {
                tenant: ctx.get_tenant(),
                index_name: index_name.to_string(),
            },
            meta: IndexMeta {
                table_id: plan.table_id,
                index_type: IndexType::AGGREGATING,
                created_on: Utc::now(),
                dropped_on: None,
                updated_on: None,
                query: query.to_string(),
                sync_creation,
            },
        };

        let handler = get_agg_index_handler();
        let res = handler.do_create_index(catalog, create_index_req).await?;

        return Ok(res.index_id);
    }

    unreachable!()
}

async fn refresh_index(
    ctx: Arc<QueryContext>,
    index_name: &str,
    limit: Option<usize>,
) -> Result<()> {
    let sql = match limit {
        Some(l) => format!("REFRESH AGGREGATING INDEX {index_name} LIMIT {l}"),
        None => format!("REFRESH AGGREGATING INDEX {index_name}"),
    };
    execute_sql(ctx, &sql).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_agg_index() -> Result<()> {
    let fixture = TestFixture::with_setup(EESetup::new()).await?;

    fixture
        .execute_command("CREATE TABLE t0 (a int, b int, c int) storage_format = 'parquet'")
        .await?;

    // Insert data
    fixture
        .execute_command("INSERT INTO t0 VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)")
        .await?;

    // Create index
    let index_name = "index0";

    let ctx = fixture.new_query_ctx().await?;
    let index_id = create_index(
        ctx,
        index_name,
        "SELECT b, SUM(a) from t0 WHERE c > 1 GROUP BY b",
        false,
    )
    .await?;

    // Refresh Index
    let ctx = fixture.new_query_ctx().await?;
    refresh_index(ctx, index_name, None).await?;

    let root = fixture.storage_root();
    let block_path = find_block_path(root)?.unwrap();
    let block_name_prefix = PathBuf::from(
        block_path
            .strip_prefix(root)
            .map_err(|e| ErrorCode::Internal(e.to_string()))?,
    );
    let blocks = collect_file_names(&block_path)?;

    // Get aggregating index files
    let agg_index_path = find_agg_index_path(root, index_id)?.unwrap();
    let indexes = collect_file_names(&agg_index_path)?;

    assert_eq!(blocks, indexes);

    // Check aggregating index is correct.
    {
        let res = fixture
            .execute_query("SELECT b, SUM_STATE(a) from t0 WHERE c > 1 GROUP BY b")
            .await?;
        let data_blocks: Vec<DataBlock> = res.try_collect().await?;

        let agg_res = fixture
            .execute_query(&format!(
                "SELECT * FROM 'fs://{}'",
                agg_index_path.join(&indexes[0]).to_str().unwrap()
            ))
            .await?;
        let agg_data_blocks: Vec<DataBlock> = agg_res.try_collect().await?;

        assert_two_blocks_sorted_eq_with_name("refresh index", &data_blocks, &agg_data_blocks);
    }

    fixture
        .execute_command("INSERT INTO t0 VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)")
        .await?;

    let pre_block = blocks[0].clone();
    let mut blocks = collect_file_names(&block_path)?;
    assert!(blocks.len() > indexes.len());

    let ctx = fixture.new_query_ctx().await?;
    refresh_index(ctx, index_name, None).await?;

    // check the new added index is correct.
    {
        let pre_agg_index = indexes[0].clone();
        let mut indexes = collect_file_names(&agg_index_path)?;
        assert_eq!(blocks, indexes);

        let new_block = {
            blocks.retain(|s| s != &pre_block);
            blocks[0].clone()
        };

        let new_agg_index = {
            indexes.retain(|i| i != &pre_agg_index);
            indexes[0].clone()
        };

        let data_blocks: Vec<DataBlock> = fixture
            .execute_query(&format!(
                "SELECT b, SUM_STATE(a) from t0 WHERE c > 1 and _block_name = '{}' GROUP BY b",
                block_name_prefix.join(&new_block).to_str().unwrap()
            ))
            .await?
            .try_collect()
            .await?;

        let agg_data_blocks: Vec<DataBlock> = fixture
            .execute_query(&format!(
                "SELECT * FROM 'fs://{}'",
                agg_index_path.join(&new_agg_index).to_str().unwrap()
            ))
            .await?
            .try_collect()
            .await?;

        assert_two_blocks_sorted_eq_with_name(
            "refresh index again",
            &data_blocks,
            &agg_data_blocks,
        );
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_refresh_agg_index_with_limit() -> Result<()> {
    let fixture = TestFixture::with_setup(EESetup::new()).await?;

    // Create table
    fixture
        .execute_command("CREATE TABLE t1 (a int, b int, c int) storage_format = 'parquet'")
        .await?;

    // Insert data
    fixture
        .execute_command("INSERT INTO t1 VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)")
        .await?;

    // Create index
    let index_name = "index1";
    let ctx = fixture.new_query_ctx().await?;
    let index_id = create_index(
        ctx,
        index_name,
        "SELECT b, SUM(a) from t1 WHERE c > 1 GROUP BY b",
        false,
    )
    .await?;

    // Insert more data
    fixture
        .execute_command("INSERT INTO t1 VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)")
        .await?;

    fixture
        .execute_command("INSERT INTO t1 VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)")
        .await?;

    // Refresh index with limit 1
    let ctx = fixture.new_query_ctx().await?;
    refresh_index(ctx, index_name, Some(1)).await?;

    let root = fixture.storage_root();
    let block_path = find_block_path(root)?.unwrap();
    let blocks = collect_file_names(&block_path)?;

    // Get aggregating index files
    let agg_index_path = find_agg_index_path(root, index_id)?.unwrap();
    let indexes = collect_file_names(&agg_index_path)?;
    assert_eq!(blocks.len() - indexes.len(), 2);

    // Refresh index with limit 1 again.
    let ctx = fixture.new_query_ctx().await?;
    refresh_index(ctx, index_name, Some(1)).await?;
    let indexes = collect_file_names(&agg_index_path)?;
    assert_eq!(blocks.len() - indexes.len(), 1);

    // Refresh index with limit 1 again.
    let ctx = fixture.new_query_ctx().await?;
    refresh_index(ctx, index_name, Some(1)).await?;
    let indexes = collect_file_names(&agg_index_path)?;
    assert_eq!(blocks.len(), indexes.len());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sync_agg_index() -> Result<()> {
    test_sync_agg_index_after_update().await?;
    test_sync_agg_index_after_insert().await?;
    test_sync_agg_index_after_copy_into().await?;

    Ok(())
}

async fn test_sync_agg_index_after_update() -> Result<()> {
    // let (_guard, ctx, root) = create_ee_query_context(None).await.unwrap();
    let fixture = TestFixture::with_setup(EESetup::new()).await?;
    fixture
        .default_session()
        .get_settings()
        .set_enable_refresh_aggregating_index_after_write(true)?;

    // ctx.get_settings()
    //     .set_enable_refresh_aggregating_index_after_write(true)?;
    // let fixture = TestFixture::new_with_ctx(_guard, ctx).await;
    // let ctx = fixture.ctx();

    // Create table
    fixture
        .execute_command("CREATE TABLE t0 (a int, b int, c int) storage_format = 'parquet'")
        .await?;

    // Create agg index `index0`
    let index_name = "index0";

    let ctx = fixture.new_query_ctx().await?;
    let index_id0 = create_index(
        ctx,
        index_name,
        "SELECT b, SUM(a) from t0 WHERE c > 1 GROUP BY b",
        true,
    )
    .await?;

    // Insert data
    fixture
        .execute_command("INSERT INTO t0 VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)")
        .await?;

    let root = fixture.storage_root();
    let block_path = find_block_path(root)?.unwrap();
    let blocks = collect_file_names(&block_path)?;

    // Get aggregating index files
    let agg_index_path_0 = find_agg_index_path(root, index_id0)?.unwrap();
    let indexes_0 = collect_file_names(&agg_index_path_0)?;

    assert_eq!(blocks, indexes_0);

    // Check aggregating index_0 is correct.
    {
        let res = fixture
            .execute_query("SELECT b, SUM_STATE(a) from t0 WHERE c > 1 GROUP BY b")
            .await?;
        let data_blocks: Vec<DataBlock> = res.try_collect().await?;

        let agg_res = fixture
            .execute_query(&format!(
                "SELECT * FROM 'fs://{}'",
                agg_index_path_0.join(&indexes_0[0]).to_str().unwrap()
            ))
            .await?;
        let agg_data_blocks: Vec<DataBlock> = agg_res.try_collect().await?;

        assert_two_blocks_sorted_eq_with_name(
            "test_sync_agg_index_after_update",
            &data_blocks,
            &agg_data_blocks,
        );
    }

    // Update
    fixture
        .execute_command("UPDATE t0 SET c = 2 WHERE b = 2")
        .await?;

    let first_block = blocks[0].clone();
    let first_agg_index = indexes_0[0].clone();

    let blocks = collect_file_names(&block_path)?;

    // check index0
    let indexes_0 = collect_file_names(&agg_index_path_0)?;
    assert_eq!(blocks, indexes_0);

    // Check aggregating index_0 is correct after update.
    {
        let updated_block = blocks
            .iter()
            .find(|b| !b.eq_ignore_ascii_case(&first_block))
            .unwrap();
        let updated_agg_index = indexes_0
            .iter()
            .find(|i| !i.eq_ignore_ascii_case(&first_agg_index))
            .unwrap();
        let res = fixture
            .execute_query(&format!(
                "SELECT b, SUM_STATE(a) from 'fs://{}' WHERE c > 1 GROUP BY b",
                block_path.join(updated_block).to_str().unwrap()
            ))
            .await?;
        let data_blocks: Vec<DataBlock> = res.try_collect().await?;

        let agg_res = fixture
            .execute_query(&format!(
                "SELECT * FROM 'fs://{}'",
                agg_index_path_0.join(updated_agg_index).to_str().unwrap()
            ))
            .await?;
        let agg_data_blocks: Vec<DataBlock> = agg_res.try_collect().await?;

        assert_two_blocks_sorted_eq_with_name(
            "test_sync_agg_index_after_update",
            &data_blocks,
            &agg_data_blocks,
        );
    }

    Ok(())
}

async fn test_sync_agg_index_after_insert() -> Result<()> {
    // let (_guard, ctx, root) = create_ee_query_context(None).await.unwrap();
    // ctx.get_settings()
    //    .set_enable_refresh_aggregating_index_after_write(true)?;

    let fixture = TestFixture::with_setup(EESetup::new()).await?;
    fixture
        .default_session()
        .get_settings()
        .set_enable_refresh_aggregating_index_after_write(true)?;
    // Create table
    fixture
        .execute_command("CREATE TABLE t0 (a int, b int, c int) storage_format = 'parquet'")
        .await?;

    // Create agg index `index0`
    let index_name = "index0";

    let ctx = fixture.new_query_ctx().await?;
    let index_id0 = create_index(
        ctx,
        index_name,
        "SELECT b, SUM(a) from t0 WHERE c > 1 GROUP BY b",
        true,
    )
    .await?;

    // Create agg index `index1`
    let index_name = "index1";

    let ctx = fixture.new_query_ctx().await?;
    let index_id1 = create_index(
        ctx,
        index_name,
        "SELECT a, SUM(b) from t0 WHERE c > 1 GROUP BY a",
        true,
    )
    .await?;

    // Insert data
    fixture
        .execute_command("INSERT INTO t0 VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)")
        .await?;

    let root = fixture.storage_root();
    let block_path = find_block_path(root)?.unwrap();
    let blocks = collect_file_names(&block_path)?;

    // Get aggregating index files
    let agg_index_path_0 = find_agg_index_path(root, index_id0)?.unwrap();
    let indexes_0 = collect_file_names(&agg_index_path_0)?;

    // Get aggregating index files
    let agg_index_path_1 = find_agg_index_path(root, index_id1)?.unwrap();
    let indexes_1 = collect_file_names(&agg_index_path_1)?;

    assert_eq!(blocks, indexes_1);

    // Check aggregating index_0 is correct.
    {
        let res = fixture
            .execute_query("SELECT b, SUM_STATE(a) from t0 WHERE c > 1 GROUP BY b")
            .await?;
        let data_blocks: Vec<DataBlock> = res.try_collect().await?;

        let agg_res = fixture
            .execute_query(&format!(
                "SELECT * FROM 'fs://{}'",
                agg_index_path_0.join(&indexes_0[0]).to_str().unwrap()
            ))
            .await?;
        let agg_data_blocks: Vec<DataBlock> = agg_res.try_collect().await?;

        assert_two_blocks_sorted_eq_with_name(
            "test_sync_agg_index_after_insert",
            &data_blocks,
            &agg_data_blocks,
        );
    }

    // Check aggregating index_1 is correct.
    {
        let res = fixture
            .execute_query("SELECT a, SUM_STATE(b) from t0 WHERE c > 1 GROUP BY a")
            .await?;
        let data_blocks: Vec<DataBlock> = res.try_collect().await?;

        let agg_res = fixture
            .execute_query(&format!(
                "SELECT * FROM 'fs://{}'",
                agg_index_path_1.join(&indexes_1[0]).to_str().unwrap()
            ))
            .await?;
        let agg_data_blocks: Vec<DataBlock> = agg_res.try_collect().await?;

        assert_two_blocks_sorted_eq_with_name(
            "test_sync_agg_index_after_insert",
            &data_blocks,
            &agg_data_blocks,
        );
    }

    // Insert more data with insert into ... select ...
    fixture
        .execute_query("INSERT INTO t0 SELECT * FROM t0")
        .await?;

    let blocks = collect_file_names(&block_path)?;

    // check index0
    let indexes_0 = collect_file_names(&agg_index_path_0)?;
    assert_eq!(blocks, indexes_0);

    // check index1
    let indexes_1 = collect_file_names(&agg_index_path_1)?;
    assert_eq!(blocks, indexes_1);

    Ok(())
}

async fn test_sync_agg_index_after_copy_into() -> Result<()> {
    let fixture = TestFixture::with_setup(EESetup::new()).await?;
    fixture
        .default_session()
        .get_settings()
        .set_enable_refresh_aggregating_index_after_write(true)?;

    // Create table
    fixture.execute_command(
        "CREATE TABLE books (title VARCHAR, author VARCHAR, date VARCHAR) storage_format = 'parquet'",
    )
        .await?;

    // Create agg index `index0`
    let index_name = "index0";

    let ctx = fixture.new_query_ctx().await?;
    let index_id0 = create_index(ctx, index_name, "SELECT MAX(title) from books", true).await?;

    // Copy into data
    fixture.execute_query(
        "COPY INTO books FROM 'https://datafuse-1253727613.cos.ap-hongkong.myqcloud.com/data/books.csv' FILE_FORMAT = (TYPE = CSV);",
    )
        .await?;

    let root = fixture.storage_root();
    let block_path = find_block_path(root)?.unwrap();
    let blocks = collect_file_names(&block_path)?;

    // Get aggregating index files
    let agg_index_path_0 = find_agg_index_path(root, index_id0)?.unwrap();
    let indexes_0 = collect_file_names(&agg_index_path_0)?;

    assert_eq!(blocks, indexes_0);

    // Check aggregating index_0 is correct.
    {
        let res = fixture
            .execute_query("SELECT MAX_STATE(title) from books")
            .await?;
        let data_blocks: Vec<DataBlock> = res.try_collect().await?;

        let agg_res = fixture
            .execute_query(&format!(
                "SELECT * FROM 'fs://{}'",
                agg_index_path_0.join(&indexes_0[0]).to_str().unwrap()
            ))
            .await?;
        let agg_data_blocks: Vec<DataBlock> = agg_res.try_collect().await?;

        assert_two_blocks_sorted_eq_with_name(
            "test_sync_agg_index_after_copy_into",
            &data_blocks,
            &agg_data_blocks,
        );
    }

    Ok(())
}

fn find_block_path<P: AsRef<Path>>(dir: P) -> Result<Option<PathBuf>> {
    find_target_path(dir, "_b")
}

fn find_agg_index_path<P: AsRef<Path>>(dir: P, index_id: u64) -> Result<Option<PathBuf>> {
    let path = find_target_path(dir, "_i_a")?;
    Ok(path.map(|p| p.join(index_id.to_string())))
}

fn find_target_path<P: AsRef<Path>>(dir: P, target: &str) -> Result<Option<PathBuf>> {
    fn find_target_recursive<P: AsRef<Path>>(dir: P, target: &str) -> Result<Option<PathBuf>> {
        let dir = dir.as_ref();
        if dir.is_dir() {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    if path.file_name() == Some(target.as_ref()) {
                        return Ok(Some(path));
                    } else if let Some(result) = find_target_recursive(&path, target)? {
                        return Ok(Some(result));
                    }
                }
            }
        }
        Ok(None)
    }

    find_target_recursive(dir, target)
}

fn collect_file_names<P: AsRef<Path>>(dir: P) -> Result<Vec<String>> {
    let dir = dir.as_ref();
    let mut file_names = Vec::new();

    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(file_name) = path.file_name().and_then(|f| f.to_str()) {
                    file_names.push(file_name.to_string());
                }
            }
        }
    }

    Ok(file_names)
}
