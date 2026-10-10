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

use databend_common_expression::DataBlock;
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
