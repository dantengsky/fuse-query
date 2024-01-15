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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_meta_app::principal::GrantObject;
use common_users::UserApiProvider;

use crate::sessions::QueryContext;

#[async_backtrace::framed]
pub async fn validate_grant_object_exists(
    ctx: &Arc<QueryContext>,
    object: &GrantObject,
) -> Result<()> {
    let tenant = ctx.get_tenant();

    match &object {
        GrantObject::Table(catalog_name, database_name, table_name) => {
            let catalog = ctx.get_catalog(catalog_name).await?;
            if catalog.exists_table_function(table_name) {
                return Ok(());
            }

            if !catalog
                .exists_table(tenant.as_str(), database_name, table_name)
                .await?
            {
                return Err(common_exception::ErrorCode::UnknownTable(format!(
                    "table `{}`.`{}` not exists in catalog '{}'",
                    database_name, table_name, catalog_name,
                )));
            }
        }
        GrantObject::Database(catalog_name, database_name) => {
            let catalog = ctx.get_catalog(catalog_name).await?;
            if !catalog
                .exists_database(tenant.as_str(), database_name)
                .await?
            {
                return Err(common_exception::ErrorCode::UnknownDatabase(format!(
                    "database {} not exists",
                    database_name,
                )));
            }
        }
        GrantObject::UDF(udf) => {
            if !UserApiProvider::instance()
                .exists_udf(tenant.as_str(), udf)
                .await?
            {
                return Err(common_exception::ErrorCode::UnknownStage(format!(
                    "udf {udf} not exists"
                )));
            }
        }
        GrantObject::Stage(stage) => {
            if !UserApiProvider::instance()
                .exists_stage(ctx.get_tenant().as_str(), stage)
                .await?
            {
                return Err(common_exception::ErrorCode::UnknownStage(format!(
                    "stage {stage} not exists"
                )));
            }
        }
        GrantObject::Global => (),
    }

    Ok(())
}
