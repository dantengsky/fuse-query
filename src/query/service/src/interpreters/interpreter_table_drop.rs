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

use common_catalog::table::TableExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_management::RoleApi;
use common_meta_app::principal::GrantObjectByID;
use common_meta_app::schema::DropTableByIdReq;
use common_sql::plans::DropTablePlan;
use common_storages_fuse::FuseTable;
use common_storages_share::save_share_spec;
use common_storages_view::view_table::VIEW_ENGINE;
use common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropTablePlan,
}

impl DropTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropTablePlan) -> Result<Self> {
        Ok(DropTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTableInterpreter {
    fn name(&self) -> &str {
        "DropTableInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();
        let tbl = self
            .ctx
            .get_table(catalog_name, db_name, tbl_name)
            .await
            .ok();

        if tbl.is_none() && !self.plan.if_exists {
            return Err(ErrorCode::UnknownTable(format!(
                "Unknown table `{}`.`{}` in catalog '{}'",
                db_name, tbl_name, catalog_name
            )));
        }
        if let Some(tbl) = tbl {
            if tbl.get_table_info().engine() == VIEW_ENGINE {
                return Err(ErrorCode::TableEngineNotSupported(format!(
                    "{}.{} engine is VIEW that doesn't support drop, use `DROP VIEW {}.{}` instead",
                    &self.plan.database, &self.plan.table, &self.plan.database, &self.plan.table
                )));
            }
            let catalog = self.ctx.get_catalog(catalog_name).await?;

            // drop the ownership
            let tenant = self.ctx.get_tenant();
            let role_api = UserApiProvider::instance().get_role_api_client(&self.plan.tenant)?;
            let db = catalog.get_database(&tenant, &self.plan.database).await?;
            role_api
                .drop_ownership(&GrantObjectByID::Table {
                    catalog_name: self.plan.catalog.clone(),
                    db_id: db.get_db_info().ident.db_id,
                    table_id: tbl.get_table_info().ident.table_id,
                })
                .await?;

            // Although even if data is in READ_ONLY mode,
            // as a catalog object, the table itself is allowed to be dropped (and undropped later),
            // `drop table ALL` is NOT allowed, which implies that the table data need to be truncated.
            if self.plan.all {
                // check mutability, if the table is read only, we cannot truncate the data
                tbl.check_mutable().map_err(|e| {
                    e.add_message(" drop table ALL is not allowed for read only table, please consider remove the option ALL")
                })?
            }

            // actually drop table
            let resp = catalog
                .drop_table_by_id(DropTableByIdReq {
                    if_exists: self.plan.if_exists,
                    tenant: self.plan.tenant.clone(),
                    tb_id: tbl.get_table_info().ident.table_id,
                })
                .await?;

            // if `plan.all`, truncate, then purge the historical data
            if self.plan.all {
                // the above `catalog.drop_table` operation changed the table meta version,
                // thus if we do not refresh the table instance, `truncate` will fail
                let latest = tbl.as_ref().refresh(self.ctx.as_ref()).await?;
                let maybe_fuse_table = FuseTable::try_from_table(latest.as_ref());
                // if target table if of type FuseTable, purge its historical data
                // otherwise, plain truncate
                if let Ok(fuse_table) = maybe_fuse_table {
                    let purge = true;
                    fuse_table.do_truncate(self.ctx.clone(), purge).await?
                } else {
                    latest.truncate(self.ctx.clone()).await?
                }
            }

            // update share spec if needed
            if let Some((spec_vec, share_table_info)) = resp.spec_vec {
                save_share_spec(
                    &self.ctx.get_tenant(),
                    self.ctx.get_data_operator()?.operator(),
                    Some(spec_vec),
                    Some(share_table_info),
                )
                .await?;
            }
        }

        Ok(PipelineBuildResult::create())
    }
}
