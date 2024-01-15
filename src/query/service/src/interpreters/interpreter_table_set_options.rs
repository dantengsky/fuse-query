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

use common_catalog::table::TableExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::UpsertTableOptionReq;
use common_meta_types::MatchSeq;
use common_sql::plans::SetOptionsPlan;
use common_storages_fuse::TableContext;
use log::error;
use storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;

use super::interpreter_table_create::is_valid_block_per_segment;
use super::interpreter_table_create::is_valid_bloom_index_columns;
use super::interpreter_table_create::is_valid_create_opt;
use super::interpreter_table_create::is_valid_row_per_block;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct SetOptionsInterpreter {
    ctx: Arc<QueryContext>,
    plan: SetOptionsPlan,
}

impl SetOptionsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: SetOptionsPlan) -> Result<Self> {
        Ok(SetOptionsInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for SetOptionsInterpreter {
    fn name(&self) -> &str {
        "SetOptionsInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        // valid_options_check and do request to meta_srv
        let mut options_map = HashMap::new();
        // check block_per_segment
        is_valid_block_per_segment(&self.plan.set_options)?;
        // check row_per_block
        is_valid_row_per_block(&self.plan.set_options)?;
        // check storage_format
        let error_str = "invalid opt for fuse table in alter table statement";
        if self.plan.set_options.get(OPT_KEY_STORAGE_FORMAT).is_some() {
            error!("{}", &error_str);
            return Err(ErrorCode::TableOptionInvalid(format!(
                "can't change {} for alter table statement",
                OPT_KEY_STORAGE_FORMAT
            )));
        }
        if self.plan.set_options.get(OPT_KEY_DATABASE_ID).is_some() {
            error!("{}", &error_str);
            return Err(ErrorCode::TableOptionInvalid(format!(
                "can't change {} for alter table statement",
                OPT_KEY_DATABASE_ID
            )));
        }
        for table_option in self.plan.set_options.iter() {
            let key = table_option.0.to_lowercase();
            if !is_valid_create_opt(&key) {
                error!("{}", &error_str);
                return Err(ErrorCode::TableOptionInvalid(format!(
                    "table option {key} is invalid for alter table statement",
                )));
            }
            options_map.insert(key, Some(table_option.1.clone()));
        }
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;
        let database = self.plan.database.as_str();
        let table = self.plan.table.as_str();
        let tbl = catalog
            .get_table(self.ctx.get_tenant().as_str(), database, table)
            .await
            .ok();

        let table = if let Some(table) = &tbl {
            // check mutability
            table.check_mutable()?;
            table
        } else {
            return Err(ErrorCode::UnknownTable(format!(
                "Unknown table `{}`.`{}` in catalog '{}'",
                database,
                self.plan.table.as_str(),
                &catalog.name()
            )));
        };

        // check bloom_index_columns.
        is_valid_bloom_index_columns(&self.plan.set_options, table.schema())?;

        let req = UpsertTableOptionReq {
            table_id: table.get_id(),
            seq: MatchSeq::Exact(table.get_table_info().ident.seq),
            options: options_map,
        };

        catalog
            .upsert_table_option(self.ctx.get_tenant().as_str(), database, req)
            .await?;
        Ok(PipelineBuildResult::create())
    }
}
