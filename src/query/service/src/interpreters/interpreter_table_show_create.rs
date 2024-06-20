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

use databend_common_ast::parser::Dialect;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::plans::ShowCreateTablePlan;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_storages_view::view_table::QUERY;
use databend_common_storages_view::view_table::VIEW_ENGINE;
use databend_storages_common_table_meta::table::is_internal_opt_key;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_PREFIX;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_ATTACHED_DATA_URI;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_ATTACHED_READ_ONLY;

use crate::interpreters::util::format_name;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ShowCreateTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowCreateTablePlan,
}

pub struct ShowCreateQuerySettings {
    pub sql_dialect: Dialect,
    pub quoted_ident_case_sensitive: bool,
    pub hide_options_in_show_create_table: bool,
}

impl ShowCreateTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowCreateTablePlan) -> Result<Self> {
        Ok(ShowCreateTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowCreateTableInterpreter {
    fn name(&self) -> &str {
        "ShowCreateTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;

        let table = catalog
            .get_table(&tenant, &self.plan.database, &self.plan.table)
            .await?;

        let settings = self.ctx.get_settings();

        let settings = ShowCreateQuerySettings {
            sql_dialect: settings.get_sql_dialect()?,
            quoted_ident_case_sensitive: settings.get_quoted_ident_case_sensitive()?,
            hide_options_in_show_create_table: settings
                .get_hide_options_in_show_create_table()
                .unwrap_or(false),
        };

        let tenant = self.ctx.get_tenant();
        let create_query = Self::show_create_query(
            &tenant,
            catalog.as_ref(),
            &self.plan.database,
            table.as_ref(),
            &settings,
        )
        .await?;

        let block = DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(table.name().to_string())),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(create_query)),
                ),
            ],
            1,
        );

        PipelineBuildResult::from_blocks(vec![block])
    }
}

impl ShowCreateTableInterpreter {
    pub async fn show_create_query(
        tenant: &Tenant,
        catalog: &dyn Catalog,
        database: &str,
        table: &dyn Table,
        settings: &ShowCreateQuerySettings,
    ) -> Result<String> {
        match table.engine() {
            STREAM_ENGINE => Self::show_create_stream_query(tenant, catalog, table).await,
            VIEW_ENGINE => Self::show_create_view_query(table, database),
            _ => match table.options().get(OPT_KEY_STORAGE_PREFIX) {
                Some(_) => Ok(Self::show_attach_table_query(table, database)),
                None => Self::show_create_table_query(table, settings),
            },
        }
    }

    fn show_create_table_query(
        table: &dyn Table,
        settings: &ShowCreateQuerySettings,
    ) -> Result<String> {
        let name = table.name();
        let engine = table.engine();
        let schema = table.schema();
        let field_comments = table.field_comments();
        let n_fields = schema.fields().len();
        let sql_dialect = settings.sql_dialect;
        let quoted_ident_case_sensitive = settings.quoted_ident_case_sensitive;
        let hide_options_in_show_create_table = settings.hide_options_in_show_create_table;

        let mut table_create_sql = format!(
            "CREATE TABLE {} (\n",
            format_name(name, quoted_ident_case_sensitive, sql_dialect)
        );
        if table.options().contains_key("TRANSIENT") {
            table_create_sql = format!(
                "CREATE TRANSIENT TABLE {} (\n",
                format_name(name, quoted_ident_case_sensitive, sql_dialect)
            )
        }

        let table_info = table.get_table_info();

        // Append columns and indexes.
        {
            let mut create_defs = vec![];
            for (idx, field) in schema.fields().iter().enumerate() {
                let nullable = if field.is_nullable() {
                    " NULL".to_string()
                } else {
                    " NOT NULL".to_string()
                };
                let default_expr = match field.default_expr() {
                    Some(expr) => {
                        format!(" DEFAULT {expr}")
                    }
                    None => "".to_string(),
                };
                let computed_expr = match field.computed_expr() {
                    Some(ComputedExpr::Virtual(expr)) => {
                        format!(" AS ({expr}) VIRTUAL")
                    }
                    Some(ComputedExpr::Stored(expr)) => {
                        format!(" AS ({expr}) STORED")
                    }
                    _ => "".to_string(),
                };
                // compatibility: creating table in the old planner will not have `fields_comments`
                let comment = if field_comments.len() == n_fields && !field_comments[idx].is_empty()
                {
                    // make the display more readable.
                    // can not use debug print, will add double quote
                    format!(
                        " COMMENT '{}'",
                        &field_comments[idx].as_str().replace('\'', "\\'")
                    )
                } else {
                    "".to_string()
                };
                let column_str = format!(
                    "  {} {}{}{}{}{}",
                    format_name(field.name(), quoted_ident_case_sensitive, sql_dialect),
                    field.data_type().remove_recursive_nullable().sql_name(),
                    nullable,
                    default_expr,
                    computed_expr,
                    comment
                );

                create_defs.push(column_str);
            }

            for index_field in table_info.meta.indexes.values() {
                let sync = if index_field.sync_creation {
                    "SYNC"
                } else {
                    "ASYNC"
                };
                let mut column_names = Vec::with_capacity(index_field.column_ids.len());
                for column_id in index_field.column_ids.iter() {
                    let field = schema.field_of_column_id(*column_id)?;
                    column_names.push(field.name().to_string());
                }
                let column_names_str = column_names.join(", ").to_string();
                let mut options = Vec::with_capacity(index_field.options.len());
                for (key, value) in index_field.options.iter() {
                    let option = format!("{} = '{}'", key, value);
                    options.push(option);
                }
                let mut index_str = format!(
                    "  {} INVERTED INDEX {} ({})",
                    sync,
                    format_name(&index_field.name, quoted_ident_case_sensitive, sql_dialect),
                    column_names_str
                );
                if !options.is_empty() {
                    let options_str = options.join(", ").to_string();
                    index_str.push(' ');
                    index_str.push_str(&options_str);
                }
                create_defs.push(index_str);
            }

            // Format is:
            //  (
            //      x,
            //      y
            //  )
            let create_defs_str = format!("{}\n", create_defs.join(",\n"));
            table_create_sql.push_str(&create_defs_str);
        }
        let table_engine = format!(") ENGINE={}", engine);
        table_create_sql.push_str(table_engine.as_str());

        if let Some((_, cluster_keys_str)) = table_info.meta.cluster_key() {
            table_create_sql.push_str(format!(" CLUSTER BY {}", cluster_keys_str).as_str());
        }

        if !hide_options_in_show_create_table || engine == "ICEBERG" || engine == "DELTA" {
            table_create_sql.push_str({
                let mut opts = table_info.options().iter().collect::<Vec<_>>();
                opts.sort_by_key(|(k, _)| *k);
                opts.iter()
                    .filter(|(k, _)| !is_internal_opt_key(k))
                    .map(|(k, v)| format!(" {}='{}'", k.to_uppercase(), v))
                    .collect::<Vec<_>>()
                    .join("")
                    .as_str()
            });
        }

        if engine != "ICEBERG" && engine != "DELTA" {
            if let Some(sp) = &table_info.meta.storage_params {
                table_create_sql.push_str(format!(" LOCATION = '{}'", sp).as_str());
            }
        }

        if !table_info.meta.comment.is_empty() {
            table_create_sql.push_str(format!(" COMMENT = '{}'", table_info.meta.comment).as_str());
        }
        Ok(table_create_sql)
    }

    fn show_create_view_query(table: &dyn Table, database: &str) -> Result<String> {
        let name = table.name();
        let view_create_sql = if let Some(query) = table.options().get(QUERY) {
            Ok(format!(
                "CREATE VIEW `{}`.`{}` AS {}",
                database, name, query
            ))
        } else {
            Err(ErrorCode::Internal(
                "Logical error, View Table must have a SelectQuery inside.",
            ))
        }?;
        Ok(view_create_sql)
    }

    async fn show_create_stream_query(
        tenant: &Tenant,
        catalog: &dyn Catalog,
        table: &dyn Table,
    ) -> Result<String> {
        let stream_table = StreamTable::try_from_table(table)?;
        let mut create_sql = format!(
            "CREATE STREAM `{}` ON TABLE `{}`.`{}`",
            stream_table.name(),
            stream_table.source_table_database(tenant, catalog).await?,
            stream_table.source_table_name(tenant, catalog).await?
        );

        let comment = stream_table.get_table_info().meta.comment.clone();
        if !comment.is_empty() {
            create_sql.push_str(format!(" COMMENT = '{}'", comment).as_str());
        }
        Ok(create_sql)
    }

    fn show_attach_table_query(table: &dyn Table, database: &str) -> String {
        // TODO table that attached before this PR, could not show location properly
        let location_not_available = "N/A".to_string();
        let table_data_location = table
            .options()
            .get(OPT_KEY_TABLE_ATTACHED_DATA_URI)
            .unwrap_or(&location_not_available);

        let mut ddl = format!(
            "ATTACH TABLE `{}`.`{}` {}",
            database,
            table.name(),
            table_data_location,
        );

        if table
            .options()
            .contains_key(OPT_KEY_TABLE_ATTACHED_READ_ONLY)
        {
            ddl.push_str(" READ_ONLY")
        }
        ddl
    }
}
