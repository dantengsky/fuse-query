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

use std::any::Any;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::get_license_manager;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_storages_fuse::table_functions::parse_db_tb_args;
use databend_common_storages_fuse::table_functions::string_literal;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_vacuum_handler::get_vacuum_handler;
use databend_enterprise_vacuum_handler::VacuumHandlerWrapper;

use crate::sessions::TableContext;
use crate::table_functions::TableFunction;

const FUSE_AEEND_ENGINE_NAME: &str = "fuse_vacuum2";

pub struct FuseVacuum2 {
    table_info: TableInfo,
    arg_database_name: String,
    arg_table_name: String,
}

impl FuseVacuum2 {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let (arg_database_name, arg_table_name) =
            parse_db_tb_args(&table_args, FUSE_AEEND_ENGINE_NAME)?;

        let engine = FUSE_AEEND_ENGINE_NAME.to_owned();

        let schema =
            TableSchemaRefExt::create(vec![TableField::new("purged", TableDataType::String)]);
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema,
                engine,
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(FuseVacuum2 {
            table_info,
            arg_database_name,
            arg_table_name,
        }))
    }
}

#[async_trait::async_trait]
impl Table for FuseVacuum2 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(TableArgs::new_positioned(vec![
            string_literal(self.arg_database_name.as_str()),
            string_literal(self.arg_table_name.as_str()),
        ]))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        // let license_manager = get_license_manager();
        // license_manager
        //     .manager
        //     .check_enterprise_enabled(ctx.get_license_key(), Feature::AmendTable)?;

        pipeline.add_source(
            |output| {
                FuseVacuuTableV2Source::create(
                    ctx.clone(),
                    output,
                    self.arg_database_name.to_owned(),
                    self.arg_table_name.to_owned(),
                )
            },
            1,
        )?;

        Ok(())
    }
}

impl TableFunction for FuseVacuum2 {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

struct FuseVacuuTableV2Source {
    finish: bool,
    ctx: Arc<dyn TableContext>,
    arg_database_name: String,
    arg_table_name: String,
    vacuum_handler: Arc<VacuumHandlerWrapper>,
}

impl FuseVacuuTableV2Source {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        arg_database_name: String,
        arg_table_name: String,
    ) -> Result<ProcessorPtr> {
        let vacuum_handler = get_vacuum_handler();
        AsyncSourcer::create(ctx.clone(), output, FuseVacuuTableV2Source {
            ctx,
            finish: false,
            arg_table_name,
            arg_database_name,
            vacuum_handler: vacuum_handler,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for FuseVacuuTableV2Source {
    const NAME: &'static str = "fuse_vacuum2";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finish {
            return Ok(None);
        }

        self.finish = true;
        let tenant_id = self.ctx.get_tenant();
        let tbl = self
            .ctx
            .get_catalog(CATALOG_DEFAULT)
            .await?
            .get_table(
                &tenant_id,
                self.arg_database_name.as_str(),
                self.arg_table_name.as_str(),
            )
            .await?;

        let tbl = FuseTable::try_from_table(tbl.as_ref()).map_err(|_| {
            ErrorCode::StorageOther("Invalid table engine, only fuse table is supported")
        })?;

        self.vacuum_handler
            .do_vacuum2(tbl, self.ctx.clone(), false)
            .await?;

        let col: Vec<String> = vec!["Ok".to_owned()];

        Ok(Some(DataBlock::new_from_columns(vec![
            StringType::from_data(col),
        ])))
    }
}
