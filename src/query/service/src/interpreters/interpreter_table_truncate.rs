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
use common_config::GlobalConfig;
use common_exception::Result;
use common_sql::plans::TruncateTablePlan;

use crate::api::Packet;
use crate::api::TruncateTablePacket;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct TruncateTableInterpreter {
    ctx: Arc<QueryContext>,
    table_name: String,
    catalog_name: String,
    database_name: String,

    proxy_to_cluster: bool,
}

impl TruncateTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: TruncateTablePlan) -> Result<Self> {
        Ok(TruncateTableInterpreter {
            ctx,
            table_name: plan.table,
            catalog_name: plan.catalog,
            database_name: plan.database,
            proxy_to_cluster: true,
        })
    }

    pub fn from_flight(ctx: Arc<QueryContext>, packet: TruncateTablePacket) -> Result<Self> {
        Ok(TruncateTableInterpreter {
            ctx,
            table_name: packet.table_name,
            catalog_name: packet.catalog_name,
            database_name: packet.database_name,
            proxy_to_cluster: false,
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for TruncateTableInterpreter {
    fn name(&self) -> &str {
        "TruncateTableInterpreter"
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let table = self
            .ctx
            .get_table(&self.catalog_name, &self.database_name, &self.table_name)
            .await?;

        // check mutability
        table.check_mutable()?;

        if self.proxy_to_cluster && table.broadcast_truncate_to_cluster() {
            let settings = self.ctx.get_settings();
            let timeout = settings.get_flight_client_timeout()?;
            let conf = GlobalConfig::instance();
            let cluster = self.ctx.get_cluster();
            for node_info in &cluster.nodes {
                if node_info.id != cluster.local_id {
                    let truncate_packet = TruncateTablePacket::create(
                        node_info.clone(),
                        self.table_name.clone(),
                        self.catalog_name.clone(),
                        self.database_name.clone(),
                    );
                    truncate_packet.commit(conf.as_ref(), timeout).await?;
                }
            }
        }

        table.truncate(self.ctx.clone()).await?;
        Ok(PipelineBuildResult::create())
    }
}
