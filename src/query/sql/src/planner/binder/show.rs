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

use common_ast::ast::ShowLimit;
use common_ast::ast::ShowOptions;
use common_exception::Result;

use crate::plans::Plan;
use crate::plans::RewriteKind;
use crate::BindContext;
use crate::Binder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_functions(
        &mut self,
        bind_context: &mut BindContext,
        show_options: &Option<ShowOptions>,
    ) -> Result<Plan> {
        let (show_limit, limit_str) = get_show_options(show_options, None);
        // rewrite show functions to select * from system.functions ...
        let query = format!(
            "SELECT name, is_builtin, is_aggregate, definition, description FROM system.functions {} ORDER BY name {}",
            show_limit, limit_str,
        );
        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowFunctions)
            .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_table_functions(
        &mut self,
        bind_context: &mut BindContext,
        show_options: &Option<ShowOptions>,
    ) -> Result<Plan> {
        let (show_limit, limit_str) = get_show_options(show_options, None);
        // rewrite show functions to select * from system.table_functions ...
        let query = format!(
            "SELECT name FROM system.table_functions {} ORDER BY name {}",
            show_limit, limit_str,
        );
        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowFunctions)
            .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_settings(
        &mut self,
        bind_context: &mut BindContext,
        show_options: &Option<ShowOptions>,
    ) -> Result<Plan> {
        let (show_limit, limit_str) = get_show_options(show_options, None);
        let query = format!(
            "SELECT name, value, default, level, description, type FROM system.settings {} ORDER BY name {}",
            show_limit, limit_str,
        );

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowSettings)
            .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_metrics(
        &mut self,
        bind_context: &mut BindContext,
        show_options: &Option<ShowOptions>,
    ) -> Result<Plan> {
        let (show_limit, limit_str) = get_show_options(show_options, Some("metric".to_string()));
        let query = format!(
            "SELECT metric, kind, labels, value FROM system.metrics {} order by metric {}",
            show_limit, limit_str,
        );

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowMetrics)
            .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_process_list(
        &mut self,
        bind_context: &mut BindContext,
        show_options: &Option<ShowOptions>,
    ) -> Result<Plan> {
        let (show_limit, limit_str) =
            get_show_options(show_options, Some("extra_info".to_string()));
        let query = format!(
            "SELECT * FROM system.processes {} {}",
            show_limit, limit_str,
        );

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowProcessList)
            .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_engines(
        &mut self,
        bind_context: &mut BindContext,
        show_options: &Option<ShowOptions>,
    ) -> Result<Plan> {
        let (show_limit, limit_str) = get_show_options(show_options, Some("engine".to_string()));
        let query = format!(
            "SELECT \"Engine\", \"Comment\" FROM system.engines {} ORDER BY \"Engine\" ASC {}",
            show_limit, limit_str,
        );

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowEngines)
            .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_indexes(
        &mut self,
        bind_context: &mut BindContext,
        show_options: &Option<ShowOptions>,
    ) -> Result<Plan> {
        let (show_limit, limit_str) = get_show_options(show_options, None);
        let query = format!(
            "SELECT * FROM system.indexes {} order by name {}",
            show_limit, limit_str,
        );

        self.bind_rewrite_to_query(bind_context, &query, RewriteKind::ShowProcessList)
            .await
    }
}

fn get_show_options(show_options: &Option<ShowOptions>, col: Option<String>) -> (String, String) {
    let mut show_limit = String::new();
    let mut limit_str = String::new();

    if let Some(show_option) = show_options {
        match &show_option.show_limit {
            Some(ShowLimit::Like { pattern }) => {
                if let Some(col) = &col {
                    show_limit = format!("WHERE {} LIKE '{}'", col, pattern);
                } else {
                    show_limit = format!("WHERE name LIKE '{}'", pattern);
                }
            }
            Some(ShowLimit::Where { selection }) => {
                show_limit = format!("WHERE {}", selection);
            }
            None => {}
        }

        if let Some(limit) = show_option.limit {
            limit_str = format!("LIMIT {}", limit);
        }
    }

    (show_limit, limit_str)
}
