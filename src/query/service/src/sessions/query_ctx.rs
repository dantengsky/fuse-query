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
use std::cmp::min;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use chrono_tz::Tz;
use common_base::base::tokio::task::JoinHandle;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_base::runtime::TrySpawn;
use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Partitions;
use common_catalog::plan::StageTableInfo;
use common_catalog::query_kind::QueryKind;
use common_catalog::table_args::TableArgs;
use common_catalog::table_context::MaterializedCtesBlocks;
use common_catalog::table_context::StageAttachment;
use common_config::GlobalConfig;
use common_config::DATABEND_COMMIT_VERSION;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::date_helper::TzFactory;
use common_expression::DataBlock;
use common_expression::FunctionContext;
use common_io::prelude::FormatSettings;
use common_meta_app::principal::FileFormatParams;
use common_meta_app::principal::OnErrorMode;
use common_meta_app::principal::RoleInfo;
use common_meta_app::principal::StageFileFormatType;
use common_meta_app::principal::UserDefinedConnection;
use common_meta_app::principal::UserInfo;
use common_meta_app::schema::CatalogInfo;
use common_meta_app::schema::GetTableCopiedFileReq;
use common_meta_app::schema::TableInfo;
use common_metrics::storage::*;
use common_pipeline_core::processors::profile::Profile;
use common_pipeline_core::InputError;
use common_settings::ChangeValue;
use common_settings::Settings;
use common_sql::IndexType;
use common_storage::CopyStatus;
use common_storage::DataOperator;
use common_storage::FileStatus;
use common_storage::StageFileInfo;
use common_storage::StorageMetrics;
use common_storages_fuse::TableContext;
use common_storages_parquet::Parquet2Table;
use common_storages_parquet::ParquetRSTable;
use common_storages_result_cache::ResultScan;
use common_storages_stage::StageTable;
use common_users::GrantObjectVisibilityChecker;
use common_users::UserApiProvider;
use dashmap::mapref::multiple::RefMulti;
use dashmap::DashMap;
use log::debug;
use log::info;
use parking_lot::RwLock;
use storages_common_table_meta::meta::Location;

use crate::api::DataExchangeManager;
use crate::catalogs::Catalog;
use crate::clusters::Cluster;
use crate::pipelines::executor::PipelineExecutor;
use crate::sessions::query_affect::QueryAffect;
use crate::sessions::ProcessInfo;
use crate::sessions::QueryContextShared;
use crate::sessions::Session;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;
use crate::storages::Table;

const MYSQL_VERSION: &str = "8.0.26";
const CLICKHOUSE_VERSION: &str = "8.12.14";
const MAX_QUERY_COPIED_FILES_NUM: usize = 1000;

#[derive(Clone)]
pub struct QueryContext {
    version: String,
    mysql_version: String,
    clickhouse_version: String,
    partition_queue: Arc<RwLock<VecDeque<PartInfoPtr>>>,
    shared: Arc<QueryContextShared>,
    query_settings: Arc<Settings>,
    fragment_id: Arc<AtomicUsize>,
    // Used by synchronized generate aggregating indexes when new data written.
    inserted_segment_locs: Arc<RwLock<HashSet<Location>>>,
}

impl QueryContext {
    pub fn create_from(other: Arc<QueryContext>) -> Arc<QueryContext> {
        QueryContext::create_from_shared(other.shared.clone())
    }

    pub fn create_from_shared(shared: Arc<QueryContextShared>) -> Arc<QueryContext> {
        debug!("Create QueryContext");

        let tenant = GlobalConfig::instance().query.tenant_id.clone();
        let query_settings = Settings::create(tenant);
        Arc::new(QueryContext {
            partition_queue: Arc::new(RwLock::new(VecDeque::new())),
            version: format!("DatabendQuery {}", *DATABEND_COMMIT_VERSION),
            mysql_version: format!("{}-{}", MYSQL_VERSION, *DATABEND_COMMIT_VERSION),
            clickhouse_version: CLICKHOUSE_VERSION.to_string(),
            shared,
            query_settings,
            fragment_id: Arc::new(AtomicUsize::new(0)),
            inserted_segment_locs: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    /// Build fuse/system normal table by table info.
    ///
    /// TODO(xuanwo): we should support build table via table info in the future.
    pub fn build_table_by_table_info(
        &self,
        catalog_info: &CatalogInfo,
        table_info: &TableInfo,
        table_args: Option<TableArgs>,
    ) -> Result<Arc<dyn Table>> {
        let catalog = self.shared.catalog_manager.build_catalog(catalog_info)?;
        match table_args {
            None => {
                let table = catalog.get_table_by_info(table_info);
                if table.is_err() {
                    let table_function = catalog
                        .get_table_function(&table_info.name, TableArgs::new_positioned(vec![]));

                    if table_function.is_err() {
                        table
                    } else {
                        Ok(table_function?.as_table())
                    }
                } else {
                    table
                }
            }

            Some(table_args) => Ok(catalog
                .get_table_function(&table_info.name, table_args)?
                .as_table()),
        }
    }

    // Build external table by stage info, this is used in:
    // COPY INTO t1 FROM 's3://'
    // 's3://' here is a s3 external stage, and build it to the external table.
    fn build_external_by_table_info(
        &self,
        _catalog: &CatalogInfo,
        table_info: &StageTableInfo,
        _table_args: Option<TableArgs>,
    ) -> Result<Arc<dyn Table>> {
        StageTable::try_create(table_info.clone())
    }

    #[async_backtrace::framed]
    pub async fn set_current_database(&self, new_database_name: String) -> Result<()> {
        let tenant_id = self.get_tenant();
        let catalog = self
            .get_catalog(self.get_current_catalog().as_str())
            .await?;
        match catalog
            .get_database(tenant_id.as_str(), &new_database_name)
            .await
        {
            Ok(_) => self.shared.set_current_database(new_database_name),
            Err(_) => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Cannot USE '{}', because the '{}' doesn't exist",
                    new_database_name, new_database_name
                )));
            }
        };

        Ok(())
    }

    pub fn attach_table(&self, catalog: &str, database: &str, name: &str, table: Arc<dyn Table>) {
        self.shared.attach_table(catalog, database, name, table)
    }

    pub fn get_exchange_manager(&self) -> Arc<DataExchangeManager> {
        DataExchangeManager::instance()
    }

    // Get the current session.
    pub fn get_current_session(&self) -> Arc<Session> {
        self.shared.session.clone()
    }

    // Get one session by session id.
    pub fn get_session_by_id(self: &Arc<Self>, id: &str) -> Option<Arc<Session>> {
        SessionManager::instance().get_session_by_id(id)
    }

    // Get session id by mysql connection id.
    pub fn get_id_by_mysql_conn_id(self: &Arc<Self>, conn_id: &Option<u32>) -> Option<String> {
        SessionManager::instance().get_id_by_mysql_conn_id(conn_id)
    }

    // Get all the processes list info.
    pub fn get_processes_info(self: &Arc<Self>) -> Vec<ProcessInfo> {
        SessionManager::instance().processes_info()
    }

    /// Get the client socket address.
    pub fn get_client_address(&self) -> Option<SocketAddr> {
        self.shared.session.session_ctx.get_client_host()
    }

    pub fn get_affect(self: &Arc<Self>) -> Option<QueryAffect> {
        self.shared.get_affect()
    }

    pub fn get_data_metrics(&self) -> StorageMetrics {
        self.shared.get_data_metrics()
    }

    pub fn set_affect(self: &Arc<Self>, affect: QueryAffect) {
        self.shared.set_affect(affect)
    }

    pub fn set_id(&self, id: String) {
        *self.shared.init_query_id.write() = id;
    }

    pub fn set_executor(&self, weak_ptr: Arc<PipelineExecutor>) -> Result<()> {
        self.shared.set_executor(weak_ptr)
    }

    pub fn attach_stage(&self, attachment: StageAttachment) {
        self.shared.attach_stage(attachment);
    }

    pub fn set_ua(&self, ua: String) {
        *self.shared.user_agent.write() = ua;
    }

    pub fn get_ua(&self) -> String {
        let ua = self.shared.user_agent.read();
        ua.clone()
    }

    pub fn get_query_duration_ms(&self) -> i64 {
        let query_start_time = convert_query_log_timestamp(self.shared.created_time);
        let finish_time = *self.shared.finish_time.read();
        let finish_time = finish_time.unwrap_or_else(SystemTime::now);
        let finish_time = convert_query_log_timestamp(finish_time);
        (finish_time - query_start_time) / 1_000
    }

    pub fn get_created_time(&self) -> SystemTime {
        self.shared.created_time
    }

    pub fn set_finish_time(&self, time: SystemTime) {
        *self.shared.finish_time.write() = Some(time)
    }

    pub fn evict_table_from_cache(&self, catalog: &str, database: &str, table: &str) -> Result<()> {
        self.shared.evict_table_from_cache(catalog, database, table)
    }
}

#[async_trait::async_trait]
impl TableContext for QueryContext {
    fn as_any(&self) -> &dyn Any {
        self
    }
    /// Build a table instance the plan wants to operate on.
    ///
    /// A plan just contains raw information about a table or table function.
    /// This method builds a `dyn Table`, which provides table specific io methods the plan needs.
    fn build_table_from_source_plan(&self, plan: &DataSourcePlan) -> Result<Arc<dyn Table>> {
        match &plan.source_info {
            DataSourceInfo::TableSource(table_info) => self.build_table_by_table_info(
                &plan.catalog_info,
                table_info,
                plan.tbl_args.clone(),
            ),
            DataSourceInfo::StageSource(stage_info) => self.build_external_by_table_info(
                &plan.catalog_info,
                stage_info,
                plan.tbl_args.clone(),
            ),
            DataSourceInfo::Parquet2Source(table_info) => Parquet2Table::from_info(table_info),
            DataSourceInfo::ParquetSource(table_info) => ParquetRSTable::from_info(table_info),
            DataSourceInfo::ResultScanSource(table_info) => ResultScan::from_info(table_info),
        }
    }

    fn incr_total_scan_value(&self, value: ProgressValues) {
        self.shared.total_scan_values.as_ref().incr(&value);
    }

    fn get_total_scan_value(&self) -> ProgressValues {
        self.shared.total_scan_values.as_ref().get_values()
    }

    fn get_scan_progress(&self) -> Arc<Progress> {
        self.shared.scan_progress.clone()
    }

    fn get_scan_progress_value(&self) -> ProgressValues {
        self.shared.scan_progress.as_ref().get_values()
    }

    fn get_write_progress(&self) -> Arc<Progress> {
        self.shared.write_progress.clone()
    }

    fn get_join_spill_progress(&self) -> Arc<Progress> {
        self.shared.join_spill_progress.clone()
    }

    fn get_aggregate_spill_progress(&self) -> Arc<Progress> {
        self.shared.agg_spill_progress.clone()
    }

    fn get_group_by_spill_progress(&self) -> Arc<Progress> {
        self.shared.group_by_spill_progress.clone()
    }

    fn get_write_progress_value(&self) -> ProgressValues {
        self.shared.write_progress.as_ref().get_values()
    }

    fn get_join_spill_progress_value(&self) -> ProgressValues {
        self.shared.join_spill_progress.as_ref().get_values()
    }

    fn get_aggregate_spill_progress_value(&self) -> ProgressValues {
        self.shared.agg_spill_progress.as_ref().get_values()
    }

    fn get_group_by_spill_progress_value(&self) -> ProgressValues {
        self.shared.group_by_spill_progress.as_ref().get_values()
    }

    fn get_result_progress(&self) -> Arc<Progress> {
        self.shared.result_progress.clone()
    }

    fn get_result_progress_value(&self) -> ProgressValues {
        self.shared.result_progress.as_ref().get_values()
    }

    fn get_status_info(&self) -> String {
        let status = self.shared.status.read();
        status.clone()
    }

    fn set_status_info(&self, info: &str) {
        // set_status_info is not called frequently, so we can use info! here.
        // make it easier to match the status to the log.
        info!("{}: {}", self.get_id(), info);
        let mut status = self.shared.status.write();
        *status = info.to_string();
    }

    fn get_partition(&self) -> Option<PartInfoPtr> {
        self.partition_queue.write().pop_front()
    }

    fn get_partitions(&self, num: usize) -> Vec<PartInfoPtr> {
        let mut res = Vec::with_capacity(num);
        let mut partition_queue = self.partition_queue.write();

        for _index in 0..num {
            match partition_queue.pop_front() {
                None => {
                    break;
                }
                Some(part) => {
                    res.push(part);
                }
            };
        }

        res
    }

    // Update the context partition pool from the pipeline builder.
    fn set_partitions(&self, partitions: Partitions) -> Result<()> {
        let mut partition_queue = self.partition_queue.write();

        partition_queue.clear();
        for part in partitions.partitions {
            partition_queue.push_back(part);
        }
        Ok(())
    }

    fn partition_num(&self) -> usize {
        self.partition_queue.read().len()
    }

    fn add_partitions_sha(&self, s: String) {
        let mut shas = self.shared.partitions_shas.write();
        shas.push(s);
    }

    fn get_partitions_shas(&self) -> Vec<String> {
        let mut sha = self.shared.partitions_shas.read().clone();
        // Sort to make sure the SHAs are stable for the same query.
        sha.sort();
        sha
    }

    fn get_cacheable(&self) -> bool {
        self.shared.cacheable.load(Ordering::Acquire)
    }

    fn set_cacheable(&self, cacheable: bool) {
        self.shared.cacheable.store(cacheable, Ordering::Release);
    }

    fn get_can_scan_from_agg_index(&self) -> bool {
        self.shared.can_scan_from_agg_index.load(Ordering::Acquire)
    }

    fn set_can_scan_from_agg_index(&self, enable: bool) {
        self.shared
            .can_scan_from_agg_index
            .store(enable, Ordering::Release);
    }

    fn attach_query_str(&self, kind: QueryKind, query: String) {
        self.shared.attach_query_str(kind, query);
    }

    /// Get the session running query.
    fn get_query_str(&self) -> String {
        self.shared.get_query_str()
    }

    fn get_fragment_id(&self) -> usize {
        self.fragment_id.fetch_add(1, Ordering::Release)
    }

    #[async_backtrace::framed]
    async fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>> {
        self.shared
            .catalog_manager
            .get_catalog(&self.get_tenant(), catalog_name.as_ref())
            .await
    }

    fn get_default_catalog(&self) -> Result<Arc<dyn Catalog>> {
        self.shared.catalog_manager.get_default_catalog()
    }

    fn get_id(&self) -> String {
        self.shared.init_query_id.as_ref().read().clone()
    }

    fn get_current_catalog(&self) -> String {
        self.shared.get_current_catalog()
    }

    fn check_aborting(&self) -> Result<()> {
        self.shared.check_aborting()
    }

    fn get_error(&self) -> Option<ErrorCode> {
        self.shared.get_error()
    }

    fn get_current_database(&self) -> String {
        self.shared.get_current_database()
    }

    fn get_current_user(&self) -> Result<UserInfo> {
        self.shared.get_current_user()
    }

    fn get_current_role(&self) -> Option<RoleInfo> {
        self.shared.get_current_role()
    }
    async fn get_available_roles(&self) -> Result<Vec<RoleInfo>> {
        self.get_current_session().get_all_available_roles().await
    }

    fn get_current_session_id(&self) -> String {
        self.get_current_session().get_id()
    }

    async fn get_visibility_checker(&self) -> Result<GrantObjectVisibilityChecker> {
        self.shared.session.get_visibility_checker().await
    }

    fn get_fuse_version(&self) -> String {
        let session = self.get_current_session();
        match session.get_type() {
            SessionType::ClickHouseHttpHandler => self.clickhouse_version.clone(),
            SessionType::MySQL => self.mysql_version.clone(),
            _ => self.version.clone(),
        }
    }

    fn get_format_settings(&self) -> Result<FormatSettings> {
        let tz = self.query_settings.get_timezone()?;
        let timezone = tz.parse::<Tz>().map_err(|_| {
            ErrorCode::InvalidTimezone("Timezone has been checked and should be valid")
        })?;
        let format = FormatSettings { timezone };
        Ok(format)
    }

    fn get_tenant(&self) -> String {
        self.shared.get_tenant()
    }

    fn get_query_kind(&self) -> QueryKind {
        self.shared.get_query_kind()
    }

    fn get_function_context(&self) -> Result<FunctionContext> {
        let external_server_connect_timeout_secs = self
            .get_settings()
            .get_external_server_connect_timeout_secs()?;
        let external_server_request_timeout_secs = self
            .get_settings()
            .get_external_server_request_timeout_secs()?;

        let tz = self.get_settings().get_timezone()?;
        let tz = TzFactory::instance().get_by_name(&tz)?;
        let numeric_cast_option = self.get_settings().get_numeric_cast_option()?;
        let rounding_mode = numeric_cast_option.as_str() == "rounding";

        let query_config = &GlobalConfig::instance().query;

        Ok(FunctionContext {
            tz,
            rounding_mode,

            openai_api_key: query_config.openai_api_key.clone(),
            openai_api_version: query_config.openai_api_version.clone(),
            openai_api_chat_base_url: query_config.openai_api_chat_base_url.clone(),
            openai_api_embedding_base_url: query_config.openai_api_embedding_base_url.clone(),
            openai_api_embedding_model: query_config.openai_api_embedding_model.clone(),
            openai_api_completion_model: query_config.openai_api_completion_model.clone(),

            external_server_connect_timeout_secs,
            external_server_request_timeout_secs,
        })
    }

    fn get_connection_id(&self) -> String {
        self.shared.get_connection_id()
    }

    fn get_settings(&self) -> Arc<Settings> {
        if self.query_settings.get_changes().is_empty() {
            let session_change = self.shared.get_changed_settings();
            unsafe {
                self.query_settings.unchecked_apply_changes(session_change);
            }
        }
        self.query_settings.clone()
    }

    fn get_shared_settings(&self) -> Arc<Settings> {
        self.shared.get_settings()
    }

    fn get_cluster(&self) -> Arc<Cluster> {
        self.shared.get_cluster()
    }

    // Get all the processes list info.
    fn get_processes_info(&self) -> Vec<ProcessInfo> {
        SessionManager::instance().processes_info()
    }

    // Get Stage Attachment.
    fn get_stage_attachment(&self) -> Option<StageAttachment> {
        self.shared.get_stage_attachment()
    }

    fn get_last_query_id(&self, index: i32) -> String {
        self.shared.session.session_ctx.get_last_query_id(index)
    }

    fn get_query_id_history(&self) -> HashSet<String> {
        self.shared.session.session_ctx.get_query_id_history()
    }

    fn get_result_cache_key(&self, query_id: &str) -> Option<String> {
        self.shared
            .session
            .session_ctx
            .get_query_result_cache_key(query_id)
    }

    fn set_query_id_result_cache(&self, query_id: String, result_cache_key: String) {
        self.shared
            .session
            .session_ctx
            .update_query_ids_results(query_id, Some(result_cache_key))
    }

    fn get_on_error_map(&self) -> Option<Arc<DashMap<String, HashMap<u16, InputError>>>> {
        self.shared.get_on_error_map()
    }

    fn set_on_error_map(&self, map: Arc<DashMap<String, HashMap<u16, InputError>>>) {
        self.shared.set_on_error_map(map);
    }

    fn get_on_error_mode(&self) -> Option<OnErrorMode> {
        self.shared.get_on_error_mode()
    }
    fn set_on_error_mode(&self, mode: OnErrorMode) {
        self.shared.set_on_error_mode(mode)
    }

    fn get_maximum_error_per_file(&self) -> Option<HashMap<String, ErrorCode>> {
        if let Some(on_error_map) = self.get_on_error_map() {
            if on_error_map.is_empty() {
                return None;
            }
            let mut m = HashMap::<String, ErrorCode>::new();
            on_error_map
                .iter()
                .for_each(|x: RefMulti<String, HashMap<u16, InputError>>| {
                    if let Some(max_v) = x.value().iter().max_by_key(|entry| entry.1.num) {
                        m.insert(x.key().to_string(), max_v.1.err.clone());
                    }
                });
            return Some(m);
        }
        None
    }

    fn apply_changed_settings(&self, changes: HashMap<String, ChangeValue>) -> Result<()> {
        self.shared.apply_changed_settings(changes)
    }

    fn get_changed_settings(&self) -> HashMap<String, ChangeValue> {
        if self.query_settings.get_changes().is_empty() {
            let session_change = self.shared.get_changed_settings();
            unsafe {
                self.query_settings.unchecked_apply_changes(session_change);
            }
        }
        self.query_settings.get_changes()
    }

    // Get the storage data accessor operator from the session manager.
    fn get_data_operator(&self) -> Result<DataOperator> {
        Ok(self.shared.data_operator.clone())
    }

    #[async_backtrace::framed]
    async fn get_file_format(&self, name: &str) -> Result<FileFormatParams> {
        match StageFileFormatType::from_str(name) {
            Ok(typ) => FileFormatParams::default_by_type(typ),
            Err(_) => {
                let user_mgr = UserApiProvider::instance();
                let tenant = self.get_tenant();
                Ok(user_mgr
                    .get_file_format(&tenant, name)
                    .await?
                    .file_format_params)
            }
        }
    }
    async fn get_connection(&self, name: &str) -> Result<UserDefinedConnection> {
        let user_mgr = UserApiProvider::instance();
        let tenant = self.get_tenant();
        user_mgr.get_connection(&tenant, name).await
    }

    /// Fetch a Table by db and table name.
    ///
    /// It guaranteed to return a consistent result for multiple calls, in a same query.
    /// E.g.:
    /// ```sql
    /// SELECT * FROM (SELECT * FROM db.table_name) as subquery_1, (SELECT * FROM db.table_name) AS subquery_2
    /// ```
    #[async_backtrace::framed]
    async fn get_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Arc<dyn Table>> {
        self.shared.get_table(catalog, database, table).await
    }

    #[async_backtrace::framed]
    async fn filter_out_copied_files(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        files: &[StageFileInfo],
        max_files: Option<usize>,
    ) -> Result<Vec<StageFileInfo>> {
        let tenant = self.get_tenant();
        let catalog = self.get_catalog(catalog_name).await?;
        let table = catalog
            .get_table(&tenant, database_name, table_name)
            .await?;
        let table_id = table.get_id();

        let mut limit: usize = 0;
        let max_files = max_files.unwrap_or(usize::MAX);
        let batch_size = min(MAX_QUERY_COPIED_FILES_NUM, max_files);

        let mut results = Vec::with_capacity(files.len());

        for chunk in files.chunks(batch_size) {
            let files = chunk.iter().map(|v| v.path.clone()).collect::<Vec<_>>();
            let req = GetTableCopiedFileReq { table_id, files };
            let start_request = Instant::now();
            let copied_files = catalog
                .get_table_copied_file_info(&tenant, database_name, req)
                .await?
                .file_info;

            metrics_inc_copy_filter_out_copied_files_request_milliseconds(
                Instant::now().duration_since(start_request).as_millis() as u64,
            );
            // Colored
            for file in chunk {
                if !copied_files.contains_key(&file.path) {
                    results.push(file.clone());
                    limit += 1;
                    if limit == max_files {
                        return Ok(results);
                    }
                }
            }
        }
        Ok(results)
    }

    fn set_materialized_cte(
        &self,
        idx: (IndexType, IndexType),
        blocks: Arc<RwLock<Vec<DataBlock>>>,
    ) -> Result<()> {
        let mut ctes = self.shared.materialized_cte_tables.write();
        ctes.insert(idx, blocks);
        Ok(())
    }

    fn get_materialized_cte(
        &self,
        idx: (IndexType, IndexType),
    ) -> Result<Option<Arc<RwLock<Vec<DataBlock>>>>> {
        let ctes = self.shared.materialized_cte_tables.read();
        Ok(ctes.get(&idx).cloned())
    }

    fn get_materialized_ctes(&self) -> MaterializedCtesBlocks {
        self.shared.materialized_cte_tables.clone()
    }

    fn add_segment_location(&self, segment_loc: Location) -> Result<()> {
        let mut segment_locations = self.inserted_segment_locs.write();
        segment_locations.insert(segment_loc);
        Ok(())
    }

    fn get_segment_locations(&self) -> Result<Vec<Location>> {
        Ok(self
            .inserted_segment_locs
            .read()
            .iter()
            .cloned()
            .collect::<Vec<_>>())
    }

    fn add_file_status(&self, file_path: &str, file_status: FileStatus) -> Result<()> {
        if matches!(self.get_query_kind(), QueryKind::CopyIntoTable) {
            self.shared.copy_status.add_chunk(file_path, file_status);
        }
        Ok(())
    }

    fn get_copy_status(&self) -> Arc<CopyStatus> {
        self.shared.copy_status.clone()
    }

    fn get_license_key(&self) -> String {
        self.get_settings()
            .get_enterprise_license()
            .unwrap_or_default()
    }

    fn get_queries_profile(&self) -> HashMap<String, Vec<Arc<Profile>>> {
        let mut queries_profile = SessionManager::instance().get_queries_profile();

        let exchange_profiles = DataExchangeManager::instance().get_queries_profile();

        for (query_id, profiles) in exchange_profiles {
            match queries_profile.entry(query_id) {
                Entry::Vacant(v) => {
                    v.insert(profiles);
                }
                Entry::Occupied(mut v) => {
                    v.get_mut().extend(profiles);
                }
            }
        }

        queries_profile
    }
}

impl TrySpawn for QueryContext {
    /// Spawns a new asynchronous task, returning a tokio::JoinHandle for it.
    /// The task will run in the current context thread_pool not the global.
    fn try_spawn<T>(&self, name: impl Into<String>, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        Ok(self.shared.try_get_runtime()?.spawn(name, task))
    }
}

impl std::fmt::Debug for QueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.get_current_user())
    }
}

pub fn convert_query_log_timestamp(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::new(0, 0))
        .as_micros() as i64
}
