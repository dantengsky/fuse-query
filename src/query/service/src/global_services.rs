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

use common_base::base::GlobalInstance;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::GlobalQueryRuntime;
use common_catalog::catalog::CatalogCreator;
use common_catalog::catalog::CatalogManager;
use common_cloud_control::cloud_api::CloudControlApiProvider;
use common_config::GlobalConfig;
use common_config::InnerConfig;
use common_exception::Result;
use common_meta_app::schema::CatalogType;
use common_profile::QueryProfileManager;
use common_sharing::ShareEndpointManager;
use common_storage::DataOperator;
use common_storage::ShareTableConfig;
use common_storages_hive::HiveCreator;
use common_storages_iceberg::IcebergCreator;
use common_tracing::GlobalLogger;
use common_users::RoleCacheManager;
use common_users::UserApiProvider;
use storages_common_cache_manager::CacheManager;
use storages_common_locks::LockManager;

use crate::api::DataExchangeManager;
use crate::auth::AuthMgr;
use crate::catalogs::DatabaseCatalog;
use crate::clusters::ClusterDiscovery;
use crate::servers::http::v1::HttpQueryManager;
use crate::sessions::SessionManager;

pub struct GlobalServices;

impl GlobalServices {
    #[async_backtrace::framed]
    pub async fn init(config: InnerConfig) -> Result<()> {
        GlobalInstance::init_production();
        GlobalServices::init_with(config).await
    }

    #[async_backtrace::framed]
    pub async fn init_with(config: InnerConfig) -> Result<()> {
        // app name format: node_id[0..7]@cluster_id
        let app_name_shuffle = format!(
            "databend-query-{}@{}",
            if config.query.node_id.len() >= 7 {
                &config.query.node_id[0..7]
            } else {
                &config.query.node_id
            },
            config.query.cluster_id
        );

        // The order of initialization is very important
        // 1. global config init.
        GlobalConfig::init(config.clone())?;

        // 2. log init.
        GlobalLogger::init(&app_name_shuffle, &config.log);

        // 3. runtime init.
        GlobalIORuntime::init(config.storage.num_cpus as usize)?;
        GlobalQueryRuntime::init(config.storage.num_cpus as usize)?;

        // 4. cluster discovery init.
        ClusterDiscovery::init(config.clone()).await?;

        // TODO(xuanwo):
        //
        // This part is a bit complex because catalog are used widely in different
        // crates that we don't have a good place for different kinds of creators.
        //
        // Maybe we can do some refactor to simplify the logic here.
        {
            // Init default catalog.

            let default_catalog = DatabaseCatalog::try_create_with_config(config.clone()).await?;

            let catalog_creator: Vec<(CatalogType, Arc<dyn CatalogCreator>)> = vec![
                (CatalogType::Iceberg, Arc::new(IcebergCreator)),
                (CatalogType::Hive, Arc::new(HiveCreator)),
            ];

            CatalogManager::init(&config, Arc::new(default_catalog), catalog_creator).await?;
        }

        HttpQueryManager::init(&config).await?;
        DataExchangeManager::init()?;
        SessionManager::init(&config)?;
        LockManager::init()?;
        AuthMgr::init(&config)?;
        UserApiProvider::init(
            config.meta.to_meta_grpc_client_conf(),
            config.query.idm,
            config.query.tenant_id.as_str(),
            config.query.tenant_quota,
        )
        .await?;
        RoleCacheManager::init()?;
        ShareEndpointManager::init()?;
        QueryProfileManager::init();

        DataOperator::init(&config.storage).await?;
        ShareTableConfig::init(
            &config.query.share_endpoint_address,
            &config.query.share_endpoint_auth_token_file,
            config.query.tenant_id.clone(),
        )?;
        CacheManager::init(&config.cache, &config.query.tenant_id)?;

        if let Some(addr) = config.query.cloud_control_grpc_server_address.clone() {
            CloudControlApiProvider::init(addr).await?;
        }

        Ok(())
    }
}
