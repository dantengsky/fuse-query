// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_base::base::GlobalInstance;
use common_config::InnerConfig;
use common_exception::Result;
use common_license::license_manager::LicenseManagerWrapper;

use crate::aggregating_index::RealAggregatingIndexHandler;
use crate::data_mask::RealDatamaskHandler;
use crate::license::RealLicenseManager;
use crate::storages::fuse::operations::RealVacuumHandler;
use crate::virtual_column::RealVirtualColumnHandler;

pub struct MockServices;
impl MockServices {
    #[async_backtrace::framed]
    pub async fn init(cfg: InnerConfig, public_key: String) -> Result<()> {
        let rm = RealLicenseManager::new(cfg.query.tenant_id.clone(), public_key);
        let wrapper = LicenseManagerWrapper {
            manager: Box::new(rm),
        };
        GlobalInstance::set(Arc::new(wrapper));
        RealVacuumHandler::init()?;
        RealAggregatingIndexHandler::init()?;
        RealDatamaskHandler::init()?;
        RealVirtualColumnHandler::init()?;
        Ok(())
    }
}
