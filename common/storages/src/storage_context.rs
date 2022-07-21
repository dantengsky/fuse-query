// Copyright 2021 Datafuse Labs.
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

use common_datablocks::InMemoryData;
use common_meta_api::SchemaApi;
use parking_lot::RwLock;

/// Storage Context.
#[derive(Clone)]
pub struct StorageContext {
    pub meta: Arc<dyn SchemaApi>,
    // For shared data in memory.
    pub in_memory_data: Arc<RwLock<InMemoryData<u64>>>,
}
