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

#![allow(clippy::uninlined_format_args)]
#![feature(type_alias_impl_trait)]
#![feature(iter_order_by)]
#![feature(let_chains)]
#![feature(impl_trait_in_assoc_type)]
#![feature(int_roundings)]
#![feature(result_option_inspect)]
#![recursion_limit = "256"]

mod constants;
mod fuse_column;
mod fuse_part;
mod fuse_table;
mod fuse_type;

pub mod io;
pub mod operations;
pub mod pruning;
pub mod statistics;
pub mod table_functions;

use common_catalog::table::NavigationPoint;
use common_catalog::table::Table;
use common_catalog::table::TableStatistics;
pub use common_catalog::table_context::TableContext;
pub use constants::*;
pub use fuse_column::FuseTableColumnStatisticsProvider;
pub use fuse_part::FuseLazyPartInfo;
pub use fuse_part::FusePartInfo;
pub use fuse_table::FuseTable;
pub use fuse_type::FuseStorageFormat;
pub use fuse_type::FuseTableType;
pub use io::MergeIOReadResult;
pub use pruning::SegmentLocation;

mod sessions {
    pub use common_catalog::table_context::TableContext;
}
pub use storages_common_index as index;
