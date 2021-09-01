//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_planners::Extras;
use common_planners::Partitions;

use crate::datasources::fuse_table::types::table_snapshot::BlockMeta;
use crate::datasources::fuse_table::types::table_snapshot::ChunkLocation;
use crate::datasources::fuse_table::types::table_snapshot::ColStats;
use crate::datasources::fuse_table::types::table_snapshot::TableSnapshot;

pub fn filter(_table_snapshot: &TableSnapshot, _push_down: &Extras) -> Vec<BlockMeta> {
    todo!()
}

//pub fn filter_by_col_stats(_col_statistics: &ColStats, _push_down: &Extras) -> Vec<ChunkLocation> {
//    todo!()
//}
