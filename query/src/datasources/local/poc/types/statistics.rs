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

/*


//    ───────────────────────────────────────────────────────────────────────────────────
//
//                ┌─────────┐            chunk_info
//                └─────────┘         ┌───────────────┐
//                                    │---------------│
//                ┌─────────┐         │               │
//                └─────────┘         └───────────────┘
//
//                                                              table_info
//                                                           ┌─────────────────┐
//                                                           │                 │
//                                                           │                 │
//                                                           │                 │
//                                                           │                 │
//                                                           └─────────────────┘
//                ┌─────────┐
//                └─────────┘             chunk_info
//                                     ┌───────────────┐
//                ┌─────────┐          │               │
//                └─────────┘          │               │
//                                     └───────────────┘
//                ┌─────────┐
//                └─────────┘
//
//
//
//
//
//
*/
use std::collections::HashMap;

use uuid::Uuid;

pub type Bytes = Vec<u8>;
pub type SnapshotId = Uuid;
pub type ColumnId = i32;
pub type ChunkLocation = String;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct TableSnapshot {
    pub snapshot_id: SnapshotId,
    pub prev_snapshot_id: SnapshotId,
    pub summary: Summary,
    pub segment_info_location: String,
    pub segment_info_byte_size: u64,
}

/// A segment comprised of one or more blocks
pub struct SegmentMeta {
    pub chunks: Vec<BlockMeta>,
    pub summary: Summary,
}

pub struct Summary {
    pub row_count: u64,
    pub block_count: u64,
    pub uncompressed_byte_size: u64,
    pub compressed_byte_size: u64,
    pub col_stats: HashMap<ColumnId, ColStats>,
}

/// Meta information of a block
/// A block is the basic unit which stored in the object storage as an object
pub struct BlockMeta {
    pub location: String,
    pub file_byte_size: u64,
    pub compressed_size: u64,
    pub total_byte_size: u64,
    pub row_count: u64,
    pub col_stats: HashMap<ColumnId, ColStats>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ColStats {
    pub min: Bytes,
    pub max: Bytes,
    pub null_count: u64,
    pub distinct_count: Option<u64>,
    pub uncompressed_size: u64,
    pub compressed_size: u64,
}
