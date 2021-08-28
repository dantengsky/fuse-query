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

use common_datavalues::DataSchema;

type TableId = u64;
type SnapshotId = u64;
struct PartitioningDef;

struct Statistics;
struct Location;

struct TableSnapshot {
    /// version of table spec
    spec_version: u32,
    /// unique id of the table
    table_id: TableId,
    /// unique id of this snapshot
    snapshot_id: SnapshotId,
    /// parent snapshot's id of this snapshot
    previous_snapshot_id: SnapshotId,
    /// current table schema
    schema: DataSchema,
    /// current partitioning definition
    partition_def: PartitioningDef,

    chunk_list: Vec<Location>,
}

struct ChunkListInfo {
    //    summary: TableStatistics,
    chunk_list: Vec<Location>,
}

struct ChunkMeta {
    table_id: TableId,
    partition_def: PartitioningDef,
    // summary_stats: TableStatistics,
}
