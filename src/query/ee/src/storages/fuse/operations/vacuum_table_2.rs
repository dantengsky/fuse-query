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

use std::collections::HashSet;
use std::panic::Location;
use std::sync::Arc;
use std::time::Instant;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::SnapshotLiteExtended;
use databend_common_storages_fuse::io::SnapshotsIO;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::FormatVersion;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures_util::TryStreamExt;
use log::info;
use opendal::Operator;

use crate::storages::fuse::get_snapshot_referenced_segments;

#[async_backtrace::framed]
pub async fn do_vacuum2(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    retention_time: DateTime<Utc>,
    dry_run: bool,
) -> Result<Option<Vec<String>>> {
    let start = Instant::now();

    // TODO get table lvt

    let snapshot = fuse_table.read_table_snapshot().await?;

    let Some(snapshot) = snapshot else {
        // nothing to do
        return Ok(None);
    };

    if snapshot.snapshot_id.get_timestamp().is_none() {
        // not working for snapshot before v5
        return Ok(None);
    }

    // safe to unwrap, all snapshots of v5 have a lvt
    let lvt = snapshot.least_visible_timestamp.unwrap();

    // navigate to timestamp

    let anchor = navigate_to(lvt).await?;

    let Some(anchor) = anchor else {
        // other ones may have vacuumed this table, no anchor found
        return Ok(None);
    };

    let anchor_snapshot = load_snapshot(&anchor).await?;

    let Some((gc_root_id, gc_root_ver)) = anchor_snapshot.prev_snapshot_id else {
        // we are at the first snapshot
        return Ok(None);
    };

    if gc_root_id.get_timestamp().is_some() {
        // not support
        return Ok(None);
    }

    // **************
    // load the root snapshot, which may be the current snapshot

    let gc_root = load_snapshot_by_location(gc_root_id, gc_root_ver).await?;

    let lvt = gc_root.least_visible_timestamp.unwrap();

    let operator = fuse_table.get_operator_ref();

    let deleter = Deleter::new(operator.clone());

    // delete all the snapshots that created before root as stream

    {
        let mut snapshot_paths = operator.lister("ss_/").await?;

        while let Some(entry) = snapshot_paths.try_next().await? {
            let path = entry.path();
            if !is_v5_path(path) {
                info!("deleting snapshot {}", path);
                deleter.del(path)
            } else {
                let ts = ts_from_path(path);
                if ts < lvt {
                    info!("deleting snapshot {}, which has lesser ts {}", path, ts);
                    deleter.del(path)
                } else {
                    break;
                }
            }
        }
    }

    // list segments, for segment that
    //
    // - not referenced by the GC root
    // - and
    //    - have no timestamp embedded in its object key
    //    - or have timestamp `ts_seg` embedded in its object key, where ts_seg < root.lvt
    // delete it

    {
        let mut snapshot_paths = operator.lister("_sg/").await?;

        while let Some(entry) = snapshot_paths.try_next().await? {
            let path = entry.path();

            if referenced(path) {
                continue;
            }

            if !is_v5_path(path) {
                info!("deleting segment {}", path);
                deleter.del(path)
            } else {
                let ts = ts_from_path(path);
                if ts < lvt {
                    info!("deleting segment {}, which has lesser ts {}", path, ts);
                    deleter.del(path)
                } else {
                    break;
                }
            }
        }
    }

    // list blocks, for block that
    //
    // - not referenced by the GC root
    // - and
    //    - have no timestamp embedded in its object key
    //    - or have timestamp `ts_blk` embedded in its object key, where ts_blk < root.lvt
    // delete it

    // we are done

    Ok(None)
}

fn is_v5_path(path: &str) -> bool {
    todo!()
}

async fn navigate_to(time: DateTime<Utc>) -> Result<Option<String>> {
    todo!()
}

async fn load_snapshot(path: &str) -> Result<Arc<TableSnapshot>> {
    todo!()
}

async fn load_snapshot_by_location(
    snapshot_id: SnapshotId,
    version: FormatVersion,
) -> Result<Arc<TableSnapshot>> {
    todo!()
}

fn to_prefix(ts: DateTime<Utc>) -> String {
    todo!()
}

fn ts_from_path(path: &str) -> DateTime<Utc> {
    todo!()
}

fn referenced(path: &str) -> bool {
    todo!()
}

struct Deleter {}

impl Deleter {
    fn new(operator: Operator) -> Self {
        todo!()
    }
    fn del(&self, path: impl Into<String>) {}
}
