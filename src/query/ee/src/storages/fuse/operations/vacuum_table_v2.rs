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
use chrono::Days;
use chrono::Duration;
use chrono::TimeZone;
use chrono::Utc;
use databend_common_base::base::uuid::Uuid;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::SetLVTReq;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::SnapshotLiteExtended;
use databend_common_storages_fuse::io::SnapshotsIO;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::FUSE_TBL_BLOCK_PREFIX;
use databend_common_storages_fuse::FUSE_TBL_SEGMENT_PREFIX;
use databend_common_storages_fuse::FUSE_TBL_SNAPSHOT_PREFIX;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta;
use databend_storages_common_table_meta::meta::uuid_from_data_time;
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
    dry_run: bool,
) -> Result<Option<Vec<String>>> {
    let start = Instant::now();

    // TODO set lvt

    let snapshot = fuse_table.read_table_snapshot().await?;

    let Some(snapshot) = snapshot else {
        // nothing to do
        return Ok(None);
    };

    if snapshot.snapshot_id.get_timestamp().is_none() {
        // not working for snapshot before v5
        return Err(ErrorCode::StorageOther("legacy snapshot is not supported"));
    }

    let retention_period_in_days = ctx.get_settings().get_data_retention_time_in_days()?;

    let self_ts = snapshot.timestamp.unwrap();

    let now = Utc::now();
    let lvt_point_candidate = now
        .checked_sub_days(Days::new(retention_period_in_days))
        .unwrap();

    let lvt_point = std::cmp::min(lvt_point_candidate, self_ts);

    let cat = ctx.get_default_catalog()?;
    let reply = cat
        .set_table_lvt(SetLVTReq {
            table_id: fuse_table.get_table_info().ident.table_id,
            time: lvt_point,
        })
        .await?;

    let meta_lvt = reply.time;

    eprintln!(" meta lvt is {}", meta_lvt);

    // safe to unwrap, all snapshots of v5 have a lvt
    let lvt = snapshot.least_visible_timestamp.unwrap();

    // navigate to timestamp

    let navigator = Navigator {
        ctx,
        location_gen: fuse_table.meta_location_generator().clone(),
        operator: fuse_table.get_operator(),
        dry_run: false,
    };

    // doc why navigate to meta lvt
    let anchor = navigator
        .navigate_to_snapshot_by_timestamp(meta_lvt)
        .await?;

    eprintln!("anchor sanpshot path {:?}", anchor);
    let Some(anchor) = anchor else {
        // other ones may have vacuumed this table, no anchor found
        return Ok(None);
    };

    let anchor_snapshot = navigator.load_snapshot_by_path(&anchor).await?;

    let Some((gc_root_id, gc_root_ver)) = anchor_snapshot.prev_snapshot_id else {
        info!("not previous?");
        eprintln!("not previous?");
        // we are at the first snapshot
        return Ok(None);
    };

    if gc_root_id.get_timestamp().is_none() {
        info!("non supported gc root snapshot version");
        eprintln!("non supported gc root snapshot version");
        // not support
        return Ok(None);
    }

    // **************
    // load the root snapshot, which may be the current snapshot

    let gc_root = navigator
        .load_snapshot_by_id(gc_root_id, gc_root_ver)
        .await?;

    let lvt = gc_root.least_visible_timestamp.unwrap();

    let operator = fuse_table.get_operator_ref();

    // delete all the snapshots that created before root as stream

    // note: why using the meta_lvt
    let prefix = format!(
        "{}/{}/",
        fuse_table.meta_location_generator().prefix(),
        FUSE_TBL_SNAPSHOT_PREFIX,
    );

    info!("vacuuming snapshots");
    eprintln!("vacuuming snapshots");
    let deleter = Deleter {
        operator: operator.clone(),
        list_prefix: prefix,
        root_set: HashSet::default(),
        lvt: meta_lvt,
        target_description: "snapshot".to_owned(),
    };
    deleter.cleanup().await?;

    // list segments, for segment that
    //
    // - not referenced by the GC root
    // - and
    //    - have no timestamp embedded in its object key
    //    - or have timestamp `ts_seg` embedded in its object key, where ts_seg < root.lvt
    // delete it

    let deleter = Deleter {
        operator: operator.clone(),
        list_prefix: FUSE_TBL_SEGMENT_PREFIX.to_owned(),
        root_set: HashSet::from_iter(gc_root.segments.iter().map(|(path, _v)| path.to_owned())),
        lvt: lvt.clone(),
        target_description: "segment".to_owned(),
    };
    deleter.cleanup().await?;

    // list blocks, for block that
    //
    // - not referenced by the GC root
    // - and
    //    - have no timestamp embedded in its object key
    //    - or have timestamp `ts_blk` embedded in its object key, where ts_blk < root.lvt
    // delete it

    let segment_reader = MetaReaders::segment_info_reader(
        operator.clone(),
        fuse_table.get_table_info().meta.schema.clone(),
    );

    let block_gc_root_set = {
        let mut root_set = HashSet::new();
        for x in &gc_root.segments {
            let params = LoadParams {
                location: x.0.clone(),
                len_hint: None,
                ver: x.1,
                put_cache: false,
            };
            let segment = segment_reader.read(&params).await?;

            let block_metas = segment.block_metas()?;
            for y in block_metas {
                root_set.insert(y.location.0.to_owned());
            }
        }
        root_set
    };

    let deleter = Deleter {
        operator: operator.clone(),
        list_prefix: FUSE_TBL_BLOCK_PREFIX.to_owned(),
        root_set: block_gc_root_set,
        lvt: lvt.clone(),
        target_description: "block".to_owned(),
    };
    deleter.cleanup().await?;

    // we are done

    Ok(None)
}

struct Deleter {
    operator: Operator,
    list_prefix: String,
    root_set: HashSet<String>,
    lvt: DateTime<Utc>,
    target_description: String,
}

impl Deleter {
    fn del(&self, path: &str) -> Result<()> {
        // TODO
        eprintln!("file to be deleted {}", path);
        Ok(())
    }

    fn is_v5_path(path: &str) -> bool {
        // TODO re-consider this, it is dangerous
        path.starts_with('g')
    }

    fn ts_from_path(path: &str) -> Result<DateTime<Utc>> {
        let without_g = &path[1..];
        // uuid in simple string form, has 32 characters
        let uuid_str = &without_g[..32];

        let uuid = Uuid::try_parse(uuid_str).map_err(|e| {
            ErrorCode::StorageOther(format!("Failed to parse as uuid {}. {}", uuid_str, e))
        })?;

        let timestamp = uuid
            .get_timestamp()
            .expect("no uuid other than v7 is expected");

        let (secs, nanos) = timestamp.to_unix();

        // TODO may panic
        let data_time_ts = Utc.timestamp(secs as i64, nanos);
        Ok(data_time_ts)
    }

    async fn cleanup(&self) -> Result<()> {
        eprintln!("{} prefix is {}", self.target_description, self.list_prefix);
        let mut snapshot_paths = self.operator.lister(&self.list_prefix).await?;
        while let Some(entry) = snapshot_paths.try_next().await? {
            let path = entry.path();
            if self.root_set.contains(path) {
                continue;
            }

            let trimmed_path = &path[self.list_prefix.len()..];

            eprintln!("trimmed path slice is {}", trimmed_path);

            if !Self::is_v5_path(trimmed_path) {
                info!("deleting {} {}", self.target_description, path);
                self.del(path)?
            } else {
                let ts = Self::ts_from_path(trimmed_path)?;
                if ts < self.lvt {
                    // TODO doc why
                    info!(
                        "deleting {} {}, which has lesser ts {}",
                        self.target_description, path, ts
                    );
                    self.del(path)?
                } else {
                    // TODO doc why
                    break;
                }
            }
        }
        Ok(())
    }
}

struct Navigator {
    ctx: Arc<dyn TableContext>,
    location_gen: TableMetaLocationGenerator,
    operator: Operator,
    dry_run: bool,
}

impl Navigator {
    async fn navigate_to_snapshot_by_timestamp(&self, ts: DateTime<Utc>) -> Result<Option<String>> {
        let prefix = format!("{}", self.location_gen.snapshot_prefix_from_timestamp(ts));
        // find the first one which has a larger or equal timestamp embedded in object key
        let mut lister = self.operator.lister(&prefix).await?;
        let next = lister.try_next().await?;
        let path = next.map(|v| v.path().to_owned());
        Ok(path)
    }

    async fn load_snapshot_by_path(&self, path: &str) -> Result<Arc<TableSnapshot>> {
        let reader = MetaReaders::table_snapshot_reader(self.operator.clone());
        let ver = TableMetaLocationGenerator::snapshot_version(path);
        let params = LoadParams {
            location: path.to_owned(),
            len_hint: None,
            ver,
            put_cache: false,
        };
        reader.read(&params).await
    }

    // TODO duplicated code
    async fn load_snapshot_by_id(
        &self,
        snapshot_id: SnapshotId,
        version: FormatVersion,
    ) -> Result<Arc<TableSnapshot>> {
        let snapshot_path = self
            .location_gen
            .snapshot_location_from_uuid(&snapshot_id, version)?;
        let reader = MetaReaders::table_snapshot_reader(self.operator.clone());
        let params = LoadParams {
            location: snapshot_path,
            len_hint: None,
            ver: version,
            put_cache: false,
        };
        reader.read(&params).await
    }
}
