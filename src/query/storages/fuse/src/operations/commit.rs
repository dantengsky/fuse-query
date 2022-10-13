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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use backoff::backoff::Backoff;
use backoff::ExponentialBackoffBuilder;
use common_base::base::tokio::sync::Mutex as AsyncMutex;
use common_base::base::ProgressValues;
use common_cache::Cache;
use common_catalog::table::Table;
use common_catalog::table::TableExt;
use common_catalog::table_context::TableContext;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::meta::ClusterKey;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::Statistics;
use common_fuse_meta::meta::TableSnapshot;
use common_fuse_meta::meta::Versioned;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableStatistics;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_types::MatchSeq;
use metrics::histogram;
use once_cell::sync::OnceCell;
use opendal::Operator;
use parking_lot::Mutex as StdSyncMutex;
use tracing::debug;
use tracing::info;
use tracing::warn;
use uuid::Uuid;

use crate::io::write_meta;
use crate::io::TableMetaLocationGenerator;
use crate::metrics::constants::METRIC_FUSE_COMMIT_DURATION_SECOND;
use crate::metrics::constants::METRIC_FUSE_COMMIT_UPDATE_META_DURATION_SECOND;
use crate::operations::AppendOperationLogEntry;
use crate::operations::TableOperationLog;
use crate::statistics;
use crate::FuseTable;
use crate::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use crate::OPT_KEY_SNAPSHOT_LOCATION;

const OCC_DEFAULT_BACKOFF_INIT_DELAY_MS: Duration = Duration::from_millis(5);
const OCC_DEFAULT_BACKOFF_MAX_DELAY_MS: Duration = Duration::from_millis(20 * 1000);
const OCC_DEFAULT_BACKOFF_MAX_ELAPSED_MS: Duration = Duration::from_millis(120 * 1000);

struct TableCommitLock(StdSyncMutex<HashMap<u64, Arc<AsyncMutex<()>>>>);

impl TableCommitLock {
    fn new() -> Self {
        Self {
            0: StdSyncMutex::new(HashMap::new()),
        }
    }
    fn table_mutex(&self, table_id: u64) -> Arc<AsyncMutex<()>> {
        let mut lock = self.0.lock();
        lock.entry(table_id)
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }
}

#[inline]
fn commit_lock() -> &'static TableCommitLock {
    static INSTANCE: OnceCell<TableCommitLock> = OnceCell::new();
    INSTANCE.get_or_init(|| TableCommitLock::new())
}

impl FuseTable {
    pub async fn do_commit(
        &self,
        ctx: Arc<dyn TableContext>,
        operation_log: TableOperationLog,
        overwrite: bool,
    ) -> Result<()> {
        let now = Instant::now();

        let mut tbl = self;
        let mut latest: Arc<dyn Table>;
        let mut retry_times = 0;
        let mut backoff = utils::new_backoff();

        let transient = self.transient();
        let table_id = self.table_info.ident.table_id;
        let table_commit_mutex = commit_lock().table_mutex(table_id);
        let _guard = table_commit_mutex.lock().await;

        // refresh table
        let refresh_timer = Instant::now();
        let latest_table_ref = tbl.refresh(ctx.as_ref()).await?;
        histogram!("fuse_commit_table_refresh", refresh_timer.elapsed());

        tbl = FuseTable::try_from_table(latest_table_ref.as_ref())?;

        loop {
            match tbl.try_commit(ctx.clone(), &operation_log, overwrite).await {
                Ok(_) => {
                    break {
                        if transient {
                            tbl.transient_clean_up(&ctx).await?;
                        }
                        histogram!(METRIC_FUSE_COMMIT_DURATION_SECOND, now.elapsed());
                        Ok(())
                    };
                }
                Err(e) if self::utils::is_error_recoverable(&e, transient) => match backoff
                    .next_backoff()
                {
                    Some(d) => {
                        let name = tbl.table_info.name.clone();
                        debug!(
                            "got error TableVersionMismatched, tx will be retried {} ms later. table name {}, identity {}",
                            d.as_millis(),
                            name.as_str(),
                            tbl.table_info.ident
                        );
                        common_base::base::tokio::time::sleep(d).await;
                        latest = tbl.refresh(ctx.as_ref()).await?;
                        tbl = FuseTable::try_from_table(latest.as_ref())?;
                        retry_times += 1;
                        continue;
                    }
                    None => {
                        info!("aborting operations");
                        let _ = utils::abort_operations(self.get_operator(), operation_log).await;
                        histogram!(METRIC_FUSE_COMMIT_DURATION_SECOND, now.elapsed());
                        break Err(ErrorCode::OCCRetryFailure(format!(
                            "can not fulfill the tx after retries({} times, {} ms), aborted. table name {}, identity {}",
                            retry_times,
                            Instant::now()
                                .duration_since(backoff.start_time)
                                .as_millis(),
                            tbl.table_info.name.as_str(),
                            tbl.table_info.ident,
                        )));
                    }
                },
                Err(e) => break Err(e),
            }
        }
    }

    #[inline]
    pub async fn try_commit(
        &self,
        ctx: Arc<dyn TableContext>,
        operation_log: &TableOperationLog,
        overwrite: bool,
    ) -> Result<()> {
        let prev = self.read_table_snapshot(ctx.clone()).await?;
        let prev_version = self.snapshot_format_version();
        let prev_timestamp = prev.as_ref().and_then(|v| v.timestamp);
        let schema = self.table_info.meta.schema.as_ref().clone();
        let (segments, summary) = Self::merge_append_operations(operation_log)?;

        let progress_values = ProgressValues {
            rows: summary.row_count as usize,
            bytes: summary.uncompressed_byte_size as usize,
        };
        ctx.get_write_progress().incr(&progress_values);

        let segments = segments
            .into_iter()
            .map(|loc| (loc, SegmentInfo::VERSION))
            .collect();

        let new_snapshot = if overwrite {
            TableSnapshot::new(
                Uuid::new_v4(),
                &prev_timestamp,
                prev.as_ref().map(|v| (v.snapshot_id, prev_version)),
                schema,
                summary,
                segments,
                self.cluster_key_meta.clone(),
            )
        } else {
            let merge_timer = Instant::now();
            let r = Self::merge_table_operations(
                self.table_info.meta.schema.as_ref(),
                prev,
                prev_version,
                segments,
                summary,
                self.cluster_key_meta.clone(),
            );
            histogram!("fuse_commit_merge_table", merge_timer.elapsed());
            r?
        };

        let mut new_table_meta = self.get_table_info().meta.clone();
        // update statistics
        new_table_meta.statistics = TableStatistics {
            number_of_rows: new_snapshot.summary.row_count,
            data_bytes: new_snapshot.summary.uncompressed_byte_size,
            compressed_data_bytes: new_snapshot.summary.compressed_byte_size,
            index_data_bytes: new_snapshot.summary.index_size,
        };

        FuseTable::commit_to_meta_server(
            ctx.as_ref(),
            &self.table_info,
            &self.meta_location_generator,
            new_snapshot,
            &self.operator,
        )
        .await
    }

    fn merge_table_operations(
        schema: &DataSchema,
        previous: Option<Arc<TableSnapshot>>,
        prev_version: u64,
        mut new_segments: Vec<Location>,
        statistics: Statistics,
        cluster_key_meta: Option<ClusterKey>,
    ) -> Result<TableSnapshot> {
        // 1. merge stats with previous snapshot, if any
        let stats = if let Some(snapshot) = &previous {
            let summary = &snapshot.summary;
            statistics::merge_statistics(&statistics, summary)?
        } else {
            statistics
        };
        let prev_snapshot_id = previous.as_ref().map(|v| (v.snapshot_id, prev_version));
        let prev_snapshot_timestamp = previous.as_ref().and_then(|v| v.timestamp);

        // 2. merge segment locations with previous snapshot, if any
        if let Some(snapshot) = &previous {
            let mut segments = snapshot.segments.clone();
            new_segments.append(&mut segments)
        };

        let new_snapshot = TableSnapshot::new(
            Uuid::new_v4(),
            &prev_snapshot_timestamp,
            prev_snapshot_id,
            schema.clone(),
            stats,
            new_segments,
            cluster_key_meta,
        );
        Ok(new_snapshot)
    }

    pub async fn commit_to_meta_server(
        ctx: &dyn TableContext,
        table_info: &TableInfo,
        location_generator: &TableMetaLocationGenerator,
        snapshot: TableSnapshot,
        operator: &Operator,
    ) -> Result<()> {
        let snapshot_location = location_generator
            .snapshot_location_from_uuid(&snapshot.snapshot_id, snapshot.format_version())?;

        // 1. write down snapshot
        write_meta(operator, &snapshot_location, &snapshot).await?;

        // 2. prepare table meta
        let mut new_table_meta = table_info.meta.clone();
        // 2.1 set new snapshot location
        new_table_meta.options.insert(
            OPT_KEY_SNAPSHOT_LOCATION.to_owned(),
            snapshot_location.clone(),
        );
        // remove legacy options
        self::utils::remove_legacy_options(&mut new_table_meta.options);

        // 2.2 setup table statistics
        let stats = &snapshot.summary;
        // update statistics
        new_table_meta.statistics = TableStatistics {
            number_of_rows: stats.row_count,
            data_bytes: stats.uncompressed_byte_size,
            compressed_data_bytes: stats.compressed_byte_size,
            index_data_bytes: stats.index_size,
        };

        // 3. prepare the request
        let catalog = ctx.get_catalog(&table_info.meta.catalog)?;
        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(table_version),
            new_table_meta,
        };

        // 3. let's roll
        let tenant = ctx.get_tenant();
        let db_name = ctx.get_current_database();
        let now = Instant::now();
        let reply = catalog.update_table_meta(&tenant, &db_name, req).await;
        histogram!(
            METRIC_FUSE_COMMIT_UPDATE_META_DURATION_SECOND,
            now.elapsed()
        );
        match reply {
            Ok(_) => {
                if let Some(snapshot_cache) = CacheManager::instance().get_table_snapshot_cache() {
                    let cache = &mut snapshot_cache.write();
                    cache.put(snapshot_location.clone(), Arc::new(snapshot));
                }
                // try keep a hit file of last snapshot
                Self::write_last_snapshot_hint(operator, location_generator, snapshot_location)
                    .await;
                Ok(())
            }
            Err(e) => {
                // commit snapshot to meta server failed, try to delete it.
                // "major GC" will collect this, if deletion failure (even after DAL retried)
                let _ = operator.object(&snapshot_location).delete().await;
                Err(e)
            }
        }
    }

    pub fn merge_append_operations(
        append_log_entries: &[AppendOperationLogEntry],
    ) -> Result<(Vec<String>, Statistics)> {
        let (s, seg_locs) = append_log_entries.iter().try_fold(
            (
                Statistics::default(),
                Vec::with_capacity(append_log_entries.len()),
            ),
            |(mut acc, mut seg_acc), log_entry| {
                let loc = &log_entry.segment_location;
                let stats = &log_entry.segment_info.summary;
                acc.row_count += stats.row_count;
                acc.block_count += stats.block_count;
                acc.uncompressed_byte_size += stats.uncompressed_byte_size;
                acc.compressed_byte_size += stats.compressed_byte_size;
                acc.index_size = stats.index_size;
                acc.col_stats = if acc.col_stats.is_empty() {
                    stats.col_stats.clone()
                } else {
                    statistics::reduce_column_statistics(&[&acc.col_stats, &stats.col_stats])?
                };
                seg_acc.push(loc.clone());
                Ok::<_, ErrorCode>((acc, seg_acc))
            },
        )?;
        // let blocks = append_log_entries
        // .iter()
        // .flat_map(|x| x.segment_info.blocks.iter());
        //
        // let chunks = blocks.into_iter().chunks(1000);
        // let mut acc = StatisticsAccumulator::new();
        //
        // chunks.into_iter().map(|v| {
        // let blocks = v.cloned().collect::<Vec<_>>();
        // acc.add_block(blocks);
        // });
        //

        // let segment_info = SegmentInfo::new(acc.blocks_metas, Statistics {
        //    row_count: acc.summary_row_count,
        //    block_count: acc.summary_block_count,
        //    uncompressed_byte_size: acc.in_memory_size,
        //    compressed_byte_size: acc.file_size,
        //    index_size: acc.index_size,
        //    col_stats,
        //});

        Ok((seg_locs, s))
    }

    // Left a hint file which indicates the location of the latest snapshot
    async fn write_last_snapshot_hint(
        operator: &Operator,
        location_generator: &TableMetaLocationGenerator,
        last_snapshot_path: String,
    ) {
        // Just try our best to write down the hint file of last snapshot
        // - will retry in the case of temporary failure
        // but
        // - errors are ignored if writing is eventually failed
        // - errors (if any) will not be propagated to caller
        // - "data race" ignored
        //   if multiple different versions of hints are written concurrently
        //   it is NOT guaranteed that the latest version will be kept

        let hint_path = location_generator.gen_last_snapshot_hint_location();
        let last_snapshot_path = {
            let operator_meta_data = operator.metadata();
            let storage_prefix = operator_meta_data.root();
            format!("{}{}", storage_prefix, last_snapshot_path)
        };

        operator
            .object(&hint_path)
            .write(last_snapshot_path)
            .await
            .unwrap_or_else(|e| {
                warn!("write last snapshot hint failure. {}", e);
            })
    }

    async fn transient_clean_up(&self, ctx: &Arc<dyn TableContext>) -> Result<()> {
        // Removes historical data, if table is transient
        warn!(
            "transient table detected, purging historical data. ({})",
            self.table_info.ident
        );

        let latest = self.refresh(ctx.as_ref()).await?;
        let local_tbl = FuseTable::try_from_table(latest.as_ref())?;

        let keep_last_snapshot = true;
        if let Err(e) = local_tbl.do_gc(ctx, keep_last_snapshot).await {
            // Errors of GC, if any, are ignored, since GC task can be picked up
            warn!(
                "GC of transient table not success (this is not a permanent error). the error : {}",
                e
            );
        } else {
            info!("GC of transient table done");
        }
        Ok(())
    }
}

mod utils {
    use std::collections::BTreeMap;

    use backoff::exponential::ExponentialBackoff;
    use backoff::SystemClock;

    use super::*;
    #[inline]
    pub async fn abort_operations(
        operator: Operator,
        operation_log: TableOperationLog,
    ) -> Result<()> {
        for entry in operation_log {
            for block in &entry.segment_info.blocks {
                let block_location = &block.location.0;
                // if deletion operation failed (after DAL retried)
                // we just left them there, and let the "major GC" collect them
                let _ = operator.object(block_location).delete().await;
            }
            let _ = operator.object(&entry.segment_location).delete().await;
        }
        Ok(())
    }

    #[inline]
    pub fn is_error_recoverable(e: &ErrorCode, is_table_transient: bool) -> bool {
        let code = e.code();
        code == ErrorCode::table_version_mismatched_code()
            || (is_table_transient && code == ErrorCode::storage_not_found_code())
    }

    // check if there are any fuse table legacy options
    pub fn remove_legacy_options(table_options: &mut BTreeMap<String, String>) {
        table_options.remove(OPT_KEY_LEGACY_SNAPSHOT_LOC);
    }

    pub fn new_backoff() -> ExponentialBackoff<SystemClock> {
        // The initial retry delay in millisecond. By default,  it is 5 ms.
        let init_delay = OCC_DEFAULT_BACKOFF_INIT_DELAY_MS;

        // The maximum  back off delay in millisecond, once the retry interval reaches this value, it stops increasing.
        // By default, it is 20 seconds.
        let max_delay = OCC_DEFAULT_BACKOFF_MAX_DELAY_MS;

        // The maximum elapsed time after the occ starts, beyond which there will be no more retries.
        // By default, it is 2 minutes
        let max_elapsed = OCC_DEFAULT_BACKOFF_MAX_ELAPSED_MS;

        // To simplify the settings, using fixed common values for randomization_factor and multiplier
        ExponentialBackoffBuilder::new()
            .with_initial_interval(init_delay)
            .with_max_interval(max_delay)
            .with_randomization_factor(0.5)
            .with_multiplier(1.5)
            .with_max_elapsed_time(Some(max_elapsed))
            .build()
    }
}
