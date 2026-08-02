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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_expression::Expr;
use tokio::sync::watch;
use tokio::sync::watch::Receiver;
use tokio::sync::watch::Sender;

use crate::sbbf::Sbbf;

pub type RuntimeBloomFilter = Arc<Sbbf>;

#[derive(Clone, Default)]
pub struct RuntimeFilterInfo {
    pub filters: Vec<RuntimeFilterEntry>,
}

impl Debug for RuntimeFilterInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RuntimeFilterInfo {{ filters: [{}] }}",
            self.filters
                .iter()
                .map(|entry| format!("#{}(probe:{})", entry.id, entry.probe_expr.sql_display()))
                .collect::<Vec<String>>()
                .join(",")
        )
    }
}

impl RuntimeFilterInfo {
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    pub fn is_blooms_empty(&self) -> bool {
        self.filters.iter().all(|entry| entry.bloom.is_none())
    }
}

#[derive(Clone)]
pub struct RuntimeFilterEntry {
    pub id: usize,
    pub probe_expr: Expr<String>,
    pub bloom: Option<RuntimeFilterBloom>,
    pub spatial: Option<RuntimeFilterSpatial>,
    pub inlist: Option<Expr<String>>,
    pub inlist_value_count: usize,
    pub min_max: Option<Expr<String>>,
    pub stats: Arc<RuntimeFilterStats>,
    pub build_rows: usize,
    pub build_table_rows: Option<u64>,
    pub enabled: bool,
}

#[derive(Clone)]
pub struct RuntimeFilterBloom {
    pub column_name: String,
    pub filter: RuntimeBloomFilter,
}

#[derive(Clone)]
pub struct RuntimeFilterSpatial {
    pub column_name: String,
    pub srid: i32,
    pub rtrees: Arc<Vec<u8>>,
    pub rtree_bounds: Option<[f64; 4]>,
}

#[derive(Default)]
pub struct RuntimeFilterStats {
    bloom_time_ns: AtomicU64,
    bloom_rows_filtered: AtomicU64,
    bloom_rows_checked: AtomicU64,
    bloom_disabled: AtomicBool,
    inlist_min_max_time_ns: AtomicU64,
    min_max_rows_filtered: AtomicU64,
    min_max_partitions_pruned: AtomicU64,
    spatial_time_ns: AtomicU64,
    spatial_rows_filtered: AtomicU64,
    spatial_partitions_pruned: AtomicU64,
}

impl RuntimeFilterStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_bloom(&self, time_ns: u64, rows_filtered: u64, rows_checked: u64) {
        self.bloom_time_ns.fetch_add(time_ns, Ordering::Relaxed);
        self.bloom_rows_filtered
            .fetch_add(rows_filtered, Ordering::SeqCst);
        self.bloom_rows_checked
            .fetch_add(rows_checked, Ordering::SeqCst);
    }

    pub fn bloom_disabled(&self) -> bool {
        self.bloom_disabled.load(Ordering::SeqCst)
    }

    /// Stop paying the row-level bloom cost when a shared sample shows little reduction.
    ///
    /// Disabling a runtime filter is always safe: subsequent rows continue to the join instead of
    /// being discarded early. The decision is shared by all scan workers using this filter.
    pub fn disable_bloom_if_unselective(
        &self,
        sample_rows: u64,
        min_filtered_percent: u64,
    ) -> bool {
        if self.bloom_disabled() {
            return true;
        }

        let rows_checked = self.bloom_rows_checked.load(Ordering::SeqCst);
        if rows_checked < sample_rows {
            return false;
        }

        let rows_filtered = self.bloom_rows_filtered.load(Ordering::SeqCst);
        if u128::from(rows_filtered) * 100
            >= u128::from(rows_checked) * u128::from(min_filtered_percent)
        {
            return false;
        }

        self.bloom_disabled.store(true, Ordering::SeqCst);
        true
    }

    pub fn record_inlist_min_max(&self, time_ns: u64, rows_filtered: u64, partitions_pruned: u64) {
        self.inlist_min_max_time_ns
            .fetch_add(time_ns, Ordering::Relaxed);
        self.min_max_rows_filtered
            .fetch_add(rows_filtered, Ordering::Relaxed);
        self.min_max_partitions_pruned
            .fetch_add(partitions_pruned, Ordering::Relaxed);
    }

    pub fn record_spatial(&self, time_ns: u64, rows_filtered: u64, partitions_pruned: u64) {
        self.spatial_time_ns.fetch_add(time_ns, Ordering::Relaxed);
        self.spatial_rows_filtered
            .fetch_add(rows_filtered, Ordering::Relaxed);
        self.spatial_partitions_pruned
            .fetch_add(partitions_pruned, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> RuntimeFilterStatsSnapshot {
        RuntimeFilterStatsSnapshot {
            bloom_time_ns: self.bloom_time_ns.load(Ordering::Relaxed),
            bloom_rows_filtered: self.bloom_rows_filtered.load(Ordering::Relaxed),
            bloom_rows_checked: self.bloom_rows_checked.load(Ordering::SeqCst),
            bloom_disabled: self.bloom_disabled.load(Ordering::SeqCst),
            inlist_min_max_time_ns: self.inlist_min_max_time_ns.load(Ordering::Relaxed),
            min_max_rows_filtered: self.min_max_rows_filtered.load(Ordering::Relaxed),
            min_max_partitions_pruned: self.min_max_partitions_pruned.load(Ordering::Relaxed),
            spatial_time_ns: self.spatial_time_ns.load(Ordering::Relaxed),
            spatial_rows_filtered: self.spatial_rows_filtered.load(Ordering::Relaxed),
            spatial_partitions_pruned: self.spatial_partitions_pruned.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default, Clone, Debug)]
pub struct RuntimeFilterStatsSnapshot {
    pub bloom_time_ns: u64,
    pub bloom_rows_filtered: u64,
    pub bloom_rows_checked: u64,
    pub bloom_disabled: bool,
    pub inlist_min_max_time_ns: u64,
    pub min_max_rows_filtered: u64,
    pub min_max_partitions_pruned: u64,
    pub spatial_time_ns: u64,
    pub spatial_rows_filtered: u64,
    pub spatial_partitions_pruned: u64,
}

#[derive(Clone, Debug)]
pub struct RuntimeFilterReport {
    pub filter_id: usize,
    pub has_bloom: bool,
    pub has_inlist: bool,
    pub has_min_max: bool,
    pub stats: RuntimeFilterStatsSnapshot,
}

pub struct RuntimeFilterReady {
    pub runtime_filter_watcher: Sender<Option<()>>,
    /// A dummy receiver to make runtime_filter_watcher channel open.
    pub _runtime_filter_dummy_receiver: Receiver<Option<()>>,
}

impl Default for RuntimeFilterReady {
    fn default() -> Self {
        let (watcher, dummy_receiver) = watch::channel(None);
        Self {
            runtime_filter_watcher: watcher,
            _runtime_filter_dummy_receiver: dummy_receiver,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_filter_stays_enabled_until_sample_is_large_enough() {
        let stats = RuntimeFilterStats::new();
        stats.record_bloom(10, 0, 999);

        assert!(!stats.disable_bloom_if_unselective(1_000, 5));
        assert!(!stats.bloom_disabled());
    }

    #[test]
    fn bloom_filter_disables_only_for_low_reduction() {
        let unselective = RuntimeFilterStats::new();
        unselective.record_bloom(10, 49, 1_000);
        assert!(unselective.disable_bloom_if_unselective(1_000, 5));
        assert!(unselective.bloom_disabled());

        let selective = RuntimeFilterStats::new();
        selective.record_bloom(10, 50, 1_000);
        assert!(!selective.disable_bloom_if_unselective(1_000, 5));
        assert!(!selective.bloom_disabled());
    }
}
