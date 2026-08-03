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

use crate::pipelines::processors::transforms::hash_join::desc::RuntimeFilterDesc;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct RuntimeFilterDecision {
    pub enabled: bool,
    pub adaptive: bool,
}

pub(super) fn should_enable_runtime_filter(
    desc: &RuntimeFilterDesc,
    build_num_rows: usize,
    selectivity_threshold: u64,
) -> RuntimeFilterDecision {
    if build_num_rows == 0 {
        return RuntimeFilterDecision::default();
    }

    let decision = runtime_filter_decision(
        build_num_rows,
        desc.build_table_rows,
        desc.probe_table_rows,
        selectivity_threshold,
    );
    if decision.enabled && !decision.adaptive {
        log::info!(
            "RUNTIME-FILTER: Enable bloom runtime filter {} - selective against build table (threshold={}%, build_rows={}, build_table_rows={:?})",
            desc.id,
            selectivity_threshold,
            build_num_rows,
            desc.build_table_rows,
        );
    } else if decision.enabled {
        log::info!(
            "RUNTIME-FILTER: Enable adaptive bloom runtime filter {} - selective against probe table (threshold={}%, build_rows={}, build_table_rows={:?}, probe_table_rows={:?})",
            desc.id,
            selectivity_threshold,
            build_num_rows,
            desc.build_table_rows,
            desc.probe_table_rows,
        );
    } else if desc.build_table_rows.is_none() && desc.probe_table_rows.is_none() {
        log::info!(
            "RUNTIME-FILTER: Disable bloom runtime filter {} - no table statistics available",
            desc.id
        );
    } else {
        log::info!(
            "RUNTIME-FILTER: Disable bloom runtime filter {} - unselective against build and probe tables (threshold={}%, build_rows={}, build_table_rows={:?}, probe_table_rows={:?})",
            desc.id,
            selectivity_threshold,
            build_num_rows,
            desc.build_table_rows,
            desc.probe_table_rows,
        );
    }

    decision
}

fn runtime_filter_decision(
    build_num_rows: usize,
    build_table_rows: Option<u64>,
    probe_table_rows: Option<u64>,
    threshold_percent: u64,
) -> RuntimeFilterDecision {
    if build_num_rows == 0 {
        return RuntimeFilterDecision::default();
    }

    if selectivity_below_threshold(build_num_rows, build_table_rows, threshold_percent) {
        RuntimeFilterDecision {
            enabled: true,
            adaptive: false,
        }
    } else if selectivity_below_threshold(build_num_rows, probe_table_rows, threshold_percent) {
        RuntimeFilterDecision {
            enabled: true,
            adaptive: true,
        }
    } else {
        RuntimeFilterDecision::default()
    }
}

fn selectivity_below_threshold(
    build_num_rows: usize,
    table_rows: Option<u64>,
    threshold_percent: u64,
) -> bool {
    table_rows.is_some_and(|table_rows| {
        table_rows != 0
            && (build_num_rows as u128) * 100
                < u128::from(table_rows) * u128::from(threshold_percent)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_table_selectivity_is_adaptive() {
        assert_eq!(
            runtime_filter_decision(200_000, Some(200_000), Some(20_000_000), 10),
            RuntimeFilterDecision {
                enabled: true,
                adaptive: true,
            }
        );
    }

    #[test]
    fn missing_probe_statistics_preserves_original_decision() {
        assert_eq!(
            runtime_filter_decision(200_000, Some(200_000), None, 10),
            RuntimeFilterDecision::default()
        );
    }

    #[test]
    fn build_table_selectivity_is_not_adaptive() {
        assert_eq!(
            runtime_filter_decision(10_000, Some(200_000), Some(20_000_000), 10),
            RuntimeFilterDecision {
                enabled: true,
                adaptive: false,
            }
        );
    }

    #[test]
    fn selectivity_rejects_missing_and_zero_statistics() {
        assert!(!selectivity_below_threshold(1, None, 10));
        assert!(!selectivity_below_threshold(1, Some(0), 10));
    }
}
