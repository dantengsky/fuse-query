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

pub(super) fn should_enable_runtime_filter(
    desc: &RuntimeFilterDesc,
    build_num_rows: usize,
    selectivity_threshold: u64,
) -> bool {
    if build_num_rows == 0 {
        return false;
    }

    let has_table_statistics = desc.build_table_rows.is_some_and(|rows| rows != 0)
        || desc.probe_table_rows.is_some_and(|rows| rows != 0);
    if !has_table_statistics {
        log::info!(
            "RUNTIME-FILTER: Disable bloom runtime filter {} - no table statistics available",
            desc.id
        );
        return false;
    }

    let enabled =
        selectivity_below_threshold(build_num_rows, desc.build_table_rows, selectivity_threshold)
            || selectivity_below_threshold(
                build_num_rows,
                desc.probe_table_rows,
                selectivity_threshold,
            );
    if enabled {
        log::info!(
            "RUNTIME-FILTER: Enable bloom runtime filter {} - selective against build or probe table (threshold={}%, build_rows={}, build_table_rows={:?}, probe_table_rows={:?})",
            desc.id,
            selectivity_threshold,
            build_num_rows,
            desc.build_table_rows,
            desc.probe_table_rows,
        );
        true
    } else {
        log::info!(
            "RUNTIME-FILTER: Disable bloom runtime filter {} - unselective against build and probe tables (threshold={}%, build_rows={}, build_table_rows={:?}, probe_table_rows={:?})",
            desc.id,
            selectivity_threshold,
            build_num_rows,
            desc.build_table_rows,
            desc.probe_table_rows,
        );
        false
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
    fn selectivity_can_use_probe_table_size() {
        assert!(!selectivity_below_threshold(200_000, Some(200_000), 10));
        assert!(selectivity_below_threshold(200_000, Some(20_000_000), 10));
    }

    #[test]
    fn selectivity_rejects_missing_and_zero_statistics() {
        assert!(!selectivity_below_threshold(1, None, 10));
        assert!(!selectivity_below_threshold(1, Some(0), 10));
    }
}
