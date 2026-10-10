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

use databend_common_statistics::Datum;
use databend_storages_common_table_meta::meta::ColumnStatistics;

// #[derive(Debug, Clone)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
/// Basic statistics information of a column
pub struct BasicColumnStatistics {
    /// Min value of the column
    pub min: Option<Datum>,
    /// Max value of the column
    pub max: Option<Datum>,
    // Number of Distinct Value
    pub ndv: Option<u64>,
    // Count of null values
    pub null_count: u64,
    // Memory size of the column
    pub in_memory_size: u64,
}

impl From<ColumnStatistics> for BasicColumnStatistics {
    fn from(value: ColumnStatistics) -> Self {
        Self {
            min: value.min.to_datum(),
            max: value.max.to_datum(),
            ndv: value.distinct_of_values,
            null_count: value.null_count,
            in_memory_size: value.in_memory_size,
        }
    }
}

impl BasicColumnStatistics {
    pub fn new_null() -> Self {
        Self {
            min: None,
            max: None,
            ndv: None,
            null_count: 0,
            in_memory_size: 0,
        }
    }

    pub fn merge(&mut self, other: BasicColumnStatistics) {
        self.min = Datum::min(self.min.clone(), other.min);
        self.max = Datum::max(self.max.clone(), other.max);
        self.ndv = match (self.ndv, other.ndv) {
            (Some(x), Some(y)) => Some(x + y),
            (Some(x), None) | (None, Some(x)) => Some(x),
            _ => None,
        };
        self.null_count += other.null_count;
        self.in_memory_size += other.in_memory_size;
    }

    // The number of distinct values a column can hold within `[min, max]`.
    // `None` when the domain cannot be derived (long or empty strings).
    fn domain_size(mut min: Datum, mut max: Datum) -> Option<u64> {
        let range = match (&mut min, &mut max) {
            (Datum::Bytes(min), Datum::Bytes(max)) => {
                // There are 128 characters in ASCII code and 128^4 = 268435456 < 2^32 < 128^5.
                if min.is_empty() || max.is_empty() || min.len() > 4 || max.len() > 4 {
                    return None;
                }
                let mut min_value: u32 = 0;
                let mut max_value: u32 = 0;
                while min.len() != max.len() {
                    if min.len() < max.len() {
                        min.push(0);
                    } else {
                        max.push(0);
                    }
                }
                for idx in 0..min.len() {
                    min_value = min_value * 128 + min[idx] as u32;
                    max_value = max_value * 128 + max[idx] as u32;
                }
                (max_value - min_value) as u64
            }
            _ => {
                // Safe to unwrap: min and max are either both Datum::Bytes or neither
                let min = min.as_double().unwrap();
                let max = max.as_double().unwrap();
                (max - min) as u64
            }
        };
        Some(range.saturating_add(1))
    }

    // If the data type is int and max - min + 1 < ndv, then adjust ndv to max - min + 1.
    fn adjust_ndv_by_min_max(ndv: Option<u64>, min: Datum, max: Datum) -> Option<u64> {
        let Some(range) = Self::domain_size(min, max) else {
            return ndv;
        };
        let ndv = match ndv {
            Some(ndv) if range > ndv && ndv != 0 => ndv,
            _ => range,
        };
        Some(ndv)
    }

    // Get useful statistics: min, max and ndv are all `Some(_)`.
    pub fn get_useful_stat(&self, num_rows: u64, stats_row_count: u64) -> Option<Self> {
        if self.min.is_none() || self.max.is_none() {
            return None;
        }
        // min and max are either both Datum::Bytes or neither
        if self.min.as_ref().unwrap().is_bytes() ^ self.max.as_ref().unwrap().is_bytes() {
            return None;
        }
        let min = self.min.clone().unwrap();
        let max = self.max.clone().unwrap();
        let ndv = Self::adjust_ndv_by_min_max(self.ndv, min.clone(), max.clone());
        let ndv = match ndv {
            None => num_rows,
            Some(v) => Self::estimate_ndv(v, stats_row_count, num_rows),
        };
        // The sample-based extrapolation can overshoot badly when only a few
        // rows have been analyzed (e.g. a legacy table that received a handful
        // of writes after an upgrade). Whatever the sample says, the column
        // cannot hold more distinct values than its `[min, max]` domain.
        let ndv = match Self::domain_size(min, max) {
            Some(range) if range > 0 => ndv.min(range),
            _ => ndv,
        };
        Some(Self {
            min: self.min.clone(),
            max: self.max.clone(),
            ndv: Some(ndv),
            null_count: self.null_count,
            in_memory_size: self.in_memory_size,
        })
    }

    // Inspired by duckdb (https://github.com/duckdb/duckdb/blob/main/src/storage/statistics/distinct_statistics.cpp#L55-L69)
    //
    // `ndv` has already been bounded by the `[min, max]` domain, so it stays a
    // valid upper bound even when it did not come from a sample. Only scale it
    // up when a sample covering `stats_row_count < num_rows` rows exists.
    fn estimate_ndv(ndv: u64, stats_row_count: u64, num_rows: u64) -> u64 {
        if ndv == 0 {
            return num_rows;
        }

        // No sample was collected (the table was never analyzed), or the sample
        // already covers the whole table. The domain bound is the best estimate
        // available; falling back to `num_rows` would make an equality filter on
        // a low-cardinality column look like it matches a single row.
        if stats_row_count == 0 || stats_row_count >= num_rows {
            return ndv.min(num_rows);
        }

        let s = stats_row_count as f64;
        let n = num_rows as f64;
        let u = ndv.min(stats_row_count) as f64;

        let u1 = (u / s).powi(2) * u;
        // Good–Turing Estimation
        let estimate = u + u1 / s * (n - s);

        estimate.round().clamp(0.0, n) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::BasicColumnStatistics;
    use super::Datum;

    #[test]
    fn test_estimate_ndv() {
        assert_eq!(BasicColumnStatistics::estimate_ndv(0, 1, 3), 3);
        assert_eq!(BasicColumnStatistics::estimate_ndv(1, 1, 3), 3);
        assert_eq!(BasicColumnStatistics::estimate_ndv(12, 100, 3000), 17);
        assert_eq!(
            BasicColumnStatistics::estimate_ndv(6000, 10000, 1000000),
            219840
        );
        // Without a sample the domain-bounded ndv must be kept instead of
        // degrading to the row count.
        assert_eq!(BasicColumnStatistics::estimate_ndv(9, 0, 1_548_415_710), 9);
        assert_eq!(BasicColumnStatistics::estimate_ndv(0, 0, 100), 100);
        assert_eq!(BasicColumnStatistics::estimate_ndv(500, 0, 100), 100);
    }

    #[test]
    fn test_useful_stat_without_sample_keeps_domain_bounded_ndv() {
        // Mirrors `FuseTableColumnStatisticsProvider` for a table that was never
        // analyzed: the ndv defaults to the row count and `stats_row_count` is 0.
        // A status-like integer column in `[0, 8]` must not end up with an ndv
        // equal to the row count, which estimates `status = 7` as one row.
        let num_rows = 1_548_415_710;
        let stat = BasicColumnStatistics {
            min: Some(Datum::Int(0)),
            max: Some(Datum::Int(8)),
            ndv: Some(num_rows),
            null_count: 0,
            in_memory_size: 0,
        };
        let useful = stat.get_useful_stat(num_rows, 0).unwrap();
        assert_eq!(useful.ndv, Some(9));

        // A wide-domain column still falls back to the row count.
        let stat = BasicColumnStatistics {
            min: Some(Datum::Int(10_000_001)),
            max: Some(Datum::Int(1_278_189_672)),
            ndv: Some(num_rows),
            null_count: 0,
            in_memory_size: 0,
        };
        let useful = stat.get_useful_stat(num_rows, 0).unwrap();
        assert_eq!(useful.ndv, Some(1_268_189_672));

        // A tiny sample (a few rows written after the upgrade) must not let the
        // Good-Turing extrapolation exceed the domain either.
        let stat = BasicColumnStatistics {
            min: Some(Datum::Int(0)),
            max: Some(Datum::Int(8)),
            ndv: Some(3),
            null_count: 0,
            in_memory_size: 0,
        };
        assert_eq!(
            BasicColumnStatistics::estimate_ndv(3, 100, num_rows),
            41_810
        );
        let useful = stat.get_useful_stat(num_rows, 100).unwrap();
        assert_eq!(useful.ndv, Some(9));

        // A collected sample is still scaled with the Good-Turing estimator.
        let stat = BasicColumnStatistics {
            min: Some(Datum::Int(0)),
            max: Some(Datum::Int(1_000_000)),
            ndv: Some(6000),
            null_count: 0,
            in_memory_size: 0,
        };
        let useful = stat.get_useful_stat(1_000_000, 10_000).unwrap();
        assert_eq!(useful.ndv, Some(219_840));
    }
}
