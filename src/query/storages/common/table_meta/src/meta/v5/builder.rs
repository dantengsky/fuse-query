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

use chrono::DateTime;
use chrono::Days;
use chrono::Utc;
use databend_common_base::base::uuid::Uuid;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;

use super::snapshot::TableSnapshot;
use crate::meta::monotonically_increased_timestamp;
use crate::meta::ClusterKey;
use crate::meta::Location;
use crate::meta::Statistics;
use crate::meta::Versioned;

#[derive(Default, Debug)]
pub struct TableSnapshotBuilder {
    snapshot_template: TableSnapshot,

    // TODO doc these
    base_snapshot_timestamp: Option<DateTime<Utc>>,
    retention_period_in_days: u64,
    prev_table_seq: Option<u64>,
}

impl TableSnapshotBuilder {
    pub fn new(retention_period_in_days: u64) -> Self {
        Self {
            retention_period_in_days,
            ..Default::default()
        }
    }

    pub fn set_prev_table_seq(mut self, prev_table_seq: u64) -> Self {
        self.prev_table_seq = Some(prev_table_seq);
        self
    }

    pub fn set_prev_table_seq_opt(mut self, prev_table_seq: Option<u64>) -> Self {
        self.prev_table_seq = prev_table_seq;
        self
    }
    pub fn set_previous_snapshot(mut self, prev: &TableSnapshot) -> Self {
        self.snapshot_template = prev.clone();
        self
    }

    pub fn set_previous_snapshot_opt<T: AsRef<TableSnapshot>>(mut self, prev: Option<T>) -> Self {
        if let Some(s) = prev {
            self.snapshot_template = s.as_ref().clone();
        }
        self
    }

    pub fn set_schema(mut self, schema: TableSchema) -> Self {
        self.snapshot_template.schema = schema;
        self
    }

    pub fn set_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.snapshot_template.timestamp = Some(timestamp);
        self
    }

    pub fn set_summary(mut self, summary: Statistics) -> Self {
        self.snapshot_template.summary = summary;
        self
    }

    pub fn set_segments(mut self, segment_locations: Vec<Location>) -> Self {
        self.snapshot_template.segments = segment_locations;
        self
    }

    pub fn set_cluster_key_meta(mut self, cluster_key_meta: Option<ClusterKey>) -> Self {
        self.snapshot_template.cluster_key_meta = cluster_key_meta;
        self
    }

    pub fn set_table_statistics_location(
        mut self,
        table_statistics_location: Option<String>,
    ) -> Self {
        self.snapshot_template.table_statistics_location = table_statistics_location;
        self
    }

    pub fn set_base_snapshot_timestamp(mut self, base_snapshot_lvt: DateTime<Utc>) -> Self {
        self.base_snapshot_timestamp = Some(base_snapshot_lvt);
        self
    }

    pub fn set_base_snapshot_opt(mut self, base_snapshot: Option<&TableSnapshot>) -> Self {
        if let Some(base) = base_snapshot {
            self.base_snapshot_timestamp = base.timestamp;
        }
        self
    }

    pub fn build(self) -> Result<TableSnapshot> {
        let now = Utc::now();
        let timestamp = monotonically_increased_timestamp(now, &self.snapshot_template.timestamp);
        let snapshot_id = Self::uuid_from_data_time(timestamp);
        let lvt_candidate = timestamp
            // TODO investigate this
            .checked_sub_days(Days::new(self.retention_period_in_days))
            .unwrap();

        // lvt is allowed to be increased, but NOT decreased
        let lvt = monotonically_increased_timestamp(
            lvt_candidate,
            &self.snapshot_template.least_visible_timestamp,
        );

        if let Some(base_snapshot_timestamp) = self.base_snapshot_timestamp {
            // TODO <= or <?
            if base_snapshot_timestamp <= lvt {
                // TODO more error info
                return Err(ErrorCode::UnresolvableConflict(
                    "generating new snapshot based on staled snapshot not allowed.",
                ));
            }
        }

        Ok(TableSnapshot {
            format_version: TableSnapshot::VERSION,
            snapshot_id,
            timestamp: Some(timestamp),
            prev_table_seq: self.prev_table_seq,
            prev_snapshot_id: Some((
                self.snapshot_template.snapshot_id,
                self.snapshot_template.format_version,
            )),
            least_visible_timestamp: Some(lvt),
            ..self.snapshot_template
        })
    }

    fn uuid_from_data_time(ts: DateTime<Utc>) -> Uuid {
        todo!()
    }
}
