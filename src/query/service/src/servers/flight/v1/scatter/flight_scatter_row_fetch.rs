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

use std::collections::HashSet;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::split_row_id;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_metrics::storage::metrics_inc_row_fetch_distributed_batches;
use databend_common_metrics::storage::metrics_inc_row_fetch_local_batches;
use log::info;

use super::FlightScatter;

pub struct AdaptiveRowFetchFlightScatter {
    hash_scatter: Box<dyn FlightScatter>,
    query_id: String,
    row_id_col_offset: usize,
    local_block_threshold: usize,
    local_pos: usize,
    scatter_size: usize,
}

impl AdaptiveRowFetchFlightScatter {
    pub fn create(
        hash_scatter: Box<dyn FlightScatter>,
        query_id: String,
        row_id_col_offset: usize,
        local_block_threshold: usize,
        local_pos: usize,
        scatter_size: usize,
    ) -> Box<dyn FlightScatter> {
        Box::new(Self {
            hash_scatter,
            query_id,
            row_id_col_offset,
            local_block_threshold,
            local_pos,
            scatter_size,
        })
    }

    fn distinct_blocks(&self, data_block: &DataBlock) -> Result<usize> {
        let entry = data_block
            .columns()
            .get(self.row_id_col_offset)
            .ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Adaptive RowFetch row ID column offset {} is out of bounds for {} columns",
                    self.row_id_col_offset,
                    data_block.num_columns()
                ))
            })?;
        let column = entry.to_column();
        let row_ids = match entry.data_type() {
            DataType::Number(NumberDataType::UInt64) => {
                column.into_number().unwrap().into_u_int64().unwrap()
            }
            DataType::Nullable(inner)
                if matches!(inner.as_ref(), DataType::Number(NumberDataType::UInt64)) =>
            {
                column
                    .into_nullable()
                    .unwrap()
                    .column
                    .into_number()
                    .unwrap()
                    .into_u_int64()
                    .unwrap()
            }
            data_type => {
                return Err(ErrorCode::Internal(format!(
                    "Adaptive RowFetch row ID column must be UInt64, but got {data_type}"
                )));
            }
        };

        Ok(row_ids
            .iter()
            .map(|row_id| split_row_id(*row_id).0)
            .collect::<HashSet<_>>()
            .len())
    }

    fn use_local(&self, data_block: &DataBlock) -> Result<(bool, usize)> {
        let distinct_blocks = self.distinct_blocks(data_block)?;
        Ok((
            distinct_blocks <= self.local_block_threshold,
            distinct_blocks,
        ))
    }

    fn local_indices(&self, rows: usize) -> Vec<u64> {
        vec![self.local_pos as u64; rows]
    }

    fn record_decision(&self, local: bool, rows: usize, distinct_blocks: usize) {
        let mode = if local {
            Profile::record_usize_profile(ProfileStatisticsName::RowFetchLocalBatches, 1);
            metrics_inc_row_fetch_local_batches(1);
            "local"
        } else {
            Profile::record_usize_profile(ProfileStatisticsName::RowFetchDistributedBatches, 1);
            metrics_inc_row_fetch_distributed_batches(1);
            "distributed"
        };

        info!(
            "Adaptive RowFetch routing query_id={} mode={} rows={} distinct_blocks={} local_block_threshold={} destinations={}",
            self.query_id,
            mode,
            rows,
            distinct_blocks,
            self.local_block_threshold,
            self.scatter_size
        );
    }
}

impl FlightScatter for AdaptiveRowFetchFlightScatter {
    fn name(&self) -> &'static str {
        "AdaptiveRowFetch"
    }

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        let (local, distinct_blocks) = self.use_local(&data_block)?;
        self.record_decision(local, data_block.num_rows(), distinct_blocks);
        if !local {
            return self.hash_scatter.execute(data_block);
        }

        let block_meta = data_block.get_meta().cloned();
        let blocks = DataBlock::scatter(
            &data_block,
            &self.local_indices(data_block.num_rows()),
            self.scatter_size,
        )?;
        blocks
            .into_iter()
            .map(|block| block.add_meta(block_meta.clone()))
            .collect()
    }

    fn scatter_indices(&self, data_block: &DataBlock) -> Result<Option<Vec<u64>>> {
        if self.use_local(data_block)?.0 {
            Ok(Some(self.local_indices(data_block.num_rows())))
        } else {
            self.hash_scatter.scatter_indices(data_block)
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_catalog::plan::compute_row_id;
    use databend_common_catalog::plan::compute_row_id_prefix;
    use databend_common_expression::FromData;
    use databend_common_expression::types::UInt64Type;

    use super::*;

    struct TestHashScatter;

    impl FlightScatter for TestHashScatter {
        fn name(&self) -> &'static str {
            "TestHash"
        }

        fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
            let indices = self.scatter_indices(&data_block)?.unwrap();
            DataBlock::scatter(&data_block, &indices, 3)
        }

        fn scatter_indices(&self, data_block: &DataBlock) -> Result<Option<Vec<u64>>> {
            Ok(Some(
                (0..data_block.num_rows())
                    .map(|index| (index % 3) as u64)
                    .collect(),
            ))
        }
    }

    fn row_id_block(block_ids: &[u64]) -> DataBlock {
        let row_ids = block_ids
            .iter()
            .map(|block_id| compute_row_id(compute_row_id_prefix(0, *block_id), 0))
            .collect();
        DataBlock::new_from_columns(vec![UInt64Type::from_data(row_ids)])
    }

    fn scatter(local_block_threshold: usize) -> Box<dyn FlightScatter> {
        AdaptiveRowFetchFlightScatter::create(
            Box::new(TestHashScatter),
            "test-query".to_string(),
            0,
            local_block_threshold,
            1,
            3,
        )
    }

    #[test]
    fn keeps_compact_row_fetch_local() -> Result<()> {
        let block = row_id_block(&[1, 1, 2, 2]);
        assert_eq!(scatter(2).scatter_indices(&block)?, Some(vec![1, 1, 1, 1]));
        Ok(())
    }

    #[test]
    fn distributes_dispersed_row_fetch() -> Result<()> {
        let block = row_id_block(&[1, 2, 3, 4]);
        assert_eq!(scatter(2).scatter_indices(&block)?, Some(vec![0, 1, 2, 0]));
        Ok(())
    }
}
