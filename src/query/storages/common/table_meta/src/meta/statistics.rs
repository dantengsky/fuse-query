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

use std::collections::HashMap;
use std::io::Cursor;

use databend_common_base::base::uuid::Uuid;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnId;
use databend_common_storage::MetaHLL;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::format::compress;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;
use crate::meta::ColumnStatistics;
use crate::meta::SegmentStatistics;

pub type FormatVersion = u64;
pub type SnapshotId = Uuid;
pub type Location = (String, FormatVersion);
pub type ClusterKey = (u32, String);
pub type StatisticsOfColumns = HashMap<ColumnId, ColumnStatistics>;
pub type BlockHLL = HashMap<ColumnId, MetaHLL>;
pub type RawBlockHLL = Vec<u8>;

// Assigned to executors, describes that which blocks of given segment, an executor should take care of
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct BlockSlotDescription {
    // number of slots
    pub num_slots: usize,
    // index of slot that current executor should take care of:
    // let `block_index` be the index of block in segment,
    // `block_index` mod `num_slots` == `slot_index` indicates that the block should be taken care of by current executor
    // otherwise, the block should be taken care of by other executors
    pub slot: u32,
}

pub fn supported_stat_type(data_type: &DataType) -> bool {
    let inner_type = data_type.remove_nullable();
    matches!(
        inner_type,
        DataType::Number(_)
            | DataType::Date
            | DataType::Timestamp
            | DataType::String
            | DataType::Decimal(_)
    )
}

pub fn encode_column_hll(hll: &BlockHLL) -> Result<RawBlockHLL> {
    let encoding = SegmentStatistics::encoding();
    let compression = SegmentStatistics::compression();

    let data = encode(&encoding, hll)?;
    let data_compress = compress(&compression, data)?;
    Ok(data_compress)
}

pub fn decode_column_hll(data: &RawBlockHLL) -> Result<Option<BlockHLL>> {
    if data.is_empty() {
        return Ok(None);
    }
    let encoding = SegmentStatistics::encoding();
    let compression = SegmentStatistics::compression();
    let mut reader = Cursor::new(&data);
    let res = read_and_deserialize(&mut reader, data.len() as u64, &encoding, &compression)?;
    Ok(Some(res))
}

pub fn merge_column_hll(mut lhs: BlockHLL, rhs: BlockHLL) -> BlockHLL {
    merge_column_hll_mut(&mut lhs, &rhs);
    lhs
}

pub fn merge_column_hll_mut(lhs: &mut BlockHLL, rhs: &BlockHLL) {
    for (column_id, column_hll) in rhs.iter() {
        lhs.entry(*column_id)
            .and_modify(|hll| hll.merge(column_hll))
            .or_insert_with(|| column_hll.clone());
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum BlockHLLState {
    Serialized(RawBlockHLL),
    Deserialized(BlockHLL),
}

impl BlockHLLState {
    pub fn merge_column_hll(lhs: &mut BlockHLL, rhs: &Option<BlockHLLState>) {
        if let Some(BlockHLLState::Deserialized(v)) = rhs {
            merge_column_hll_mut(lhs, v);
        }
    }

    pub fn encode_column_hll(hll: Option<BlockHLLState>) -> Result<Option<RawBlockHLL>> {
        hll.map(|h| match h {
            BlockHLLState::Deserialized(v) => encode_column_hll(&v),
            BlockHLLState::Serialized(v) => Ok(v),
        })
        .transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::io::Error;
    use std::io::ErrorKind;

    use databend_common_exception::Result;
    use databend_common_expression::TableSchema;
    use databend_common_io::prelude::bincode_deserialize_from_slice;
    use databend_common_io::prelude::bincode_serialize_into_buf;

    use crate::meta::AdditionalStatsMeta;
    use crate::meta::Location;
    use crate::meta::RawBlockHLL;
    use crate::meta::TableSnapshotV4;

    #[test]
    fn test_decode_snapshot() -> Result<()> {
        // Create test table snapshot
        let table_snapshot = TableSnapshotV4::new_empty_snapshot(TableSchema::empty(), None);
        let data = table_snapshot.to_bytes()?;
        let val = TableSnapshotV4::from_slice(&data)?;

        let mut buffer = Cursor::new(Vec::new());
        bincode_serialize_into_buf(&mut buffer, &val).unwrap();
        let slice = buffer.get_ref().as_slice();
        let deserialized: TableSnapshotV4 = bincode_deserialize_from_slice(slice).unwrap();
        assert_eq!(val.summary, deserialized.summary);
        Ok(())
    }

    #[test]
    fn test_additional_stats_meta_compatibility() -> Result<()> {
        #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Default)]
        pub struct AdditionalStatsMeta802 {
            /// The size of the stats data in bytes.
            pub size: u64,
            /// The file location of the stats data.
            pub location: Option<Location>,
            /// An optional HyperLogLog data structure.
            pub hll: Option<RawBlockHLL>,
            /// The count of the stats rows.
            #[serde(default)]
            pub row_count: u64,
        }

        // 790 and 797 have the same definition as 801
        #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq, Default)]
        pub struct AdditionalStatsMeta801 {
            /// The size of the stats data in bytes.
            pub size: u64,
            /// The file location of the stats data.
            pub location: Location,
        }

        // Simulate using current PR read data of v802
        {
            let v802 = AdditionalStatsMeta802::default();
            let bytes = rmp_serde::to_vec_named(&v802)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            let current: std::result::Result<AdditionalStatsMeta, Error> =
                rmp_serde::from_slice(&bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e));
            // v802 is not compatible with current PR
            assert!(current.is_err());
        }

        // Simulate using current PR read data of v802, with location patched
        {
            let mut v802 = AdditionalStatsMeta802::default();
            v802.location = Some(("aaa".to_string(), 0));
            let bytes = rmp_serde::to_vec_named(&v802)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            let current: std::result::Result<AdditionalStatsMeta, Error> =
                rmp_serde::from_slice(&bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e));
            // v802 is not compatible with current PR
            assert!(current.is_ok(), "patched 802");
            eprintln!("patched 802 to current is : {:#?}", current)
        }

        // v790, 797, 801 have been deployed, we MUST support them
        // Simulate using current PR read data of v790, 797, 801
        {
            let v801 = AdditionalStatsMeta801::default();
            let bytes = rmp_serde::to_vec_named(&v801)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            let current: std::result::Result<AdditionalStatsMeta, Error> =
                rmp_serde::from_slice(&bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e));
            assert!(current.is_ok());
        }

        // Simulate using v790, 797, 801 read data created by current PR
        {
            let mut current = AdditionalStatsMeta::default();
            current.hll = Some(RawBlockHLL::default());
            let bytes = rmp_serde::to_vec_named(&current)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            let current: std::result::Result<AdditionalStatsMeta801, Error> =
                rmp_serde::from_slice(&bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e));
            assert!(current.is_ok());
            eprintln!(" current {:#?}", current);
        }

        Ok(())
    }
}
