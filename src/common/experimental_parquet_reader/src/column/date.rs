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

use databend_common_expression::Column;
use parquet2::schema::types::PhysicalType;

use crate::column::{DictionarySupport, ParquetColumnType, ParquetPhysicalMapping};
use crate::column::common::ParquetColumnIterator;
use crate::column::number::IntegerMetadata;

/// Date type alias for i32 (days since epoch)
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Date(i32);

impl ParquetColumnType for Date {
    type Metadata = IntegerMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int32;

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        let raw_data: Vec<i32> = unsafe { std::mem::transmute(data) };
        Column::Date(raw_data.into())
    }
}

impl ParquetPhysicalMapping for Date {
    const PHYSICAL_SIZE: usize = 4; // Int32 -> Date
    const TARGET_SIZE: usize = 4; // Same size
}

// =============================================================================
// Dictionary Support Implementation
// =============================================================================

impl DictionarySupport for Date {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        if entry.len() != 4 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid Date dictionary entry length: expected 4, got {}",
                entry.len()
            )));
        }

        // Parquet stores dates as i32 in little-endian format
        let bytes: [u8; 4] = entry.try_into().map_err(|_| {
            databend_common_exception::ErrorCode::Internal(
                "Failed to convert bytes to Date".to_string(),
            )
        })?;

        Ok(Date(i32::from_le_bytes(bytes)))
    }
}

pub type DateIter<'a> = ParquetColumnIterator<'a, Date>;

#[cfg(test)]
mod tests {
    use databend_common_exception::Result;

    use super::*;

    #[test]
    fn test_date_dictionary_support() -> Result<()> {
        // Test from_dictionary_entry
        let entry = [42u8, 0, 0, 0]; // 42 in little-endian (days since epoch)
        let value = Date::from_dictionary_entry(&entry)?;
        assert_eq!(value, Date(42));

        // Test zero date (epoch)
        let entry = [0u8, 0, 0, 0]; // 0 in little-endian
        let value = Date::from_dictionary_entry(&entry)?;
        assert_eq!(value, Date(0));

        // Test large date value
        let entry = [255u8, 255, 255, 127]; // i32::MAX in little-endian
        let value = Date::from_dictionary_entry(&entry)?;
        assert_eq!(value, Date(i32::MAX));

        // Test invalid entry size
        let entry = [42u8, 0, 0]; // Only 3 bytes
        let result = Date::from_dictionary_entry(&entry);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Expected 4 bytes"));

        Ok(())
    }
}
