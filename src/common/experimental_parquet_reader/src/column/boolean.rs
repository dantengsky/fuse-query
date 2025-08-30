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

use bytes::Bytes;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use parquet::util::bit_util::BitReader;
use parquet2::schema::types::PhysicalType;

use crate::column::common::batch_dictionary_lookup;
use crate::column::common::DictionarySupport;
use crate::column::common::ParquetColumnIterator;
use crate::column::common::ParquetColumnType;
use crate::reader::decompressor::Decompressor;

#[derive(Clone, Copy)]
pub struct BooleanMetadata;

pub type BooleanIter<'a> = ParquetColumnIterator<'a, bool>;

pub fn new_boolean_iter(
    pages: Decompressor,
    num_rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> BooleanIter {
    ParquetColumnIterator::new(pages, num_rows, is_nullable, BooleanMetadata, chunk_size)
}

impl ParquetColumnType for bool {
    type Metadata = BooleanMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Boolean;

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        Column::Boolean(Bitmap::from(data))
    }
}

impl DictionarySupport for bool {
    fn from_dictionary_entry(entry: &[u8]) -> Result<Self> {
        if entry.len() != 1 {
            return Err(ErrorCode::Internal(format!(
                "Invalid bool dictionary entry length: expected 1, got {}",
                entry.len()
            )));
        }
        Ok(entry[0] != 0)
    }

    fn batch_from_dictionary_into_slice(
        dictionary: &[Self],
        indices: &[i32],
        output: &mut [Self],
    ) -> Result<()> {
        batch_dictionary_lookup(dictionary, indices, output)
    }
}

/// Process boolean values encoded using PLAIN encoding
///
/// Boolean values in Parquet are bit-packed, with 8 boolean values per byte,
/// using LSB-first bit ordering as specified in the Parquet format.
pub fn process_boolean_plain_encoding(
    values_buffer: &[u8],
    page_rows: usize,
    column_data: &mut Vec<bool>,
    validity_bitmap: Option<&Bitmap>,
) -> Result<()> {
    let mut bit_reader = BitReader::new(Bytes::copy_from_slice(values_buffer));
    let old_len = column_data.len();

    // Pre-allocate capacity to avoid multiple reallocations
    column_data.reserve(page_rows);

    if let Some(bitmap) = validity_bitmap {
        // Nullable column: process values based on validity bitmap
        for is_valid in bitmap.iter() {
            if is_valid {
                match bit_reader.get_value::<u8>(1) {
                    Some(byte_val) => column_data.push(byte_val != 0),
                    None => {
                        return Err(ErrorCode::Internal(
                            "Insufficient data in boolean values buffer".to_string(),
                        ));
                    }
                }
            } else {
                // Push default value for NULL entries
                column_data.push(false);
            }
        }
    } else {
        // Non-nullable column: read all values directly
        for _ in 0..page_rows {
            match bit_reader.get_value::<u8>(1) {
                Some(byte_val) => column_data.push(byte_val != 0),
                None => {
                    return Err(ErrorCode::Internal(
                        "Insufficient data in boolean values buffer".to_string(),
                    ));
                }
            }
        }
    }

    // Verify we read the expected number of values
    let actual_read = column_data.len() - old_len;
    if actual_read != page_rows {
        return Err(ErrorCode::Internal(format!(
            "Boolean decoder mismatch: expected {} values, got {}",
            page_rows, actual_read
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_column_creation() {
        let data = vec![true, false, true, false];
        let metadata = BooleanMetadata;
        let column = bool::create_column(data.clone(), &metadata);

        match column {
            Column::Boolean(bitmap) => {
                assert_eq!(bitmap.len(), 4);
                let values: Vec<bool> = bitmap.iter().collect();
                assert_eq!(values, data);
            }
            _ => panic!("Expected Boolean column"),
        }
    }

    #[test]
    fn test_boolean_dictionary_support() {
        // Test dictionary entry parsing
        assert_eq!(bool::from_dictionary_entry(&[1]).unwrap(), true);
        assert_eq!(bool::from_dictionary_entry(&[0]).unwrap(), false);
        assert_eq!(bool::from_dictionary_entry(&[42]).unwrap(), true); // Non-zero is true

        // Test error handling
        assert!(bool::from_dictionary_entry(&[]).is_err());
        assert!(bool::from_dictionary_entry(&[1, 2]).is_err());
    }

    #[test]
    fn test_boolean_batch_dictionary_lookup() {
        let dictionary = [true, false, true];
        let indices = [0i32, 2, 1, 0];
        let mut output = [false; 4];

        let result = bool::batch_from_dictionary_into_slice(&dictionary, &indices, &mut output);
        assert!(result.is_ok());
        assert_eq!(output, [true, true, false, true]);
    }
}
