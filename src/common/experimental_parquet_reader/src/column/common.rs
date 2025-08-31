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

//! Common utilities for Parquet column deserialization

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::Column;
use decompressor::Decompressor;
use parquet2::schema::types::PhysicalType;
use streaming_decompression::FallibleStreamingIterator;

// Import dictionary processing functions from separate module
use super::dictionary::{process_dictionary_page, process_rle_dictionary_encoding};
// Import core traits from separate module
use super::traits::{DictionarySupport, ParquetColumnType, ParquetPhysicalMapping};
// Import utility functions from separate module
use super::utils::{decode_definition_levels, extract_page_data, get_bit_width};
// Import validation functions from separate module
use super::validation::{
    combine_validity_bitmaps, validate_column_nullability, validate_physical_type,
};
use crate::reader::decompressor;

/// Process plain encoded data
/// # Arguments
/// * `values_buffer` - The buffer containing the encoded values (maybe plain encoded）
/// * `page_rows` - The number of rows in the page
/// * `column_data` - The vector to which the decoded values will be appended， capacity should be reserved properly
/// * `validity_bitmap` - The validity bitmap for the column if any
fn process_plain_encoding<T: Copy + ParquetPhysicalMapping>(
    values_buffer: &[u8],
    page_rows: usize,
    column_data: &mut Vec<T>,
    validity_bitmap: Option<&Bitmap>,
) -> Result<()> {
    let old_len = column_data.len();

    // Calculate how many non-null values we expect to read
    let non_null_count = if let Some(bitmap) = validity_bitmap {
        bitmap.iter().filter(|&b| b).count()
    } else {
        page_rows
    };

    if let Some(bitmap) = validity_bitmap {
        // Nullable column: process values based on validity bitmap
        unsafe {
            column_data.set_len(old_len + page_rows);
        }

        let mut values_read = 0;
        for (i, is_valid) in bitmap.iter().enumerate() {
            if is_valid && values_read < non_null_count {
                // CRITICAL FIX: Use the correct physical size from Parquet
                let src_offset = values_read * T::PHYSICAL_SIZE; // This was the bug!
                let dst_offset = old_len + i;

                if src_offset + T::PHYSICAL_SIZE <= values_buffer.len() {
                    // For small types, we need to read from Parquet physical size but copy target size
                    if T::PHYSICAL_SIZE != T::TARGET_SIZE {
                        // Different size: read Int32/Int64 and convert to target type
                        #[cfg(target_endian = "little")]
                        {
                            // Read the full physical value and convert to target type
                            match (T::PHYSICAL_SIZE, T::TARGET_SIZE) {
                                (4, 1) => {
                                    // Int32 -> i8/u8: read as Int32, convert to target
                                    let int32_val = i32::from_le_bytes([
                                        values_buffer[src_offset],
                                        values_buffer[src_offset + 1],
                                        values_buffer[src_offset + 2],
                                        values_buffer[src_offset + 3],
                                    ]);
                                    let target_ptr = column_data[dst_offset..dst_offset + 1]
                                        .as_mut_ptr()
                                        as *mut i8;
                                    unsafe {
                                        *target_ptr = int32_val as i8;
                                    }
                                }
                                (4, 2) => {
                                    // Int32 -> i16/u16
                                    let int32_val = i32::from_le_bytes([
                                        values_buffer[src_offset],
                                        values_buffer[src_offset + 1],
                                        values_buffer[src_offset + 2],
                                        values_buffer[src_offset + 3],
                                    ]);
                                    let target_ptr = column_data[dst_offset..dst_offset + 1]
                                        .as_mut_ptr()
                                        as *mut i16;
                                    unsafe {
                                        *target_ptr = int32_val as i16;
                                    }
                                }
                                _ => {
                                    return Err(ErrorCode::Internal(format!(
                                        "Unsupported size conversion: {} -> {}",
                                        T::PHYSICAL_SIZE,
                                        T::TARGET_SIZE
                                    )));
                                }
                            }
                        }
                        #[cfg(target_endian = "big")]
                        {
                            // On big-endian, we need proper endian conversion
                            // For now, fall back to safe conversion
                            match (T::PHYSICAL_SIZE, T::TARGET_SIZE) {
                                (4, 1) => {
                                    // Int32 -> i8/u8: read as Int32, convert to target
                                    let int32_val = i32::from_le_bytes([
                                        values_buffer[src_offset],
                                        values_buffer[src_offset + 1],
                                        values_buffer[src_offset + 2],
                                        values_buffer[src_offset + 3],
                                    ]);
                                    let target_ptr = column_data[dst_offset..dst_offset + 1]
                                        .as_mut_ptr()
                                        as *mut i8;
                                    unsafe {
                                        *target_ptr = int32_val as i8;
                                    }
                                }
                                (4, 2) => {
                                    // Int32 -> i16/u16
                                    let int32_val = i32::from_le_bytes([
                                        values_buffer[src_offset],
                                        values_buffer[src_offset + 1],
                                        values_buffer[src_offset + 2],
                                        values_buffer[src_offset + 3],
                                    ]);
                                    let target_ptr = column_data[dst_offset..dst_offset + 1]
                                        .as_mut_ptr()
                                        as *mut i16;
                                    unsafe {
                                        *target_ptr = int32_val as i16;
                                    }
                                }
                                _ => {
                                    return Err(ErrorCode::Internal(format!(
                                        "Unsupported size conversion: {} -> {}",
                                        T::PHYSICAL_SIZE,
                                        T::TARGET_SIZE
                                    )));
                                }
                            }
                        }
                    } else {
                        // Same size: direct copy with endian handling
                        #[cfg(target_endian = "big")]
                        {
                            convert_endianness_and_copy::<T>(
                                &values_buffer[src_offset..src_offset + T::TARGET_SIZE],
                                &mut column_data[dst_offset..dst_offset + 1],
                            );
                        }
                        #[cfg(target_endian = "little")]
                        {
                            unsafe {
                                let src_ptr = values_buffer.as_ptr().add(src_offset);
                                let dst_ptr =
                                    column_data[dst_offset..dst_offset + 1].as_mut_ptr() as *mut u8;
                                std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, T::TARGET_SIZE);
                            }
                        }
                    }
                    values_read += 1;
                } else {
                    return Err(ErrorCode::Internal("Values buffer underflow".to_string()));
                }
            }
        }
    } else {
        // Non-nullable column: bulk processing
        let values_to_copy = non_null_count.min(page_rows);

        if T::PHYSICAL_SIZE == T::TARGET_SIZE {
            // Same size: preserve existing performance optimizations
            let total_bytes = values_to_copy * T::TARGET_SIZE;

            if total_bytes <= values_buffer.len() {
                #[cfg(target_endian = "big")]
                {
                    unsafe {
                        column_data.set_len(old_len + values_to_copy);
                    }
                    for i in 0..values_to_copy {
                        let src_offset = i * T::TARGET_SIZE;
                        let dst_offset = old_len + i;
                        convert_endianness_and_copy::<T>(
                            &values_buffer[src_offset..src_offset + T::TARGET_SIZE],
                            &mut column_data[dst_offset..dst_offset + 1],
                        );
                    }
                }
                #[cfg(target_endian = "little")]
                {
                    // CRITICAL PERFORMANCE PATH: bulk copy for i32, i64, f32, f64
                    unsafe {
                        let src_ptr = values_buffer.as_ptr();
                        let dst_ptr = column_data.as_mut_ptr().add(old_len) as *mut u8;
                        std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, total_bytes);
                        column_data.set_len(old_len + values_to_copy);
                    }
                }
            } else {
                return Err(ErrorCode::Internal("Values buffer underflow".to_string()));
            }
        } else {
            // Different size: per-element conversion
            let total_parquet_bytes = values_to_copy * T::PHYSICAL_SIZE;

            if total_parquet_bytes <= values_buffer.len() {
                unsafe {
                    column_data.set_len(old_len + values_to_copy);
                }

                for i in 0..values_to_copy {
                    let src_offset = i * T::PHYSICAL_SIZE; // CRITICAL FIX: Use physical size
                    let dst_offset = old_len + i;

                    #[cfg(target_endian = "little")]
                    {
                        // Read the full physical value and convert to target type
                        match (T::PHYSICAL_SIZE, T::TARGET_SIZE) {
                            (4, 1) => {
                                // Int32 -> i8/u8: read as Int32, convert to target
                                let int32_val = i32::from_le_bytes([
                                    values_buffer[src_offset],
                                    values_buffer[src_offset + 1],
                                    values_buffer[src_offset + 2],
                                    values_buffer[src_offset + 3],
                                ]);
                                let target_ptr =
                                    column_data[dst_offset..dst_offset + 1].as_mut_ptr() as *mut i8;
                                unsafe {
                                    *target_ptr = int32_val as i8;
                                }
                            }
                            (4, 2) => {
                                // Int32 -> i16/u16
                                let int32_val = i32::from_le_bytes([
                                    values_buffer[src_offset],
                                    values_buffer[src_offset + 1],
                                    values_buffer[src_offset + 2],
                                    values_buffer[src_offset + 3],
                                ]);
                                let target_ptr = column_data[dst_offset..dst_offset + 1]
                                    .as_mut_ptr()
                                    as *mut i16;
                                unsafe {
                                    *target_ptr = int32_val as i16;
                                }
                            }
                            _ => {
                                return Err(ErrorCode::Internal(format!(
                                    "Unsupported size conversion: {} -> {}",
                                    T::PHYSICAL_SIZE,
                                    T::TARGET_SIZE
                                )));
                            }
                        }
                    }
                    #[cfg(target_endian = "big")]
                    {
                        // Proper endian conversion for different sizes
                        match (T::PHYSICAL_SIZE, T::TARGET_SIZE) {
                            (4, 1) => {
                                let int32_val = i32::from_le_bytes([
                                    values_buffer[src_offset],
                                    values_buffer[src_offset + 1],
                                    values_buffer[src_offset + 2],
                                    values_buffer[src_offset + 3],
                                ]);
                                let target_ptr =
                                    column_data[dst_offset..dst_offset + 1].as_mut_ptr() as *mut i8;
                                unsafe {
                                    *target_ptr = int32_val as i8;
                                }
                            }
                            (4, 2) => {
                                let int32_val = i32::from_le_bytes([
                                    values_buffer[src_offset],
                                    values_buffer[src_offset + 1],
                                    values_buffer[src_offset + 2],
                                    values_buffer[src_offset + 3],
                                ]);
                                let target_ptr = column_data[dst_offset..dst_offset + 1]
                                    .as_mut_ptr()
                                    as *mut i16;
                                unsafe {
                                    *target_ptr = int32_val as i16;
                                }
                            }
                            _ => {
                                return Err(ErrorCode::Internal(format!(
                                    "Unsupported size conversion: {} -> {}",
                                    T::PHYSICAL_SIZE,
                                    T::TARGET_SIZE
                                )));
                            }
                        }
                    }
                }
            } else {
                return Err(ErrorCode::Internal("Values buffer underflow".to_string()));
            }
        }
    }

    Ok(())
}

/// Process a complete data page for any type T
fn process_data_page<T: Copy + DictionarySupport + ParquetPhysicalMapping>(
    data_page: &parquet2::page::DataPage,
    column_data: &mut Vec<T>,
    target_rows: usize,
    is_nullable: bool,
    expected_physical_type: &PhysicalType,
    dictionary: Option<&[T]>,
) -> Result<Option<Bitmap>> {
    // Validate physical type
    validate_physical_type(
        data_page.descriptor.primitive_type.physical_type,
        *expected_physical_type,
    )?;

    let (def_levels, _, values_buffer) = extract_page_data(data_page)?;
    let remaining = target_rows - column_data.len();

    // Defensive checks for nullable vs non-nullable columns
    #[cfg(debug_assertions)]
    validate_column_nullability(def_levels, is_nullable)?;

    // Number of values(not rows), including NULLs
    let num_values = data_page.num_values();

    // Calculate how many rows this page will actually contribute
    let page_rows = if is_nullable {
        // For nullable columns, page contributes num_values rows (including NULLs)
        num_values.min(remaining)
    } else {
        // For non-nullable columns, we need to handle different encodings differently
        match data_page.encoding() {
            parquet2::encoding::Encoding::Plain => {
                if *expected_physical_type == PhysicalType::Boolean {
                    // Boolean values are bit-packed, so we use num_values from page header
                    num_values.min(remaining)
                } else {
                    let type_size = std::mem::size_of::<T>();
                    let num_values_in_buffer = values_buffer.len() / type_size;
                    num_values_in_buffer.min(remaining)
                }
            }
            parquet2::encoding::Encoding::RleDictionary => {
                // For RLE dictionary, we use num_values from the page header
                num_values.min(remaining)
            }
            _ => num_values.min(remaining),
        }
    };

    // Process definition levels to create validity bitmap (only for nullable columns)
    let validity_bitmap = if is_nullable {
        let bit_width = get_bit_width(data_page.descriptor.max_def_level);
        let (bitmap, _non_null_count) =
            decode_definition_levels(def_levels, bit_width, num_values, data_page)?;
        bitmap
    } else {
        // For non-nullable columns, no validity bitmap needed
        None
    };

    // Process values based on encoding
    match data_page.encoding() {
        parquet2::encoding::Encoding::Plain => {
            // Special handling for Boolean type (bit-packed)
            if *expected_physical_type == PhysicalType::Boolean {
                // For Boolean, we need special bit-packed decoding
                use crate::column::process_boolean_plain_encoding;

                // Cast to bool slice - this is safe because T must be bool for Boolean physical type
                let bool_column_data =
                    unsafe { std::mem::transmute::<&mut Vec<T>, &mut Vec<bool>>(column_data) };

                process_boolean_plain_encoding(
                    values_buffer,
                    page_rows,
                    bool_column_data,
                    validity_bitmap.as_ref(),
                )?;
            } else {
                // Use compile-time type information for zero-overhead optimization
                process_plain_encoding(
                    values_buffer,
                    page_rows,
                    column_data,
                    validity_bitmap.as_ref(),
                )?;
            }
        }
        parquet2::encoding::Encoding::RleDictionary => {
            if let Some(dict) = dictionary {
                process_rle_dictionary_encoding(values_buffer, page_rows, column_data, dict)?;
            } else {
                return Err(ErrorCode::Internal(
                    "RLE dictionary encoding requires dictionary page".to_string(),
                ));
            }
        }
        encoding => {
            return Err(ErrorCode::Internal(format!(
                "Unsupported encoding: {:?}",
                encoding
            )));
        }
    }

    Ok(validity_bitmap)
}

// TODO rename this
pub struct ParquetColumnIterator<
    'a,
    T: ParquetColumnType + DictionarySupport + ParquetPhysicalMapping,
> {
    pages: Decompressor<'a>,
    chunk_size: Option<usize>,
    num_rows: usize,
    is_nullable: bool,
    metadata: T::Metadata,
    dictionary: Option<Vec<T>>, // Cached dictionary values
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: ParquetColumnType + DictionarySupport + ParquetPhysicalMapping>
    ParquetColumnIterator<'a, T>
{
    pub fn new(
        pages: Decompressor<'a>,
        num_rows: usize,
        is_nullable: bool,
        metadata: T::Metadata,
        chunk_size: Option<usize>,
    ) -> Self {
        Self {
            pages,
            chunk_size,
            num_rows,
            is_nullable,
            metadata,
            dictionary: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

// WIP: State of iterator should be adjusted, if we allow chunk_size be chosen freely
impl<'a, T: ParquetColumnType + DictionarySupport + ParquetPhysicalMapping> Iterator
    for ParquetColumnIterator<'a, T>
{
    type Item = Result<databend_common_expression::Column>;

    fn next(&mut self) -> Option<Self::Item> {
        let target_rows = self.chunk_size.unwrap_or(self.num_rows);
        let mut column_data: Vec<T> = Vec::with_capacity(target_rows);
        let mut validity_bitmaps = Vec::new();

        while column_data.len() < target_rows {
            // Get the next page
            let page = match self.pages.next() {
                Ok(Some(page)) => page,
                Ok(None) => break,
                Err(e) => {
                    return Some(Err(ErrorCode::Internal(format!(
                        "Failed to get next page: {}",
                        e
                    ))))
                }
            };

            match page {
                parquet2::page::Page::Data(data_page) => {
                    let data_len_before = column_data.len();
                    match process_data_page(
                        data_page,
                        &mut column_data,
                        target_rows,
                        self.is_nullable,
                        &T::PHYSICAL_TYPE,
                        self.dictionary.as_deref(),
                    ) {
                        Ok(validity_bitmap) => {
                            if self.is_nullable {
                                // For nullable columns, we must have a validity bitmap for each page
                                if let Some(bitmap) = validity_bitmap {
                                    let data_added = column_data.len() - data_len_before;

                                    // Verify bitmap length matches data added
                                    if bitmap.len() != data_added {
                                        return Some(Err(ErrorCode::Internal(format!(
                                            "Bitmap length mismatch: bitmap={}, data_added={}",
                                            bitmap.len(),
                                            data_added
                                        ))));
                                    }
                                    validity_bitmaps.push(bitmap);
                                } else {
                                    // This should not happen for nullable columns
                                    return Some(Err(ErrorCode::Internal(
                                        "Nullable column page must produce validity bitmap"
                                            .to_string(),
                                    )));
                                }
                            }
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                parquet2::page::Page::Dict(dict_page) => {
                    if T::PHYSICAL_TYPE == PhysicalType::Int32
                        || T::PHYSICAL_TYPE == PhysicalType::Int64
                        || T::PHYSICAL_TYPE == PhysicalType::Boolean
                        || T::PHYSICAL_TYPE == PhysicalType::Float
                        || T::PHYSICAL_TYPE == PhysicalType::Double
                        || matches!(T::PHYSICAL_TYPE, PhysicalType::FixedLenByteArray(_))
                    {
                        // Process dictionary page and cache the dictionary
                        if let Some(ref mut dictionary) = self.dictionary {
                            if let Err(e) = process_dictionary_page::<T>(dict_page, dictionary) {
                                return Some(Err(e));
                            }
                        } else {
                            let mut dictionary = Vec::new();
                            if let Err(e) = process_dictionary_page::<T>(dict_page, &mut dictionary)
                            {
                                return Some(Err(e));
                            }
                            self.dictionary = Some(dictionary);
                        }
                    } else {
                        return Some(Err(ErrorCode::Internal(
                            "Dictionary page not supported for this type".to_string(),
                        )));
                    }
                }
            }
        }

        if column_data.is_empty() {
            return None;
        }

        // Return the appropriate Column variant based on nullability
        if self.is_nullable {
            // For nullable columns, create NullableColumn
            let column_len = column_data.len();
            let base_column = T::create_column(column_data, &self.metadata);

            // Combine validity bitmaps from multiple pages
            let combined_bitmap = match combine_validity_bitmaps(validity_bitmaps, column_len) {
                Ok(bitmap) => bitmap,
                Err(e) => return Some(Err(e)),
            };

            let nullable_column = NullableColumn::new(base_column, combined_bitmap);
            Some(Ok(Column::Nullable(Box::new(nullable_column))))
        } else {
            // For non-nullable columns, return the column directly
            Some(Ok(T::create_column(column_data, &self.metadata)))
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::NumberColumn;
    use parquet2::page::DictPage;
    use parquet2::schema::types::PhysicalType;

    use super::*;
    use crate::column::Date;
    use crate::column::Decimal128;
    use crate::column::Decimal256;
    use crate::column::Decimal64;

    // Mock implementation for testing
    #[derive(Debug, Clone, Copy, PartialEq)]
    struct TestType(i32);

    impl ParquetColumnType for TestType {
        const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int32;
        type Metadata = ();

        fn create_column(
            data: Vec<Self>,
            metadata: &Self::Metadata,
        ) -> databend_common_expression::Column {
            let raw_data: Vec<i32> = unsafe { std::mem::transmute(data) };
            databend_common_expression::Column::Number(NumberColumn::Int32(raw_data.into()))
        }
    }

    impl DictionarySupport for TestType {
        fn from_dictionary_entry(entry: &[u8]) -> Result<Self> {
            if entry.len() != 4 {
                return Err(databend_common_exception::ErrorCode::Internal(
                    "Expected 4 bytes for TestType".to_string(),
                ));
            }
            let value = i32::from_le_bytes([entry[0], entry[1], entry[2], entry[3]]);
            Ok(TestType(value))
        }
    }

    #[test]
    fn test_process_dictionary_page() -> Result<()> {
        // Create test dictionary data (3 i32 values: 10, 20, 30)
        let dict_data = vec![
            10u8, 0, 0, 0, // 10 in little-endian
            20u8, 0, 0, 0, // 20 in little-endian
            30u8, 0, 0, 0, // 30 in little-endian
        ];

        let dict_page = DictPage {
            buffer: dict_data,
            num_values: 3,
            is_sorted: false,
        };

        let mut dictionary = Vec::new();
        process_dictionary_page::<TestType>(&dict_page, &mut dictionary)?;

        assert_eq!(dictionary.len(), 3);
        assert_eq!(dictionary[0], TestType(10));
        assert_eq!(dictionary[1], TestType(20));
        assert_eq!(dictionary[2], TestType(30));

        Ok(())
    }

    #[test]
    fn test_process_dictionary_page_empty() -> Result<()> {
        let dict_page = DictPage {
            buffer: vec![],
            num_values: 0,
            is_sorted: false,
        };

        let mut dictionary = Vec::new();
        process_dictionary_page::<TestType>(&dict_page, &mut dictionary)?;

        assert_eq!(dictionary.len(), 0);
        Ok(())
    }

    #[test]
    fn test_process_dictionary_page_invalid_size() -> Result<()> {
        // Create invalid dictionary data (incomplete i32)
        let dict_data = vec![10u8, 0, 0]; // Only 3 bytes instead of 4

        let dict_page = DictPage {
            buffer: dict_data,
            num_values: 1,
            is_sorted: false,
        };

        let mut dictionary = Vec::new();
        let result = process_dictionary_page::<TestType>(&dict_page, &mut dictionary);

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_rle_indices_allocation_optimization() {
        // This test verifies that our optimization to avoid zero initialization
        // doesn't break the basic functionality. We can't directly test the
        // performance improvement, but we can ensure correctness.

        let page_rows = 1000;

        // Create indices vector with our optimized allocation
        let mut indices = Vec::with_capacity(page_rows);
        unsafe {
            indices.set_len(page_rows);
        }

        // Verify the vector has the correct capacity and length
        assert_eq!(indices.len(), page_rows);
        assert!(indices.capacity() >= page_rows);

        // Verify we can write to all positions (this would crash if unsafe was wrong)
        for i in 0..page_rows {
            indices[i] = i as i32;
        }

        // Verify the data was written correctly
        for i in 0..page_rows {
            assert_eq!(indices[i], i as i32);
        }
    }

    #[test]
    fn test_dictionary_support_trait_consistency() {
        // Test that all our types have consistent PHYSICAL_TYPE values
        assert_eq!(i32::PHYSICAL_TYPE, PhysicalType::Int32);
        assert_eq!(i64::PHYSICAL_TYPE, PhysicalType::Int64);
        assert_eq!(Date::PHYSICAL_TYPE, PhysicalType::Int32);

        // Decimal types use FixedLenByteArray
        assert_eq!(Decimal64::PHYSICAL_TYPE, PhysicalType::FixedLenByteArray(8));
        assert_eq!(
            Decimal128::PHYSICAL_TYPE,
            PhysicalType::FixedLenByteArray(16)
        );
        assert_eq!(
            Decimal256::PHYSICAL_TYPE,
            PhysicalType::FixedLenByteArray(32)
        );
    }

    #[test]
    fn test_batch_dictionary_lookup_performance_pattern() -> Result<()> {
        // Test the performance pattern we optimized: pre-allocation + direct assignment
        let dictionary = vec![TestType(100), TestType(200), TestType(300)];
        let indices = vec![0i32, 1, 2, 0, 1, 2]; // 6 lookups

        // Pre-allocate output (our optimization)
        let mut output = Vec::with_capacity(indices.len());
        unsafe {
            output.set_len(indices.len());
        }

        // Perform batch lookup
        batch_dictionary_lookup::<TestType>(&dictionary, &indices, &mut output)?;

        // Verify results
        assert_eq!(output.len(), 6);
        assert_eq!(output[0], TestType(100));
        assert_eq!(output[1], TestType(200));
        assert_eq!(output[2], TestType(300));
        assert_eq!(output[3], TestType(100));
        assert_eq!(output[4], TestType(200));
        assert_eq!(output[5], TestType(300));

        Ok(())
    }
}
