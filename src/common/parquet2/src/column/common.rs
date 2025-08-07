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
//!
//! This module provides shared functionality for different column types,
//! reducing code duplication and providing a consistent interface.

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::Column;
use decompressor::Decompressor;
use parquet::encodings::rle::RleDecoder;
use parquet2::schema::types::PhysicalType;
use streaming_decompression::FallibleStreamingIterator;

use crate::reader::decompressor;

// =============================================================================
// Common Parquet Deserialization Functions
// =============================================================================

/// Extract definition levels, repetition levels, and values from a data page
pub fn extract_page_data(data_page: &parquet2::page::DataPage) -> Result<(&[u8], &[u8], &[u8])> {
    // Use parquet2's split_buffer function to extract page components
    match parquet2::page::split_buffer(data_page) {
        Ok((rep_levels, def_levels, values_buffer)) => Ok((def_levels, rep_levels, values_buffer)),
        Err(e) => Err(ErrorCode::Internal(format!(
            "Failed to split buffer: {}",
            e
        ))),
    }
}

/// Decode definition levels and create validity bitmap
pub fn decode_definition_levels(
    def_levels: &[u8],
    data_page: &parquet2::page::DataPage,
    num_values: usize,
    _page_rows: usize,
) -> Result<(Option<Bitmap>, usize)> {
    let bit_width = {
        let max_def_level = data_page.descriptor.max_def_level;
        if max_def_level == 1 {
            1
        } else {
            get_bit_width(max_def_level)
        }
    };

    let mut rle_decoder = RleDecoder::new(bit_width as u8);
    rle_decoder.set_data(bytes::Bytes::copy_from_slice(def_levels));

    // Definition levels count should equal the number of values in the page
    // The number of definition levels is determined by the page content, not by how many rows we want to process
    let expected_levels = num_values;
    let mut levels = vec![0i32; expected_levels];
    let decoded_count = rle_decoder
        .get_batch(&mut levels)
        .map_err(|e| ErrorCode::Internal(format!("Failed to decode definition levels: {}", e)))?;

    // Verify we got the expected number of levels
    if decoded_count != expected_levels {
        return Err(ErrorCode::Internal(format!(
            "Definition level decoder returned wrong count: expected={}, got={}. This indicates corrupted Parquet data or decoder issues.",
            expected_levels, decoded_count
        )));
    }

    // Create validity bitmap from definition levels
    let max_def_level = data_page.descriptor.max_def_level as i32;
    let mut validity_bits = Vec::with_capacity(expected_levels);
    let mut non_null_count = 0;
    let mut has_nulls = false;

    for &level in &levels {
        let is_valid = level == max_def_level;
        validity_bits.push(is_valid);
        if is_valid {
            non_null_count += 1;
        } else {
            has_nulls = true;
        }
    }

    let bitmap = if has_nulls {
        Some(Bitmap::from_iter(validity_bits))
    } else {
        // Optimization: no NULL values, but still need to create an all-true validity bitmap for nullable columns
        Some(Bitmap::new_constant(true, expected_levels))
    };
    Ok((bitmap, non_null_count))
}

/// Process plain encoded data with proper endianness handling
///
/// Parquet format stores all numeric data in little-endian format.
/// On big-endian systems, we need to convert byte order.
pub fn process_plain_encoding<T: Copy>(
    values_buffer: &[u8],
    page_rows: usize,
    column_data: &mut Vec<T>,
    validity_bitmap: Option<&Bitmap>,
) -> Result<()> {
    let type_size = std::mem::size_of::<T>();
    let old_len = column_data.len();

    // Reserve space for new values
    column_data.reserve(page_rows);

    // Calculate how many non-null values we expect to read
    let non_null_count = if let Some(bitmap) = validity_bitmap {
        bitmap.iter().filter(|&b| b).count()
    } else {
        page_rows
    };

    if let Some(bitmap) = validity_bitmap {
        // Nullable column: process values based on validity bitmap
        // Extend vector to final size, leaving NULL positions uninitialized
        unsafe {
            column_data.set_len(old_len + page_rows);
        }

        let mut values_read = 0;
        for (i, is_valid) in bitmap.iter().enumerate() {
            if is_valid && values_read < non_null_count {
                let src_offset = values_read * type_size;
                let dst_offset = old_len + i;

                if src_offset + type_size <= values_buffer.len() {
                    // Handle endianness conversion for numeric types
                    #[cfg(target_endian = "big")]
                    {
                        // On big-endian systems, convert from Parquet's little-endian format
                        convert_endianness_and_copy::<T>(
                            &values_buffer[src_offset..src_offset + type_size],
                            &mut column_data[dst_offset..dst_offset + 1],
                        );
                    }
                    #[cfg(target_endian = "little")]
                    {
                        // On little-endian systems, direct copy is sufficient
                        unsafe {
                            let src_ptr = values_buffer.as_ptr().add(src_offset);
                            let dst_ptr =
                                column_data[dst_offset..dst_offset + 1].as_mut_ptr() as *mut u8;
                            std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, type_size);
                        }
                    }
                    values_read += 1;
                } else {
                    return Err(ErrorCode::Internal("Values buffer underflow".to_string()));
                }
            }
            // Note: NULL positions (is_valid == false) are left uninitialized
            // This is safe because the validity bitmap controls access
        }
    } else {
        // Non-nullable column: batch copy all values for maximum performance
        let values_to_copy = non_null_count.min(page_rows);
        let total_bytes = values_to_copy * type_size;

        if total_bytes <= values_buffer.len() {
            #[cfg(target_endian = "big")]
            {
                // On big-endian systems, convert each value individually
                unsafe {
                    column_data.set_len(old_len + values_to_copy);
                }
                for i in 0..values_to_copy {
                    let src_offset = i * type_size;
                    let dst_offset = old_len + i;
                    convert_endianness_and_copy::<T>(
                        &values_buffer[src_offset..src_offset + type_size],
                        &mut column_data[dst_offset..dst_offset + 1],
                    );
                }
            }
            #[cfg(target_endian = "little")]
            {
                // On little-endian systems, batch copy for maximum performance
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
    }

    Ok(())
}

/// Convert endianness and copy data for big-endian systems
///
/// This function handles the conversion from Parquet's little-endian format
/// to the native big-endian format on big-endian systems.
#[cfg(target_endian = "big")]
fn convert_endianness_and_copy<T: Copy>(src_bytes: &[u8], dst_slice: &mut [T]) {
    let type_size = std::mem::size_of::<T>();

    match type_size {
        1 => {
            // Single byte: no endianness conversion needed
            unsafe {
                let dst_ptr = dst_slice.as_mut_ptr() as *mut u8;
                std::ptr::copy_nonoverlapping(src_bytes.as_ptr(), dst_ptr, 1);
            }
        }
        2 => {
            // 2-byte integer (i16): convert from little-endian
            let mut bytes = [0u8; 2];
            bytes.copy_from_slice(src_bytes);
            let value = i16::from_le_bytes(bytes);
            unsafe {
                let dst_ptr = dst_slice.as_mut_ptr() as *mut i16;
                *dst_ptr = value;
            }
        }
        4 => {
            // 4-byte integer (i32): convert from little-endian
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(src_bytes);
            let value = i32::from_le_bytes(bytes);
            unsafe {
                let dst_ptr = dst_slice.as_mut_ptr() as *mut i32;
                *dst_ptr = value;
            }
        }
        8 => {
            // 8-byte integer (i64): convert from little-endian
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(src_bytes);
            let value = i64::from_le_bytes(bytes);
            unsafe {
                let dst_ptr = dst_slice.as_mut_ptr() as *mut i64;
                *dst_ptr = value;
            }
        }
        16 => {
            // 16-byte integer (i128): convert from little-endian
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(src_bytes);
            let value = i128::from_le_bytes(bytes);
            unsafe {
                let dst_ptr = dst_slice.as_mut_ptr() as *mut i128;
                *dst_ptr = value;
            }
        }
        32 => {
            // 32-byte integer (i256): convert from little-endian
            // Note: i256 doesn't have from_le_bytes, so we reverse the bytes manually
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(src_bytes);
            bytes.reverse(); // Convert from little-endian to big-endian
            unsafe {
                let dst_ptr = dst_slice.as_mut_ptr() as *mut u8;
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), dst_ptr, 32);
            }
        }
        _ => {
            // For other sizes, fall back to direct copy (may not be correct for all types)
            unsafe {
                let dst_ptr = dst_slice.as_mut_ptr() as *mut u8;
                std::ptr::copy_nonoverlapping(src_bytes.as_ptr(), dst_ptr, type_size);
            }
        }
    }
}

/// Perform defensive checks for nullable vs non-nullable columns
pub fn validate_column_nullability(def_levels: &[u8], is_nullable: bool) -> Result<()> {
    if is_nullable {
        // Nullable columns must have definition levels
        if def_levels.is_empty() {
            return Err(ErrorCode::Internal(
                "Nullable column must have definition levels".to_string(),
            ));
        }
    } else {
        // Non-nullable columns should not have definition levels
        if !def_levels.is_empty() {
            return Err(ErrorCode::Internal(
                "Non-nullable column should not have definition levels".to_string(),
            ));
        }
    }
    Ok(())
}

/// Validate physical type matches expected type
pub fn validate_physical_type(actual: PhysicalType, expected: PhysicalType) -> Result<()> {
    if actual != expected {
        return Err(ErrorCode::Internal(format!(
            "Physical type mismatch: expected {:?}, got {:?}",
            expected, actual
        )));
    }
    Ok(())
}

/// Validate values buffer alignment
pub fn validate_buffer_alignment<T>(values_buffer: &[u8]) -> Result<()> {
    let type_size = std::mem::size_of::<T>();
    if values_buffer.len() % type_size != 0 {
        return Err(ErrorCode::Internal(format!(
            "Values buffer length ({}) is not aligned to type size ({}). Buffer may be corrupted.",
            values_buffer.len(),
            type_size
        )));
    }
    Ok(())
}

/// Combine multiple validity bitmaps from different pages
pub fn combine_validity_bitmaps(
    validity_bitmaps: Vec<Bitmap>,
    expected_total_len: usize,
) -> Result<Bitmap> {
    if validity_bitmaps.is_empty() {
        Ok(Bitmap::new_constant(true, expected_total_len))
    } else if validity_bitmaps.len() == 1 {
        Ok(validity_bitmaps.into_iter().next().unwrap())
    } else {
        // Combine multiple validity bitmaps
        let total_len: usize = validity_bitmaps.iter().map(|b| b.len()).sum();
        if total_len != expected_total_len {
            return Err(ErrorCode::Internal(format!(
                "Combined bitmap length ({}) does not match expected length ({})",
                total_len, expected_total_len
            )));
        }
        let mut combined_bits = Vec::with_capacity(total_len);
        for bitmap in validity_bitmaps {
            combined_bits.extend(bitmap.iter());
        }
        Ok(Bitmap::from_iter(combined_bits))
    }
}

/// Process a complete data page for any type T
pub fn process_data_page<T: Copy>(
    data_page: &parquet2::page::DataPage,
    column_data: &mut Vec<T>,
    target_rows: usize,
    is_nullable: bool,
    expected_physical_type: &PhysicalType,
) -> Result<Option<Bitmap>> {
    // Validate physical type
    validate_physical_type(
        data_page.descriptor.primitive_type.physical_type,
        *expected_physical_type,
    )?;

    let (def_levels, _, values_buffer) = extract_page_data(data_page)?;
    let remaining = target_rows - column_data.len();

    // Defensive checks for nullable vs non-nullable columns
    validate_column_nullability(def_levels, is_nullable)?;

    // Get page metadata
    let num_values = data_page.num_values();

    // Validate values_buffer alignment
    validate_buffer_alignment::<T>(values_buffer)?;

    let type_size = std::mem::size_of::<T>();
    let num_values_in_buffer = values_buffer.len() / type_size;

    // Calculate how many rows this page will actually contribute
    let page_rows = if is_nullable {
        // For nullable columns, page contributes num_values rows (including NULLs)
        num_values.min(remaining)
    } else {
        // For non-nullable columns, page contributes num_values_in_buffer rows
        num_values_in_buffer.min(remaining)
    };

    // Process definition levels to create validity bitmap
    let (validity_bitmap, _non_null_count) = if is_nullable {
        // Performance optimization: check if we need to decode definition levels
        let has_nulls_in_page = num_values > num_values_in_buffer;

        if has_nulls_in_page {
            decode_definition_levels(def_levels, data_page, num_values, page_rows)?
        } else {
            // Optimization: no NULL values, skip definition level decoding
            // But still need to create an all-true validity bitmap for nullable columns
            let all_valid_bitmap = Bitmap::new_constant(true, page_rows);
            (Some(all_valid_bitmap), page_rows)
        }
    } else {
        // Non-nullable column: no validity bitmap needed
        (None, page_rows)
    };

    // Process values based on encoding
    match data_page.encoding() {
        parquet2::encoding::Encoding::Plain => {
            process_plain_encoding(
                values_buffer,
                page_rows,
                column_data,
                validity_bitmap.as_ref(),
            )?;
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
pub trait ParquetColumnType: Copy + Send + Sync + 'static {
    /// Additional metadata needed to create columns (e.g., precision/scale for decimals)
    type Metadata: Clone;

    /// The Parquet physical type for this column type
    const PHYSICAL_TYPE: PhysicalType;

    /// Create a column from the deserialized data
    fn create_column(data: Vec<Self>, metadata: &Self::Metadata) -> Column;
}

// TODO rename this
pub struct ParquetColumnIterator<'a, T: ParquetColumnType> {
    pages: Decompressor<'a>,
    chunk_size: Option<usize>,
    num_rows: usize,
    is_nullable: bool,
    metadata: T::Metadata,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: ParquetColumnType> ParquetColumnIterator<'a, T> {
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
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, T: ParquetColumnType> Iterator for ParquetColumnIterator<'a, T> {
    type Item = Result<Column>;

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
                        &data_page,
                        &mut column_data,
                        target_rows,
                        self.is_nullable,
                        &T::PHYSICAL_TYPE,
                    ) {
                        Ok(validity_bitmap) => {
                            let data_added = column_data.len() - data_len_before;

                            if self.is_nullable {
                                // For nullable columns, we must have a validity bitmap for each page
                                if let Some(bitmap) = validity_bitmap {
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
                parquet2::page::Page::Dict(_) => {
                    return Some(Err(ErrorCode::Internal(
                        "Dictionary page not supported yet".to_string(),
                    )));
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

fn get_bit_width(max_level: i16) -> u32 {
    16 - max_level.leading_zeros()
}
