//! Number column deserialization for Parquet files
//!
//! This module provides functionality to deserialize integer columns from Parquet files,
//! with support for both nullable and non-nullable columns. It includes performance
//! optimizations for definition level decoding and proper null value handling.

use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_expression::types::Number;
use databend_common_expression::Column;
use parquet::encodings::rle::RleDecoder;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::FallibleStreamingIterator;

use crate::util::get_bit_width;
use crate::wip::decompressor::Decompressor;

type Result<T> = databend_common_exception::Result<T>;

// =============================================================================
// Trait Definitions
// =============================================================================

/// Trait for types that can be deserialized from Parquet integer data
pub trait ParquetInteger: Copy + Send + Sync + 'static {
    /// The Parquet physical type for this integer type
    const PHYSICAL_TYPE: parquet2::schema::types::PhysicalType;

    /// Convert little-endian bytes to this integer type (for big-endian systems)
    #[cfg(target_endian = "big")]
    #[inline]
    fn convert_from_le_bytes(bytes: &[u8]) -> Self;

    /// Create a column from a vector of this type
    fn create_column(data: Vec<Self>) -> Column;
}

// =============================================================================
// Trait Implementations
// =============================================================================

impl ParquetInteger for i32 {
    const PHYSICAL_TYPE: parquet2::schema::types::PhysicalType =
        parquet2::schema::types::PhysicalType::Int32;

    #[cfg(target_endian = "big")]
    #[inline]
    fn convert_from_le_bytes(bytes: &[u8]) -> Self {
        let mut byte_array = [0u8; 4];
        byte_array.copy_from_slice(bytes);
        i32::from_le_bytes(byte_array)
    }

    fn create_column(data: Vec<Self>) -> Column {
        Column::Number(i32::upcast_column(Buffer::from(data)))
    }
}

impl ParquetInteger for i64 {
    const PHYSICAL_TYPE: parquet2::schema::types::PhysicalType =
        parquet2::schema::types::PhysicalType::Int64;

    #[cfg(target_endian = "big")]
    #[inline]
    fn convert_from_le_bytes(bytes: &[u8]) -> Self {
        let mut byte_array = [0u8; 8];
        byte_array.copy_from_slice(bytes);
        i64::from_le_bytes(byte_array)
    }

    fn create_column(data: Vec<Self>) -> Column {
        Column::Number(i64::upcast_column(Buffer::from(data)))
    }
}

// =============================================================================
// Iterator Implementation
// =============================================================================

/// Generic iterator for reading integer values from Parquet pages
pub struct IntegerIter<'a, T: ParquetInteger> {
    pages: Decompressor<'a>,
    chunk_size: Option<usize>,
    num_rows: usize,
    is_nullable: bool,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: ParquetInteger> IntegerIter<'a, T> {
    /// Create a new integer iterator
    pub fn new(
        pages: Decompressor<'a>,
        num_rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Self {
        Self {
            pages,
            chunk_size,
            num_rows,
            is_nullable,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: ParquetInteger> Iterator for IntegerIter<'_, T> {
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
                Page::Data(data_page) => {
                    let data_len_before = column_data.len();
                    match Self::process_data_page(
                        data_page,
                        &mut column_data,
                        target_rows,
                        self.is_nullable,
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
                Page::Dict(_) => {
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
            let base_column = T::create_column(column_data);

            // Combine validity bitmaps if we have multiple pages
            let combined_bitmap = if validity_bitmaps.is_empty() {
                // This should not happen for nullable columns, but handle gracefully
                Bitmap::new_constant(true, column_len)
            } else if validity_bitmaps.len() == 1 {
                validity_bitmaps.into_iter().next().unwrap()
            } else {
                // Concatenate multiple bitmaps efficiently
                let total_len: usize = validity_bitmaps.iter().map(|b| b.len()).sum();

                // Verify total bitmap length matches column length
                if total_len != column_len {
                    return Some(Err(ErrorCode::Internal(format!(
                        "Total bitmap length mismatch: bitmap_total={}, column_len={}",
                        total_len, column_len
                    ))));
                }

                let mut combined_bits = Vec::with_capacity(total_len);
                for bitmap in validity_bitmaps {
                    // Use extend instead of individual push operations for better performance
                    combined_bits.extend(bitmap.iter());
                }
                Bitmap::from_iter(combined_bits)
            };

            use databend_common_expression::types::nullable::NullableColumn;
            let nullable_column = NullableColumn::new(base_column, combined_bitmap);
            Some(Ok(Column::Nullable(Box::new(nullable_column))))
        } else {
            // For non-nullable columns, return regular column
            Some(Ok(T::create_column(column_data)))
        }
    }
}

// =============================================================================
// Page Processing Implementation
// =============================================================================

impl<'a, T: ParquetInteger> IntegerIter<'a, T> {
    /// Process a data page
    fn process_data_page(
        data_page: &parquet2::page::DataPage,
        column_data: &mut Vec<T>,
        target_rows: usize,
        is_nullable: bool,
    ) -> Result<Option<Bitmap>> {
        // Validate physical type
        if data_page.descriptor.primitive_type.physical_type != T::PHYSICAL_TYPE {
            return Err(ErrorCode::Internal(format!(
                "Physical type mismatch: expected {:?}, got {:?}",
                T::PHYSICAL_TYPE,
                data_page.descriptor.primitive_type.physical_type
            )));
        }

        let (def_levels, _, values_buffer) = Self::extract_page_data(data_page)?;
        let remaining = target_rows - column_data.len();

        // Defensive checks for nullable vs non-nullable columns
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

        // Get page metadata
        let num_values = data_page.num_values();
        let num_values_in_buffer = values_buffer.len() / std::mem::size_of::<T>();

        // Calculate how many rows this page will actually contribute
        let page_rows = if is_nullable {
            // For nullable columns, page contributes num_values rows (including NULLs)
            num_values.min(remaining)
        } else {
            // For non-nullable columns, page contributes num_values_in_buffer rows
            num_values_in_buffer.min(remaining)
        };

        // Process definition levels to create validity bitmap
        let (validity_bitmap, non_null_count) = if is_nullable {
            // Performance optimization: check if we need to decode definition levels
            let has_nulls_in_page = num_values > num_values_in_buffer;

            if has_nulls_in_page {
                Self::decode_definition_levels(&def_levels, data_page, num_values, page_rows)?
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
            Encoding::Plain => {
                Self::process_plain_encoding(
                    values_buffer,
                    column_data,
                    page_rows, // Use page_rows instead of remaining
                    non_null_count,
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

    /// Extract definition levels, repetition levels, and values from a data page
    fn extract_page_data(data_page: &parquet2::page::DataPage) -> Result<(&[u8], &[u8], &[u8])> {
        // Use parquet2's split_buffer function to extract page components
        match parquet2::page::split_buffer(data_page) {
            Ok((rep_levels, def_levels, values_buffer)) => {
                Ok((def_levels, rep_levels, values_buffer))
            }
            Err(e) => Err(ErrorCode::Internal(format!(
                "Failed to split buffer: {}",
                e
            ))),
        }
    }

    /// Decode definition levels and create validity bitmap
    fn decode_definition_levels(
        def_levels: &[u8],
        data_page: &parquet2::page::DataPage,
        num_values: usize,
        page_rows: usize,
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
        let expected_levels = num_values.min(page_rows);
        let mut levels = vec![0i32; expected_levels];
        let decoded_count = rle_decoder.get_batch(&mut levels).map_err(|e| {
            ErrorCode::Internal(format!("Failed to decode definition levels: {}", e))
        })?;

        // This is not correct
        if decoded_count != expected_levels {
            return Err(ErrorCode::Internal(format!(
                "Definition level decoder returned wrong count: expected={}, got={}",
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

    /// Process plain encoding values
    fn process_plain_encoding(
        values_buffer: &[u8],
        column_data: &mut Vec<T>,
        page_rows: usize,
        non_null_count: usize,
        validity_bitmap: Option<&Bitmap>,
    ) -> Result<()> {
        let num_values_in_buffer = values_buffer.len() / std::mem::size_of::<T>();

        // Allocate space for all positions (including NULLs)
        let old_len = column_data.len();
        // Fill NULL positions with zero values as default
        let default_value = unsafe { std::mem::zeroed::<T>() };
        column_data.resize(old_len + page_rows, default_value);

        if non_null_count > 0 {
            // Fill only non-NULL positions with actual values
            let mut values_read = 0;

            if let Some(bitmap) = validity_bitmap {
                // Copy values based on validity bitmap
                for (i, is_valid) in bitmap.iter().enumerate() {
                    if is_valid && values_read < non_null_count {
                        let src_offset = values_read * std::mem::size_of::<T>();
                        let dst_offset = old_len + i;

                        if src_offset + std::mem::size_of::<T>() <= values_buffer.len() {
                            // Safe byte-level copying to handle unaligned memory
                            let src_bytes =
                                &values_buffer[src_offset..src_offset + std::mem::size_of::<T>()];
                            let dst_bytes = unsafe {
                                std::slice::from_raw_parts_mut(
                                    column_data[dst_offset..dst_offset + 1].as_mut_ptr() as *mut u8,
                                    std::mem::size_of::<T>(),
                                )
                            };
                            dst_bytes.copy_from_slice(src_bytes);
                            values_read += 1;
                        }
                    }
                }
            } else {
                // No validity bitmap, copy all values sequentially
                for i in 0..non_null_count.min(page_rows) {
                    let src_offset = i * std::mem::size_of::<T>();
                    let dst_offset = old_len + i;

                    if src_offset + std::mem::size_of::<T>() <= values_buffer.len() {
                        // Safe byte-level copying to handle unaligned memory
                        let src_bytes =
                            &values_buffer[src_offset..src_offset + std::mem::size_of::<T>()];
                        let dst_bytes = unsafe {
                            std::slice::from_raw_parts_mut(
                                column_data[dst_offset..dst_offset + 1].as_mut_ptr() as *mut u8,
                                std::mem::size_of::<T>(),
                            )
                        };
                        dst_bytes.copy_from_slice(src_bytes);
                    }
                }
            }
        }

        Ok(())
    }
}

// =============================================================================
// Type Aliases
// =============================================================================

/// Type alias for 64-bit integer iterator
pub type Int64Iter<'a> = IntegerIter<'a, i64>;

/// Type alias for 32-bit integer iterator
pub type Int32Iter<'a> = IntegerIter<'a, i32>;

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    // TODO: Add meaningful integration tests with real Parquet data
}
