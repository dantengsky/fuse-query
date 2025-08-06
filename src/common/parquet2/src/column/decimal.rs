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

use databend_common_column::buffer::Buffer;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::DecimalColumn;
use databend_common_expression::types::i256;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::Column;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PhysicalType;
use streaming_decompression::FallibleStreamingIterator;

use crate::util::get_bit_width;
use crate::wip::decompressor::Decompressor;

// =============================================================================
// ParquetDecimal Trait
// =============================================================================

/// Trait for types that can be used as Parquet decimal values
pub trait ParquetDecimal: Copy + Send + Sync + 'static {
    const PHYSICAL_TYPE: PhysicalType;
    
    /// Create a decimal column from a vector of values
    fn create_column(data: Vec<Self>, precision: u8, scale: u8) -> Column;
}

impl ParquetDecimal for i64 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int64;
    
    fn create_column(data: Vec<Self>, precision: u8, scale: u8) -> Column {
        let decimal_size = DecimalSize::new_unchecked(precision, scale);
        Column::Decimal(DecimalColumn::Decimal64(Buffer::from(data), decimal_size))
    }
}

impl ParquetDecimal for i128 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::FixedLenByteArray(16);
    
    fn create_column(data: Vec<Self>, precision: u8, scale: u8) -> Column {
        let decimal_size = DecimalSize::new_unchecked(precision, scale);
        Column::Decimal(DecimalColumn::Decimal128(Buffer::from(data), decimal_size))
    }
}

impl ParquetDecimal for i256 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::FixedLenByteArray(32);
    
    fn create_column(data: Vec<Self>, precision: u8, scale: u8) -> Column {
        let decimal_size = DecimalSize::new_unchecked(precision, scale);
        Column::Decimal(DecimalColumn::Decimal256(Buffer::from(data), decimal_size))
    }
}

// =============================================================================
// DecimalIter
// =============================================================================

/// Generic iterator for reading decimal values from Parquet pages
pub struct DecimalIter<'a, T: ParquetDecimal> {
    pages: Decompressor<'a>,
    chunk_size: Option<usize>,
    num_rows: usize,
    precision: u8,
    scale: u8,
    is_nullable: bool,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: ParquetDecimal> DecimalIter<'a, T> {
    /// Create a new decimal iterator
    pub fn new(
        pages: Decompressor<'a>,
        num_rows: usize,
        precision: u8,
        scale: u8,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Self {
        Self {
            pages,
            chunk_size,
            num_rows,
            precision,
            scale,
            is_nullable,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, T: ParquetDecimal> Iterator for DecimalIter<'a, T> {
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        let target_rows = self.chunk_size.unwrap_or(self.num_rows);
        let mut column_data: Vec<T> = Vec::with_capacity(target_rows);
        let mut validity_bitmaps = Vec::new();

        while column_data.len() < target_rows {
            let page = match self.pages.next() {
                Ok(Some(page)) => page,
                Ok(None) => break,
                Err(e) => return Some(Err(ErrorCode::Internal(format!("Page error: {}", e)))),
            };

            match page {
                Page::Data(data_page) => {
                    let data_len_before = column_data.len();
                    match Self::process_data_page(&data_page, &mut column_data, target_rows, self.is_nullable) {
                        Ok(validity_bitmap) => {
                            let data_added = column_data.len() - data_len_before;
                            if self.is_nullable {
                                if let Some(bitmap) = validity_bitmap {
                                    if bitmap.len() != data_added {
                                        return Some(Err(ErrorCode::Internal(format!(
                                            "Validity bitmap length ({}) does not match data added ({})",
                                            bitmap.len(), data_added
                                        ))));
                                    }
                                    validity_bitmaps.push(bitmap);
                                } else {
                                    return Some(Err(ErrorCode::Internal(
                                        "Expected validity bitmap for nullable column".to_string(),
                                    )));
                                }
                            }
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                Page::Dict(_) => {
                    return Some(Err(ErrorCode::Internal(
                        "Dictionary pages are not supported for decimal columns".to_string(),
                    )));
                }
            }
        }

        if column_data.is_empty() {
            return None;
        }

        if self.is_nullable {
            let column_len = column_data.len();
            let base_column = T::create_column(column_data.clone(), self.precision, self.scale);
            let combined_bitmap = if validity_bitmaps.is_empty() {
                Bitmap::new_constant(true, column_len)
            } else if validity_bitmaps.len() == 1 {
                validity_bitmaps.into_iter().next().unwrap()
            } else {
                // Combine multiple validity bitmaps
                let total_len: usize = validity_bitmaps.iter().map(|b| b.len()).sum();
                if total_len != column_len {
                    return Some(Err(ErrorCode::Internal(format!(
                        "Combined bitmap length ({}) does not match column length ({})",
                        total_len, column_len
                    ))));
                }
                let mut combined_bits = Vec::with_capacity(total_len);
                for bitmap in validity_bitmaps {
                    combined_bits.extend(bitmap.iter());
                }
                Bitmap::from_iter(combined_bits)
            };
            let nullable_column = NullableColumn::new(base_column, combined_bitmap);
            Some(Ok(Column::Nullable(Box::new(nullable_column))))
        } else {
            Some(Ok(T::create_column(column_data, self.precision, self.scale)))
        }
    }
}

// =============================================================================
// Page Processing Implementation
// =============================================================================

impl<'a, T: ParquetDecimal> DecimalIter<'a, T> {
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

        // Validate values_buffer length is aligned to type size
        let type_size = std::mem::size_of::<T>();
        if values_buffer.len() % type_size != 0 {
            return Err(ErrorCode::Internal(format!(
                "Values buffer length ({}) is not aligned to type size ({}). Buffer may be corrupted.",
                values_buffer.len(), type_size
            )));
        }

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
        use parquet::encodings::rle::RleDecoder;

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
        let type_size = std::mem::size_of::<T>();
        let old_len = column_data.len();
        
        // Reserve space for new values
        column_data.reserve(page_rows);

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
                        // High-performance byte-level copying using copy_nonoverlapping
                        // This avoids alignment issues by treating data as u8 arrays
                        unsafe {
                            let src_ptr = values_buffer.as_ptr().add(src_offset);
                            let dst_ptr = column_data[dst_offset..dst_offset + 1].as_mut_ptr() as *mut u8;
                            std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, type_size);
                        }
                        values_read += 1;
                    } else {
                        return Err(ErrorCode::Internal(
                            "Values buffer underflow".to_string(),
                        ));
                    }
                } else if !is_valid {
                    // Null value: position left uninitialized (per Arrow standard)
                    // The validity bitmap controls access, so arbitrary values are safe
                }
            }
        } else {
            // Non-nullable column: batch copy all values for maximum performance
            let values_to_copy = non_null_count.min(page_rows);
            let total_bytes = values_to_copy * type_size;

            if total_bytes <= values_buffer.len() {
                unsafe {
                    // Batch copy entire buffer using byte-level copy_nonoverlapping
                    // This is the fastest possible approach, avoiding all alignment issues
                    let src_ptr = values_buffer.as_ptr();
                    let dst_ptr = column_data.as_mut_ptr().add(old_len) as *mut u8;
                    std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, total_bytes);
                    column_data.set_len(old_len + values_to_copy);
                }
            } else {
                return Err(ErrorCode::Internal(
                    "Values buffer underflow".to_string(),
                ));
            }
        }

        Ok(())
    }
}
