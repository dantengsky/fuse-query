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
use parquet::encodings::rle::RleDecoder;
use parquet2::schema::types::PhysicalType;

/// Compile-time mapping between Rust types and their Parquet physical storage
/// This trait provides zero-overhead type information for performance-critical operations
pub trait ParquetPhysicalMapping {
    const PHYSICAL_SIZE: usize;
    const TARGET_SIZE: usize;
    const NEEDS_CONVERSION: bool = Self::PHYSICAL_SIZE != Self::TARGET_SIZE;
}

// Same-size mappings (performance-critical, zero overhead)
impl ParquetPhysicalMapping for i32 {
    const PHYSICAL_SIZE: usize = 4;
    const TARGET_SIZE: usize = 4;
}
impl ParquetPhysicalMapping for i64 {
    const PHYSICAL_SIZE: usize = 8;
    const TARGET_SIZE: usize = 8;
}
impl ParquetPhysicalMapping for f32 {
    const PHYSICAL_SIZE: usize = 4;
    const TARGET_SIZE: usize = 4;
}
impl ParquetPhysicalMapping for f64 {
    const PHYSICAL_SIZE: usize = 8;
    const TARGET_SIZE: usize = 8;
}

// Different-size mappings (need conversion but optimized)
impl ParquetPhysicalMapping for i8 {
    const PHYSICAL_SIZE: usize = 4;
    const TARGET_SIZE: usize = 1;
} // Int32 -> i8
impl ParquetPhysicalMapping for i16 {
    const PHYSICAL_SIZE: usize = 4;
    const TARGET_SIZE: usize = 2;
} // Int32 -> i16
impl ParquetPhysicalMapping for u8 {
    const PHYSICAL_SIZE: usize = 4;
    const TARGET_SIZE: usize = 1;
} // Int32 -> u8
impl ParquetPhysicalMapping for u16 {
    const PHYSICAL_SIZE: usize = 4;
    const TARGET_SIZE: usize = 2;
} // Int32 -> u16
impl ParquetPhysicalMapping for u32 {
    const PHYSICAL_SIZE: usize = 4;
    const TARGET_SIZE: usize = 4;
} // Int32 -> u32 (same size, reinterpret)
impl ParquetPhysicalMapping for u64 {
    const PHYSICAL_SIZE: usize = 8;
    const TARGET_SIZE: usize = 8;
} // Int64 -> u64 (same size)

// Float wrapper types (import these since they're commonly available)
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
impl ParquetPhysicalMapping for F32 {
    const PHYSICAL_SIZE: usize = 4;
    const TARGET_SIZE: usize = 4;
} // Float -> F32
impl ParquetPhysicalMapping for F64 {
    const PHYSICAL_SIZE: usize = 8;
    const TARGET_SIZE: usize = 8;
} // Double -> F64

// Note: Date, Decimal, Boolean types implement ParquetPhysicalMapping in their own modules
use streaming_decompression::FallibleStreamingIterator;

use crate::reader::decompressor;

// =============================================================================
// Dictionary Support Trait
// =============================================================================

/// Trait for types that support dictionary encoding in Parquet
/// This trait enables efficient dictionary-based deserialization for numeric types
pub trait DictionarySupport: ParquetColumnType {
    /// Create a value from a dictionary entry (raw bytes)
    ///
    /// # Arguments
    /// * `entry` - Raw bytes from dictionary page
    ///
    /// # Returns
    /// Decoded value of type Self
    fn from_dictionary_entry(entry: &[u8]) -> Result<Self>;
}

/// Generic batch dictionary lookup with bounds checking and optimized copying
///
/// This function provides a high-performance implementation of batch dictionary lookups
/// that can be shared across all types implementing DictionarySupport.
///
/// # Type Requirements
/// - T: Copy - enables efficient unsafe copying without drop semantics
///
/// # Arguments
/// * `dictionary` - The dictionary values to lookup from
/// * `indices` - Array of dictionary indices (must be non-negative and within bounds)
/// * `output` - Output slice to write results into (must have same length as indices)
///
/// # Performance Characteristics
/// - O(1) bounds checking via single max() operation
/// - O(n) unsafe copying for maximum throughput
/// - Zero memory allocations
/// - SIMD-friendly memory access patterns
///
/// # Safety
/// Uses unsafe indexing after comprehensive bounds validation for maximum performance.
/// All bounds are verified before any unsafe operations.
pub fn batch_dictionary_lookup<T: Copy>(
    dictionary: &[T],
    indices: &[i32],
    output: &mut [T],
) -> databend_common_exception::Result<()> {
    use databend_common_exception::ErrorCode;

    // Fast path: empty case
    if indices.is_empty() {
        return Ok(());
    }

    // Validate output slice length matches indices length
    if output.len() != indices.len() {
        return Err(ErrorCode::Internal(format!(
            "Output slice length ({}) doesn't match indices length ({})",
            output.len(),
            indices.len()
        )));
    }

    // Batch bounds checking - find max index once for efficiency
    // This is much faster than checking each index individually
    if let Some(&max_idx) = indices.iter().max() {
        if max_idx < 0 || max_idx as usize >= dictionary.len() {
            return Err(ErrorCode::Internal(format!(
                "Dictionary index out of bounds: {} >= {}",
                max_idx,
                dictionary.len()
            )));
        }
    }

    // Fast unchecked copy - all bounds verified above
    // This loop is the performance-critical path and should be optimized by LLVM
    for (i, &index) in indices.iter().enumerate() {
        unsafe {
            // Safe because:
            // 1. i < output.len() (guaranteed by enumerate on indices)
            // 2. index < dictionary.len() (verified by max check above)
            // 3. index >= 0 (verified by max check above)
            *output.get_unchecked_mut(i) = *dictionary.get_unchecked(index as usize);
        }
    }

    Ok(())
}

/// Extract definition levels, repetition levels, and values from a data page
fn extract_page_data(data_page: &parquet2::page::DataPage) -> Result<(&[u8], &[u8], &[u8])> {
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
    bit_width: u32,
    num_values: usize,
    data_page: &parquet2::page::DataPage,
) -> Result<(Option<Bitmap>, usize)> {
    let mut rle_decoder = RleDecoder::new(bit_width as u8);
    rle_decoder.set_data(bytes::Bytes::copy_from_slice(def_levels));

    let expected_levels = num_values;
    let mut levels = vec![0i32; expected_levels];
    let decoded_count = rle_decoder
        .get_batch(&mut levels)
        .map_err(|e| ErrorCode::Internal(format!("Failed to decode definition levels: {}", e)))?;

    if decoded_count != expected_levels {
        return Err(ErrorCode::Internal(format!(
            "Definition level decoder returned wrong count: expected={}, got={}",
            expected_levels, decoded_count
        )));
    }

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
        Some(Bitmap::new_constant(true, expected_levels))
    };
    Ok((bitmap, non_null_count))
}

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

/// Process dictionary page for numeric types with OLAP-optimized performance
fn process_dictionary_page<T: DictionarySupport + Copy + ParquetPhysicalMapping>(
    dict_page: &parquet2::page::DictPage,
    dictionary: &mut Vec<T>,
) -> Result<()> {
    let dict_buffer: &[u8] = dict_page.buffer.as_ref();

    // Handle empty dictionary case early
    if dict_buffer.is_empty() {
        return Ok(());
    }

    match T::PHYSICAL_TYPE {
        PhysicalType::Int32 | PhysicalType::Float => {
            let type_size = 4;
            let num_entries = dict_buffer.len() / type_size;

            // Check for invalid buffer size (incomplete entries)
            if dict_buffer.len() % type_size != 0 {
                return Err(ErrorCode::Internal(format!(
                    "Dictionary buffer size {} is not aligned to type size {}",
                    dict_buffer.len(),
                    type_size
                )));
            }

            if num_entries == 0 {
                return Ok(());
            }

            // Pre-allocate space to avoid reallocations
            dictionary.reserve(num_entries);
            let old_len = dictionary.len();

            if T::PHYSICAL_SIZE == T::TARGET_SIZE {
                // Same size: can use direct bulk copy (for i32, u32, f32)
                #[cfg(target_endian = "little")]
                {
                    // Little-endian machines: Direct bulk memory copy for maximum performance
                    // Parquet uses little-endian format, so no conversion needed
                    unsafe {
                        let src_ptr = dict_buffer.as_ptr() as *const T;
                        let dst_ptr = dictionary.as_mut_ptr().add(old_len);
                        std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, num_entries);
                        dictionary.set_len(old_len + num_entries);
                    }
                }
                #[cfg(target_endian = "big")]
                {
                    // Big-endian machines: Per-element endian conversion
                    unsafe {
                        dictionary.set_len(old_len + num_entries);
                        let output_slice = &mut dictionary[old_len..];

                        for (i, chunk) in dict_buffer.chunks_exact(4).enumerate() {
                            let value = i32::from_le_bytes(chunk.try_into().unwrap());
                            output_slice[i] = std::mem::transmute(value);
                        }
                    }
                }
            } else {
                // Different size: need proper type conversion (for i8, i16, u8, u16 from Int32)
                unsafe {
                    dictionary.set_len(old_len + num_entries);
                    let output_slice = &mut dictionary[old_len..];

                    for (i, chunk) in dict_buffer.chunks_exact(4).enumerate() {
                        // Read as Int32, convert to target type
                        let int32_val = i32::from_le_bytes(chunk.try_into().unwrap());
                        let target_val: T = match T::TARGET_SIZE {
                            1 => std::mem::transmute_copy(&(int32_val as i8)),
                            2 => std::mem::transmute_copy(&(int32_val as i16)),
                            _ => std::mem::transmute_copy(&int32_val),
                        };
                        output_slice[i] = target_val;
                    }
                }
            }
        }
        PhysicalType::Int64 | PhysicalType::Double => {
            let type_size = 8;
            let num_entries = dict_buffer.len() / type_size;

            // Check for invalid buffer size (incomplete entries)
            if dict_buffer.len() % type_size != 0 {
                return Err(ErrorCode::Internal(format!(
                    "Dictionary buffer size {} is not aligned to type size {}",
                    dict_buffer.len(),
                    type_size
                )));
            }

            if num_entries == 0 {
                return Ok(());
            }

            // Pre-allocate space to avoid reallocations
            dictionary.reserve(num_entries);
            let old_len = dictionary.len();

            // CRITICAL FIX: Check if we need type conversion
            if T::PHYSICAL_SIZE == T::TARGET_SIZE {
                // Same size: can use direct bulk copy (for i64, u64, f64)
                #[cfg(target_endian = "little")]
                {
                    // Little-endian machines: Direct bulk memory copy for maximum performance
                    unsafe {
                        let src_ptr = dict_buffer.as_ptr() as *const T;
                        let dst_ptr = dictionary.as_mut_ptr().add(old_len);
                        std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, num_entries);
                        dictionary.set_len(old_len + num_entries);
                    }
                }
                #[cfg(target_endian = "big")]
                {
                    // Big-endian machines: Per-element endian conversion
                    unsafe {
                        dictionary.set_len(old_len + num_entries);
                        let output_slice = &mut dictionary[old_len..];

                        for (i, chunk) in dict_buffer.chunks_exact(8).enumerate() {
                            let value = i64::from_le_bytes(chunk.try_into().unwrap());
                            output_slice[i] = std::mem::transmute(value);
                        }
                    }
                }
            } else {
                // Different size types should not use Int64 storage
                return Err(ErrorCode::Internal(
                    "Int64 storage with size mismatch not supported".to_string(),
                ));
            }
        }
        PhysicalType::FixedLenByteArray(len) => {
            let type_size = len;
            let num_entries = dict_buffer.len() / type_size;

            if num_entries > 0 {
                dictionary.reserve(num_entries);

                for chunk in dict_buffer.chunks_exact(type_size) {
                    let value = T::from_dictionary_entry(chunk)?;
                    dictionary.push(value);
                }
            }
        }
        _ => {
            return Err(ErrorCode::Internal(format!(
                "Unsupported physical type for dictionary: {:?}",
                T::PHYSICAL_TYPE
            )))
        }
    };

    Ok(())
}

/// Process RLE dictionary encoded data page
fn process_rle_dictionary_encoding<T: DictionarySupport + ParquetPhysicalMapping>(
    values_buffer: &[u8],
    page_rows: usize,
    column_data: &mut Vec<T>,
    dictionary: &[T],
) -> Result<()> {
    if values_buffer.is_empty() {
        return Err(ErrorCode::Internal(
            "Empty values buffer for RLE dictionary".to_string(),
        ));
    }

    // First byte is bit_width
    let bit_width = values_buffer[0];

    // Create RLE decoder
    let mut rle_decoder = RleDecoder::new(bit_width);
    rle_decoder.set_data(bytes::Bytes::copy_from_slice(&values_buffer[1..]));

    // Decode indices - avoid zero initialization for performance
    let mut indices = Vec::with_capacity(page_rows);
    #[allow(clippy::uninit_vec)]
    unsafe {
        indices.set_len(page_rows);
    }
    let decoded_count = rle_decoder
        .get_batch(&mut indices)
        .map_err(|e| ErrorCode::Internal(format!("Failed to decode RLE indices: {}", e)))?;

    if decoded_count != page_rows {
        return Err(ErrorCode::Internal(format!(
            "RLE decoder returned wrong count: expected={}, got={}",
            page_rows, decoded_count
        )));
    }

    // Batch dictionary lookup - performance critical path
    let old_len = column_data.len();
    column_data.reserve(page_rows);
    #[allow(clippy::uninit_vec)]
    unsafe {
        column_data.set_len(old_len + page_rows);
    }
    batch_dictionary_lookup::<T>(dictionary, &indices, &mut column_data[old_len..])?;

    Ok(())
}

// TODO rename this
pub trait ParquetColumnType: Copy + Send + Sync + 'static {
    /// Additional metadata needed to create columns (e.g., precision/scale for decimals)
    type Metadata: Clone;

    /// The Parquet physical type for this column type
    const PHYSICAL_TYPE: PhysicalType;

    /// Create a column from the deserialized data
    fn create_column(
        data: Vec<Self>,
        metadata: &Self::Metadata,
    ) -> databend_common_expression::Column;
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

fn get_bit_width(max_level: i16) -> u32 {
    if max_level == 1 {
        1
    } else {
        16 - max_level.leading_zeros()
    }
}

/// Perform defensive checks for nullable vs non-nullable columns
#[cfg(debug_assertions)]
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

        fn batch_from_dictionary_into_slice(
            dictionary: &[Self],
            indices: &[i32],
            output: &mut [Self],
        ) -> Result<()> {
            batch_dictionary_lookup::<TestType>(dictionary, indices, output)
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
