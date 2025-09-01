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

//! Plain encoding implementation for Parquet columns

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::column::traits::ParquetPhysicalMapping;

/// Process plain encoded data
/// # Arguments
/// * `values_buffer` - The buffer containing the encoded values (maybe plain encoded）
/// * `page_rows` - The number of rows in the page
/// * `column_data` - The vector to which the decoded values will be appended， capacity should be reserved properly
/// * `validity_bitmap` - The validity bitmap for the column if any
pub fn process_plain_encoding<T: Copy + ParquetPhysicalMapping>(
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
                            // Note: convert_endianness_and_copy function needs to be implemented
                            // for big-endian support. For now, we'll error on big-endian systems
                            return Err(ErrorCode::Internal(
                                "Big-endian support not yet implemented for plain encoding"
                                    .to_string(),
                            ));
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
                    // Note: convert_endianness_and_copy function needs to be implemented
                    // for big-endian support. For now, we'll error on big-endian systems
                    return Err(ErrorCode::Internal(
                        "Big-endian support not yet implemented for plain encoding".to_string(),
                    ));
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