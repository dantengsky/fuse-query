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

//! Dictionary processing for Parquet columns

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parquet::encodings::rle::RleDecoder;
use parquet2::schema::types::PhysicalType;

use super::traits::{DictionarySupport, ParquetPhysicalMapping};
use super::utils::batch_dictionary_lookup;

/// Process dictionary page for numeric types with OLAP-optimized performance
pub fn process_dictionary_page<T: DictionarySupport + Copy + ParquetPhysicalMapping>(
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
pub fn process_rle_dictionary_encoding<T: DictionarySupport + ParquetPhysicalMapping>(
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