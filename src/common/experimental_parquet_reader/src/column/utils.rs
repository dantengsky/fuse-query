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

//! Utility functions for Parquet column processing

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parquet::encodings::rle::RleDecoder;

/// # Safety
/// Uses unsafe indexing after comprehensive bounds validation for maximum performance.
/// All bounds are verified before any unsafe operations.
pub fn batch_dictionary_lookup<T: Copy>(
    dictionary: &[T],
    indices: &[i32],
    output: &mut [T],
) -> Result<()> {
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
                max_idx, dictionary.len()
            )));
        }
    }

    // Check for negative indices separately for efficiency
    if let Some(&min_idx) = indices.iter().min() {
        if min_idx < 0 {
            return Err(ErrorCode::Internal(format!(
                "Negative dictionary index: {}",
                min_idx
            )));
        }
    }

    // SAFETY: All bounds have been validated above
    // - indices.len() == output.len()
    // - All indices are non-negative and < dictionary.len()
    unsafe {
        for (i, &dict_idx) in indices.iter().enumerate() {
            // SAFETY: dict_idx is guaranteed to be valid by bounds check above
            let value = *dictionary.get_unchecked(dict_idx as usize);
            // SAFETY: i is guaranteed to be valid since it's from enumerate() over indices
            *output.get_unchecked_mut(i) = value;
        }
    }

    Ok(())
}

/// Extract definition levels, repetition levels, and values from a data page
pub fn extract_page_data(data_page: &parquet2::page::DataPage) -> Result<(&[u8], &[u8], &[u8])> {
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

/// Calculate bit width for definition levels
pub fn get_bit_width(max_level: i16) -> u32 {
    if max_level == 1 {
        1
    } else {
        16 - max_level.leading_zeros()
    }
}