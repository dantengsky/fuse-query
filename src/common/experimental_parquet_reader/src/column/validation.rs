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

//! Validation functions for Parquet column processing

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parquet2::schema::types::PhysicalType;

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