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

use std::convert::TryFrom;
use std::mem::size_of;

use databend_common_column::bitmap::MutableBitmap;
use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Float32Type;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::OrderedFloat;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use parquet2::page::DataPage;
use parquet2::schema::types::PhysicalType;

use crate::not_implemented;
use crate::utils::PageValidity;

/// Deserialize a Parquet page directly to a Databend Column
pub fn deserialize_page_to_column(page: &DataPage) -> Result<Column> {
    let descriptor = &page.descriptor;
    let physical_type = descriptor.primitive_type.physical_type;
    let num_rows = page.num_values() as usize;

    let validity = PageValidity::try_from(page).ok();

    match physical_type {
        PhysicalType::Boolean => deserialize_boolean(page, validity),
        PhysicalType::Int32 => deserialize_int32(page, validity),
        PhysicalType::Int64 => deserialize_int64(page, validity),
        PhysicalType::Float => deserialize_float32(page, validity),
        PhysicalType::Double => deserialize_float64(page, validity),
        PhysicalType::ByteArray => deserialize_string(page, validity),
        _ => Err(not_implemented(format!(
            "Physical type {:?} not supported yet",
            physical_type
        ))),
    }
}

/// Create a DataBlock from a vector of Column objects
pub fn create_data_block_from_columns(
    columns: Vec<Column>,
    data_types: Vec<&DataType>,
) -> Result<DataBlock> {
    if columns.len() != data_types.len() {
        return Err(ErrorCode::Internal(format!(
            "Mismatch between columns ({}) and data types ({})",
            columns.len(),
            data_types.len()
        )));
    }

    let mut block_entries = Vec::with_capacity(columns.len());

    for column in columns {
        block_entries.push(BlockEntry::Column(column));
    }

    let num_rows = if block_entries.is_empty() {
        0
    } else {
        block_entries[0].len()
    };

    Ok(DataBlock::new(block_entries, num_rows))
}

/// Deserialize a boolean page directly to a Databend Column::Boolean
fn deserialize_boolean(page: &DataPage, validity: Option<PageValidity>) -> Result<Column> {
    let buffer = page.buffer();

    match validity {
        None => {
            // All values are non-null, directly create a Bitmap
            let mut bitmap_builder = MutableBitmap::with_capacity(buffer.len() * 8);

            // Process bytes bit by bit to get boolean values
            for idx in 0..buffer.len() * 8 {
                let byte_idx = idx / 8;
                let bit_idx = idx % 8;

                if byte_idx >= buffer.len() {
                    // Fill remaining with default values (false)
                    bitmap_builder.push(false);
                } else {
                    let byte = buffer[byte_idx];
                    let value = ((byte >> bit_idx) & 1) == 1;
                    bitmap_builder.push(value);
                }
            }

            Ok(Column::Boolean(bitmap_builder.into()))
        }
        Some(PageValidity::Required) => {
            // All values are non-null, directly create a Bitmap
            let mut bitmap_builder = MutableBitmap::with_capacity(buffer.len() * 8);

            // Process bytes bit by bit to get boolean values
            for idx in 0..buffer.len() * 8 {
                let byte_idx = idx / 8;
                let bit_idx = idx % 8;

                if byte_idx >= buffer.len() {
                    // Fill remaining with default values (false)
                    bitmap_builder.push(false);
                } else {
                    let byte = buffer[byte_idx];
                    let value = ((byte >> bit_idx) & 1) == 1;
                    bitmap_builder.push(value);
                }
            }

            Ok(Column::Boolean(bitmap_builder.into()))
        }
        Some(PageValidity::Optional(mut null_bitmap)) => {
            // Create a bitmap for values
            let mut bitmap_builder = MutableBitmap::with_capacity(buffer.len() * 8);
            let mut validity_builder = MutableBitmap::with_capacity(buffer.len() * 8);

            // Process bytes bit by bit to get boolean values, filling with defaults for nulls
            let mut idx = 0;
            while let Some(is_valid) = null_bitmap.next() {
                validity_builder.push(is_valid);

                if is_valid {
                    let byte_idx = idx / 8;
                    let bit_idx = idx % 8;

                    if byte_idx >= buffer.len() {
                        // Fill remaining with default values (false)
                        bitmap_builder.push(false);
                    } else {
                        let byte = buffer[byte_idx];
                        let value = ((byte >> bit_idx) & 1) == 1;
                        bitmap_builder.push(value);
                    }
                } else {
                    // Default value for null
                    bitmap_builder.push(false);
                }

                idx += 1;
            }

            // Fill any remaining positions with defaults
            while idx < buffer.len() * 8 {
                validity_builder.push(false);
                bitmap_builder.push(false);
                idx += 1;
            }

            // Create the boolean column with the bitmap and wrap it with nullable
            let column = Column::Boolean(bitmap_builder.into());
            Ok(column.wrap_nullable(Some(validity_builder.into())))
        }
        Some(PageValidity::FilteredOptional(mut filtered_iter)) => {
            // Create a bitmap for values
            let mut bitmap_builder = MutableBitmap::with_capacity(buffer.len() * 8);
            let mut validity_builder = MutableBitmap::with_capacity(buffer.len() * 8);

            // Process bytes bit by bit to get boolean values, filling with defaults for nulls
            let mut idx = 0;
            while let Some(is_valid) = filtered_iter.next() {
                validity_builder.push(is_valid);

                if is_valid {
                    let byte_idx = idx / 8;
                    let bit_idx = idx % 8;

                    if byte_idx >= buffer.len() {
                        // Fill remaining with default values (false)
                        bitmap_builder.push(false);
                    } else {
                        let byte = buffer[byte_idx];
                        let value = ((byte >> bit_idx) & 1) == 1;
                        bitmap_builder.push(value);
                    }
                } else {
                    // Default value for null
                    bitmap_builder.push(false);
                }

                idx += 1;
            }

            // Fill any remaining positions with defaults
            while idx < buffer.len() * 8 {
                validity_builder.push(false);
                bitmap_builder.push(false);
                idx += 1;
            }

            // Create the boolean column with the bitmap and wrap it with nullable
            let column = Column::Boolean(bitmap_builder.into());
            Ok(column.wrap_nullable(Some(validity_builder.into())))
        }
    }
}

/// Deserialize an Int32 page directly to a Databend Column::Number
fn deserialize_int32(page: &DataPage, validity: Option<PageValidity>) -> Result<Column> {
    let buffer = page.buffer();

    match validity {
        None => {
            // All values are non-null, we can copy memory directly
            let values_size = buffer.len();

            if buffer.len() < values_size {
                return Err(ErrorCode::Internal(format!(
                    "Buffer too small: {} bytes for {} i32 values",
                    buffer.len(),
                    values_size / size_of::<i32>()
                )));
            }

            // Create a vector and copy bytes into it
            let mut values = Vec::with_capacity(values_size / size_of::<i32>());

            // Direct memory copy for better performance
            unsafe {
                let src_ptr = buffer.as_ptr() as *const i32;
                values.extend_from_slice(std::slice::from_raw_parts(
                    src_ptr,
                    values_size / size_of::<i32>(),
                ));
            }

            // Create a Buffer from the vector
            let column_buffer = Buffer::from(values);

            // Return the appropriate Column variant
            Ok(Int32Type::upcast_column(column_buffer))
        }
        Some(PageValidity::Required) => {
            // All values are non-null, we can copy memory directly
            let values_size = buffer.len();

            if buffer.len() < values_size {
                return Err(ErrorCode::Internal(format!(
                    "Buffer too small: {} bytes for {} i32 values",
                    buffer.len(),
                    values_size / size_of::<i32>()
                )));
            }

            // Create a vector and copy bytes into it
            let mut values = Vec::with_capacity(values_size / size_of::<i32>());

            // Direct memory copy for better performance
            unsafe {
                let src_ptr = buffer.as_ptr() as *const i32;
                values.extend_from_slice(std::slice::from_raw_parts(
                    src_ptr,
                    values_size / size_of::<i32>(),
                ));
            }

            // Create a Buffer from the vector
            let column_buffer = Buffer::from(values);

            // Return the appropriate Column variant
            Ok(Int32Type::upcast_column(column_buffer))
        }
        Some(PageValidity::Optional(mut null_bitmap)) => {
            // Values might be null, need to process with nullability
            let mut values = Vec::with_capacity(buffer.len() / size_of::<i32>());
            let mut validity_builder =
                MutableBitmap::with_capacity(buffer.len() / size_of::<i32>());
            let mut idx = 0;

            while let Some(is_valid) = null_bitmap.next() {
                validity_builder.push(is_valid);

                if is_valid && idx * size_of::<i32>() + size_of::<i32>() <= buffer.len() {
                    let value_offset = idx * size_of::<i32>();
                    let value_bytes = [
                        buffer[value_offset],
                        buffer[value_offset + 1],
                        buffer[value_offset + 2],
                        buffer[value_offset + 3],
                    ];
                    let value = i32::from_le_bytes(value_bytes);
                    values.push(value);
                } else {
                    // Default value for null or out-of-bounds
                    values.push(0);
                }

                idx += 1;
            }

            // Fill any remaining positions with defaults
            while idx < buffer.len() / size_of::<i32>() {
                validity_builder.push(false);
                values.push(0);
                idx += 1;
            }

            let column_buffer = Buffer::from(values);
            let column = Int32Type::upcast_column(column_buffer);

            // Wrap with nullable
            Ok(column.wrap_nullable(Some(validity_builder.into())))
        }
        Some(PageValidity::FilteredOptional(mut filtered_iter)) => {
            // Values might be null and filtered, need to process with care
            let mut values = Vec::with_capacity(buffer.len() / size_of::<i32>());
            let mut validity_builder =
                MutableBitmap::with_capacity(buffer.len() / size_of::<i32>());
            let mut idx = 0;

            while let Some(is_valid) = filtered_iter.next() {
                validity_builder.push(is_valid);

                if is_valid && idx * size_of::<i32>() + size_of::<i32>() <= buffer.len() {
                    let value_offset = idx * size_of::<i32>();
                    let value_bytes = [
                        buffer[value_offset],
                        buffer[value_offset + 1],
                        buffer[value_offset + 2],
                        buffer[value_offset + 3],
                    ];
                    let value = i32::from_le_bytes(value_bytes);
                    values.push(value);
                } else {
                    // Default value for null or out-of-bounds
                    values.push(0);
                }

                idx += 1;
            }

            // Fill any remaining positions with defaults
            while idx < buffer.len() / size_of::<i32>() {
                validity_builder.push(false);
                values.push(0);
                idx += 1;
            }

            let column_buffer = Buffer::from(values);
            let column = Int32Type::upcast_column(column_buffer);

            // Wrap with nullable
            Ok(column.wrap_nullable(Some(validity_builder.into())))
        }
    }
}

/// Deserialize an Int64 page directly to a Databend Column::Number
fn deserialize_int64(page: &DataPage, validity: Option<PageValidity>) -> Result<Column> {
    let buffer = page.buffer();

    match validity {
        None => {
            // All values are non-null, we can copy memory directly
            let values_size = buffer.len();

            if buffer.len() < values_size {
                return Err(ErrorCode::Internal(format!(
                    "Buffer too small: {} bytes for {} i64 values",
                    buffer.len(),
                    values_size / size_of::<i64>()
                )));
            }

            // Create a vector and copy bytes into it
            let mut values = Vec::with_capacity(values_size / size_of::<i64>());

            // Direct memory copy for better performance
            unsafe {
                let src_ptr = buffer.as_ptr() as *const i64;
                values.extend_from_slice(std::slice::from_raw_parts(
                    src_ptr,
                    values_size / size_of::<i64>(),
                ));
            }

            // Create a Buffer from the vector
            let column_buffer = Buffer::from(values);

            // Return the appropriate Column variant
            Ok(Int64Type::upcast_column(column_buffer))
        }
        Some(PageValidity::Required) => {
            // All values are non-null, we can copy memory directly
            let values_size = buffer.len();

            if buffer.len() < values_size {
                return Err(ErrorCode::Internal(format!(
                    "Buffer too small: {} bytes for {} i64 values",
                    buffer.len(),
                    values_size / size_of::<i64>()
                )));
            }

            // Create a vector and copy bytes into it
            let mut values = Vec::with_capacity(values_size / size_of::<i64>());

            // Direct memory copy for better performance
            unsafe {
                let src_ptr = buffer.as_ptr() as *const i64;
                values.extend_from_slice(std::slice::from_raw_parts(
                    src_ptr,
                    values_size / size_of::<i64>(),
                ));
            }

            // Create a Buffer from the vector
            let column_buffer = Buffer::from(values);

            // Return the appropriate Column variant
            Ok(Int64Type::upcast_column(column_buffer))
        }
        Some(PageValidity::Optional(mut null_bitmap)) => {
            // Values might be null, need to process with nullability
            let mut values = Vec::with_capacity(buffer.len() / size_of::<i64>());
            let mut validity_builder =
                MutableBitmap::with_capacity(buffer.len() / size_of::<i64>());
            let mut idx = 0;

            while let Some(is_valid) = null_bitmap.next() {
                validity_builder.push(is_valid);

                if is_valid && idx * size_of::<i64>() + size_of::<i64>() <= buffer.len() {
                    let value_offset = idx * size_of::<i64>();
                    let value_bytes = [
                        buffer[value_offset],
                        buffer[value_offset + 1],
                        buffer[value_offset + 2],
                        buffer[value_offset + 3],
                        buffer[value_offset + 4],
                        buffer[value_offset + 5],
                        buffer[value_offset + 6],
                        buffer[value_offset + 7],
                    ];
                    let value = i64::from_le_bytes(value_bytes);
                    values.push(value);
                } else {
                    // Default value for null or out-of-bounds
                    values.push(0);
                }

                idx += 1;
            }

            // Fill any remaining positions with defaults
            while idx < buffer.len() / size_of::<i64>() {
                validity_builder.push(false);
                values.push(0);
                idx += 1;
            }

            let column_buffer = Buffer::from(values);
            let column = Int64Type::upcast_column(column_buffer);

            // Wrap with nullable
            Ok(column.wrap_nullable(Some(validity_builder.into())))
        }
        Some(PageValidity::FilteredOptional(mut filtered_iter)) => {
            // Values might be null and filtered, need to process with care
            let mut values = Vec::with_capacity(buffer.len() / size_of::<i64>());
            let mut validity_builder =
                MutableBitmap::with_capacity(buffer.len() / size_of::<i64>());
            let mut idx = 0;

            while let Some(is_valid) = filtered_iter.next() {
                validity_builder.push(is_valid);

                if is_valid && idx * size_of::<i64>() + size_of::<i64>() <= buffer.len() {
                    let value_offset = idx * size_of::<i64>();
                    let value_bytes = [
                        buffer[value_offset],
                        buffer[value_offset + 1],
                        buffer[value_offset + 2],
                        buffer[value_offset + 3],
                        buffer[value_offset + 4],
                        buffer[value_offset + 5],
                        buffer[value_offset + 6],
                        buffer[value_offset + 7],
                    ];
                    let value = i64::from_le_bytes(value_bytes);
                    values.push(value);
                } else {
                    // Default value for null or out-of-bounds
                    values.push(0);
                }

                idx += 1;
            }

            // Fill any remaining positions with defaults
            while idx < buffer.len() / size_of::<i64>() {
                validity_builder.push(false);
                values.push(0);
                idx += 1;
            }

            let column_buffer = Buffer::from(values);
            let column = Int64Type::upcast_column(column_buffer);

            // Wrap with nullable
            Ok(column.wrap_nullable(Some(validity_builder.into())))
        }
    }
}

/// Deserialize float32 values
fn deserialize_float32_values(values: &[u8], num_rows: usize) -> Result<Buffer<OrderedFloat<f32>>> {
    let mut result = Vec::with_capacity(num_rows);

    let mut input = values;
    for _ in 0..num_rows {
        if input.len() >= 4 {
            // Read 4 bytes for f32
            let mut buffer = [0u8; 4];
            buffer.copy_from_slice(&input[..4]);
            let value = f32::from_le_bytes(buffer);
            result.push(OrderedFloat(value));

            // Move forward in the input slice
            input = &input[4..];
        } else {
            // Not enough bytes, should not happen if the page is valid
            break;
        }
    }

    Ok(Buffer::from(result))
}

/// Deserialize float64 values
fn deserialize_float64_values(values: &[u8], num_rows: usize) -> Result<Buffer<OrderedFloat<f64>>> {
    let mut result = Vec::with_capacity(num_rows);

    let mut input = values;
    for _ in 0..num_rows {
        if input.len() >= 8 {
            // Read 8 bytes for f64
            let mut buffer = [0u8; 8];
            buffer.copy_from_slice(&input[..8]);
            let value = f64::from_le_bytes(buffer);
            result.push(OrderedFloat(value));

            // Move forward in the input slice
            input = &input[8..];
        } else {
            // Not enough bytes, should not happen if the page is valid
            break;
        }
    }

    Ok(Buffer::from(result))
}

/// Deserialize a float32 page into a column
pub fn deserialize_float32(page: &DataPage, validity: Option<PageValidity>) -> Result<Column> {
    let values = page.buffer();
    let values_count = page.num_values() as usize;
    let mut buffer: Vec<OrderedFloat<f32>> = Vec::with_capacity(values_count);

    match validity {
        Some(PageValidity::Required) => {
            // All values are required (non-null), just deserialize them all
            for chunk in values.chunks_exact(4) {
                let value = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                buffer.push(OrderedFloat(value));
            }
            // Create column from buffer
            Ok(Float32Type::upcast_column(Buffer::from(buffer)))
        }
        Some(PageValidity::Optional(mut bitmap_iter)) => {
            // Some values might be null
            let mut mutable_bitmap = MutableBitmap::with_capacity(values_count);
            let mut i = 0;
            let mut value_index = 0;

            while let Some(is_valid) = bitmap_iter.next() {
                if is_valid {
                    // Valid value
                    mutable_bitmap.push(true);
                    if value_index * 4 + 3 < values.len() {
                        let value = f32::from_le_bytes([
                            values[value_index * 4],
                            values[value_index * 4 + 1],
                            values[value_index * 4 + 2],
                            values[value_index * 4 + 3],
                        ]);
                        buffer.push(OrderedFloat(value));
                    } else {
                        // Handle case where buffer is shorter than expected
                        buffer.push(OrderedFloat(0.0));
                    }
                } else {
                    // Null value
                    mutable_bitmap.push(false);
                    buffer.push(OrderedFloat(0.0)); // Placeholder for null
                }
                i += 1;
                value_index += 1;
            }

            // Fill any remaining slots
            while i < values_count {
                mutable_bitmap.push(false);
                buffer.push(OrderedFloat(0.0)); // Placeholder for null
                i += 1;
            }

            // Create column from buffer and bitmap
            let column = Float32Type::upcast_column(Buffer::from(buffer));
            Ok(Column::wrap_nullable(column, Some(mutable_bitmap.into())))
        }
        Some(PageValidity::FilteredOptional(mut bitmap_iter)) => {
            // Some values might be null and some rows are filtered
            let mut mutable_bitmap = MutableBitmap::with_capacity(values_count);
            let mut i = 0;
            let mut value_index = 0;

            while let Some(is_valid) = bitmap_iter.next() {
                if is_valid {
                    // Valid value
                    mutable_bitmap.push(true);
                    if value_index * 4 + 3 < values.len() {
                        let value = f32::from_le_bytes([
                            values[value_index * 4],
                            values[value_index * 4 + 1],
                            values[value_index * 4 + 2],
                            values[value_index * 4 + 3],
                        ]);
                        buffer.push(OrderedFloat(value));
                    } else {
                        // Handle case where buffer is shorter than expected
                        buffer.push(OrderedFloat(0.0));
                    }
                } else {
                    // Null value
                    mutable_bitmap.push(false);
                    buffer.push(OrderedFloat(0.0)); // Placeholder for null
                }
                i += 1;
                value_index += 1;
            }

            // Fill any remaining slots
            while i < values_count {
                mutable_bitmap.push(false);
                buffer.push(OrderedFloat(0.0)); // Placeholder for null
                i += 1;
            }

            // Create column from buffer and bitmap
            let column = Float32Type::upcast_column(Buffer::from(buffer));
            Ok(Column::wrap_nullable(column, Some(mutable_bitmap.into())))
        }
        None => {
            // Default to required if no validity is provided
            for chunk in values.chunks_exact(4) {
                let value = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                buffer.push(OrderedFloat(value));
            }
            // Create column from buffer
            Ok(Float32Type::upcast_column(Buffer::from(buffer)))
        }
    }
}

/// Deserialize a float64 page into a column
pub fn deserialize_float64(page: &DataPage, validity: Option<PageValidity>) -> Result<Column> {
    let values = page.buffer();
    let values_count = page.num_values() as usize;
    let mut buffer: Vec<OrderedFloat<f64>> = Vec::with_capacity(values_count);

    match validity {
        Some(PageValidity::Required) => {
            // All values are required (non-null), just deserialize them all
            for chunk in values.chunks_exact(8) {
                let value = f64::from_le_bytes([
                    chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
                ]);
                buffer.push(OrderedFloat(value));
            }
            // Create column from buffer
            Ok(Float64Type::upcast_column(Buffer::from(buffer)))
        }
        Some(PageValidity::Optional(mut bitmap_iter)) => {
            // Some values might be null
            let mut mutable_bitmap = MutableBitmap::with_capacity(values_count);
            let mut i = 0;
            let mut value_index = 0;

            while let Some(is_valid) = bitmap_iter.next() {
                if is_valid {
                    // Valid value
                    mutable_bitmap.push(true);
                    if value_index * 8 + 7 < values.len() {
                        let value = f64::from_le_bytes([
                            values[value_index * 8],
                            values[value_index * 8 + 1],
                            values[value_index * 8 + 2],
                            values[value_index * 8 + 3],
                            values[value_index * 8 + 4],
                            values[value_index * 8 + 5],
                            values[value_index * 8 + 6],
                            values[value_index * 8 + 7],
                        ]);
                        buffer.push(OrderedFloat(value));
                    } else {
                        // Handle case where buffer is shorter than expected
                        buffer.push(OrderedFloat(0.0));
                    }
                } else {
                    // Null value
                    mutable_bitmap.push(false);
                    buffer.push(OrderedFloat(0.0)); // Placeholder for null
                }
                i += 1;
                value_index += 1;
            }

            // Fill any remaining slots
            while i < values_count {
                mutable_bitmap.push(false);
                buffer.push(OrderedFloat(0.0)); // Placeholder for null
                i += 1;
            }

            // Create column from buffer and bitmap
            let column = Float64Type::upcast_column(Buffer::from(buffer));
            Ok(Column::wrap_nullable(column, Some(mutable_bitmap.into())))
        }
        Some(PageValidity::FilteredOptional(mut bitmap_iter)) => {
            // Some values might be null and some rows are filtered
            let mut mutable_bitmap = MutableBitmap::with_capacity(values_count);
            let mut i = 0;
            let mut value_index = 0;

            while let Some(is_valid) = bitmap_iter.next() {
                if is_valid {
                    // Valid value
                    mutable_bitmap.push(true);
                    if value_index * 8 + 7 < values.len() {
                        let value = f64::from_le_bytes([
                            values[value_index * 8],
                            values[value_index * 8 + 1],
                            values[value_index * 8 + 2],
                            values[value_index * 8 + 3],
                            values[value_index * 8 + 4],
                            values[value_index * 8 + 5],
                            values[value_index * 8 + 6],
                            values[value_index * 8 + 7],
                        ]);
                        buffer.push(OrderedFloat(value));
                    } else {
                        // Handle case where buffer is shorter than expected
                        buffer.push(OrderedFloat(0.0));
                    }
                } else {
                    // Null value
                    mutable_bitmap.push(false);
                    buffer.push(OrderedFloat(0.0)); // Placeholder for null
                }
                i += 1;
                value_index += 1;
            }

            // Fill any remaining slots
            while i < values_count {
                mutable_bitmap.push(false);
                buffer.push(OrderedFloat(0.0)); // Placeholder for null
                i += 1;
            }

            // Create column from buffer and bitmap
            let column = Float64Type::upcast_column(Buffer::from(buffer));
            Ok(Column::wrap_nullable(column, Some(mutable_bitmap.into())))
        }
        None => {
            // Default to required if no validity is provided
            for chunk in values.chunks_exact(8) {
                let value = f64::from_le_bytes([
                    chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
                ]);
                buffer.push(OrderedFloat(value));
            }
            // Create column from buffer
            Ok(Float64Type::upcast_column(Buffer::from(buffer)))
        }
    }
}

/// Deserialize a ByteArray (String) page directly to a Databend Column::String
fn deserialize_string(page: &DataPage, validity: Option<PageValidity>) -> Result<Column> {
    let buffer = page.buffer();

    match validity {
        None => {
            // All values are non-null
            let mut builder = databend_common_expression::ColumnBuilder::with_capacity(
                &DataType::String,
                buffer.len(),
            );
            let mut offset = 0;

            for _ in 0..buffer.len() {
                if offset >= buffer.len() {
                    // Empty string for out of bounds
                    builder.push(ScalarRef::String(""));
                    continue;
                }

                // Read length (4 bytes, little endian)
                if offset + 4 > buffer.len() {
                    // Not enough bytes for length, push empty string
                    builder.push(ScalarRef::String(""));
                    continue;
                }

                let len_bytes = [
                    buffer[offset],
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                ];
                let len = u32::from_le_bytes(len_bytes) as usize;
                offset += 4;

                // Read string data
                if offset + len > buffer.len() {
                    // Not enough bytes for string data, push empty string
                    builder.push(ScalarRef::String(""));
                    continue;
                }

                // Convert bytes to string
                match std::str::from_utf8(&buffer[offset..offset + len]) {
                    Ok(s) => {
                        builder.push(ScalarRef::String(s));
                    }
                    Err(_) => {
                        // Invalid UTF-8, push empty string
                        builder.push(ScalarRef::String(""));
                    }
                }

                offset += len;
            }

            Ok(builder.build())
        }
        Some(PageValidity::Required) => {
            // All values are non-null
            let mut builder = databend_common_expression::ColumnBuilder::with_capacity(
                &DataType::String,
                buffer.len(),
            );
            let mut offset = 0;

            for _ in 0..buffer.len() {
                if offset >= buffer.len() {
                    // Empty string for out of bounds
                    builder.push(ScalarRef::String(""));
                    continue;
                }

                // Read length (4 bytes, little endian)
                if offset + 4 > buffer.len() {
                    // Not enough bytes for length, push empty string
                    builder.push(ScalarRef::String(""));
                    continue;
                }

                let len_bytes = [
                    buffer[offset],
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                ];
                let len = u32::from_le_bytes(len_bytes) as usize;
                offset += 4;

                // Read string data
                if offset + len > buffer.len() {
                    // Not enough bytes for string data, push empty string
                    builder.push(ScalarRef::String(""));
                    continue;
                }

                // Convert bytes to string
                match std::str::from_utf8(&buffer[offset..offset + len]) {
                    Ok(s) => {
                        builder.push(ScalarRef::String(s));
                    }
                    Err(_) => {
                        // Invalid UTF-8, push empty string
                        builder.push(ScalarRef::String(""));
                    }
                }

                offset += len;
            }

            Ok(builder.build())
        }
        Some(PageValidity::Optional(mut null_bitmap)) => {
            // Values might be null
            let mut builder = databend_common_expression::ColumnBuilder::with_capacity(
                &DataType::String,
                buffer.len(),
            );
            let mut offset = 0;
            let mut idx = 0;

            while let Some(is_valid) = null_bitmap.next() {
                if !is_valid {
                    // This is a null value
                    builder.push(ScalarRef::Null);
                    idx += 1;
                    continue;
                }

                if offset >= buffer.len() {
                    // Empty string for out of bounds
                    builder.push(ScalarRef::String(""));
                    idx += 1;
                    continue;
                }

                // Read length (4 bytes, little endian)
                if offset + 4 > buffer.len() {
                    // Not enough bytes for length, push empty string
                    builder.push(ScalarRef::String(""));
                    idx += 1;
                    continue;
                }

                let len_bytes = [
                    buffer[offset],
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                ];
                let len = u32::from_le_bytes(len_bytes) as usize;
                offset += 4;

                // Read string data
                if offset + len > buffer.len() {
                    // Not enough bytes for string data, push empty string
                    builder.push(ScalarRef::String(""));
                    idx += 1;
                    continue;
                }

                // Convert bytes to string
                match std::str::from_utf8(&buffer[offset..offset + len]) {
                    Ok(s) => {
                        builder.push(ScalarRef::String(s));
                    }
                    Err(_) => {
                        // Invalid UTF-8, push empty string
                        builder.push(ScalarRef::String(""));
                    }
                }

                offset += len;
                idx += 1;
            }

            // Fill any remaining positions with nulls
            while idx < buffer.len() {
                builder.push(ScalarRef::Null);
                idx += 1;
            }

            Ok(builder.build())
        }
        Some(PageValidity::FilteredOptional(mut filtered_iter)) => {
            // Values might be null and filtered
            let mut builder = databend_common_expression::ColumnBuilder::with_capacity(
                &DataType::String,
                buffer.len(),
            );
            let mut offset = 0;
            let mut idx = 0;

            while let Some(is_valid) = filtered_iter.next() {
                if !is_valid {
                    // This is a null value
                    builder.push(ScalarRef::Null);
                    idx += 1;
                    continue;
                }

                if offset >= buffer.len() {
                    // Empty string for out of bounds
                    builder.push(ScalarRef::String(""));
                    idx += 1;
                    continue;
                }

                // Read length (4 bytes, little endian)
                if offset + 4 > buffer.len() {
                    // Not enough bytes for length, push empty string
                    builder.push(ScalarRef::String(""));
                    idx += 1;
                    continue;
                }

                let len_bytes = [
                    buffer[offset],
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                ];
                let len = u32::from_le_bytes(len_bytes) as usize;
                offset += 4;

                // Read string data
                if offset + len > buffer.len() {
                    // Not enough bytes for string data, push empty string
                    builder.push(ScalarRef::String(""));
                    idx += 1;
                    continue;
                }

                // Convert bytes to string
                match std::str::from_utf8(&buffer[offset..offset + len]) {
                    Ok(s) => {
                        builder.push(ScalarRef::String(s));
                    }
                    Err(_) => {
                        // Invalid UTF-8, push empty string
                        builder.push(ScalarRef::String(""));
                    }
                }

                offset += len;
                idx += 1;
            }

            // Fill any remaining positions with nulls
            while idx < buffer.len() {
                builder.push(ScalarRef::Null);
                idx += 1;
            }

            Ok(builder.build())
        }
    }
}
