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
use databend_common_expression::types::Number;
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
use databend_common_expression::Column;
use parquet2::schema::types::PhysicalType;

use crate::column::{DictionarySupport, ParquetColumnType};
use crate::column::common::ParquetColumnIterator;
use crate::reader::decompressor::Decompressor;

#[derive(Clone, Copy)]
pub struct IntegerMetadata;

// ===== Signed Integer Types =====

impl ParquetColumnType for i8 {
    type Metadata = IntegerMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int32; // Parquet stores i8 as Int32

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        Column::Number(i8::upcast_column(Buffer::from(data)))
    }
}

impl ParquetColumnType for i16 {
    type Metadata = IntegerMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int32; // Parquet stores i16 as Int32

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        Column::Number(i16::upcast_column(Buffer::from(data)))
    }
}

impl ParquetColumnType for i32 {
    type Metadata = IntegerMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int32;

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        Column::Number(i32::upcast_column(Buffer::from(data)))
    }
}

impl ParquetColumnType for i64 {
    type Metadata = IntegerMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int64;

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        Column::Number(i64::upcast_column(Buffer::from(data)))
    }
}

// ===== Unsigned Integer Types =====

impl ParquetColumnType for u8 {
    type Metadata = IntegerMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int32; // Parquet stores u8 as Int32

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        Column::Number(u8::upcast_column(Buffer::from(data)))
    }
}

impl ParquetColumnType for u16 {
    type Metadata = IntegerMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int32; // Parquet stores u16 as Int32

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        Column::Number(u16::upcast_column(Buffer::from(data)))
    }
}

impl ParquetColumnType for u32 {
    type Metadata = IntegerMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int32; // Parquet stores u32 as Int32 with reinterpret casting

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        Column::Number(u32::upcast_column(Buffer::from(data)))
    }
}

impl ParquetColumnType for u64 {
    type Metadata = IntegerMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int64; // Store as signed i64, check for overflow

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        Column::Number(u64::upcast_column(Buffer::from(data)))
    }
}

// ===== Float Types =====

impl ParquetColumnType for F32 {
    type Metadata = IntegerMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Float;

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        Column::Number(F32::upcast_column(Buffer::from(data)))
    }
}

impl ParquetColumnType for F64 {
    type Metadata = IntegerMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Double;

    fn create_column(data: Vec<Self>, _metadata: &Self::Metadata) -> Column {
        Column::Number(F64::upcast_column(Buffer::from(data)))
    }
}

// ===== Dictionary Support for Signed Integer Types =====

impl DictionarySupport for i8 {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        // i8 is stored as Int32 in Parquet, but we only use the least significant byte
        if entry.len() != 4 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid i8 dictionary entry length: expected 4, got {}",
                entry.len()
            )));
        }

        let i32_value = i32::from_le_bytes(entry.try_into().unwrap());
        if i32_value < i8::MIN as i32 || i32_value > i8::MAX as i32 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "i8 overflow: value {} out of range [{}, {}]",
                i32_value,
                i8::MIN,
                i8::MAX
            )));
        }

        Ok(i32_value as i8)
    }
}

impl DictionarySupport for i16 {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        // i16 is stored as Int32 in Parquet
        if entry.len() != 4 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid i16 dictionary entry length: expected 4, got {}",
                entry.len()
            )));
        }

        let i32_value = i32::from_le_bytes(entry.try_into().unwrap());
        if i32_value < i16::MIN as i32 || i32_value > i16::MAX as i32 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "i16 overflow: value {} out of range [{}, {}]",
                i32_value,
                i16::MIN,
                i16::MAX
            )));
        }

        Ok(i32_value as i16)
    }
}

impl DictionarySupport for i32 {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        if entry.len() != 4 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid i32 dictionary entry length: expected 4, got {}",
                entry.len()
            )));
        }

        // Parquet stores integers in little-endian format
        let bytes: [u8; 4] = entry.try_into().map_err(|_| {
            databend_common_exception::ErrorCode::Internal(
                "Failed to convert bytes to i32".to_string(),
            )
        })?;

        Ok(i32::from_le_bytes(bytes))
    }
}

impl DictionarySupport for i64 {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        if entry.len() != 8 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid i64 dictionary entry length: expected 8, got {}",
                entry.len()
            )));
        }

        // Parquet stores integers in little-endian format
        let bytes: [u8; 8] = entry.try_into().map_err(|_| {
            databend_common_exception::ErrorCode::Internal(
                "Failed to convert bytes to i64".to_string(),
            )
        })?;

        Ok(i64::from_le_bytes(bytes))
    }
}

// ===== Dictionary Support for Unsigned Integer Types =====

impl DictionarySupport for u8 {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        // u8 is stored as Int32 in Parquet
        if entry.len() != 4 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid u8 dictionary entry length: expected 4, got {}",
                entry.len()
            )));
        }

        let i32_value = i32::from_le_bytes(entry.try_into().unwrap());
        if i32_value < 0 || i32_value > u8::MAX as i32 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "u8 overflow: value {} out of range [0, {}]",
                i32_value,
                u8::MAX
            )));
        }

        Ok(i32_value as u8)
    }
}

impl DictionarySupport for u16 {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        // u16 is stored as Int32 in Parquet
        if entry.len() != 4 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid u16 dictionary entry length: expected 4, got {}",
                entry.len()
            )));
        }

        let i32_value = i32::from_le_bytes(entry.try_into().unwrap());
        if i32_value < 0 || i32_value > u16::MAX as i32 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "u16 overflow: value {} out of range [0, {}]",
                i32_value,
                u16::MAX
            )));
        }

        Ok(i32_value as u16)
    }
}

impl DictionarySupport for u32 {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        // u32 is stored as Int32 in Parquet with reinterpret casting
        if entry.len() != 4 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid u32 dictionary entry length: expected 4, got {}",
                entry.len()
            )));
        }

        let i32_value = i32::from_le_bytes(entry.try_into().unwrap());
        // Reinterpret i32 bits as u32 (no range checking needed)
        Ok(i32_value as u32)
    }
}

impl DictionarySupport for u64 {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        // u64 is stored as Int64 in Parquet, we need to handle the sign bit carefully
        if entry.len() != 8 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid u64 dictionary entry length: expected 8, got {}",
                entry.len()
            )));
        }

        let i64_value = i64::from_le_bytes(entry.try_into().unwrap());
        // For u64, we interpret the i64 bits as u64 (two's complement interpretation)
        Ok(i64_value as u64)
    }
}

// ===== Dictionary Support for Float Types =====

impl DictionarySupport for F32 {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        if entry.len() != 4 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid f32 dictionary entry length: expected 4, got {}",
                entry.len()
            )));
        }

        // Parquet stores floats in IEEE 754 little-endian format
        let bytes: [u8; 4] = entry.try_into().map_err(|_| {
            databend_common_exception::ErrorCode::Internal(
                "Failed to convert bytes to f32".to_string(),
            )
        })?;

        Ok(F32::from(f32::from_le_bytes(bytes)))
    }
}

impl DictionarySupport for F64 {
    fn from_dictionary_entry(entry: &[u8]) -> databend_common_exception::Result<Self> {
        if entry.len() != 8 {
            return Err(databend_common_exception::ErrorCode::Internal(format!(
                "Invalid f64 dictionary entry length: expected 8, got {}",
                entry.len()
            )));
        }

        // Parquet stores doubles in IEEE 754 little-endian format
        let bytes: [u8; 8] = entry.try_into().map_err(|_| {
            databend_common_exception::ErrorCode::Internal(
                "Failed to convert bytes to f64".to_string(),
            )
        })?;

        Ok(F64::from(f64::from_le_bytes(bytes)))
    }
}

// ===== Iterator Type Definitions =====

// Signed Integer Iterators
pub type Int8Iter<'a> = ParquetColumnIterator<'a, i8>;
pub type Int16Iter<'a> = ParquetColumnIterator<'a, i16>;
pub type Int32Iter<'a> = ParquetColumnIterator<'a, i32>;
pub type Int64Iter<'a> = ParquetColumnIterator<'a, i64>;

// Unsigned Integer Iterators
pub type UInt8Iter<'a> = ParquetColumnIterator<'a, u8>;
pub type UInt16Iter<'a> = ParquetColumnIterator<'a, u16>;
pub type UInt32Iter<'a> = ParquetColumnIterator<'a, u32>;
pub type UInt64Iter<'a> = ParquetColumnIterator<'a, u64>;

// Float Iterators
pub type Float32Iter<'a> = ParquetColumnIterator<'a, F32>;
pub type Float64Iter<'a> = ParquetColumnIterator<'a, F64>;

// ===== Iterator Constructor Functions =====

// Signed Integer Iterator Constructors
pub fn new_int8_iter(
    pages: Decompressor,
    num_rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> Int8Iter {
    ParquetColumnIterator::new(pages, num_rows, is_nullable, IntegerMetadata, chunk_size)
}

pub fn new_int16_iter(
    pages: Decompressor,
    num_rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> Int16Iter {
    ParquetColumnIterator::new(pages, num_rows, is_nullable, IntegerMetadata, chunk_size)
}

pub fn new_int32_iter(
    pages: Decompressor,
    num_rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> Int32Iter {
    ParquetColumnIterator::new(pages, num_rows, is_nullable, IntegerMetadata, chunk_size)
}

pub fn new_int64_iter(
    pages: Decompressor,
    num_rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> Int64Iter {
    ParquetColumnIterator::new(pages, num_rows, is_nullable, IntegerMetadata, chunk_size)
}

// Unsigned Integer Iterator Constructors
pub fn new_uint8_iter(
    pages: Decompressor,
    num_rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> UInt8Iter {
    ParquetColumnIterator::new(pages, num_rows, is_nullable, IntegerMetadata, chunk_size)
}

pub fn new_uint16_iter(
    pages: Decompressor,
    num_rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> UInt16Iter {
    ParquetColumnIterator::new(pages, num_rows, is_nullable, IntegerMetadata, chunk_size)
}

pub fn new_uint32_iter(
    pages: Decompressor,
    num_rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> UInt32Iter {
    ParquetColumnIterator::new(pages, num_rows, is_nullable, IntegerMetadata, chunk_size)
}

pub fn new_uint64_iter(
    pages: Decompressor,
    num_rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> UInt64Iter {
    ParquetColumnIterator::new(pages, num_rows, is_nullable, IntegerMetadata, chunk_size)
}

// Float Iterator Constructors
pub fn new_float32_iter(
    pages: Decompressor,
    num_rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> Float32Iter {
    ParquetColumnIterator::new(pages, num_rows, is_nullable, IntegerMetadata, chunk_size)
}

pub fn new_float64_iter(
    pages: Decompressor,
    num_rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> Float64Iter {
    ParquetColumnIterator::new(pages, num_rows, is_nullable, IntegerMetadata, chunk_size)
}

#[cfg(test)]
mod tests {
    use databend_common_exception::Result;

    use super::*;

    #[test]
    fn test_i32_dictionary_support() -> Result<()> {
        // Test from_dictionary_entry
        let entry = [42u8, 0, 0, 0]; // 42 in little-endian
        let value = i32::from_dictionary_entry(&entry)?;
        assert_eq!(value, 42);

        // Test negative number
        let entry = [255u8, 255, 255, 255]; // -1 in little-endian
        let value = i32::from_dictionary_entry(&entry)?;
        assert_eq!(value, -1);

        // Test invalid entry size
        let entry = [42u8, 0, 0]; // Only 3 bytes
        let result = i32::from_dictionary_entry(&entry);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_i64_dictionary_support() -> Result<()> {
        // Test from_dictionary_entry
        let entry = [42u8, 0, 0, 0, 0, 0, 0, 0]; // 42 in little-endian
        let value = i64::from_dictionary_entry(&entry)?;
        assert_eq!(value, 42);

        // Test large number
        let entry = [255u8, 255, 255, 255, 255, 255, 255, 127]; // i64::MAX
        let value = i64::from_dictionary_entry(&entry)?;
        assert_eq!(value, i64::MAX);

        // Test invalid entry size
        let entry = [42u8, 0, 0, 0, 0, 0, 0]; // Only 7 bytes
        let result = i64::from_dictionary_entry(&entry);
        assert!(result.is_err());

        Ok(())
    }
}
