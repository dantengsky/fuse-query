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

//! Decimal column deserialization for Parquet data
//!
//! This module provides efficient deserialization of decimal columns (Decimal64, Decimal128, Decimal256)
//! from Parquet format, with support for nullable columns, definition level processing,
//! and performance optimizations.

use databend_common_column::buffer::Buffer;
use databend_common_expression::types::i256;
use databend_common_expression::types::DecimalColumn;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::Column;
use parquet2::schema::types::PhysicalType;

use crate::column::common::ParquetColumnIterator;
use crate::column::common::ParquetColumnType;
use crate::wip::decompressor::Decompressor;

// =============================================================================
// Wrapper Types for Decimal Usage
// =============================================================================

/// Wrapper type for i64 when used as Decimal64
///
/// Using #[repr(transparent)] ensures this has the same memory layout as i64,
/// allowing for zero-cost transmute operations.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Decimal64(pub i64);

/// Wrapper type for i128 when used as Decimal128  
///
/// Using #[repr(transparent)] ensures this has the same memory layout as i128,
/// allowing for zero-cost transmute operations.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Decimal128(pub i128);

/// Wrapper type for i256 when used as Decimal256
///
/// Using #[repr(transparent)] ensures this has the same memory layout as i256,
/// allowing for zero-cost transmute operations.
#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct Decimal256(pub i256);

// =============================================================================
// Trait Definitions
// =============================================================================

/// Trait for types that can be used as Parquet decimal values
pub trait ParquetDecimal: Copy + Send + Sync + 'static {
    /// The Parquet physical type for this decimal type
    const PHYSICAL_TYPE: PhysicalType;

    /// Create a column from the deserialized data
    fn create_column(data: Vec<Self>, precision: u8, scale: u8) -> Column;
}

impl ParquetDecimal for Decimal64 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int64;

    fn create_column(data: Vec<Self>, precision: u8, scale: u8) -> Column {
        let decimal_size = DecimalSize::new_unchecked(precision, scale);
        // Zero-cost transmute: Vec<Decimal64> -> Vec<i64>
        // Safe because Decimal64 is #[repr(transparent)] over i64
        let raw_data: Vec<i64> = unsafe { std::mem::transmute(data) };
        Column::Decimal(DecimalColumn::Decimal64(
            Buffer::from(raw_data),
            decimal_size,
        ))
    }
}

impl ParquetDecimal for Decimal128 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::FixedLenByteArray(16);

    fn create_column(data: Vec<Self>, precision: u8, scale: u8) -> Column {
        let decimal_size = DecimalSize::new_unchecked(precision, scale);
        // Zero-cost transmute: Vec<Decimal128> -> Vec<i128>
        // Safe because Decimal128 is #[repr(transparent)] over i128
        let raw_data: Vec<i128> = unsafe { std::mem::transmute(data) };
        Column::Decimal(DecimalColumn::Decimal128(
            Buffer::from(raw_data),
            decimal_size,
        ))
    }
}

impl ParquetDecimal for Decimal256 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::FixedLenByteArray(32);

    fn create_column(data: Vec<Self>, precision: u8, scale: u8) -> Column {
        let decimal_size = DecimalSize::new_unchecked(precision, scale);
        // Zero-cost transmute: Vec<Decimal256> -> Vec<i256>
        // Safe because Decimal256 is #[repr(transparent)] over i256
        let raw_data: Vec<i256> = unsafe { std::mem::transmute(data) };
        Column::Decimal(DecimalColumn::Decimal256(
            Buffer::from(raw_data),
            decimal_size,
        ))
    }
}

// =============================================================================
// ParquetColumnType Implementation for Decimal Types
// =============================================================================

/// Metadata for decimal types (precision and scale)
#[derive(Clone)]
pub struct DecimalMetadata {
    pub precision: u8,
    pub scale: u8,
}

impl ParquetColumnType for Decimal64 {
    type Metadata = DecimalMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int64;

    fn create_column(data: Vec<Self>, metadata: &Self::Metadata) -> Column {
        let decimal_size = DecimalSize::new_unchecked(metadata.precision, metadata.scale);
        // Zero-cost transmute: Vec<Decimal64> -> Vec<i64>
        // Safe because Decimal64 is #[repr(transparent)] over i64
        let raw_data: Vec<i64> = unsafe { std::mem::transmute(data) };
        Column::Decimal(DecimalColumn::Decimal64(
            Buffer::from(raw_data),
            decimal_size,
        ))
    }
}

impl ParquetColumnType for Decimal128 {
    type Metadata = DecimalMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::FixedLenByteArray(16);

    fn create_column(data: Vec<Self>, metadata: &Self::Metadata) -> Column {
        let decimal_size = DecimalSize::new_unchecked(metadata.precision, metadata.scale);
        // Zero-cost transmute: Vec<Decimal128> -> Vec<i128>
        // Safe because Decimal128 is #[repr(transparent)] over i128
        let raw_data: Vec<i128> = unsafe { std::mem::transmute(data) };
        Column::Decimal(DecimalColumn::Decimal128(
            Buffer::from(raw_data),
            decimal_size,
        ))
    }
}

impl ParquetColumnType for Decimal256 {
    type Metadata = DecimalMetadata;
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::FixedLenByteArray(32);

    fn create_column(data: Vec<Self>, metadata: &Self::Metadata) -> Column {
        let decimal_size = DecimalSize::new_unchecked(metadata.precision, metadata.scale);
        // Zero-cost transmute: Vec<Decimal256> -> Vec<i256>
        // Safe because Decimal256 is #[repr(transparent)] over i256
        let raw_data: Vec<i256> = unsafe { std::mem::transmute(data) };
        Column::Decimal(DecimalColumn::Decimal256(
            Buffer::from(raw_data),
            decimal_size,
        ))
    }
}

// =============================================================================
// Iterator Type Aliases
// =============================================================================

/// Generic iterator for reading decimal values from Parquet pages
pub type DecimalIter<'a, T> = ParquetColumnIterator<'a, T>;

// =============================================================================
// Constructor Functions
// =============================================================================

/// Create a new decimal iterator for any decimal type
///
/// This generic function replaces the individual type-specific constructors,
/// eliminating code duplication while maintaining type safety.
pub fn new_decimal_iter<T>(
    pages: Decompressor,
    num_rows: usize,
    precision: u8,
    scale: u8,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> DecimalIter<T>
where
    T: ParquetColumnType<Metadata = DecimalMetadata>,
{
    let metadata = DecimalMetadata { precision, scale };
    ParquetColumnIterator::new(pages, num_rows, is_nullable, metadata, chunk_size)
}

/// Create a new decimal iterator for i64 (Decimal64)
///
/// Convenience wrapper around the generic function for backward compatibility.
pub fn new_decimal64_iter(
    pages: Decompressor,
    num_rows: usize,
    precision: u8,
    scale: u8,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> DecimalIter<Decimal64> {
    new_decimal_iter(pages, num_rows, precision, scale, is_nullable, chunk_size)
}

/// Create a new decimal iterator for i128 (Decimal128)
///
/// Convenience wrapper around the generic function for backward compatibility.
pub fn new_decimal128_iter(
    pages: Decompressor,
    num_rows: usize,
    precision: u8,
    scale: u8,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> DecimalIter<Decimal128> {
    new_decimal_iter(pages, num_rows, precision, scale, is_nullable, chunk_size)
}

/// Create a new decimal iterator for i256 (Decimal256)
///
/// Convenience wrapper around the generic function for backward compatibility.
pub fn new_decimal256_iter(
    pages: Decompressor,
    num_rows: usize,
    precision: u8,
    scale: u8,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> DecimalIter<Decimal256> {
    new_decimal_iter(pages, num_rows, precision, scale, is_nullable, chunk_size)
}
