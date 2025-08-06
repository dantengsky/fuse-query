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

//! Number column deserialization for Parquet data
//!
//! This module provides efficient deserialization of integer columns (Int32, Int64)
//! from Parquet format, with support for nullable columns, definition level processing,
//! and performance optimizations.

use databend_common_column::buffer::Buffer;
use databend_common_expression::types::Number;
use databend_common_expression::Column;
use parquet2::schema::types::PhysicalType;

use crate::column::common::ParquetColumnIterator;
use crate::column::common::ParquetColumnType;
use crate::wip::decompressor::Decompressor;

// =============================================================================
// Trait Definitions
// =============================================================================

/// Metadata for integer columns (currently empty, but allows for future extensions)
#[derive(Clone, Copy)]
pub struct IntegerMetadata;

// =============================================================================
// Trait Implementations
// =============================================================================

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

// =============================================================================
// Iterator Type Aliases
// =============================================================================

/// Iterator for reading i32 values from Parquet pages
pub type Int32Iter<'a> = ParquetColumnIterator<'a, i32>;

/// Iterator for reading i64 values from Parquet pages  
pub type Int64Iter<'a> = ParquetColumnIterator<'a, i64>;

// =============================================================================
// Convenience Constructor Functions
// =============================================================================

/// Create a new i32 iterator
pub fn new_int32_iter<'a>(
    pages: Decompressor<'a>,
    num_rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> Int32Iter<'a> {
    ParquetColumnIterator::new(pages, num_rows, is_nullable, IntegerMetadata, chunk_size)
}

/// Create a new i64 iterator
pub fn new_int64_iter<'a>(
    pages: Decompressor<'a>,
    num_rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> Int64Iter<'a> {
    ParquetColumnIterator::new(pages, num_rows, is_nullable, IntegerMetadata, chunk_size)
}
