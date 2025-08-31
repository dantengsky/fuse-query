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

//! Core traits for Parquet column processing

use databend_common_exception::Result;
use parquet2::schema::types::PhysicalType;

// Float wrapper types (import these since they're commonly available)
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;

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

impl ParquetPhysicalMapping for F32 {
    const PHYSICAL_SIZE: usize = 4;
    const TARGET_SIZE: usize = 4;
} // Float -> F32
impl ParquetPhysicalMapping for F64 {
    const PHYSICAL_SIZE: usize = 8;
    const TARGET_SIZE: usize = 8;
} // Double -> F64

// Note: Date, Decimal, Boolean types implement ParquetPhysicalMapping in their own modules

/// Core trait for Parquet column types
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