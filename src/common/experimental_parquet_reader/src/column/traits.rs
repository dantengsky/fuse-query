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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
// Float wrapper types (import these since they're commonly available)
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
use databend_common_expression::Column;
use parquet2::schema::types::PhysicalType;

use crate::column::levels::LevelInfo;

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

/// Extended trait for column iterators that can provide level information
/// This trait enables nested type support by exposing definition and repetition levels
pub trait ColumnIteratorWithLevels: Iterator<Item = Result<Column>> {
    /// Get the current level information
    /// 
    /// Returns the definition and repetition levels for the current batch of values.
    /// This is required for nested type processing.
    fn current_levels(&self) -> Option<&LevelInfo>;

    /// Check if this iterator provides level information
    fn has_levels(&self) -> bool {
        self.current_levels().is_some()
    }
    
    /// Get maximum definition level for this column
    fn max_def_level(&self) -> u16;
    
    /// Get maximum repetition level for this column
    fn max_rep_level(&self) -> u16;
    
    /// Check if this column requires definition levels (has nullable components)
    fn requires_def_levels(&self) -> bool {
        self.max_def_level() > 0
    }
    
    /// Check if this column requires repetition levels (has repeated components)
    fn requires_rep_levels(&self) -> bool {
        self.max_rep_level() > 0
    }
}

/// Wrapper to add level information to any column iterator
/// This allows existing iterators to be upgraded to support levels without breaking changes
pub struct ColumnIteratorLevels<I> {
    /// The underlying column iterator
    inner: I,
    /// Current level information
    current_levels: Option<LevelInfo>,
    /// Maximum definition level
    max_def_level: u16,
    /// Maximum repetition level
    max_rep_level: u16,
}

impl<I> ColumnIteratorLevels<I> {
    /// Create a new leveled iterator wrapper
    pub fn new(inner: I, max_def_level: u16, max_rep_level: u16) -> Self {
        Self {
            inner,
            current_levels: None,
            max_def_level,
            max_rep_level,
        }
    }

    /// Update the current level information
    pub fn set_current_levels(&mut self, levels: LevelInfo) {
        self.current_levels = Some(levels);
    }

    /// Clear current level information
    pub fn clear_levels(&mut self) {
        self.current_levels = None;
    }

    /// Get a reference to the inner iterator
    pub fn inner(&self) -> &I {
        &self.inner
    }

    /// Get a mutable reference to the inner iterator
    pub fn inner_mut(&mut self) -> &mut I {
        &mut self.inner
    }

    /// Consume this wrapper and return the inner iterator
    pub fn into_inner(self) -> I {
        self.inner
    }
}

impl<I> Iterator for ColumnIteratorLevels<I>
where
    I: Iterator<Item = Result<Column>>,
{
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<I> ColumnIteratorWithLevels for ColumnIteratorLevels<I>
where
    I: Iterator<Item = Result<Column>>,
{
    fn current_levels(&self) -> Option<&LevelInfo> {
        self.current_levels.as_ref()
    }

    fn max_def_level(&self) -> u16 {
        self.max_def_level
    }

    fn max_rep_level(&self) -> u16 {
        self.max_rep_level
    }
}

/// Extension trait to convert any column iterator to support levels
/// This provides a convenient way to upgrade existing iterators
pub trait ColumnIteratorExt: Iterator<Item = Result<Column>> + Sized {
    /// Add level support to this iterator
    fn with_levels(self, max_def_level: u16, max_rep_level: u16) -> ColumnIteratorLevels<Self> {
        ColumnIteratorLevels::new(self, max_def_level, max_rep_level)
    }
}

// Blanket implementation for all compatible iterators
impl<I> ColumnIteratorExt for I
where
    I: Iterator<Item = Result<Column>>,
{
}
