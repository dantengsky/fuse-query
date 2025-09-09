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

//! Arrow-inspired unified ArrayReader trait for parquet column reading
//! 
//! This implements the revolutionary design patterns from Apache Arrow:
//! - Uniform interface with def/rep level exposure
//! - Composition-based architecture
//! - Level-driven state management
//! 
//! This replaces all our complex factory/dispatch systems with elegant simplicity.

use std::any::Any;
use databend_common_exception::Result;
use databend_common_expression::Column;

/// Unified trait for reading parquet columns into Databend columns
/// 
/// This mirrors Apache Arrow's ArrayReader trait, providing:
/// - Uniform interface for all column types (primitive, array, tuple, etc.)
/// - Level information exposure for parent readers
/// - Composable architecture for nested types
pub trait ColumnArrayReader: Send {
    /// Returns the reader as Any for downcasting if needed
    fn as_any(&self) -> &dyn Any;
    
    /// Read at most `batch_size` records into internal buffers
    /// 
    /// Returns the number of records actually read, which may be less than
    /// `batch_size` if pages are exhausted.
    fn read_records(&mut self, batch_size: usize) -> Result<usize>;
    
    /// Consume all buffered data and return as a Databend Column
    /// 
    /// This produces the final column after all level processing is complete.
    fn consume_batch(&mut self) -> Result<Column>;
    
    /// Skip `num_records` without reading them into buffers
    /// 
    /// Returns the actual number of records skipped.
    fn skip_records(&mut self, num_records: usize) -> Result<usize>;
    
    /// Get definition levels from the last read batch
    /// 
    /// Definition levels indicate which values are null/missing at each nesting level.
    /// Returns None for required (non-nullable) primitive columns.
    /// 
    /// This is crucial for parent readers to determine their null bitmaps.
    fn get_def_levels(&self) -> Option<&[i16]>;
    
    /// Get repetition levels from the last read batch
    /// 
    /// Repetition levels indicate list/array boundaries at each nesting level.
    /// Returns None for non-repeated (scalar) columns.
    /// 
    /// This is crucial for parent readers to determine their array offsets.
    fn get_rep_levels(&self) -> Option<&[i16]>;
    
    /// Convenience method: read batch_size records and consume them
    /// 
    /// This is equivalent to read_records() followed by consume_batch().
    fn next_batch(&mut self, batch_size: usize) -> Result<Column> {
        self.read_records(batch_size)?;
        self.consume_batch()
    }
}

/// Level information for a parquet column
/// 
/// This encapsulates the def/rep level concept from Apache Arrow,
/// which is the key to handling all nested structures uniformly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LevelInfo {
    /// Definition level for this column
    /// 
    /// - 0: Required field (always present)  
    /// - 1: Optional field (may be null)
    /// - 2+: Nested optional fields or list elements
    pub def_level: i16,
    
    /// Repetition level for this column
    /// 
    /// - 0: Scalar value (not repeated)
    /// - 1: Element of top-level list/array
    /// - 2+: Element of nested list/array
    pub rep_level: i16,
    
    /// Whether this field can be null
    pub nullable: bool,
}

impl LevelInfo {
    /// Create level info for a required (non-nullable) primitive field
    pub fn required() -> Self {
        Self {
            def_level: 0,
            rep_level: 0, 
            nullable: false,
        }
    }
    
    /// Create level info for an optional (nullable) primitive field
    pub fn optional() -> Self {
        Self {
            def_level: 1,
            rep_level: 0,
            nullable: true,
        }
    }
    
    /// Create level info for a list element
    pub fn list_element(parent: LevelInfo, element_nullable: bool) -> Self {
        Self {
            def_level: parent.def_level + if element_nullable { 2 } else { 1 },
            rep_level: parent.rep_level + 1,
            nullable: element_nullable,
        }
    }
    
    /// Create level info for a struct field
    pub fn struct_field(parent: LevelInfo, field_nullable: bool) -> Self {
        Self {
            def_level: parent.def_level + if field_nullable { 1 } else { 0 },
            rep_level: parent.rep_level,
            nullable: field_nullable,
        }
    }
}

/// Result of a read operation, including both data and level information
pub struct ReadResult {
    /// Number of records read
    pub records_read: usize,
    
    /// Definition levels for this batch (if applicable)
    pub def_levels: Option<Vec<i16>>,
    
    /// Repetition levels for this batch (if applicable) 
    pub rep_levels: Option<Vec<i16>>,
}

impl ReadResult {
    /// Create a result for a primitive (non-nested) read
    pub fn primitive(records_read: usize, nullable: bool) -> Self {
        Self {
            records_read,
            def_levels: if nullable { Some(Vec::new()) } else { None },
            rep_levels: None,
        }
    }
    
    /// Create a result for an array read
    pub fn array(records_read: usize, def_levels: Vec<i16>, rep_levels: Vec<i16>) -> Self {
        Self {
            records_read,
            def_levels: Some(def_levels),
            rep_levels: Some(rep_levels),
        }
    }
}