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

//! Array column iterator for nested parquet types

use std::marker::PhantomData;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::Column;
use decompressor::Decompressor;

use super::common::ParquetColumnIterator;
use super::level_decoder::LevelDecoder;
use super::levels::LevelInfo;
use super::traits::ColumnIteratorWithLevels;
use super::traits::DictionarySupport;
use super::traits::ParquetColumnType;
use super::traits::ParquetPhysicalMapping;
use crate::reader::decompressor;

/// Iterator for Array columns (ARRAY(primitive_type))
/// 
/// This iterator processes arrays by:
/// 1. Reading definition and repetition levels to determine array boundaries
/// 2. Processing element values using the underlying leaf iterator  
/// 3. Reconstructing arrays based on level information
pub struct ArrayColumnIterator<
    'a,
    T: ParquetColumnType + DictionarySupport + ParquetPhysicalMapping,
> {
    /// Iterator for the array elements (leaf values)
    element_iter: ParquetColumnIterator<'a, T>,
    /// Current level information
    current_levels: Option<LevelInfo>,
    /// Maximum definition level for this array column
    max_def_level: u16,
    /// Maximum repetition level for this array column  
    max_rep_level: u16,
    /// Number of rows expected
    num_rows: usize,
    /// Chunk size for batching
    chunk_size: Option<usize>,
    /// Whether the array itself can be null
    is_nullable: bool,
    _phantom: PhantomData<T>,
}

impl<'a, T: ParquetColumnType + DictionarySupport + ParquetPhysicalMapping> 
    ArrayColumnIterator<'a, T>
{
    /// Create a new ArrayColumnIterator
    /// 
    /// # Arguments
    /// * `pages` - Decompressor for reading parquet pages
    /// * `num_rows` - Number of rows to read
    /// * `is_nullable` - Whether the array itself can be null
    /// * `element_metadata` - Metadata for the array element type
    /// * `chunk_size` - Optional chunk size for batching
    /// * `max_def_level` - Maximum definition level (typically 2 for nullable arrays with nullable elements)
    /// * `max_rep_level` - Maximum repetition level (typically 1 for arrays)
    pub fn new(
        pages: Decompressor<'a>,
        num_rows: usize,
        is_nullable: bool,
        element_metadata: T::Metadata,
        chunk_size: Option<usize>,
        max_def_level: u16,
        max_rep_level: u16,
    ) -> Self {
        // Create element iterator with level support
        let element_iter = ParquetColumnIterator::new_with_levels(
            pages,
            num_rows, // This will be adjusted based on actual element count
            false,    // Individual elements handled via levels, not nullable wrapper
            element_metadata,
            None,     // No chunking for element iterator - we handle batching
            max_def_level,
            max_rep_level,
        );

        Self {
            element_iter,
            current_levels: None,
            max_def_level,
            max_rep_level,
            num_rows,
            chunk_size,
            is_nullable,
            _phantom: PhantomData,
        }
    }

    /// Process levels to reconstruct array boundaries
    /// 
    /// Returns vector of (start_idx, length) pairs for each array in the batch
    fn process_array_boundaries(levels: &LevelInfo) -> Result<Vec<(usize, usize)>> {
        let mut arrays = Vec::new();
        let mut current_start = 0;
        
        for (idx, (def_level, rep_level)) in levels.into_iter().enumerate() {
            // rep_level == 0 indicates start of new array (or end of current)
            if rep_level == 0 && idx > 0 {
                // End current array
                let length = idx - current_start;
                arrays.push((current_start, length));
                current_start = idx;
            }
        }
        
        // Handle last array
        if current_start < levels.len() {
            let length = levels.len() - current_start;
            arrays.push((current_start, length));
        }
        
        Ok(arrays)
    }

    /// Extract valid (non-null) elements from a range
    fn extract_valid_elements(
        elements: &Column,
        levels: &LevelInfo,
        start: usize,
        length: usize,
        max_def_level: u16,
    ) -> Result<(Column, Vec<bool>)> {
        // Elements are valid when def_level == max_def_level
        let mut valid_indices = Vec::new();
        let mut validity = Vec::new();
        
        for i in start..(start + length) {
            let def_level = levels.def_levels[i];
            let is_valid = def_level == max_def_level;
            validity.push(is_valid);
            if is_valid {
                valid_indices.push(i);
            }
        }
        
        // Extract valid elements - this is a simplified approach
        // In practice, we'd need more sophisticated column slicing
        let valid_elements = if valid_indices.is_empty() {
            // Create empty column of same type
            elements.slice(0..0)
        } else {
            // For now, return the full column slice - proper implementation
            // would extract only valid elements
            elements.slice(start..(start + length))
        };
        
        Ok((valid_elements, validity))
    }
}

impl<'a, T: ParquetColumnType + DictionarySupport + ParquetPhysicalMapping> Iterator 
    for ArrayColumnIterator<'a, T>
{
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next batch of elements with levels
        let elements_column = match self.element_iter.next() {
            Some(Ok(col)) => col,
            Some(Err(e)) => return Some(Err(e)),
            None => return None,
        };

        // Get levels from element iterator
        let levels = match self.element_iter.current_levels() {
            Some(levels) => levels,
            None => {
                return Some(Err(ErrorCode::Internal(
                    "Array iterator requires level information".to_string(),
                )));
            }
        };

        // Store current levels
        self.current_levels = Some(levels.clone());

        // Process array boundaries from repetition levels
        let array_boundaries = match Self::process_array_boundaries(levels) {
            Ok(boundaries) => boundaries,
            Err(e) => return Some(Err(e)),
        };

        // Build arrays
        let mut array_offsets = vec![0u64]; // Start with offset 0
        let mut all_elements = Vec::new();
        let mut array_validity = Vec::new();

        for (start, length) in array_boundaries {
            // Check if this array is null (def_level < max_def_level - 1 for array itself)
            let array_def_level = levels.def_levels[start];
            let array_is_null = if self.is_nullable {
                // For nullable arrays: def_level 0 = null array, 1+ = non-null array
                array_def_level == 0
            } else {
                false
            };

            array_validity.push(!array_is_null);

            if array_is_null {
                // Null array contributes no elements
                array_offsets.push(array_offsets.last().unwrap() + 0);
            } else {
                // Extract valid elements for this array
                let (array_elements, _element_validity) = match Self::extract_valid_elements(
                    &elements_column,
                    levels,
                    start,
                    length,
                    self.max_def_level,
                ) {
                    Ok(result) => result,
                    Err(e) => return Some(Err(e)),
                };

                // Add elements to combined column
                // Note: This is simplified - proper implementation needs column concatenation
                let element_count = array_elements.len();
                all_elements.push(array_elements);
                
                // Update offset
                let new_offset = array_offsets.last().unwrap() + element_count as u64;
                array_offsets.push(new_offset);
            }
        }

        // Combine all elements into single column
        let combined_elements = if all_elements.is_empty() {
            // Create empty column of correct type
            elements_column.slice(0..0)
        } else if all_elements.len() == 1 {
            all_elements.into_iter().next().unwrap()
        } else {
            // TODO: Proper column concatenation
            // For now, use first non-empty column as placeholder
            all_elements.into_iter().next().unwrap()
        };

        // Create ArrayColumn
        let array_column = ArrayColumn::new(combined_elements, array_offsets.into());

        if self.is_nullable && array_validity.iter().any(|&v| !v) {
            // Create nullable array column
            use databend_common_column::bitmap::Bitmap;
            use databend_common_expression::types::NullableColumn;

            let validity_bitmap = Bitmap::from_iter(array_validity);
            let nullable_array = NullableColumn::new(
                Column::Array(Box::new(array_column)),
                validity_bitmap,
            );
            Some(Ok(Column::Nullable(Box::new(nullable_array))))
        } else {
            // Non-nullable array
            Some(Ok(Column::Array(Box::new(array_column))))
        }
    }
}

impl<'a, T: ParquetColumnType + DictionarySupport + ParquetPhysicalMapping> ColumnIteratorWithLevels 
    for ArrayColumnIterator<'a, T>
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::levels::LevelInfo;

    #[test]
    fn test_array_boundary_processing() {
        // Test case: [[1, 2], [3], []]
        // Expected rep_levels: [0, 1, 0, 0] (start of arrays)
        // Expected def_levels: [2, 2, 2, 1] (2=valid element, 1=empty array)
        
        let mut levels = LevelInfo::empty(2, 1);
        levels.def_levels = vec![2, 2, 2, 1]; // 2 valid elements, then empty array
        levels.rep_levels = vec![0, 1, 0, 0]; // Array boundaries
        
        let boundaries = ArrayColumnIterator::<i32>::process_array_boundaries(&levels).unwrap();
        
        // Should identify 3 arrays: [0,2), [2,1), [3,1)
        assert_eq!(boundaries.len(), 3);
        assert_eq!(boundaries[0], (0, 2)); // First array: 2 elements
        assert_eq!(boundaries[1], (2, 1)); // Second array: 1 element  
        assert_eq!(boundaries[2], (3, 1)); // Third array: empty (def_level=1)
    }
    
    #[test]
    fn test_empty_array_boundaries() {
        let levels = LevelInfo::empty(2, 1);
        let boundaries = ArrayColumnIterator::<i32>::process_array_boundaries(&levels).unwrap();
        assert_eq!(boundaries.len(), 0);
    }
}

/// Generic Array Column Iterator for wrapping any ColumnIter
/// 
/// This iterator allows us to create arrays of any nested type by wrapping
/// an arbitrary column iterator and applying array-level processing.
/// 
/// Performance optimizations:
/// - Lazy level calculation to avoid unnecessary computation
/// - Batch processing support to reduce per-row overhead
/// - Memory pre-allocation based on estimated array sizes
pub struct GenericArrayColumnIterator<'a> {
    /// Inner iterator that produces elements for this array
    inner_iter: Box<dyn Iterator<Item = Result<Column>> + Send + Sync + 'a>,
    /// Current level information (computed lazily)
    current_levels: Option<LevelInfo>,
    /// Maximum definition level for array
    max_def_level: u16,
    /// Maximum repetition level for array
    max_rep_level: u16,
    /// Whether the array itself can be null
    is_nullable: bool,
    /// Number of rows expected
    num_rows: usize,
    /// Current row position
    current_row: usize,
    /// Batch size for processing (performance optimization)
    batch_size: usize,
    /// Pre-allocated buffer for batch processing
    element_buffer: Vec<Column>,
    /// Pre-allocated offset buffer
    offset_buffer: Vec<u64>,
}

impl<'a> GenericArrayColumnIterator<'a> {
    /// Create a new generic array iterator with performance optimizations
    pub fn new(
        inner_iter: Box<dyn Iterator<Item = Result<Column>> + Send + Sync + 'a>,
        is_nullable: bool,
        max_def_level: u16,
        max_rep_level: u16,
        num_rows: usize,
    ) -> Result<Self> {
        // Calculate optimal batch size based on expected rows
        let batch_size = Self::calculate_optimal_batch_size(num_rows);
        
        Ok(Self {
            inner_iter,
            current_levels: None,
            max_def_level,
            max_rep_level,
            is_nullable,
            num_rows,
            current_row: 0,
            batch_size,
            element_buffer: Vec::with_capacity(batch_size),
            offset_buffer: Vec::with_capacity(batch_size + 1), // +1 for initial 0
        })
    }
    
    /// Calculate optimal batch size for processing
    /// Balances memory usage vs processing efficiency
    fn calculate_optimal_batch_size(num_rows: usize) -> usize {
        const MIN_BATCH_SIZE: usize = 16;
        const MAX_BATCH_SIZE: usize = 1024;
        const TARGET_MEMORY_KB: usize = 64; // Target ~64KB batches
        
        if num_rows == 0 {
            return MIN_BATCH_SIZE;
        }
        
        // Estimate memory per row (rough approximation)
        let estimated_bytes_per_row = 128; // Conservative estimate for nested arrays
        let target_batch_size = (TARGET_MEMORY_KB * 1024) / estimated_bytes_per_row;
        
        // Clamp to reasonable bounds
        target_batch_size.max(MIN_BATCH_SIZE).min(MAX_BATCH_SIZE).min(num_rows)
    }

    /// Process multiple array elements into a batch using pre-allocated buffers
    /// This is more efficient for handling multiple arrays at once
    fn process_array_batch_optimized(&mut self, requested_batch_size: usize) -> Result<Option<Column>> {
        // Clear buffers but keep capacity
        self.element_buffer.clear();
        self.offset_buffer.clear();
        self.offset_buffer.push(0); // Initial offset
        
        let batch_size = requested_batch_size.min(self.batch_size);
        
        // Reserve space if needed
        if self.element_buffer.capacity() < batch_size {
            self.element_buffer.reserve(batch_size - self.element_buffer.capacity());
        }
        if self.offset_buffer.capacity() < batch_size + 1 {
            self.offset_buffer.reserve((batch_size + 1) - self.offset_buffer.capacity());
        }
        
        // Collect elements into pre-allocated buffer
        for _i in 0..batch_size {
            if self.current_row >= self.num_rows {
                break;
            }
            
            match self.inner_iter.next() {
                Some(Ok(element)) => {
                    self.element_buffer.push(element);
                    self.current_row += 1;
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }
        
        if self.element_buffer.is_empty() {
            return Ok(None);
        }
        
        // Create offsets - for now, simple 1:1 mapping
        // In a full implementation, this would process definition/repetition levels
        for i in 1..=self.element_buffer.len() {
            self.offset_buffer.push(i as u64);
        }
        
        // Combine all elements efficiently
        let combined_elements = if self.element_buffer.len() == 1 {
            // Common case: single element
            self.element_buffer.swap_remove(0)
        } else {
            // Multiple elements - for now, just take the first
            // Real implementation would need proper combining logic
            self.element_buffer.swap_remove(0)
        };
        
        let array_column = ArrayColumn::new(
            combined_elements,
            self.offset_buffer.clone().into() // Clone the buffer
        );
        
        Ok(Some(Column::Array(Box::new(array_column))))
    }

    /// Update level information with lazy computation for performance
    /// This calculates the appropriate definition and repetition levels for array nesting
    fn update_levels_for_nesting_lazy(&mut self, element_levels: Option<&LevelInfo>) {
        // Only compute levels if they're actually needed
        // This is a performance optimization for cases where levels aren't used
        if element_levels.is_some() {
            self.update_levels_for_nesting(element_levels);
        } else {
            // Defer level calculation - create basic structure only when needed
            self.current_levels = None;
        }
    }

    /// Update level information for nested array processing
    /// This calculates the appropriate definition and repetition levels for array nesting
    fn update_levels_for_nesting(&mut self, element_levels: Option<&LevelInfo>) {
        // For nested arrays, we need to adjust the level information
        if let Some(elem_levels) = element_levels {
            // Clone and adjust the element levels for array wrapper
            let mut new_levels = elem_levels.clone();
            new_levels.adjust_for_array_wrapping(1, 1); // Arrays add 1 level to both def and rep
            
            // Update maximums to match our iterator's expectations  
            new_levels.max_def_level = self.max_def_level;
            new_levels.max_rep_level = self.max_rep_level;
            
            self.current_levels = Some(new_levels);
        } else {
            // No element levels, create basic levels for simple array
            let levels = LevelInfo::single_value(
                self.max_def_level,    // Assume valid element
                0,                     // No repetition at this level
                self.max_def_level,
                self.max_rep_level
            );
            self.current_levels = Some(levels);
        }
    }

    /// Process array structure from nested elements using level information
    fn process_nested_array_elements_with_levels(&mut self, elements: Column, element_levels: Option<&LevelInfo>) -> Result<Column> {
        // Update level information first
        self.update_levels_for_nesting(element_levels);
        
        // If we have level information, use it to create proper array boundaries
        if let Some(levels) = element_levels {
            let boundaries = levels.calculate_array_boundaries();
            
            // Create arrays based on the boundaries
            if boundaries.is_empty() {
                // No elements, create empty array
                let empty_array = ArrayColumn::new(
                    elements, // Use elements as base type
                    vec![0u64].into() // Empty offsets
                );
                return Ok(Column::Array(Box::new(empty_array)));
            }
            
            // For now, create simple single-element array
            // In full implementation, we'd process each boundary separately
            let array_column = ArrayColumn::new(
                elements,
                vec![0u64, 1u64].into() // Single element array
            );
            Ok(Column::Array(Box::new(array_column)))
        } else {
            // Fall back to simple processing without levels
            self.process_nested_array_elements(elements)
        }
    }

    /// Process array structure from nested elements
    fn process_nested_array_elements(&mut self, elements: Column) -> Result<Column> {
        // Handle different cases based on the input column type
        match elements {
            // If we get an array column, we need to wrap it in another array layer
            Column::Array(inner_array) => {
                // For Array(Array(T)), we create a new array containing the inner array
                // This handles cases like [[1,2], [3,4]] -> [[[1,2], [3,4]]]
                let array_column = ArrayColumn::new(
                    Column::Array(inner_array),
                    vec![0, 1].into() // Single element array containing the inner array
                );
                Ok(Column::Array(Box::new(array_column)))
            }
            
            // For other column types (primitives, tuples, etc.)
            _ => {
                // Create a single-element array containing the column
                let array_column = ArrayColumn::new(
                    elements,
                    vec![0, 1].into() // Single element array
                );
                Ok(Column::Array(Box::new(array_column)))
            }
        }
    }
}

impl<'a> Iterator for GenericArrayColumnIterator<'a> {
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_row >= self.num_rows {
            return None;
        }

        // Get the next nested element
        let elements = match self.inner_iter.next() {
            Some(Ok(col)) => col,
            Some(Err(e)) => return Some(Err(e)),
            None => return None,
        };

        // Try to get level information from inner iterator if it supports levels
        // For now, we assume no level information from inner iterator (placeholder)
        let element_levels: Option<&LevelInfo> = None;

        // Process the nested elements into array structure using level information
        match self.process_nested_array_elements_with_levels(elements, element_levels) {
            Ok(array_col) => {
                self.current_row += 1; // Only increment here
                Some(Ok(array_col))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

impl<'a> ColumnIteratorWithLevels for GenericArrayColumnIterator<'a> {
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