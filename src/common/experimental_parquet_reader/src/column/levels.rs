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

/// Information about definition and repetition levels for nested types
#[derive(Debug, Clone)]
pub struct LevelInfo {
    /// Definition levels - controls nullability in nested structures
    pub def_levels: Vec<u16>,
    /// Repetition levels - controls repetition boundaries in nested structures  
    pub rep_levels: Vec<u16>,
    /// Maximum definition level for this field
    pub max_def_level: u16,
    /// Maximum repetition level for this field
    pub max_rep_level: u16,
}

impl LevelInfo {
    /// Create a new LevelInfo with empty levels
    pub fn empty(max_def_level: u16, max_rep_level: u16) -> Self {
        Self {
            def_levels: Vec::new(),
            rep_levels: Vec::new(),
            max_def_level,
            max_rep_level,
        }
    }

    /// Create LevelInfo with pre-allocated capacity
    pub fn with_capacity(
        capacity: usize,
        max_def_level: u16,
        max_rep_level: u16,
    ) -> Self {
        Self {
            def_levels: Vec::with_capacity(capacity),
            rep_levels: Vec::with_capacity(capacity),
            max_def_level,
            max_rep_level,
        }
    }

    /// Check if this field requires definition levels (has nullable components)
    pub fn has_definition_levels(&self) -> bool {
        self.max_def_level > 0
    }

    /// Check if this field requires repetition levels (has repeated components)
    pub fn has_repetition_levels(&self) -> bool {
        self.max_rep_level > 0
    }

    /// Get the number of values in the levels
    pub fn len(&self) -> usize {
        debug_assert_eq!(self.def_levels.len(), self.rep_levels.len());
        self.def_levels.len()
    }

    /// Check if levels are empty
    pub fn is_empty(&self) -> bool {
        self.def_levels.is_empty()
    }

    /// Clear all levels data
    pub fn clear(&mut self) {
        self.def_levels.clear();
        self.rep_levels.clear();
    }

    /// Reserve additional capacity for levels
    pub fn reserve(&mut self, additional: usize) {
        self.def_levels.reserve(additional);
        self.rep_levels.reserve(additional);
    }

    /// Push new levels
    pub fn push(&mut self, def_level: u16, rep_level: u16) {
        self.def_levels.push(def_level);
        self.rep_levels.push(rep_level);
    }

    /// Create level information for a single value
    pub fn single_value(def_level: u16, rep_level: u16, max_def_level: u16, max_rep_level: u16) -> Self {
        Self {
            def_levels: vec![def_level],
            rep_levels: vec![rep_level],
            max_def_level,
            max_rep_level,
        }
    }

    /// Merge two level information structures
    /// This is used when combining elements from different nested structures
    pub fn merge(&mut self, other: &LevelInfo) -> Result<(), String> {
        if self.max_def_level != other.max_def_level || self.max_rep_level != other.max_rep_level {
            return Err(format!(
                "Cannot merge LevelInfo with different maximums: ({}, {}) vs ({}, {})",
                self.max_def_level, self.max_rep_level,
                other.max_def_level, other.max_rep_level
            ));
        }

        self.def_levels.extend_from_slice(&other.def_levels);
        self.rep_levels.extend_from_slice(&other.rep_levels);
        Ok(())
    }

    /// Adjust levels for array wrapping
    /// When we wrap elements in an array, we need to adjust the levels
    pub fn adjust_for_array_wrapping(&mut self, array_def_increment: u16, array_rep_increment: u16) {
        // Increase definition levels for potential null arrays
        for def_level in &mut self.def_levels {
            *def_level += array_def_increment;
        }
        
        // Increase repetition levels for array repetition
        for rep_level in &mut self.rep_levels {
            *rep_level += array_rep_increment;
        }
        
        // Update maximums
        self.max_def_level += array_def_increment;
        self.max_rep_level += array_rep_increment;
    }

    /// Create level information for a null array
    pub fn null_array(max_def_level: u16, max_rep_level: u16) -> Self {
        Self {
            def_levels: vec![max_def_level - 1], // One less than max indicates null array
            rep_levels: vec![0], // No repetition for null
            max_def_level,
            max_rep_level,
        }
    }

    /// Create level information for an empty array
    pub fn empty_array(max_def_level: u16, max_rep_level: u16) -> Self {
        Self {
            def_levels: vec![max_def_level], // Max def level indicates present but empty array
            rep_levels: vec![0], // No repetition for empty array
            max_def_level,
            max_rep_level,
        }
    }

    /// Calculate array boundaries based on repetition levels
    /// Returns a vector of (start_index, count) pairs indicating array boundaries
    pub fn calculate_array_boundaries(&self) -> Vec<(usize, usize)> {
        let mut boundaries = Vec::new();
        let mut array_start = 0;
        
        for (i, &rep_level) in self.rep_levels.iter().enumerate() {
            if rep_level == 0 && i > 0 {
                // New array starts here, finish previous array
                boundaries.push((array_start, i - array_start));
                array_start = i;
            }
        }
        
        // Add the final array if we have elements
        if !self.rep_levels.is_empty() {
            boundaries.push((array_start, self.rep_levels.len() - array_start));
        }
        
        boundaries
    }
}

/// Iterator over level pairs
impl<'a> IntoIterator for &'a LevelInfo {
    type Item = (u16, u16);
    type IntoIter = std::iter::Map<
        std::iter::Zip<std::slice::Iter<'a, u16>, std::slice::Iter<'a, u16>>,
        fn((&u16, &u16)) -> (u16, u16),
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.def_levels
            .iter()
            .zip(self.rep_levels.iter())
            .map(|(&def, &rep)| (def, rep))
    }
}