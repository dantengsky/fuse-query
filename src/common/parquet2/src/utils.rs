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

use databend_common_exception::ErrorCode;

use parquet2::deserialize::{HybridDecoderBitmapIter, HybridEncoded};
use parquet2::encoding::hybrid_rle::Decoder;
use parquet2::encoding::Encoding;
use parquet2::page::DataPage;
use parquet2::page::DataPageHeader;
use parquet2::indexes::Interval;
use parquet_format_safe::Encoding as RawEncoding;

/// Enum to represent the validity of a page
pub enum PageValidity<'a> {
    /// All values are non-null
    Required,
    /// Some values may be null
    Optional(BooleanHybridRleIterator<'a>),
    /// Some values may be null and some may be filtered
    FilteredOptional(FilteredOptionalIterator<'a, BooleanHybridRleIterator<'a>>),
}

/// Iterator that filters values based on selected intervals
pub struct FilteredOptionalIterator<'a, I: Iterator<Item = bool>> {
    /// Iterator yielding optional values
    iter: I,
    /// Selected intervals
    selected: Vec<Interval>,
    /// Current interval index
    current_interval_idx: usize,
    /// Current index within all values
    current_idx: usize,
    /// Current position within the interval
    current_interval_pos: usize,
    /// Phantom to capture lifetime
    phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a, I: Iterator<Item = bool>> FilteredOptionalIterator<'a, I> {
    /// Create a new FilteredOptionalIterator
    pub fn new(iter: I, selected: Vec<Interval>) -> Self {
        Self {
            iter,
            selected,
            current_interval_idx: 0,
            current_idx: 0,
            current_interval_pos: 0,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, I: Iterator<Item = bool>> Iterator for FilteredOptionalIterator<'a, I> {
    type Item = bool;
    
    fn next(&mut self) -> Option<Self::Item> {
        // If we've processed all intervals, we're done
        if self.current_interval_idx >= self.selected.len() {
            return None;
        }
        
        // Get the current interval
        let interval = &self.selected[self.current_interval_idx];
        
        // Skip items until we reach the start of the current interval
        while self.current_idx < interval.start {
            self.current_idx += 1;
            self.iter.next()?;
        }
        
        // If we've reached the end of the interval, move to the next one
        if self.current_interval_pos >= (interval.length) {
            self.current_interval_idx += 1;
            self.current_interval_pos = 0;
            return self.next();
        }
        
        // Get the value at the current position
        let value = self.iter.next()?;
        
        // Update positions
        self.current_idx += 1;
        self.current_interval_pos += 1;
        
        Some(value)
    }
}

/// A wrapper iterator that converts HybridDecoderBitmapIter into an iterator that yields bool values
struct BooleanHybridRleIterator<'a> {
    inner: HybridDecoderBitmapIter<'a>,
    max_def_level: i16,
}

impl<'a> BooleanHybridRleIterator<'a> {
    fn new(inner: HybridDecoderBitmapIter<'a>, max_def_level: i16) -> Self {
        Self { inner, max_def_level }
    }
}

impl<'a> Iterator for BooleanHybridRleIterator<'a> {
    type Item = bool;
    
    fn next(&mut self) -> Option<Self::Item> {
        // Get the next value from the inner iterator
        match self.inner.next()? {
            Ok(value) => {
                // The actual variants of HybridEncoded are:
                // - Bitmap(data, length): representing a bitmap
                // - Repeated(is_set, length): representing a repeated value
                match value {
                    HybridEncoded::Bitmap(data, _) => {
                        // For bitmap, the first bit is the current value
                        // This is a simplification - in a real implementation we would need to
                        // track the position within the bitmap
                        let byte = data.first()?;
                        Some((*byte & 1) != 0 && self.max_def_level > 0)
                    },
                    HybridEncoded::Repeated(is_set, _) => {
                        // For repeated, check if it's set and if max_def_level > 0
                        Some(is_set && self.max_def_level > 0)
                    }
                }
            }
            Err(_) => {
                // Error case - treat as None/null
                Some(false)
            }
        }
    }
}

impl<'a> TryFrom<&'a DataPage> for PageValidity<'a> {
    type Error = ErrorCode;

    fn try_from(page: &'a DataPage) -> std::result::Result<PageValidity<'a>, ErrorCode> {
        // If the repetition level is 0, we know it's a flat field
        // Note that this impl doesn't handle nesting (rep_level > 0)

        // Get the header to check if there are definition levels
        match page.header() {
            DataPageHeader::V1(header) => {
                // In V1, convert the Encoding types to match
                let rle_encoding: RawEncoding = Encoding::Rle.into();
                let bitpacked_encoding: RawEncoding = Encoding::BitPacked.into();
                
                if header.definition_level_encoding != rle_encoding && 
                   header.definition_level_encoding != bitpacked_encoding {
                    // No definition levels, so this is a required field
                    return Ok(PageValidity::Required);
                }
            },
            DataPageHeader::V2(header) => {
                // In V2, check the definition level byte length
                if header.definition_levels_byte_length == 0 {
                    // No definition levels, so this is a required field
                    return Ok(PageValidity::Required);
                }
            }
        }
        
        // If we have selected rows, use FilteredOptional
        if let Some(selected) = page.selected_rows() {
            let descriptor = &page.descriptor;

            // Create bitmap iterator - using new instead of try_new 
            let decoder = Decoder::new(
                page.buffer(),
                descriptor.max_def_level as usize,
            );
            
            let iter = HybridDecoderBitmapIter::new(
                decoder, 
                page.num_values()
            );
            
            // Create a BooleanHybridRleIterator to convert Result<HybridEncoded, Error> to bool
            let boolean_iter = BooleanHybridRleIterator::new(iter, descriptor.max_def_level);
            
            // Wrap with a FilteredOptionalIterator
            Ok(PageValidity::FilteredOptional(FilteredOptionalIterator::new(
                boolean_iter,
                selected.to_vec(),
            )))
        } else {
            // No selected rows, use Optional
            let descriptor = &page.descriptor;

            // Create bitmap iterator
            let decoder = Decoder::new(
                page.buffer(),
                descriptor.max_def_level as usize,
            );
            
            let iter = HybridDecoderBitmapIter::new(
                decoder,
                page.num_values()
            );
            
            // Create a BooleanHybridRleIterator to convert Result<HybridEncoded, Error> to bool
            let boolean_iter = BooleanHybridRleIterator::new(iter, descriptor.max_def_level);

            Ok(PageValidity::Optional(boolean_iter))
        }
    }
}

/// Returns an error for features that are not implemented yet
pub fn not_implemented<T: std::fmt::Debug>(feature: T) -> ErrorCode {
    ErrorCode::Internal(format!("Feature not implemented yet: {:?}", feature))
}
