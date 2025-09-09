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

use databend_common_exception::ErrorCode;
use parquet::encodings::rle::RleDecoder;
use parquet2::page::DataPage;

use crate::column::levels::LevelInfo;
use crate::column::utils::get_bit_width;

/// Decoder for definition and repetition levels in parquet pages
pub struct LevelDecoder {
    /// Decoder for definition levels (nullable components)
    def_decoder: Option<RleDecoder>,
    /// Decoder for repetition levels (repeated components)  
    rep_decoder: Option<RleDecoder>,
    /// Maximum definition level expected
    max_def_level: u16,
    /// Maximum repetition level expected
    max_rep_level: u16,
}

impl LevelDecoder {
    /// Create a new LevelDecoder
    pub fn new(max_def_level: u16, max_rep_level: u16) -> Self {
        Self {
            def_decoder: None,
            rep_decoder: None,
            max_def_level,
            max_rep_level,
        }
    }

    /// Decode levels from a data page
    pub fn decode_levels(
        &mut self,
        data_page: &DataPage,
    ) -> Result<LevelInfo, ErrorCode> {
        let num_values = data_page.num_values();
        let mut level_info = LevelInfo::with_capacity(num_values, self.max_def_level, self.max_rep_level);

        // Extract rep_levels, def_levels, and values from the page
        let (rep_levels_data, def_levels_data, _values_data) = parquet2::page::split_buffer(data_page)
            .map_err(|e| ErrorCode::Internal(format!("Failed to split page buffer: {}", e)))?;

        // Decode repetition levels if present
        if self.max_rep_level > 0 {
            let rep_bit_width = get_bit_width(self.max_rep_level as i16) as u8;
            let mut rep_decoder = RleDecoder::new(rep_bit_width);
            rep_decoder.set_data(bytes::Bytes::copy_from_slice(rep_levels_data));
            
            let mut rep_buffer = vec![0i32; num_values];
            let decoded_rep = rep_decoder.get_batch(&mut rep_buffer)
                .map_err(|e| ErrorCode::Internal(format!("Failed to decode repetition levels: {}", e)))?;
            
            if decoded_rep != num_values {
                return Err(ErrorCode::Internal(format!(
                    "Repetition level count mismatch: expected {}, got {}",
                    num_values, decoded_rep
                )));
            }
            
            level_info.rep_levels = rep_buffer.into_iter().map(|x| x as u16).collect();
        } else {
            // No repetition levels needed
            level_info.rep_levels = vec![0; num_values];
        }

        // Decode definition levels if present
        if self.max_def_level > 0 {
            let def_bit_width = get_bit_width(self.max_def_level as i16) as u8;
            let mut def_decoder = RleDecoder::new(def_bit_width);
            def_decoder.set_data(bytes::Bytes::copy_from_slice(def_levels_data));
            
            let mut def_buffer = vec![0i32; num_values];
            let decoded_def = def_decoder.get_batch(&mut def_buffer)
                .map_err(|e| ErrorCode::Internal(format!("Failed to decode definition levels: {}", e)))?;
            
            if decoded_def != num_values {
                return Err(ErrorCode::Internal(format!(
                    "Definition level count mismatch: expected {}, got {}",
                    num_values, decoded_def
                )));
            }
            
            level_info.def_levels = def_buffer.into_iter().map(|x| x as u16).collect();
        } else {
            // No definition levels needed - all values are defined
            level_info.def_levels = vec![self.max_def_level; num_values];
        }

        Ok(level_info)
    }

    /// Reset the decoder state
    pub fn reset(&mut self) {
        self.def_decoder = None;
        self.rep_decoder = None;
    }

    /// Get max definition level
    pub fn max_def_level(&self) -> u16 {
        self.max_def_level
    }

    /// Get max repetition level  
    pub fn max_rep_level(&self) -> u16 {
        self.max_rep_level
    }

    /// Check if this decoder needs to process definition levels
    pub fn has_definition_levels(&self) -> bool {
        self.max_def_level > 0
    }

    /// Check if this decoder needs to process repetition levels
    pub fn has_repetition_levels(&self) -> bool {
        self.max_rep_level > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_level_decoder_creation() {
        let decoder = LevelDecoder::new(2, 1);
        assert_eq!(decoder.max_def_level(), 2);
        assert_eq!(decoder.max_rep_level(), 1);
        assert!(decoder.has_definition_levels());
        assert!(decoder.has_repetition_levels());
    }

    #[test]
    fn test_level_decoder_no_levels() {
        let decoder = LevelDecoder::new(0, 0);
        assert_eq!(decoder.max_def_level(), 0);
        assert_eq!(decoder.max_rep_level(), 0);
        assert!(!decoder.has_definition_levels());
        assert!(!decoder.has_repetition_levels());
    }
}