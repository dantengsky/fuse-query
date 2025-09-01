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

//! Binary column iterator for Parquet files
//! Handles Binary, Variant, Bitmap, Geometry, and Geography types
//! Uses simple offset-based binary storage without view optimization

use databend_common_column::binary::BinaryColumn;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_expression::types::geography::GeographyColumn;
use databend_common_expression::Column;
use databend_common_expression::TableDataType;
use parquet::encodings::rle::RleDecoder;
use parquet2::encoding::Encoding;
use parquet2::page::Page;

use crate::reader::decompressor::Decompressor;

pub struct BinaryIter<'a> {
    /// Page decompressor for reading Parquet pages
    pages: Decompressor<'a>,
    /// Optional chunk size for batched processing
    chunk_size: Option<usize>,
    /// Total number of rows to process
    num_rows: usize,
    /// The target data type for the column
    data_type: TableDataType,
    /// Dictionary entries as simple byte vectors
    dictionary: Option<Vec<Vec<u8>>>,
    /// Scratch buffer for RLE decoding
    rle_index_buffer: Option<Vec<i32>>,
    /// Is the column nullable
    is_nullable: bool,
}

impl<'a> BinaryIter<'a> {
    pub fn new(
        pages: Decompressor<'a>,
        num_rows: usize,
        is_nullable: bool,
        data_type: TableDataType,
        chunk_size: Option<usize>,
    ) -> Self {
        Self {
            pages,
            chunk_size,
            num_rows,
            data_type,
            is_nullable,
            dictionary: None,
            rle_index_buffer: None,
        }
    }

    /// Process a dictionary page and store the dictionary entries
    fn process_dictionary_page(
        &mut self,
        dict_page: &parquet2::page::DictPage,
    ) -> Result<(), ErrorCode> {
        let mut dict_values = Vec::new();
        let mut offset = 0;
        let buffer = &dict_page.buffer;

        while offset < buffer.len() {
            if offset + 4 > buffer.len() {
                return Err(ErrorCode::Internal(
                    "Invalid dictionary page: incomplete length prefix".to_string(),
                ));
            }

            let length = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + length > buffer.len() {
                return Err(ErrorCode::Internal(
                    "Invalid dictionary page: binary length exceeds buffer".to_string(),
                ));
            }

            dict_values.push(buffer[offset..offset + length].to_vec());
            offset += length;
        }

        self.dictionary = Some(dict_values);
        Ok(())
    }

    /// Create the appropriate column type based on the TableDataType (non-nullable only)
    fn create_column(&self, offsets: Vec<u64>, data: Vec<u8>) -> Column {
        self.create_base_column(offsets, data)
    }

    /// Process a data page with plain encoding
    fn process_plain_page(
        &mut self,
        data_page: &parquet2::page::DataPage,
        offsets: &mut Vec<u64>,
        data: &mut Vec<u8>,
    ) -> Result<(usize, Option<Bitmap>), ErrorCode> {
        // Extract definition levels and values buffer properly - note split_buffer returns (rep, def, values)
        let (_, def_levels, values_buffer) = parquet2::page::split_buffer(data_page)
            .map_err(|e| ErrorCode::Internal(format!("Failed to split buffer: {}", e)))?;

        let num_values = data_page.num_values();

        // Handle definition levels for nullable columns
        let (validity_bitmap, non_null_count) = if self.is_nullable {
            // Decode definition levels to get validity bitmap
            use crate::column::utils::decode_definition_levels;
            use crate::column::utils::get_bit_width;
            let bit_width = get_bit_width(data_page.descriptor.max_def_level);
            let (bitmap, non_null_count) =
                decode_definition_levels(def_levels, bit_width, num_values, data_page)?;
            (bitmap, non_null_count)
        } else {
            (None, num_values) // All values are non-null
        };

        if self.is_nullable {
            // For nullable columns, we need to create entries for ALL positions
            // Read all non-null values first
            let mut non_null_values = Vec::new();
            let mut buffer_offset = 0;
            let mut values_processed = 0;
            let values_to_read = non_null_count;

            // BYTE_ARRAY: length in 4 bytes little endian followed by the bytes
            while values_processed < values_to_read && buffer_offset + 4 <= values_buffer.len() {
                let length = u32::from_le_bytes([
                    values_buffer[buffer_offset],
                    values_buffer[buffer_offset + 1],
                    values_buffer[buffer_offset + 2],
                    values_buffer[buffer_offset + 3],
                ]) as usize;
                buffer_offset += 4;

                if buffer_offset + length > values_buffer.len() {
                    return Err(ErrorCode::Internal(
                        "Invalid data page: binary length exceeds buffer".to_string(),
                    ));
                }

                // Store the binary value
                non_null_values.push(values_buffer[buffer_offset..buffer_offset + length].to_vec());
                buffer_offset += length;
                values_processed += 1;
            }

            // Now create entries for ALL positions, inserting empty bytes for nulls
            if let Some(ref bitmap) = validity_bitmap {
                let mut non_null_idx = 0;
                for is_valid in bitmap.iter() {
                    if is_valid {
                        // Non-null value
                        if non_null_idx >= non_null_values.len() {
                            return Err(ErrorCode::Internal(
                                "Not enough non-null values for validity bitmap".to_string(),
                            ));
                        }
                        let value = &non_null_values[non_null_idx];
                        data.extend_from_slice(value);
                        non_null_idx += 1;
                    } else {
                        // Null value - insert empty placeholder
                        // Binary columns represent nulls as empty byte arrays
                    }
                    // Record offset for both null and non-null
                    eprintln!("puting offset {}", data.len());
                    // TODO TODO TODO
                    // Way does this work? .....
                    offsets.push(data.len() as u64);
                }
            } else {
                return Err(ErrorCode::Internal(
                    "Nullable column must have validity bitmap".to_string(),
                ));
            }
        } else {
            // For non-nullable columns, process all values directly
            let mut buffer_offset = 0;
            let mut values_processed = 0;

            // BYTE_ARRAY: length in 4 bytes little endian followed by the bytes
            while values_processed < num_values && buffer_offset + 4 <= values_buffer.len() {
                let length = u32::from_le_bytes([
                    values_buffer[buffer_offset],
                    values_buffer[buffer_offset + 1],
                    values_buffer[buffer_offset + 2],
                    values_buffer[buffer_offset + 3],
                ]) as usize;
                buffer_offset += 4;

                if buffer_offset + length > values_buffer.len() {
                    return Err(ErrorCode::Internal(
                        "Invalid data page: binary length exceeds buffer".to_string(),
                    ));
                }

                // Copy binary data to the output buffer
                data.extend_from_slice(&values_buffer[buffer_offset..buffer_offset + length]);
                // Record the offset (cumulative length)
                offsets.push(data.len() as u64);

                buffer_offset += length;
                values_processed += 1;
            }
        }

        // Return the total number of rows (including NULLs for nullable columns)
        Ok((num_values, validity_bitmap))
    }
    /// Process a data page with RLE dictionary encoding
    fn process_rle_dict_page(
        &mut self,
        data_page: &parquet2::page::DataPage,
        offsets: &mut Vec<u64>,
        data: &mut Vec<u8>,
    ) -> Result<(usize, Option<Bitmap>), ErrorCode> {
        let dictionary = self.dictionary.as_ref().ok_or_else(|| {
            ErrorCode::Internal("Dictionary not found for RLE_DICTIONARY encoding".to_string())
        })?;

        // Extract definition levels and values buffer properly - note split_buffer returns (rep, def, values)
        let (_, def_levels, values_buffer) = parquet2::page::split_buffer(data_page)
            .map_err(|e| ErrorCode::Internal(format!("Failed to split buffer: {}", e)))?;

        let num_values = data_page.num_values();

        // Handle definition levels for nullable columns
        let (validity_bitmap, non_null_count) = if self.is_nullable {
            // Decode definition levels to get validity bitmap
            use crate::column::utils::decode_definition_levels;
            use crate::column::utils::get_bit_width;
            let bit_width = get_bit_width(data_page.descriptor.max_def_level);
            let (bitmap, non_null_count) =
                decode_definition_levels(def_levels, bit_width, num_values, data_page)?;
            (bitmap, non_null_count)
        } else {
            (None, num_values) // All values are non-null
        };

        if values_buffer.is_empty() {
            return Err(ErrorCode::Internal(
                "Empty RLE dictionary buffer".to_string(),
            ));
        }

        let bit_width = values_buffer[0];

        if self.is_nullable {
            // For nullable columns, we need to create entries for ALL positions
            if bit_width == 0 {
                // Special case: all non-null values are the same (dictionary index 0)
                if dictionary.is_empty() {
                    return Err(ErrorCode::Internal(
                        "Empty dictionary for RLE dictionary encoding".to_string(),
                    ));
                }

                let dict_entry = &dictionary[0];
                if let Some(ref bitmap) = validity_bitmap {
                    for is_valid in bitmap.iter() {
                        if is_valid {
                            // Non-null value - use dictionary[0]
                            data.extend_from_slice(dict_entry);
                        } else {
                            // Null value - empty placeholder
                        }
                        offsets.push(data.len() as u64);
                    }
                } else {
                    return Err(ErrorCode::Internal(
                        "Nullable column must have validity bitmap".to_string(),
                    ));
                }
                return Ok((num_values, validity_bitmap));
            }

            // Decode all non-null indices
            let mut decoder = RleDecoder::new(bit_width);
            decoder.set_data(bytes::Bytes::copy_from_slice(&values_buffer[1..]));

            if self.rle_index_buffer.is_none()
                || self.rle_index_buffer.as_ref().unwrap().len() < non_null_count
            {
                self.rle_index_buffer = Some(vec![0i32; non_null_count]);
            }

            let indices = self.rle_index_buffer.as_mut().unwrap();
            let decoded_count = decoder
                .get_batch(&mut indices[..non_null_count])
                .map_err(|e| ErrorCode::Internal(format!("RLE decode error: {}", e)))?;

            if decoded_count != non_null_count {
                return Err(ErrorCode::Internal(format!(
                    "RLE decoder returned {} values, expected {} non-null values",
                    decoded_count, non_null_count
                )));
            }

            // Create entries for ALL positions using the validity bitmap
            if let Some(ref bitmap) = validity_bitmap {
                let mut non_null_idx = 0;
                for is_valid in bitmap.iter() {
                    if is_valid {
                        // Non-null value - lookup dictionary
                        if non_null_idx >= decoded_count {
                            return Err(ErrorCode::Internal(
                                "Not enough dictionary indices for validity bitmap".to_string(),
                            ));
                        }
                        let dict_idx = indices[non_null_idx];
                        if dict_idx < 0 || dict_idx as usize >= dictionary.len() {
                            return Err(ErrorCode::Internal(format!(
                                "Dictionary index out of bounds: {} (dictionary size: {})",
                                dict_idx,
                                dictionary.len()
                            )));
                        }
                        let binary_data = &dictionary[dict_idx as usize];
                        data.extend_from_slice(binary_data);
                        non_null_idx += 1;
                    } else {
                        // Null value - empty placeholder
                    }
                    offsets.push(data.len() as u64);
                }
            } else {
                return Err(ErrorCode::Internal(
                    "Nullable column must have validity bitmap".to_string(),
                ));
            }
        } else {
            // For non-nullable columns, process all values directly
            if bit_width == 0 {
                // Special case: all values are the same (dictionary index 0)
                if dictionary.is_empty() {
                    return Err(ErrorCode::Internal(
                        "Empty dictionary for RLE dictionary encoding".to_string(),
                    ));
                }

                let dict_entry = &dictionary[0];
                for _ in 0..num_values {
                    data.extend_from_slice(dict_entry);
                    offsets.push(data.len() as u64);
                }
                return Ok((num_values, validity_bitmap));
            }

            let mut decoder = RleDecoder::new(bit_width);
            decoder.set_data(bytes::Bytes::copy_from_slice(&values_buffer[1..]));

            if self.rle_index_buffer.is_none()
                || self.rle_index_buffer.as_ref().unwrap().len() < num_values
            {
                self.rle_index_buffer = Some(vec![0i32; num_values]);
            }

            let indices = self.rle_index_buffer.as_mut().unwrap();
            let decoded_count = decoder
                .get_batch(&mut indices[..num_values])
                .map_err(|e| ErrorCode::Internal(format!("RLE decode error: {}", e)))?;

            if decoded_count != num_values {
                return Err(ErrorCode::Internal(format!(
                    "RLE decoder returned {} values, expected {}",
                    decoded_count, num_values
                )));
            }

            // Lookup dictionary values for all positions
            for &idx in &indices[..num_values] {
                if idx < 0 || idx as usize >= dictionary.len() {
                    return Err(ErrorCode::Internal(format!(
                        "Dictionary index out of bounds: {} (dictionary size: {})",
                        idx,
                        dictionary.len()
                    )));
                }

                let binary_data = &dictionary[idx as usize];
                data.extend_from_slice(binary_data);
                offsets.push(data.len() as u64);
            }
        }

        Ok((num_values, validity_bitmap))
    }
}

impl<'a> Iterator for BinaryIter<'a> {
    type Item = Result<Column, ErrorCode>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.num_rows == 0 {
            return None;
        }

        let chunk_size = self.chunk_size.unwrap_or(self.num_rows).min(self.num_rows);

        // Use simple offset-based storage
        let mut offsets = vec![0u64]; // Start with 0 offset
        let mut data = Vec::new();
        let mut rows_processed = 0;
        let mut validity_bitmaps = Vec::new();

        while rows_processed < chunk_size {
            let page = match self.pages.next_owned() {
                Ok(Some(page)) => page,
                Ok(None) => break,
                Err(e) => return Some(Err(ErrorCode::StorageOther(e.to_string()))),
            };

            match page {
                Page::Dict(dict_page) => {
                    if let Err(e) = self.process_dictionary_page(&dict_page) {
                        return Some(Err(e));
                    }
                }
                Page::Data(data_page) => {
                    let result = match data_page.encoding() {
                        Encoding::Plain => {
                            self.process_plain_page(&data_page, &mut offsets, &mut data)
                        }
                        Encoding::RleDictionary | Encoding::PlainDictionary => {
                            self.process_rle_dict_page(&data_page, &mut offsets, &mut data)
                        }
                        encoding => {
                            return Some(Err(ErrorCode::Internal(format!(
                                "Unsupported encoding for binary data: {:?}",
                                encoding
                            ))))
                        }
                    };

                    match result {
                        Ok((page_rows, validity_bitmap)) => {
                            rows_processed += page_rows;

                            // Collect validity bitmap for nullable columns
                            if self.is_nullable {
                                if let Some(bitmap) = validity_bitmap {
                                    validity_bitmaps.push(bitmap);
                                } else {
                                    return Some(Err(ErrorCode::Internal(
                                        "Nullable column must produce validity bitmap".to_string(),
                                    )));
                                }
                            }
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
            }
        }

        if rows_processed == 0 {
            return None;
        }

        self.num_rows -= rows_processed;

        // Create column with proper nullable handling
        let result = if self.is_nullable {
            self.create_nullable_column(offsets, data, validity_bitmaps, rows_processed)
        } else {
            Ok(self.create_column(offsets, data))
        };

        Some(result)
    }
}

impl<'a> BinaryIter<'a> {
    /// Create a nullable column with combined validity bitmaps
    fn create_nullable_column(
        &self,
        offsets: Vec<u64>,
        data: Vec<u8>,
        validity_bitmaps: Vec<Bitmap>,
        total_rows: usize,
    ) -> Result<Column, ErrorCode> {
        use databend_common_expression::types::NullableColumn;

        // Create base column from non-null data
        let base_column = self.create_base_column(offsets, data);

        // Combine validity bitmaps from multiple pages
        let combined_bitmap = if validity_bitmaps.len() == 1 {
            validity_bitmaps.into_iter().next().unwrap()
        } else if validity_bitmaps.len() > 1 {
            // Concatenate multiple bitmaps
            use crate::column::validation::combine_validity_bitmaps;
            combine_validity_bitmaps(validity_bitmaps, total_rows)?
        } else {
            return Err(ErrorCode::Internal(
                "No validity bitmaps for nullable column".to_string(),
            ));
        };

        let nullable_column = NullableColumn::new(base_column, combined_bitmap);
        Ok(Column::Nullable(Box::new(nullable_column)))
    }

    /// Create base column without nullable wrapper
    fn create_base_column(&self, offsets: Vec<u64>, data: Vec<u8>) -> Column {
        let binary_col = BinaryColumn::new(Buffer::from(data), Buffer::from(offsets));

        match &self.data_type {
            TableDataType::Binary => Column::Binary(binary_col),
            TableDataType::Variant => Column::Variant(binary_col),
            TableDataType::Bitmap => Column::Bitmap(binary_col),
            TableDataType::Geometry => Column::Geometry(binary_col),
            TableDataType::Geography => Column::Geography(GeographyColumn(binary_col)),
            _ => panic!("Unexpected data type in BinaryIter: {:?}", self.data_type),
        }
    }
}
