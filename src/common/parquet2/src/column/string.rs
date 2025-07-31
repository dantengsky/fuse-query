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

use databend_common_column::binview::Utf8ViewColumn;
use databend_common_column::binview::View;
use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_expression::Column;
use parquet::encodings::rle::RleDecoder;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PhysicalType;

use crate::wip::decompressor::Decompressor;

pub struct StringIter<'a> {
    pages: Decompressor<'a>,
    chunk_size: Option<usize>,
    num_rows: usize,
    dictionary: Option<Vec<Vec<u8>>>,
    cached_dict_views: Option<Vec<View>>,
}

impl<'a> StringIter<'a> {
    pub fn new(
        pages: Decompressor<'a>,
        num_rows: usize,
        chunk_size: Option<usize>,
    ) -> StringIter<'a> {
        Self {
            pages,
            chunk_size,
            num_rows,
            dictionary: None,
            cached_dict_views: None,
        }
    }

    /// Process a dictionary page and store the dictionary entries
    fn process_dictionary_page(
        &mut self,
        dict_page: &parquet2::page::DictPage,
    ) -> Result<(), ErrorCode> {
        assert!(self.dictionary.is_none());

        let dict_buffer = &dict_page.buffer;
        let mut dictionary = Vec::new();
        let mut offset = 0;

        // Parse dictionary entries (length-prefixed strings)
        while offset + 4 <= dict_buffer.len() {
            // Read 4-byte little-endian length
            let len_bytes = &dict_buffer[offset..offset + 4];
            let len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]])
                as usize;
            offset += 4;

            if offset + len > dict_buffer.len() {
                return Err(ErrorCode::Internal(
                    "Dictionary entry length exceeds buffer size".to_string(),
                ));
            }

            // Read string data
            let string_data = dict_buffer[offset..offset + len].to_vec();
            dictionary.push(string_data);
            offset += len;
        }

        if dictionary.is_empty() {
            return Err(ErrorCode::Internal("Empty dictionary page".to_string()));
        }

        self.dictionary = Some(dictionary);
        Ok(())
    }

    /// Create a View from a string slice, handling both inline and buffer storage
    /// Optimized version to reduce memory copies and improve performance
    #[inline]
    fn create_view_from_string(
        string_data: &[u8],
        page_bytes: &mut Vec<u8>,
        page_offset: &mut usize,
        buffer_index: u32,
    ) -> View {
        let len = string_data.len() as u32;

        if len <= 12 {
            // Small string: store inline - use unsafe for better performance
            unsafe {
                let mut payload = [0u8; 16];
                // Directly write length as little-endian bytes
                payload
                    .as_mut_ptr()
                    .cast::<u32>()
                    .write_unaligned(len.to_le());

                // Copy string data directly without bounds checking
                std::ptr::copy_nonoverlapping(
                    string_data.as_ptr(),
                    payload.as_mut_ptr().add(4),
                    len as usize,
                );

                // Use transmute for maximum performance - no validation overhead
                std::mem::transmute::<[u8; 16], View>(payload)
            }
        } else {
            // Large string: store in buffer
            unsafe {
                let mut payload = [0u8; 16];
                let payload_ptr = payload.as_mut_ptr();

                // Write all fields directly as u32 values
                payload_ptr.cast::<u32>().write_unaligned(len.to_le());

                // Copy prefix (first 4 bytes) directly
                std::ptr::copy_nonoverlapping(string_data.as_ptr(), payload_ptr.add(4), 4);

                // Write buffer index and offset
                payload_ptr
                    .add(8)
                    .cast::<u32>()
                    .write_unaligned(buffer_index.to_le());
                payload_ptr
                    .add(12)
                    .cast::<u32>()
                    .write_unaligned((*page_offset as u32).to_le());

                // Reserve space if needed to avoid reallocations
                let new_size = page_bytes.len() + string_data.len();
                if page_bytes.capacity() < new_size {
                    page_bytes.reserve(string_data.len().max(4096)); // Reserve at least 4KB
                }

                // Append string bytes to the current page buffer
                page_bytes.extend_from_slice(string_data);
                *page_offset += string_data.len();

                // Use transmute for maximum performance - no validation overhead
                std::mem::transmute::<[u8; 16], View>(payload)
            }
        }
    }

    /// Process plain encoded data page
    fn process_plain_encoding(
        &self,
        values_buffer: &[u8],
        remaining: usize,
        views: &mut Vec<View>,
        buffers: &mut Vec<Buffer<u8>>,
        total_bytes_len: &mut usize,
    ) -> Result<(), ErrorCode> {
        let mut bytes = Vec::new();
        let mut offset = 0;
        let current_buffer_index = buffers.len() as u32;

        for _ in 0..remaining {
            if offset + 4 > values_buffer.len() {
                break;
            }

            // Read 4-byte little-endian length
            let len_bytes = &values_buffer[offset..offset + 4];
            let len = u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]);
            offset += 4;

            if offset + len as usize > values_buffer.len() {
                return Err(ErrorCode::Internal(
                    "String length exceeds buffer size".to_string(),
                ));
            }

            let str_bytes = &values_buffer[offset..offset + len as usize];
            offset += len as usize;

            let mut payload = [0u8; 16];
            payload[0..4].copy_from_slice(&len.to_le_bytes());

            if len <= 12 {
                // Small string: store inline
                payload[4..4 + len as usize].copy_from_slice(str_bytes);
            } else {
                // Large string: store in buffer
                // Set prefix (first 4 bytes)
                payload[4..8].copy_from_slice(&str_bytes[0..4]);

                // Set buffer index
                payload[8..12].copy_from_slice(&current_buffer_index.to_le_bytes());

                // Set offset
                payload[12..16].copy_from_slice(&(bytes.len() as u32).to_le_bytes());

                // Append string bytes to the buffer
                bytes.extend_from_slice(str_bytes);
            }

            views.push(unsafe { std::mem::transmute::<[u8; 16], View>(payload) });
            // Accumulate total bytes length immediately
            *total_bytes_len += len as usize;
        }

        if !bytes.is_empty() {
            buffers.push(Buffer::from(bytes));
        }

        Ok(())
    }

    /// Process RLE dictionary encoded data page
    fn process_rle_dictionary_encoding(
        &mut self,
        values_buffer: &[u8],
        remaining: usize,
        views: &mut Vec<View>,
        buffers: &mut Vec<Buffer<u8>>,
        total_bytes_len: &mut usize,
    ) -> Result<(), ErrorCode> {
        if values_buffer.is_empty() {
            return Err(ErrorCode::Internal("Empty RLE dictionary data".to_string()));
        }

        let bit_width = values_buffer[0];

        if let Some(ref dict) = self.dictionary {
            // Fast path optimization for small dictionaries with all small strings
            // TODO: calculate this while building dictionary
            let all_small_strings = dict.iter().all(|s| s.len() <= 12);

            if all_small_strings && dict.len() <= 16 {
                // Pre-allocate exact capacity to eliminate all Vec::push overhead
                views.reserve_exact(remaining);

                if bit_width == 0 {
                    // All indices are 0, repeat dictionary[0] for all values
                    if dict.is_empty() {
                        return Err(ErrorCode::Internal(
                            "Empty dictionary for RLE dictionary encoding".to_string(),
                        ));
                    }

                    let dict_entry = &dict[0];
                    // TODO use slice fill
                    for _ in 0..remaining {
                        // TODO bench this, seems to be a hotspot
                        // Safe to use create_inline_view since we're in the small strings path
                        views.push(Self::create_inline_view(dict_entry));
                        *total_bytes_len += dict_entry.len();
                    }
                } else {
                    // Create new RleDecoder for each call (no caching for debugging)
                    let mut rle_decoder = RleDecoder::new(bit_width);
                    rle_decoder.set_data(bytes::Bytes::copy_from_slice(&values_buffer[1..]));

                    // Pre-compute dictionary views for better performance
                    // Safe since we're in the small strings path (all ≤12 bytes)
                    // Use caching to avoid repeated create_inline_view calls
                    if self.cached_dict_views.is_none() {
                        self.cached_dict_views = Some(
                            dict.iter()
                                .map(|s| Self::create_inline_view(s))
                                .collect::<Vec<_>>(),
                        );
                    }
                    let dict_views = self.cached_dict_views.as_ref().unwrap();

                    // Use get_batch_with_dict for direct dictionary decoding - most efficient
                    // Directly decode into the target views slice
                    let start_len = views.len();
                    views.reserve_exact(remaining);

                    // Get mutable slice for the new elements (uninitialized but will be fully written by get_batch_with_dict)
                    let target_slice = unsafe {
                        let ptr = views.as_mut_ptr().add(start_len);
                        std::slice::from_raw_parts_mut(ptr, remaining)
                    };

                    let decoded_count = rle_decoder
                        .get_batch_with_dict(dict_views, target_slice, remaining)
                        .map_err(|e| {
                            ErrorCode::Internal(format!("Failed to decode RLE with dict: {}", e))
                        })?;

                    if decoded_count != remaining {
                        return Err(ErrorCode::Internal(format!(
                            "RleDecoder returned wrong count: expected={}, got={}",
                            remaining, decoded_count
                        )));
                    }

                    // Now it's safe to update the length since all elements are initialized
                    unsafe {
                        views.set_len(start_len + remaining);
                    }

                    // Calculate total bytes length from the decoded views
                    for view in &views[start_len..start_len + remaining] {
                        *total_bytes_len += view.length as usize;
                    }
                }
                return Ok(());
            }
        }

        // Create new RleDecoder for general path (no caching for debugging)
        let mut rle_decoder = RleDecoder::new(bit_width);
        rle_decoder.set_data(bytes::Bytes::copy_from_slice(&values_buffer[1..]));

        if let Some(ref dict) = self.dictionary {
            // Initialize buffer management variables for general case
            let current_buffer_index = buffers.len() as u32;
            let mut page_bytes = Vec::new();
            let mut page_offset = 0usize;

            // Pre-allocate exact capacity without default initialization - much faster
            views.reserve_exact(remaining);
            let start_len = views.len();

            // Get raw indices first for efficient processing
            let mut indices = vec![0i32; remaining];
            let decoded_count = rle_decoder
                .get_batch(&mut indices)
                .map_err(|e| ErrorCode::Internal(format!("Failed to decode RLE indices: {}", e)))?;

            if decoded_count != remaining {
                return Err(ErrorCode::Internal(format!(
                    "RleDecoder returned wrong count: expected={}, got={}",
                    remaining, decoded_count
                )));
            }

            // Process indices efficiently: O(n) with proper buffer management
            unsafe {
                let views_ptr = views.as_mut_ptr().add(start_len);
                for (i, &index) in indices.iter().enumerate() {
                    let dict_idx = index as usize;
                    if dict_idx >= dict.len() {
                        return Err(ErrorCode::Internal(format!(
                            "Dictionary index {} out of bounds (dictionary size: {})",
                            dict_idx,
                            dict.len()
                        )));
                    }

                    // Use create_view_from_string for proper buffer management (handles both small and large strings)
                    let view = Self::create_view_from_string(
                        &dict[dict_idx],
                        &mut page_bytes,
                        &mut page_offset,
                        current_buffer_index,
                    );
                    *views_ptr.add(i) = view;
                    *total_bytes_len += dict[dict_idx].len();
                }

                // Update vector length once at the end
                views.set_len(start_len + remaining);
            }

            if !page_bytes.is_empty() {
                buffers.push(Buffer::from(page_bytes));
            }
        } else {
            return Err(ErrorCode::Internal(
                "Dictionary not found for RLE dictionary encoding".to_string(),
            ));
        }

        Ok(())
    }

    /// Create an inline View for small strings (≤12 bytes) with maximum performance
    #[inline]
    fn create_inline_view(string_data: &[u8]) -> View {
        unsafe {
            let mut payload = [0u8; 16];
            let len = string_data.len() as u32;

            // Write length directly
            payload
                .as_mut_ptr()
                .cast::<u32>()
                .write_unaligned(len.to_le());

            // Copy string data directly without bounds checking
            std::ptr::copy_nonoverlapping(
                string_data.as_ptr(),
                payload.as_mut_ptr().add(4),
                len as usize,
            );

            // Use transmute for maximum performance
            std::mem::transmute::<[u8; 16], View>(payload)
        }
    }

    /// Process a data page based on its encoding type
    fn process_data_page(
        &mut self,
        data_page: &parquet2::page::DataPage,
        views: &mut Vec<View>,
        buffers: &mut Vec<Buffer<u8>>,
        total_bytes_len: &mut usize,
    ) -> Result<(), ErrorCode> {
        let (_, _, values_buffer) = parquet2::page::split_buffer(data_page)
            .map_err(|e| ErrorCode::StorageOther(format!("Failed to split buffer: {}", e)))?;
        let remaining = data_page.num_values();

        match data_page.encoding() {
            Encoding::Plain => self.process_plain_encoding(
                values_buffer,
                remaining,
                views,
                buffers,
                total_bytes_len,
            ),
            Encoding::RleDictionary | Encoding::PlainDictionary => self
                .process_rle_dictionary_encoding(
                    values_buffer,
                    remaining,
                    views,
                    buffers,
                    total_bytes_len,
                ),
            _ => Err(ErrorCode::Internal(format!(
                "Unsupported encoding for string column: {:?}",
                data_page.encoding()
            ))),
        }
    }
}

impl<'a> Iterator for StringIter<'a> {
    type Item = Result<Column, ErrorCode>;

    fn next(&mut self) -> Option<Self::Item> {
        let limit = self.chunk_size.unwrap_or(self.num_rows);
        let mut views = Vec::with_capacity(limit);
        let mut buffers = Vec::new();
        let mut total_bytes_len = 0;

        while views.len() < limit {
            let page = match self.pages.next_owned() {
                Err(e) => {
                    return Some(Err(ErrorCode::StorageOther(format!(
                        "Failed to get next page: {}",
                        e
                    ))))
                }
                Ok(None) => break,
                Ok(Some(page)) => page,
            };

            match page {
                Page::Data(data_page) => {
                    let physical_type = &data_page.descriptor.primitive_type.physical_type;
                    let is_optional = data_page.descriptor.primitive_type.field_info.repetition
                        == parquet2::schema::Repetition::Optional;

                    if physical_type != &PhysicalType::ByteArray || is_optional {
                        return Some(Err(ErrorCode::StorageOther(
                            "Only BYTE_ARRAY required fields are supported in this implementation"
                                .to_string(),
                        )));
                    }

                    // Process data page and handle potential errors
                    if let Err(e) = self.process_data_page(
                        &data_page,
                        &mut views,
                        &mut buffers,
                        &mut total_bytes_len,
                    ) {
                        return Some(Err(e));
                    }
                    continue;
                }
                Page::Dict(dict_page) => {
                    // Process dictionary page and handle potential errors
                    if let Err(e) = self.process_dictionary_page(&dict_page) {
                        return Some(Err(e));
                    }
                    continue;
                }
            }
        }

        if views.is_empty() {
            return None;
        }

        // Calculate total buffer length for new_unchecked
        let total_buffer_len = buffers.iter().map(|b| b.len()).sum();

        // Convert views Vec to Buffer
        let views_buffer = Buffer::from(views);

        // Use new_unchecked for better performance (no validation overhead)
        let column = Utf8ViewColumn::new_unchecked(
            views_buffer,
            buffers.into(),
            total_bytes_len,
            total_buffer_len,
        );

        Some(Ok(Column::String(column)))
    }
}
