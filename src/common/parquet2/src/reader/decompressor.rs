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

//! Decompressor that integrates with zero-copy PageReader

use parquet2::compression::Compression;
use parquet2::error::Error;
use parquet2::page::DataPage;
use parquet2::page::DictPage;
use parquet2::page::Page;
use parquet2::FallibleStreamingIterator;

use crate::reader::page_reader::PageReader;
use crate::reader::pages::BorrowedCompressedPage;

pub struct Decompressor<'a> {
    page_reader: PageReader<'a>,
    decompression_buffer: Vec<u8>,
    current_page: Option<Page>,
    was_decompressed: bool,
}

impl<'a> Decompressor<'a> {
    pub fn new(page_reader: PageReader<'a>, decompression_buffer: Vec<u8>) -> Self {
        Self {
            page_reader,
            decompression_buffer,
            current_page: None,
            was_decompressed: false,
        }
    }

    fn decompress_borrowed_page(
        compressed_page: BorrowedCompressedPage<'_>,
        uncompressed_buffer: &mut Vec<u8>,
    ) -> parquet2::error::Result<Page> {
        let uncompressed_size = compressed_page.uncompressed_size();

        // Ensure capacity without clearing (old data will be overwritten)
        uncompressed_buffer.reserve(uncompressed_size);

        // Get raw pointer to avoid initialization
        let buffer_ptr = uncompressed_buffer.as_mut_ptr();

        let actual_len = if !compressed_page.is_compressed() {
            // Direct copy for uncompressed data
            Self::copy_uncompressed_data(compressed_page.data(), buffer_ptr)?
        } else {
            // Decompress based on compression type
            Self::decompress_data(
                compressed_page.compression(),
                compressed_page.data(),
                buffer_ptr,
                uncompressed_size,
            )?
        };

        // Set the actual length
        unsafe {
            uncompressed_buffer.set_len(actual_len);
        }

        // Create the appropriate page type
        let page = match compressed_page {
            BorrowedCompressedPage::Data(compressed_data_page) => Page::Data(DataPage::new(
                compressed_data_page.header,
                uncompressed_buffer.clone(),
                compressed_data_page.descriptor,
                None,
            )),
            BorrowedCompressedPage::Dict(compressed_dict_page) => Page::Dict(DictPage::new(
                uncompressed_buffer.clone(),
                compressed_dict_page.num_values,
                compressed_dict_page.is_sorted,
            )),
        };
        Ok(page)
    }

    /// Copy uncompressed data directly
    fn copy_uncompressed_data(
        src_data: &[u8],
        buffer_ptr: *mut u8,
    ) -> parquet2::error::Result<usize> {
        unsafe {
            std::ptr::copy_nonoverlapping(src_data.as_ptr(), buffer_ptr, src_data.len());
        }
        Ok(src_data.len())
    }

    /// Decompress data based on compression type
    fn decompress_data(
        compression: Compression,
        src_data: &[u8],
        buffer_ptr: *mut u8,
        uncompressed_size: usize,
    ) -> parquet2::error::Result<usize> {
        let buffer_slice = unsafe { std::slice::from_raw_parts_mut(buffer_ptr, uncompressed_size) };

        match compression {
            Compression::Lz4 => {
                lz4_flex::decompress_into(src_data, buffer_slice)
                    .map_err(|e| Error::OutOfSpec(format!("LZ4 decompression failed: {}", e)))?;
                Ok(uncompressed_size)
            }
            Compression::Zstd => zstd::bulk::decompress_to_buffer(src_data, buffer_slice)
                .map_err(|e| Error::OutOfSpec(format!("Zstd decompression failed: {}", e))),
            _ => Err(Error::FeatureNotSupported(format!(
                "Compression {:?} not supported",
                compression
            ))),
        }
    }

    pub fn next_owned(&mut self) -> Result<Option<Page>, Error> {
        let page_tuple = self.page_reader.next_page()?;

        if let Some(page) = page_tuple {
            self.was_decompressed = page.compression() != Compression::Uncompressed;

            let decompress_page =
                Self::decompress_borrowed_page(page, &mut self.decompression_buffer)?;

            Ok(Some(decompress_page))
        } else {
            Ok(None)
        }
    }
}

impl<'a> FallibleStreamingIterator for Decompressor<'a> {
    type Item = Page;
    type Error = Error;

    fn advance(&mut self) -> Result<(), Self::Error> {
        self.current_page = None;
        let page_tuple = self.page_reader.next_page()?;

        if let Some(page) = page_tuple {
            self.was_decompressed = page.compression() != Compression::Uncompressed;

            let decompress_page =
                Self::decompress_borrowed_page(page, &mut self.decompression_buffer)?;

            self.current_page = Some(decompress_page);
        }

        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        self.current_page.as_ref()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}
