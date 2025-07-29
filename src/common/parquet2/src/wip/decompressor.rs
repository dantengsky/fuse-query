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

//! Simple decompressor that works with the new PageReader API
//! This is an experimental version that integrates with the zero-copy PageReader

use parquet2::compression::Compression;
use parquet2::error::Error;
use parquet2::page::DataPage;
use parquet2::page::DictPage;
use parquet2::page::Page;
use parquet2::FallibleStreamingIterator;

use crate::wip::page_reader::PageReader;
use crate::wip::pages::BorrowedCompressedPage;

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

    /// Decompress borrowed page data directly into the uncompressed buffer
    /// This avoids the Vec<u8> copy by working directly with the borrowed slice
    fn decompress_borrowed_page(
        compressed_page: BorrowedCompressedPage<'_>,
        uncompressed_buffer: &mut Vec<u8>,
    ) -> parquet2::error::Result<Page> {
        // TODO Here we are assuming V1 pages, this is not correct for v2 pages
        let uncompressed_size = compressed_page.uncompressed_size();
        uncompressed_buffer.reserve(uncompressed_size);
        unsafe {
            uncompressed_buffer.set_len(uncompressed_size);
        }

        if !compressed_page.is_compressed() {
            // No decompression needed - copy directly from the borrowed slice
            uncompressed_buffer.extend_from_slice(compressed_page.data());
        } else {
            // Decompress directly into the buffer
            match compressed_page.compression() {
                Compression::Lz4 => {
                    let decompressed_len =
                        lz4_flex::decompress_into(compressed_page.data(), uncompressed_buffer)
                            .map_err(|e| {
                                Error::OutOfSpec(format!("LZ4 decompression failed: {}", e))
                            })?;
                }
                Compression::Zstd => {
                    zstd::bulk::decompress_to_buffer(compressed_page.data(), uncompressed_buffer)
                        .map(|_| ())
                        .map_err(|e| {
                            Error::OutOfSpec(format!("Zstd decompression failed: {}", e))
                        })?;
                }
                _ => {
                    return Err(Error::FeatureNotSupported(format!(
                        "Compression {:?} not supported",
                        compressed_page.compression()
                    )));
                }
            }
        };

        // Create a DataPage from the decompressed data
        // Note: We need to take ownership of the buffer data here
        let page = match compressed_page {
            BorrowedCompressedPage::Data(compressed_data_page) => Page::Data(DataPage::new(
                compressed_data_page.header,
                std::mem::take(uncompressed_buffer),
                compressed_data_page.descriptor,
                None,
            )),
            BorrowedCompressedPage::Dict(compressed_dict_page) => Page::Dict(DictPage::new(
                std::mem::take(uncompressed_buffer),
                compressed_dict_page.num_values,
                compressed_dict_page.is_sorted,
            )),
        };

        Ok(page)
    }

    fn into_buffer(self) -> Vec<u8> {
        self.decompression_buffer
    }
}

impl<'a> FallibleStreamingIterator for Decompressor<'a> {
    type Item = Page;
    type Error = Error;

    fn advance(&mut self) -> Result<(), Self::Error> {
        self.current_page = None;
        // Get the next page from our zero-copy PageReader
        let page_tuple = self.page_reader.next_page()?;

        if let Some(page) = page_tuple {
            // Set decompression flag
            self.was_decompressed = page.compression() != Compression::Uncompressed;

            // Decompress the page directly into the buffer
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
