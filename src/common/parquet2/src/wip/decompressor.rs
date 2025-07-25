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

//! Zero-copy decompressor that works with the new PageReader API
//! This is an experimental version that integrates with the zero-copy PageReader

use parquet2::compression::Compression;
use parquet2::error::Error;
use parquet2::metadata::Descriptor;
use parquet2::page::DataPage;
use parquet2::page::DataPageHeader;
use parquet2::page::Page;
use parquet2::FallibleStreamingIterator;

use crate::wip::page_reader::PageReader;

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
        header: DataPageHeader,
        compressed_data: &[u8],
        compression: Compression,
        uncompressed_size: usize,
        descriptor: Descriptor,
        uncompressed_buffer: &mut Vec<u8>,
    ) -> parquet2::error::Result<Page> {
        // Ensure the buffer has enough capacity
        uncompressed_buffer.clear();
        uncompressed_buffer.reserve(uncompressed_size);

        let decompressed_data = if compression == Compression::Uncompressed {
            // No decompression needed - copy directly from the borrowed slice
            uncompressed_buffer.extend_from_slice(compressed_data);
            uncompressed_buffer.as_mut_slice()
        } else {
            // Decompress directly into the buffer
            match compression {
                #[cfg(feature = "compression")]
                Compression::Snappy => {
                    use snap::raw::Decoder;
                    let mut decoder = Decoder::new();
                    let decompressed_len = decoder
                        .decompress(compressed_data, uncompressed_buffer)
                        .map_err(|e| {
                            Error::OutOfSpec(format!("Snappy decompression failed: {}", e))
                        })?;
                    &mut uncompressed_buffer[..decompressed_len]
                }
                #[cfg(feature = "compression")]
                Compression::Gzip => {
                    use std::io::Read;

                    use flate2::read::GzDecoder;
                    let mut decoder = GzDecoder::new(compressed_data);
                    decoder.read_to_end(uncompressed_buffer).map_err(|e| {
                        Error::OutOfSpec(format!("Gzip decompression failed: {}", e))
                    })?;
                    uncompressed_buffer.as_mut_slice()
                }
                #[cfg(feature = "compression")]
                Compression::Lzo => {
                    return Err(Error::FeatureNotSupported(
                        "LZO compression not supported".to_string(),
                    ));
                }
                #[cfg(feature = "compression")]
                Compression::Brotli => {
                    use std::io::Read;
                    let mut decoder = brotli::Decompressor::new(compressed_data, 4096);
                    decoder.read_to_end(uncompressed_buffer).map_err(|e| {
                        Error::OutOfSpec(format!("Brotli decompression failed: {}", e))
                    })?;
                    uncompressed_buffer.as_mut_slice()
                }
                #[cfg(feature = "compression")]
                Compression::Lz4 => {
                    let decompressed_len =
                        lz4_flex::decompress_into(compressed_data, uncompressed_buffer).map_err(
                            |e| Error::OutOfSpec(format!("LZ4 decompression failed: {}", e)),
                        )?;
                    &mut uncompressed_buffer[..decompressed_len]
                }
                #[cfg(feature = "compression")]
                Compression::Zstd => {
                    use std::io::Read;
                    let mut decoder =
                        zstd::stream::read::Decoder::new(compressed_data).map_err(|e| {
                            Error::OutOfSpec(format!("Zstd decoder creation failed: {}", e))
                        })?;
                    decoder.read_to_end(uncompressed_buffer).map_err(|e| {
                        Error::OutOfSpec(format!("Zstd decompression failed: {}", e))
                    })?;
                    uncompressed_buffer.as_mut_slice()
                }
                _ => {
                    return Err(Error::FeatureNotSupported(format!(
                        "Compression {:?} not supported",
                        compression
                    )));
                }
            }
        };

        // Create a DataPage from the decompressed data
        // Note: We need to take ownership of the buffer data here
        let data_page = DataPage::new(header, decompressed_data.to_vec(), descriptor, None);

        Ok(Page::Data(data_page))
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

        if let Some((header, data, compression, uncompressed_size, descriptor)) = page_tuple {
            // Set decompression flag
            self.was_decompressed = compression != Compression::Uncompressed;

            // Decompress the page directly into the buffer
            let decompress_page = Self::decompress_borrowed_page(
                header,
                data,
                compression,
                uncompressed_size,
                descriptor,
                &mut self.decompression_buffer,
            )?;

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
