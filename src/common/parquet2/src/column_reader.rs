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

//! Direct deserialization from parquet2 to DataBlock without Arrow intermediate representation
//!
//! This crate provides functionality to directly deserialize Parquet data into DataBlock
//! structures, bypassing the Arrow memory model for improved performance.

use std::io::Read;

use parquet2::compression::Compression;
use parquet2::encoding::Encoding;
use parquet2::error::Error;
use parquet2::indexes::Interval;
use parquet2::metadata::ColumnChunkMetaData;
use parquet2::metadata::Descriptor;
use parquet2::page::CompressedDataPage;
use parquet2::page::CompressedDictPage;
use parquet2::page::CompressedPage;
use parquet2::page::DataPageHeader;
use parquet2::page::PageType;
use parquet2::page::ParquetPageHeader;
use parquet2::read::PageFilter;
use parquet2::read::PageMetaData;
use parquet_format_safe::thrift::protocol::TCompactInputProtocol;

pub struct PageReader<R: Read> {
    // The source
    reader: R,

    compression: Compression,

    // The number of values we have seen so far.
    seen_num_values: i64,

    // The number of total values in this column chunk.
    total_num_values: i64,

    pages_filter: PageFilter,

    descriptor: Descriptor,

    // The currently allocated buffer.
    pub(crate) scratch: Vec<u8>,

    // Maximum page size (compressed or uncompressed) to limit allocations
    max_page_size: usize,
}

impl<R: Read> PageReader<R> {
    /// Returns a new [`parquet2::read::PageReader`].
    ///
    /// It assumes that the reader has been `seeked` to the beginning of `column`.
    /// The parameter `max_header_size`
    pub fn new(
        reader: R,
        column: &ColumnChunkMetaData,
        pages_filter: PageFilter,
        scratch: Vec<u8>,
        max_page_size: usize,
    ) -> Self {
        Self::new_with_page_meta(reader, column.into(), pages_filter, scratch, max_page_size)
    }

    /// Create a a new [`parquet2::read::PageReader`] with [`PageMetaData`].
    ///
    /// It assumes that the reader has been `seeked` to the beginning of `column`.
    pub fn new_with_page_meta(
        reader: R,
        reader_meta: PageMetaData,
        pages_filter: PageFilter,
        scratch: Vec<u8>,
        max_page_size: usize,
    ) -> Self {
        Self {
            reader,
            total_num_values: reader_meta.num_values,
            compression: reader_meta.compression,
            seen_num_values: 0,
            descriptor: reader_meta.descriptor,
            pages_filter,
            scratch,
            max_page_size,
        }
    }

    /// Returns the reader and this Readers' interval buffer
    pub fn into_inner(self) -> (R, Vec<u8>) {
        (self.reader, self.scratch)
    }

    pub fn next_page(
        &mut self,
        buffer: &mut Vec<u8>,
    ) -> parquet2::error::Result<Option<CompressedPage>> {
        if self.seen_num_values >= self.total_num_values {
            return Ok(None);
        };
        build_page(self, buffer)
    }
}

pub(super) fn build_page<R: Read>(
    reader: &mut PageReader<R>,
    buffer: &mut Vec<u8>,
) -> parquet2::error::Result<Option<CompressedPage>> {
    let page_header = read_page_header(&mut reader.reader, reader.max_page_size)?;

    reader.seen_num_values += get_page_header(&page_header)?
        .map(|x| x.num_values() as i64)
        .unwrap_or_default();

    let read_size: usize = page_header.compressed_page_size.try_into()?;

    if read_size > reader.max_page_size {
        return Err(Error::WouldOverAllocate);
    }

    buffer.clear();
    buffer.try_reserve(read_size)?;
    let bytes_read = reader
        .reader
        .by_ref()
        .take(read_size as u64)
        .read_to_end(buffer)?;

    if bytes_read != read_size {
        return Err(Error::OutOfSpec(
            "The page header reported the wrong page size".to_string(),
        ));
    }

    finish_page(page_header, buffer, reader.compression, &reader.descriptor).map(Some)
}

pub(super) fn finish_page(
    page_header: ParquetPageHeader,
    data: &mut Vec<u8>,
    compression: Compression,
    descriptor: &Descriptor,
) -> parquet2::error::Result<CompressedPage> {
    let type_ = page_header.type_.try_into()?;
    let uncompressed_page_size = page_header.uncompressed_page_size.try_into()?;
    match type_ {
        PageType::DictionaryPage => {
            let dict_header = page_header.dictionary_page_header.as_ref().ok_or_else(|| {
                Error::OutOfSpec(
                    "The page header type is a dictionary page but the dictionary header is empty"
                        .to_string(),
                )
            })?;
            let is_sorted = dict_header.is_sorted.unwrap_or(false);

            // move the buffer to `dict_page`
            let page = CompressedDictPage::new(
                std::mem::take(data),
                compression,
                uncompressed_page_size,
                dict_header.num_values.try_into()?,
                is_sorted,
            );

            Ok(CompressedPage::Dict(page))
        }
        PageType::DataPage => {
            let header = page_header.data_page_header.ok_or_else(|| {
                Error::OutOfSpec(
                    "The page header type is a v1 data page but the v1 data header is empty"
                        .to_string(),
                )
            })?;

            Ok(CompressedPage::Data(CompressedDataPage::new(
                DataPageHeader::V1(header),
                std::mem::take(data),
                compression,
                uncompressed_page_size,
                descriptor.clone(),
                None,
            )))
        }
        PageType::DataPageV2 => {
            let header = page_header.data_page_header_v2.ok_or_else(|| {
                Error::OutOfSpec(
                    "The page header type is a v2 data page but the v2 data header is empty"
                        .to_string(),
                )
            })?;

            Ok(CompressedPage::Data(CompressedDataPage::new(
                DataPageHeader::V2(header),
                std::mem::take(data),
                compression,
                uncompressed_page_size,
                descriptor.clone(),
                None,
            )))
        }
    }
}

pub(super) fn read_page_header<R: Read>(
    reader: &mut R,
    max_size: usize,
) -> parquet2::error::Result<ParquetPageHeader> {
    let mut prot = TCompactInputProtocol::new(reader, max_size);
    let page_header = ParquetPageHeader::read_from_in_protocol(&mut prot)?;
    Ok(page_header)
}

pub(super) fn get_page_header(
    header: &ParquetPageHeader,
) -> parquet2::error::Result<Option<DataPageHeader>> {
    let type_ = header.type_.try_into()?;
    Ok(match type_ {
        PageType::DataPage => {
            let header = header.data_page_header.clone().ok_or_else(|| {
                Error::OutOfSpec(
                    "The page header type is a v1 data page but the v1 header is empty".to_string(),
                )
            })?;
            let _: Encoding = header.encoding.try_into()?;
            let _: Encoding = header.repetition_level_encoding.try_into()?;
            let _: Encoding = header.definition_level_encoding.try_into()?;

            Some(DataPageHeader::V1(header))
        }
        PageType::DataPageV2 => {
            let header = header.data_page_header_v2.clone().ok_or_else(|| {
                Error::OutOfSpec(
                    "The page header type is a v1 data page but the v1 header is empty".to_string(),
                )
            })?;
            let _: Encoding = header.encoding.try_into()?;
            Some(DataPageHeader::V2(header))
        }
        _ => None,
    })
}
