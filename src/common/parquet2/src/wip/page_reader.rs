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

use parquet2::compression::Compression;
use parquet2::encoding::Encoding;
use parquet2::error::Error;
use parquet2::metadata::ColumnChunkMetaData;
use parquet2::metadata::Descriptor;
use parquet2::page::DataPageHeader;
use parquet2::page::PageType;
use parquet2::page::ParquetPageHeader;
use parquet2::read::PageFilter;
use parquet2::read::PageMetaData;
use parquet_format_safe::thrift::protocol::TCompactInputProtocol;

pub struct PageReader<'a> {
    // The source data slice
    reader: &'a [u8],

    compression: Compression,

    // The number of values we have seen so far.
    seen_num_values: i64,

    // The number of total values in this column chunk.
    total_num_values: i64,

    pages_filter: PageFilter,

    descriptor: Descriptor,

    // Maximum page size (compressed or uncompressed) to limit allocations
    max_page_size: usize,
}

impl<'a> PageReader<'a> {
    /// Returns a new [`parquet2::read::PageReader`].
    ///
    /// It assumes that the reader has been `seeked` to the beginning of `column`.
    /// The parameter `max_header_size`
    pub fn new(
        reader: &'a [u8],
        column: &ColumnChunkMetaData,
        pages_filter: PageFilter,
        max_page_size: usize,
    ) -> Self {
        Self::new_with_page_meta(reader, column.into(), pages_filter, max_page_size)
    }

    /// Create a a new [`parquet2::read::PageReader`] with [`PageMetaData`].
    ///
    /// It assumes that the reader has been `seeked` to the beginning of `column`.
    pub fn new_with_page_meta(
        reader: &'a [u8],
        reader_meta: PageMetaData,
        pages_filter: PageFilter,
        max_page_size: usize,
    ) -> Self {
        Self {
            reader,
            total_num_values: reader_meta.num_values,
            compression: reader_meta.compression,
            seen_num_values: 0,
            descriptor: reader_meta.descriptor,
            pages_filter,
            max_page_size,
        }
    }

    /// Zero-copy page reading that borrows data from the slice instead of copying
    pub fn next_page(
        &mut self,
    ) -> parquet2::error::Result<Option<(DataPageHeader, &[u8], Compression, usize, Descriptor)>>
    {
        if self.seen_num_values >= self.total_num_values {
            return Ok(None);
        };

        let page_header = read_page_header_from_slice(&mut self.reader, self.max_page_size)?;

        self.seen_num_values += get_page_header(&page_header)?
            .map(|x| x.num_values() as i64)
            .unwrap_or_default();

        let read_size: usize = page_header.compressed_page_size.try_into()?;

        if read_size > self.max_page_size {
            return Err(Error::WouldOverAllocate);
        }

        if self.reader.len() < read_size {
            return Err(Error::OutOfSpec(
                "Not enough data in slice for page".to_string(),
            ));
        }

        // Zero-copy: borrow the data directly from the slice
        let data_slice = &self.reader[..read_size];
        // Advance the reader position
        self.reader = &self.reader[read_size..];

        // Extract page information and return as tuple for zero-copy access
        match page_header.type_.try_into()? {
            PageType::DataPage => {
                let header = page_header.data_page_header.ok_or_else(|| {
                    Error::OutOfSpec(
                        "The page header type is a v1 data page but the v1 data header is empty"
                            .to_string(),
                    )
                })?;
                Ok(Some((
                    DataPageHeader::V1(header),
                    data_slice,
                    self.compression,
                    page_header.uncompressed_page_size.try_into()?,
                    self.descriptor.clone(),
                )))
            }
            PageType::DataPageV2 => {
                let header = page_header.data_page_header_v2.ok_or_else(|| {
                    Error::OutOfSpec(
                        "The page header type is a v2 data page but the v2 data header is empty"
                            .to_string(),
                    )
                })?;
                Ok(Some((
                    DataPageHeader::V2(header),
                    data_slice,
                    self.compression,
                    page_header.uncompressed_page_size.try_into()?,
                    self.descriptor.clone(),
                )))
            }
            PageType::DictionaryPage => {
                // For now, skip dictionary pages or handle them separately
                // This is a simplified implementation
                Err(Error::OutOfSpec(
                    "Dictionary pages not yet supported in simplified API".to_string(),
                ))
            }
        }
    }
}

pub(crate) fn read_page_header_from_slice(
    reader: &mut &[u8],
    max_size: usize,
) -> parquet2::error::Result<ParquetPageHeader> {
    let mut prot = TCompactInputProtocol::new(reader, max_size);
    let page_header = ParquetPageHeader::read_from_in_protocol(&mut prot)?;
    Ok(page_header)
}

pub(crate) fn get_page_header(
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
