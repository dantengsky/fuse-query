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

use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::Column;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::read::PageReader;
use parquet2::schema::types::PhysicalType;
use streaming_decompression::FallibleStreamingIterator;

use crate::BuffedBasicDecompressor;

pub struct DecimalIter<'a> {
    pages: BuffedBasicDecompressor<PageReader<&'a [u8]>>,
    chunk_size: Option<usize>,
    num_rows: usize,
    precision: u8,
    scale: u8,
}

impl<'a> DecimalIter<'a> {
    pub fn new(
        pages: BuffedBasicDecompressor<PageReader<&'a [u8]>>,
        num_rows: usize,
        chunk_size: Option<usize>,
        precision: u8,
        scale: u8,
    ) -> DecimalIter<'a> {
        Self {
            pages,
            chunk_size,
            num_rows,
            precision,
            scale,
        }
    }
}

impl Iterator for DecimalIter<'_> {
    type Item = databend_common_exception::Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut column_data = Vec::with_capacity(self.chunk_size.unwrap_or(self.num_rows));
        while column_data.len() < self.chunk_size.unwrap_or(self.num_rows) {
            let page = match self.pages.next() {
                Err(e) => {
                    return Some(Err(ErrorCode::StorageOther(format!(
                        "Failed to get next page: {}",
                        e
                    ))))
                }
                Ok(Some(page)) => page,
                Ok(None) => {
                    if column_data.is_empty() {
                        return None;
                    } else {
                        // Create a Decimal64 column with the values read so far
                        let decimal_size = DecimalSize::new_unchecked(self.precision, self.scale);
                        let col = Column::Decimal(
                            databend_common_expression::types::DecimalColumn::Decimal64(
                                Buffer::from(column_data),
                                decimal_size,
                            ),
                        );
                        return Some(Ok(col));
                    }
                }
            };
            match page {
                Page::Data(data_page) => {
                    if data_page.descriptor.primitive_type.physical_type != PhysicalType::Int64
                        || data_page.descriptor.primitive_type.field_info.repetition
                            == parquet2::schema::Repetition::Optional
                    {
                        return Some(Err(ErrorCode::StorageOther(
                            "Only required Int64 fields supported for DECIMAL(15,2)".to_string(),
                        )));
                    }
                    let (_, _, values_buffer) = match parquet2::page::split_buffer(data_page) {
                        Ok(result) => result,
                        Err(e) => {
                            return Some(Err(ErrorCode::StorageOther(format!(
                                "Failed to split buffer: {}",
                                e
                            ))))
                        }
                    };
                    match data_page.encoding() {
                        Encoding::Plain => {
                            let num_values = values_buffer.len() / std::mem::size_of::<i64>();
                            let remaining =
                                self.chunk_size.unwrap_or(self.num_rows) - column_data.len();
                            let to_read = remaining.min(num_values);
                            let old_len = column_data.len();

                            // Direct copy from the buffer to our column data
                            unsafe {
                                // Get source pointer to the raw buffer
                                let src_ptr = values_buffer.as_ptr() as *const i64;
                                // Get destination pointer to our column data
                                let dst_ptr = column_data.as_mut_ptr().add(old_len);
                                // Copy the values directly
                                std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, to_read);
                                // Update the length of our column data
                                column_data.set_len(old_len + to_read);
                            }

                            if column_data.len() >= self.chunk_size.unwrap_or(self.num_rows) {
                                break;
                            }
                        }
                        encoding => {
                            return Some(Err(ErrorCode::StorageOther(format!(
                                "Encoding {:?} is not supported for DECIMAL(15,2)",
                                encoding
                            ))));
                        }
                    }
                }
                _ => {
                    return Some(Err(ErrorCode::StorageOther(
                        "Only data pages are supported".to_string(),
                    )))
                }
            }
        }

        if column_data.is_empty() {
            return None;
        }

        // Create a Decimal64 column with the specified precision and scale
        let decimal_size = DecimalSize::new_unchecked(self.precision, self.scale);
        let col = Column::Decimal(databend_common_expression::types::DecimalColumn::Decimal64(
            Buffer::from(column_data),
            decimal_size,
        ));
        Some(Ok(col))
    }
}
