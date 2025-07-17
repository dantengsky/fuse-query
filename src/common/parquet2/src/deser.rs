use std::i64;

use databend_common_column::binview::Utf8ViewColumn;
use databend_common_column::binview::View;
use databend_common_column::buffer::Buffer;
use databend_common_column::types::NativeType;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Column;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::read::PageReader;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveType;
use parquet2::FallibleStreamingIterator;

use crate::decompressor::BuffedBasicDecompressor;

pub type ColumnIter<'a> = Box<dyn Iterator<Item = Result<Column>> + Send + Sync + 'a>;
pub fn page_iter_to_columns<'a>(
    mut columns: Vec<BuffedBasicDecompressor<PageReader<&'a [u8]>>>,
    mut types: Vec<&PrimitiveType>,
    field: TableField,
    chunk_size: Option<usize>,
    num_rows: usize,
) -> Result<ColumnIter<'a>> {
    let pages = columns.pop().unwrap();
    let parquet_physical_type = &types.pop().unwrap().physical_type;

    match (parquet_physical_type, field.data_type) {
        (PhysicalType::Int64, TableDataType::Number(NumberDataType::Int64)) => {
            Ok(Box::new(IntegerIter::new(pages, num_rows, chunk_size)))
        }
        (PhysicalType::ByteArray, TableDataType::String) => {
            Ok(Box::new(StringIter::new(pages, num_rows, chunk_size)))
        }
        _ => unimplemented!(),
    }
}

struct IntegerIter<'a> {
    pages: BuffedBasicDecompressor<PageReader<&'a [u8]>>,
    chunk_size: Option<usize>,
    num_rows: usize,
}

impl<'a> IntegerIter<'a> {
    pub fn new(
        pages: BuffedBasicDecompressor<PageReader<&'a [u8]>>,
        num_rows: usize,
        chunk_size: Option<usize>,
    ) -> IntegerIter<'a> {
        Self {
            pages,
            chunk_size,
            num_rows,
        }
    }
}

impl Iterator for IntegerIter<'_> {
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        // Initialize a vector to store all the int64 values
        let mut column_data = Vec::with_capacity(self.chunk_size.unwrap_or(self.num_rows));

        // Process pages until we get enough data or run out of pages
        while column_data.len() < self.chunk_size.unwrap_or(self.num_rows) {
            // Get the next page
            let page = match self.pages.next() {
                Err(e) => {
                    return Some(Err(ErrorCode::StorageOther(format!(
                        "Failed to get next page: {}",
                        e
                    ))))
                }
                Ok(Some(page)) => page,
                Ok(None) => {
                    // No more pages - if we collected any data, return it
                    if column_data.is_empty() {
                        // TODO review this
                        return None; // No data collected and no more pages
                    } else {
                        let col = Column::Number(i64::upcast_column(Buffer::from(column_data)));
                        return Some(Ok(col));
                    }
                }
            };

            // Process the page based on its type
            match page {
                Page::Data(data_page) => {
                    // We only implement INT64 required fields
                    let physical_type = &data_page.descriptor.primitive_type.physical_type;
                    let is_optional = data_page.descriptor.primitive_type.field_info.repetition
                        == parquet2::schema::Repetition::Optional;

                    if physical_type != &parquet2::schema::types::PhysicalType::Int64 || is_optional
                    {
                        return Some(Err(ErrorCode::StorageOther(
                            "Only INT64 required fields are supported in this implementation"
                                .to_string(),
                        )));
                    }

                    // Split the buffer to get definition levels, repetition levels, and values
                    let (_, _, values_buffer) = match parquet2::page::split_buffer(&data_page) {
                        Ok(result) => result,
                        Err(e) => {
                            return Some(Err(ErrorCode::StorageOther(format!(
                                "Failed to split buffer: {}",
                                e
                            ))))
                        }
                    };

                    // Deserialize values based on encoding
                    match data_page.encoding() {
                        Encoding::Plain => {
                            // TODO defensive check the len is multiple of size_of::<i64>
                            // For Plain encoding with INT64, we can do direct memory copy
                            // Calculate number of int64 values in the buffer
                            let num_values = values_buffer.len() / std::mem::size_of::<i64>();

                            // Calculate how many more values we need
                            let remaining =
                                self.chunk_size.unwrap_or(self.num_rows) - column_data.len();
                            let to_read = remaining.min(num_values);
                            let old_len = column_data.len();

                            // TODO buggy, assuming little endian, which might not be true
                            unsafe {
                                // Get source pointer to the raw buffer
                                let src_ptr = values_buffer.as_ptr() as *const i64;

                                // Get destination pointer in our column_data
                                let dst_ptr = column_data.as_mut_ptr().add(old_len);

                                // Copy the memory directly
                                std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, to_read);

                                // 直接更新长度而不初始化值
                                column_data.set_len(old_len + to_read);
                            }

                            // If we've read all we need, return the column
                            if column_data.len() >= self.chunk_size.unwrap_or(self.num_rows) {
                                let col =
                                    Column::Number(i64::upcast_column(Buffer::from(column_data)));
                                return Some(Ok(col));
                            }
                        }
                        encoding => {
                            return Some(Err(ErrorCode::StorageOther(format!(
                                "Encoding {:?} is not supported in this implementation",
                                encoding
                            ))))
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

        let col = Column::Number(i64::upcast_column(Buffer::from(column_data)));
        // Return the collected data
        Some(Ok(col))
    }
}

struct StringIter<'a> {
    pages: BuffedBasicDecompressor<PageReader<&'a [u8]>>,
    chunk_size: Option<usize>,
    num_rows: usize,
}

impl<'a> StringIter<'a> {
    pub fn new(
        pages: BuffedBasicDecompressor<PageReader<&'a [u8]>>,
        num_rows: usize,
        chunk_size: Option<usize>,
    ) -> StringIter<'a> {
        Self {
            pages,
            chunk_size,
            num_rows,
        }
    }
}

impl Iterator for StringIter<'_> {
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        // Use View structure and buffer directly, similar to read_view_col implementation
        let mut views = Vec::with_capacity(self.chunk_size.unwrap_or(self.num_rows));
        let mut buffers = Vec::new();
        let mut offset: usize = 0;
        let mut bytes = Vec::new(); // Store all string bytes

        while views.len() < self.chunk_size.unwrap_or(self.num_rows) {
            let page = match self.pages.next() {
                Err(e) => {
                    return Some(Err(ErrorCode::StorageOther(format!(
                        "Failed to get next page: {}",
                        e
                    ))))
                }
                Ok(Some(page)) => page,
                Ok(None) => {
                    if views.is_empty() {
                        return None;
                    } else {
                        break;
                    }
                }
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

                    let (_, _, values_buffer) = match parquet2::page::split_buffer(&data_page) {
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
                            // Parse binary data - Parquet ByteArray format is:
                            // [4-byte length][data bytes]...[4-byte length][data bytes]...
                            let mut binary_values = values_buffer;
                            let remaining = self.chunk_size.unwrap_or(self.num_rows) - views.len();
                            let mut count = 0;

                            while !binary_values.is_empty() && count < remaining {
                                if binary_values.len() < 4 {
                                    return Some(Err(ErrorCode::StorageOther(
                                        "Invalid binary data: not enough bytes for length prefix"
                                            .to_string(),
                                    )));
                                }

                                // Extract length (first 4 bytes as little-endian u32)
                                let length_bytes = &binary_values[0..4];
                                // TODO remove unwrap
                                let length = u32::from_le_bytes(
                                    length_bytes
                                        .try_into()
                                        .map_err(|e| {
                                            ErrorCode::StorageOther(format!(
                                                "Failed to read length prefix: {}",
                                                e
                                            ))
                                        })
                                        .unwrap(),
                                ) as usize;

                                // Skip the length bytes
                                binary_values = &binary_values[4..];

                                // Check if there are enough bytes for the string
                                if binary_values.len() < length {
                                    return Some(Err(ErrorCode::StorageOther(
                                        "Invalid binary data: not enough bytes for string content"
                                            .to_string(),
                                    )));
                                }

                                // Extract the string value
                                let str_bytes = &binary_values[0..length];

                                // TODO we may want to avoid this validation
                                // Validate UTF-8
                                match std::str::from_utf8(str_bytes) {
                                    Ok(_) => {
                                        // Create View record using the same approach as BinaryViewColumnBuilder
                                        let len: u32 = length as u32;
                                        let mut payload = [0u8; 16];
                                        payload[0..4].copy_from_slice(&len.to_le_bytes());

                                        if len <= 12 {
                                            // |   len   |  prefix  |  remaining(zero-padded)  |
                                            //     ^          ^             ^
                                            // | 4 bytes | 4 bytes |      8 bytes              |
                                            // For small strings (≤12 bytes), store data directly in the View
                                            payload[4..4 + length].copy_from_slice(str_bytes);
                                        } else {
                                            // |   len   |  prefix  |  buffer |  offsets  |
                                            //     ^          ^          ^         ^
                                            // | 4 bytes | 4 bytes | 4 bytes |  4 bytes  |
                                            //
                                            // For larger strings, store prefix + buffer reference

                                            // Set prefix (first 4 bytes)
                                            payload[4..8].copy_from_slice(&str_bytes[..4]);

                                            // We only use one buffer (index 0)
                                            // Since payload is initialized to zero, we don't need to set it
                                            // let buffer_idx: u32 = 0;
                                            // payload[8..12].copy_from_slice(&buffer_idx.to_le_bytes());

                                            // Set offset within buffer
                                            let offset_u32 = offset as u32;
                                            payload[12..16]
                                                .copy_from_slice(&offset_u32.to_le_bytes());

                                            // Append string bytes to the buffer
                                            bytes.extend_from_slice(str_bytes);
                                            offset += length;
                                        }

                                        // Create View from bytes
                                        let view = View::from_le_bytes(payload);
                                        views.push(view);
                                        count += 1;
                                    }

                                    Err(e) => {
                                        return Some(Err(ErrorCode::StorageOther(format!(
                                            "Invalid UTF-8 data in ByteArray: {}",
                                            e
                                        ))))
                                    }
                                }

                                // Move to next string
                                binary_values = &binary_values[length..];
                            }

                            if views.len() >= self.chunk_size.unwrap_or(self.num_rows) {
                                break;
                            }
                        }
                        encoding => {
                            return Some(Err(ErrorCode::StorageOther(format!(
                                "Encoding {:?} is not supported in this implementation",
                                encoding
                            ))))
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

        if views.is_empty() {
            return None;
        }

        // All strings collected, convert to Buffer
        buffers.push(Buffer::from(bytes));

        // Convert views Vec to Buffer
        let views_buffer = Buffer::from(views);

        // Safely create Utf8ViewColumn
        let column =
            unsafe { Utf8ViewColumn::new_unchecked_unknown_md(views_buffer, buffers.into(), None) };

        Some(Ok(Column::String(column)))
    }
}
