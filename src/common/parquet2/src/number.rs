use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_expression::Column;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::read::PageReader;
use parquet2::FallibleStreamingIterator;

use crate::BuffedBasicDecompressor;

type Result<T> = databend_common_exception::Result<T>;

/// Iterator for reading integer values from Parquet pages
pub struct IntegerIter<'a> {
    pages: BuffedBasicDecompressor<PageReader<&'a [u8]>>,
    chunk_size: Option<usize>,
    num_rows: usize,
}

impl<'a> IntegerIter<'a> {
    pub fn new(
        pages: BuffedBasicDecompressor<PageReader<&'a [u8]>>,
        num_rows: usize,
        chunk_size: Option<usize>,
    ) -> Self {
        Self {
            pages,
            chunk_size,
            num_rows,
        }
    }

    /// Create a column from the collected data
    #[inline]
    fn create_column(&self, data: Vec<i32>) -> Column {
        Column::Date(Buffer::from(data))
    }

    /// Get the next page from the iterator
    fn next_page(&mut self) -> Result<Option<&Page>> {
        self.pages
            .next()
            .map_err(|e| ErrorCode::StorageOther(format!("Failed to get next page: {}", e)))
    }
}

impl Iterator for IntegerIter<'_> {
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        let target_rows = self.chunk_size.unwrap_or(self.num_rows);
        let mut column_data = Vec::with_capacity(target_rows);

        // Process pages until we have enough data or run out of pages
        while column_data.len() < target_rows {
            let page = match self.pages.next() {
                Ok(Some(page)) => page,
                Ok(None) => {
                    // No more pages - return collected data if any
                    break;
                }
                Err(e) => {
                    return Some(Err(ErrorCode::StorageOther(format!(
                        "Failed to get next page: {}",
                        e
                    ))))
                }
            };

            match page {
                Page::Data(data_page) => {
                    // Inline validation and processing to avoid borrowing conflicts
                    let physical_type = &data_page.descriptor.primitive_type.physical_type;
                    let is_optional = data_page.descriptor.primitive_type.field_info.repetition
                        == parquet2::schema::Repetition::Optional;

                    if physical_type != &parquet2::schema::types::PhysicalType::Int32 || is_optional
                    {
                        return Some(Err(ErrorCode::StorageOther(
                            "Only INT32 required fields are supported in this implementation"
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
                            let num_values = values_buffer.len() / std::mem::size_of::<i32>();
                            let remaining = target_rows - column_data.len();
                            let to_read = remaining.min(num_values);

                            if to_read > 0 {
                                #[cfg(target_endian = "little")]
                                {
                                    let old_len = column_data.len();
                                    unsafe {
                                        let src_ptr = values_buffer.as_ptr() as *const i32;
                                        let dst_ptr = column_data.as_mut_ptr().add(old_len);
                                        std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, to_read);
                                        column_data.set_len(old_len + to_read);
                                    }
                                }

                                #[cfg(target_endian = "big")]
                                {
                                    let src_slice = unsafe {
                                        std::slice::from_raw_parts(
                                            values_buffer.as_ptr() as *const [u8; 4],
                                            to_read,
                                        )
                                    };
                                    for &bytes in src_slice {
                                        column_data.push(i32::from_le_bytes(bytes));
                                    }
                                }
                            }

                            // Continue processing if we haven't reached target_rows yet
                            // The while condition will handle the exit
                        }
                        encoding => {
                            return Some(Err(ErrorCode::StorageOther(format!(
                                "Encoding {:?} is not supported in this implementation",
                                encoding
                            ))));
                        }
                    }
                }
                _ => {
                    return Some(Err(ErrorCode::StorageOther(
                        "Only data pages are supported".to_string(),
                    )));
                }
            }
        }

        // Return collected data (could be empty if no pages were available)
        if column_data.is_empty() {
            None
        } else {
            Some(Ok(Column::Date(Buffer::from(column_data))))
        }
    }
}
