use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_expression::Column;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::read::PageReader;
use parquet2::FallibleStreamingIterator;

use crate::BuffedBasicDecompressor;

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
    ) -> IntegerIter<'a> {
        Self {
            pages,
            chunk_size,
            num_rows,
        }
    }
}

impl Iterator for IntegerIter<'_> {
    type Item = databend_common_exception::Result<Column>;

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
                        let col = Column::Date(Buffer::from(column_data));
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

                    if physical_type != &parquet2::schema::types::PhysicalType::Int32 || is_optional
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
                            let num_values = values_buffer.len() / std::mem::size_of::<i32>();

                            // Calculate how many more values we need
                            let remaining =
                                self.chunk_size.unwrap_or(self.num_rows) - column_data.len();
                            let to_read = remaining.min(num_values);
                            let old_len = column_data.len();

                            // TODO buggy, assuming little endian, which might not be true
                            unsafe {
                                // Get source pointer to the raw buffer
                                let src_ptr = values_buffer.as_ptr() as *const i32;

                                // Get destination pointer in our column_data
                                let dst_ptr = column_data.as_mut_ptr().add(old_len);

                                // Copy the memory directly
                                std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, to_read);

                                // 直接更新长度而不初始化值
                                column_data.set_len(old_len + to_read);
                            }

                            // If we've read all we need, return the column
                            if column_data.len() >= self.chunk_size.unwrap_or(self.num_rows) {
                                let col = Column::Date(Buffer::from(column_data));
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

        let col = Column::Date(Buffer::from(column_data));
        // Return the collected data
        Some(Ok(col))
    }
}
