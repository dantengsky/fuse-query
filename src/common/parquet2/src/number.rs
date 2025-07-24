use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_expression::types::Number;
use databend_common_expression::Column;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::read::PageReader;
use parquet2::FallibleStreamingIterator;

use crate::BuffedBasicDecompressor;

type Result<T> = databend_common_exception::Result<T>;

/// Trait for types that can be deserialized from Parquet integer data
pub trait ParquetInteger: Copy + Send + Sync + 'static {
    /// The Parquet physical type for this integer type
    const PHYSICAL_TYPE: parquet2::schema::types::PhysicalType;

    /// Get the appropriate from_le_bytes function for this type
    #[cfg(target_endian = "big")]
    fn convert_from_le_bytes(bytes: &[u8]) -> Self;

    /// Create a column from a vector of this type
    fn create_column(data: Vec<Self>) -> Column;
}

impl ParquetInteger for i32 {
    const PHYSICAL_TYPE: parquet2::schema::types::PhysicalType =
        parquet2::schema::types::PhysicalType::Int32;

    #[cfg(target_endian = "big")]
    fn convert_from_le_bytes(bytes: &[u8]) -> Self {
        let mut byte_array = [0u8; 4];
        byte_array.copy_from_slice(bytes);
        i32::from_le_bytes(byte_array)
    }

    fn create_column(data: Vec<Self>) -> Column {
        Column::Number(i32::upcast_column(Buffer::from(data)))
    }
}

impl ParquetInteger for i64 {
    const PHYSICAL_TYPE: parquet2::schema::types::PhysicalType =
        parquet2::schema::types::PhysicalType::Int64;

    #[cfg(target_endian = "big")]
    fn convert_from_le_bytes(bytes: &[u8]) -> Self {
        let mut byte_array = [0u8; 8];
        byte_array.copy_from_slice(bytes);
        i64::from_le_bytes(byte_array)
    }

    fn create_column(data: Vec<Self>) -> Column {
        Column::Number(i64::upcast_column(Buffer::from(data)))
    }
}

/// Generic iterator for reading integer values from Parquet pages
pub struct IntegerIter<'a, T: ParquetInteger> {
    pages: BuffedBasicDecompressor<PageReader<&'a [u8]>>,
    chunk_size: Option<usize>,
    num_rows: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: ParquetInteger> IntegerIter<'a, T> {
    pub fn new(
        pages: BuffedBasicDecompressor<PageReader<&'a [u8]>>,
        num_rows: usize,
        chunk_size: Option<usize>,
    ) -> Self {
        Self {
            pages,
            chunk_size,
            num_rows,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get the next page from the iterator
    fn next_page(&mut self) -> Result<Option<&Page>> {
        self.pages
            .next()
            .map_err(|e| ErrorCode::StorageOther(format!("Failed to get next page: {}", e)))
    }
}

impl<T: ParquetInteger> Iterator for IntegerIter<'_, T> {
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        let target_rows = self.chunk_size.unwrap_or(self.num_rows);
        let mut column_data: Vec<T> = Vec::with_capacity(target_rows);

        // Process pages until we have enough data or run out of pages
        while column_data.len() < target_rows {
            let page = match self.next_page() {
                Ok(Some(page)) => page,
                Ok(None) => {
                    // No more pages - return collected data if any
                    break;
                }
                Err(e) => return Some(Err(e)),
            };

            match page {
                Page::Data(data_page) => {
                    // Validate physical type matches expected type
                    let physical_type = &data_page.descriptor.primitive_type.physical_type;
                    if physical_type != &T::PHYSICAL_TYPE {
                        return Some(Err(ErrorCode::StorageOther(format!(
                            "Expected {:?} but got {:?}",
                            T::PHYSICAL_TYPE,
                            physical_type
                        ))));
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
                            let num_values = values_buffer.len() / std::mem::size_of::<T>();
                            let remaining = target_rows - column_data.len();
                            let to_read = remaining.min(num_values);

                            if to_read > 0 {
                                #[cfg(target_endian = "little")]
                                {
                                    // On little endian systems, use direct memory copy for maximum performance
                                    let old_len = column_data.len();
                                    unsafe {
                                        let src_ptr = values_buffer.as_ptr() as *const T;
                                        let dst_ptr = column_data.as_mut_ptr().add(old_len);
                                        std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, to_read);
                                        column_data.set_len(old_len + to_read);
                                    }
                                }

                                #[cfg(target_endian = "big")]
                                {
                                    // On big endian systems, convert byte order efficiently
                                    let byte_size = std::mem::size_of::<T>();
                                    for i in 0..to_read {
                                        let byte_offset = i * byte_size;
                                        let bytes =
                                            &values_buffer[byte_offset..byte_offset + byte_size];

                                        let value = T::convert_from_le_bytes(bytes);
                                        column_data.push(value);
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
            Some(Ok(T::create_column(column_data)))
        }
    }
}

// Type aliases for convenience
pub type Int64Iter<'a> = IntegerIter<'a, i64>;
pub type Int32Iter<'a> = IntegerIter<'a, i32>;
