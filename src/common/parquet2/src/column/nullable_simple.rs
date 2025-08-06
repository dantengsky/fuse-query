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

//! Simple nullable column support for Parquet deserialization
//!
//! This module provides a simplified implementation of nullable column support
//! that wraps existing non-nullable iterators and creates nullable columns
//! with all-valid bitmaps as a starting point.

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::{ErrorCode, Result};
use databend_common_expression::types::{NullableColumn, NumberColumn};
use databend_common_expression::Column;

use crate::column::{DateIter, DecimalIter, Int32Iter, Int64Iter, StringIter};
use crate::wip::decompressor::Decompressor;

/// Nullable Int32 iterator that wraps Int32Iter
pub struct NullableInt32Iter<'a> {
    inner: Int32Iter<'a>,
}

impl<'a> NullableInt32Iter<'a> {
    pub fn new(pages: Decompressor<'a>, num_rows: usize, chunk_size: Option<usize>) -> Self {
        Self {
            inner: Int32Iter::new(pages, num_rows, true, chunk_size), // nullable_simple always uses nullable=true
        }
    }
}

impl<'a> Iterator for NullableInt32Iter<'a> {
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok(column)) => {
                match column {
                    Column::Number(NumberColumn::Int32(values)) => {
                        let validity = Bitmap::new_constant(true, values.len());
                        let nullable_column = NullableColumn::new(
                            Column::Number(NumberColumn::Int32(values)),
                            validity,
                        );
                        Some(Ok(Column::Nullable(Box::new(nullable_column))))
                    }
                    _ => Some(Err(ErrorCode::StorageOther(
                        "Expected Int32 column".to_string(),
                    ))),
                }
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

/// Nullable Int64 iterator that wraps Int64Iter
pub struct NullableInt64Iter<'a> {
    inner: Int64Iter<'a>,
}

impl<'a> NullableInt64Iter<'a> {
    pub fn new(pages: Decompressor<'a>, num_rows: usize, chunk_size: Option<usize>) -> Self {
        Self {
            inner: Int64Iter::new(pages, num_rows, true, chunk_size), // nullable_simple always uses nullable=true
        }
    }
}

impl<'a> Iterator for NullableInt64Iter<'a> {
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok(column)) => {
                match column {
                    Column::Number(NumberColumn::Int64(values)) => {
                        let validity = Bitmap::new_constant(true, values.len());
                        let nullable_column = NullableColumn::new(
                            Column::Number(NumberColumn::Int64(values)),
                            validity,
                        );
                        Some(Ok(Column::Nullable(Box::new(nullable_column))))
                    }
                    _ => Some(Err(ErrorCode::StorageOther(
                        "Expected Int64 column".to_string(),
                    ))),
                }
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

/// Nullable String iterator that wraps StringIter
pub struct NullableStringIter<'a> {
    inner: StringIter<'a>,
}

impl<'a> NullableStringIter<'a> {
    pub fn new(pages: Decompressor<'a>, num_rows: usize, chunk_size: Option<usize>) -> Self {
        Self {
            inner: StringIter::new(pages, num_rows, chunk_size),
        }
    }
}

impl<'a> Iterator for NullableStringIter<'a> {
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok(column)) => {
                match column {
                    Column::String(string_column) => {
                        let validity = Bitmap::new_constant(true, string_column.len());
                        let nullable_column = NullableColumn::new(
                            Column::String(string_column),
                            validity,
                        );
                        Some(Ok(Column::Nullable(Box::new(nullable_column))))
                    }
                    _ => Some(Err(ErrorCode::StorageOther(
                        "Expected String column".to_string(),
                    ))),
                }
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

/// Nullable Decimal iterator that wraps DecimalIter
pub struct NullableDecimalIter<'a> {
    inner: DecimalIter<'a, i64>,
}

impl<'a> NullableDecimalIter<'a> {
    pub fn new(
        pages: Decompressor<'a>,
        num_rows: usize,
        chunk_size: Option<usize>,
        precision: u8,
        scale: u8,
    ) -> Self {
        Self {
            inner: DecimalIter::new(pages, num_rows, precision, scale, true, chunk_size),
        }
    }
}

impl<'a> Iterator for NullableDecimalIter<'a> {
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok(column)) => {
                match column {
                    Column::Decimal(decimal_column) => {
                        let validity = Bitmap::new_constant(true, decimal_column.len());
                        let nullable_column = NullableColumn::new(
                            Column::Decimal(decimal_column),
                            validity,
                        );
                        Some(Ok(Column::Nullable(Box::new(nullable_column))))
                    }
                    _ => Some(Err(ErrorCode::StorageOther(
                        "Expected Decimal column".to_string(),
                    ))),
                }
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

/// Nullable Date iterator that wraps DateIter
pub struct NullableDateIter<'a> {
    inner: DateIter<'a>,
}

impl<'a> NullableDateIter<'a> {
    pub fn new(pages: Decompressor<'a>, num_rows: usize, chunk_size: Option<usize>) -> Self {
        Self {
            inner: DateIter::new(pages, num_rows, true, chunk_size), // nullable_simple always uses nullable=true
        }
    }
}

impl<'a> Iterator for NullableDateIter<'a> {
    type Item = Result<Column>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok(column)) => {
                match column {
                    Column::Date(date_column) => {
                        let validity = Bitmap::new_constant(true, date_column.len());
                        let nullable_column = NullableColumn::new(
                            Column::Date(date_column),
                            validity,
                        );
                        Some(Ok(Column::Nullable(Box::new(nullable_column))))
                    }
                    _ => Some(Err(ErrorCode::StorageOther(
                        "Expected Date column".to_string(),
                    ))),
                }
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}
