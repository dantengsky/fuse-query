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
use databend_common_expression::Column;

use crate::column::number::IntegerIter;
use crate::column::number::ParquetInteger;

#[derive(Copy, Clone)]
#[repr(transparent)]
pub struct Date(i32);
pub type DateIter<'a> = IntegerIter<'a, Date>;

impl ParquetInteger for Date {
    const PHYSICAL_TYPE: parquet2::schema::types::PhysicalType =
        parquet2::schema::types::PhysicalType::Int32;

    #[cfg(target_endian = "big")]
    #[inline]
    fn convert_from_le_bytes(bytes: &[u8]) -> Self {
        let mut byte_array = [0u8; 4];
        byte_array.copy_from_slice(bytes);
        Date(i32::from_le_bytes(byte_array))
    }

    fn create_column(data: Vec<Self>) -> Column {
        let data: Vec<i32> = unsafe { std::mem::transmute(data) };
        Column::Date(Buffer::from(data))
    }
}
