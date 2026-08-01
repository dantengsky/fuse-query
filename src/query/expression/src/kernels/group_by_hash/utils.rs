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

use crate::Column;
use crate::ProjectedBlock;
use crate::types::AnyType;
use crate::types::BinaryColumn;
use crate::types::NullableColumn;
use crate::types::NumberColumn;
use crate::types::decimal::DecimalColumn;
use crate::types::vector::VectorScalarRef;
use crate::utils::bitmap::normalize_bitmap_column;
use crate::with_decimal_type;
use crate::with_number_mapped_type;
use crate::with_vector_number_type;

/// The serialize_size is equal to the number of bytes required by serialization.
pub fn serialize_group_columns(
    columns: ProjectedBlock,
    num_rows: usize,
    serialize_size: usize,
) -> BinaryColumn {
    let columns: Vec<Column> = columns
        .iter()
        .map(|entry| normalize_bitmap_in_column(entry.to_column()))
        .collect();
    let mut data = Vec::with_capacity(serialize_size);
    let mut offsets: Vec<u64> = Vec::with_capacity(num_rows + 1);

    unsafe {
        let data_start = data.as_mut_ptr();
        let mut data_ptr = data_start;
        let mut offsets_ptr = offsets.as_mut_ptr();

        offsets_ptr.write(0);
        offsets_ptr = offsets_ptr.add(1);

        for row in 0..num_rows {
            for column in &columns {
                serialize_column_binary(column, row, &mut data_ptr);
            }

            let offset = data_ptr as usize - data_start as usize;
            offsets_ptr.write(offset as u64);
            offsets_ptr = offsets_ptr.add(1);
        }

        // Nullable columns only serialize values from valid rows, so the exact output can be
        // smaller than the capacity derived from `serialize_size`.
        let data_len = data_ptr as usize - data_start as usize;
        debug_assert!(data_len <= serialize_size);
        data.set_len(data_len);
        offsets.set_len(num_rows + 1);
    }

    BinaryColumn::new(data.into(), offsets.into())
}

/// This function must be consistent with the `push_binary` function of `src/query/expression/src/values.rs`.
/// # Safety
///
/// * `row_space` points into an allocation with enough remaining capacity for the serialized
///   value.
unsafe fn serialize_column_binary(column: &Column, row: usize, row_space: &mut *mut u8) {
    unsafe {
        match column {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {}
            Column::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
                NumberColumn::NUM_TYPE(v) => {
                    store_advance(&v[row], row_space);
                }
            }),
            Column::Decimal(v) => {
                with_decimal_type!(|DECIMAL_TYPE| match v {
                    DecimalColumn::DECIMAL_TYPE(v, _) => {
                        store_advance(&v[row], row_space);
                    }
                })
            }
            Column::Boolean(v) => store_advance(&(v.get_bit(row) as u8), row_space),
            Column::Binary(v) | Column::Bitmap(v) | Column::Variant(v) | Column::Geometry(v) => {
                let value = v.index_unchecked(row);
                let len = value.len();

                store_advance(&(len as u64), row_space);
                copy_advance(value, row_space);
            }
            Column::Geography(v) => {
                let value = v.index_unchecked(row);
                let value = borsh::to_vec(&value.0).unwrap();
                let len = value.len();

                store_advance(&(len as u64), row_space);
                copy_advance(&value, row_space);
            }
            Column::String(v) => {
                let value = v.index_unchecked(row);
                let len = value.len();

                store_advance(&(len as u64), row_space);
                copy_advance(value.as_bytes(), row_space);
            }
            Column::Opaque(_v) => {
                unimplemented!()
            }
            Column::Timestamp(v) => store_advance(&v[row], row_space),
            Column::TimestampTz(v) => store_advance(&v[row], row_space),
            Column::Date(v) => store_advance(&v[row], row_space),
            Column::Interval(v) => store_advance(&v[row], row_space),
            Column::Array(array) | Column::Map(array) => {
                let data = array.index(row).unwrap();
                store_advance(&(data.len() as u64), row_space);

                for i in 0..data.len() {
                    serialize_column_binary(&data, i, row_space);
                }
            }
            Column::Nullable(c) => {
                let valid = c.validity.get_bit(row);

                store_advance(&(valid as u8), row_space);

                if valid {
                    serialize_column_binary(&c.column, row, row_space);
                }
            }
            Column::Tuple(fields) => {
                for inner_col in fields.iter() {
                    serialize_column_binary(inner_col, row, row_space);
                }
            }
            Column::Vector(col) => {
                let scalar = col.index_unchecked(row);
                with_vector_number_type!(|NUM_TYPE| match scalar {
                    VectorScalarRef::NUM_TYPE(vals) => {
                        for val in vals {
                            store_advance(val, row_space);
                        }
                    }
                })
            }
        }
    }
}

#[inline]
unsafe fn store_advance<T: Copy>(value: &T, row_space: &mut *mut u8) {
    unsafe {
        std::ptr::copy_nonoverlapping(
            value as *const T as *const u8,
            *row_space,
            std::mem::size_of::<T>(),
        );
        *row_space = (*row_space).add(std::mem::size_of::<T>());
    }
}

#[inline]
unsafe fn copy_advance(value: &[u8], row_space: &mut *mut u8) {
    unsafe {
        std::ptr::copy_nonoverlapping(value.as_ptr(), *row_space, value.len());
        *row_space = (*row_space).add(value.len());
    }
}

fn normalize_bitmap_in_column(column: Column) -> Column {
    match column {
        Column::Bitmap(col) => match normalize_bitmap_column(&col) {
            std::borrow::Cow::Borrowed(_) => Column::Bitmap(col),
            std::borrow::Cow::Owned(col) => Column::Bitmap(col),
        },
        Column::Nullable(box nullable) => {
            let (col, validity) = nullable.destructure();
            Column::Nullable(Box::new(NullableColumn::<AnyType> {
                column: normalize_bitmap_in_column(col),
                validity,
            }))
        }
        Column::Tuple(columns) => Column::Tuple(
            columns
                .into_iter()
                .map(normalize_bitmap_in_column)
                .collect(),
        ),
        other => other,
    }
}
