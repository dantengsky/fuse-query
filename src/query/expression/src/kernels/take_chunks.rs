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

use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::compute::merge_sort::MergeSlice;
use common_hashtable::RowPtr;
use itertools::Itertools;

use crate::kernels::take::BIT_MASK;
use crate::kernels::utils::copy_advance_aligned;
use crate::kernels::utils::set_vec_len_by_ptr;
use crate::types::array::ArrayColumnBuilder;
use crate::types::bitmap::BitmapType;
use crate::types::decimal::DecimalColumn;
use crate::types::decimal::DecimalColumnVec;
use crate::types::map::KvColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::nullable::NullableColumnVec;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumn;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::MapType;
use crate::types::NumberColumnVec;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::types::F32;
use crate::types::F64;
use crate::with_decimal_type;
use crate::with_number_mapped_type;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::ColumnVec;
use crate::DataBlock;
use crate::Scalar;
use crate::Value;

// Block idx, row idx in the block, repeat times
pub type BlockRowIndex = (u32, u32, usize);

impl DataBlock {
    pub fn take_blocks(
        blocks: &[DataBlock],
        indices: &[BlockRowIndex],
        result_size: usize,
    ) -> Self {
        debug_assert!(!blocks.is_empty());

        let num_columns = blocks[0].num_columns();
        let result_size = if result_size > 0 {
            result_size
        } else {
            indices.iter().map(|(_, _, c)| *c).sum()
        };

        let result_columns = (0..num_columns)
            .map(|index| {
                let columns = blocks
                    .iter()
                    .map(|block| (block.get_by_offset(index), block.num_rows()))
                    .collect_vec();

                let ty = columns[0].0.data_type.clone();
                if ty.is_null() {
                    return BlockEntry::new(ty, Value::Scalar(Scalar::Null));
                }

                // if they are all same scalars
                if matches!(columns[0].0.value, Value::Scalar(_)) {
                    let all_same_scalar = columns.iter().map(|(entry, _)| &entry.value).all_equal();
                    if all_same_scalar {
                        return (*columns[0].0).clone();
                    }
                }

                let full_columns: Vec<Column> = columns
                    .iter()
                    .map(|(entry, rows)| match &entry.value {
                        Value::Scalar(s) => {
                            let builder =
                                ColumnBuilder::repeat(&s.as_ref(), *rows, &entry.data_type);
                            builder.build()
                        }
                        Value::Column(c) => c.clone(),
                    })
                    .collect();

                let column =
                    Column::take_column_indices(&full_columns, ty.clone(), indices, result_size);

                BlockEntry::new(ty, Value::Column(column))
            })
            .collect();

        DataBlock::new(result_columns, result_size)
    }

    pub fn take_column_vec(
        build_columns: &[ColumnVec],
        build_columns_data_type: &[DataType],
        indices: &[RowPtr],
        result_size: usize,
        string_items_buf: &mut Option<Vec<(u64, usize)>>,
    ) -> Self {
        let num_columns = build_columns.len();
        let result_columns = (0..num_columns)
            .map(|index| {
                let data_type = &build_columns_data_type[index];
                let column = Column::take_column_vec_indices(
                    &build_columns[index],
                    data_type.clone(),
                    indices,
                    result_size,
                    string_items_buf,
                );
                BlockEntry::new(data_type.clone(), Value::Column(column))
            })
            .collect();
        DataBlock::new(result_columns, result_size)
    }

    pub fn take_by_slice_limit(
        block: &DataBlock,
        slice: (usize, usize),
        limit: Option<usize>,
    ) -> Self {
        let columns = block
            .columns()
            .iter()
            .map(|entry| {
                Self::take_column_by_slices_limit(&[entry.clone()], &[(0, slice.0, slice.1)], limit)
            })
            .collect::<Vec<_>>();

        let num_rows = block.num_rows().min(slice.1.min(limit.unwrap_or(slice.1)));
        DataBlock::new(columns, num_rows)
    }

    pub fn take_by_slices_limit_from_blocks(
        blocks: &[DataBlock],
        slices: &[MergeSlice],
        limit: Option<usize>,
    ) -> Self {
        debug_assert!(!blocks.is_empty());
        let total_num_rows: usize = blocks.iter().map(|c| c.num_rows()).sum();
        let result_size: usize = slices.iter().map(|(_, _, c)| *c).sum();
        let result_size = total_num_rows.min(result_size.min(limit.unwrap_or(result_size)));

        let mut result_columns = Vec::with_capacity(blocks[0].num_columns());

        for index in 0..blocks[0].num_columns() {
            let cols = blocks
                .iter()
                .map(|c| c.get_by_offset(index).clone())
                .collect::<Vec<_>>();

            let merged_col = Self::take_column_by_slices_limit(&cols, slices, limit);

            result_columns.push(merged_col);
        }

        DataBlock::new(result_columns, result_size)
    }

    pub fn take_column_by_slices_limit(
        columns: &[BlockEntry],
        slices: &[MergeSlice],
        limit: Option<usize>,
    ) -> BlockEntry {
        assert!(!columns.is_empty());
        let ty = &columns[0].data_type;
        let num_rows = limit
            .unwrap_or(usize::MAX)
            .min(slices.iter().map(|(_, _, c)| *c).sum());

        let mut builder = ColumnBuilder::with_capacity(ty, num_rows);
        let mut remain = num_rows;

        for (index, start, len) in slices {
            let len = (*len).min(remain);
            remain -= len;

            let col = &columns[*index];
            match &col.value {
                Value::Scalar(scalar) => {
                    let other = ColumnBuilder::repeat(&scalar.as_ref(), len, &col.data_type);
                    builder.append_column(&other.build());
                }
                Value::Column(c) => {
                    let c = c.slice(*start..(*start + len));
                    builder.append_column(&c);
                }
            }
            if remain == 0 {
                break;
            }
        }

        let col = builder.build();

        BlockEntry::new(ty.clone(), Value::Column(col))
    }
}

impl Column {
    pub fn take_column_indices(
        columns: &[Column],
        datatype: DataType,
        indices: &[BlockRowIndex],
        result_size: usize,
    ) -> Column {
        match &columns[0] {
            Column::Null { .. } => Column::Null { len: result_size },
            Column::EmptyArray { .. } => Column::EmptyArray { len: result_size },
            Column::EmptyMap { .. } => Column::EmptyMap { len: result_size },
            Column::Number(column) => with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(_) => {
                    let builder = NumberType::<NUM_TYPE>::create_builder(result_size, &[]);
                    Self::take_block_value_types::<NumberType<NUM_TYPE>>(columns, builder, indices)
                }
            }),
            Column::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumn::DECIMAL_TYPE(_, size) => {
                    let columns = columns
                        .iter()
                        .map(|col| match col {
                            Column::Decimal(DecimalColumn::DECIMAL_TYPE(col, _)) => col,
                            _ => unreachable!(),
                        })
                        .collect_vec();
                    let mut builder = Vec::with_capacity(result_size);
                    for &(block_index, row, times) in indices {
                        let val =
                            unsafe { columns[block_index as usize].get_unchecked(row as usize) };
                        for _ in 0..times {
                            builder.push(*val);
                        }
                    }
                    Column::Decimal(DecimalColumn::DECIMAL_TYPE(builder.into(), *size))
                }
            }),
            Column::Boolean(_) => {
                let builder = BooleanType::create_builder(result_size, &[]);
                Self::take_block_value_types::<BooleanType>(columns, builder, indices)
            }
            Column::String(_) => {
                let builder = StringType::create_builder(result_size, &[]);
                Self::take_block_value_types::<StringType>(columns, builder, indices)
            }
            Column::Timestamp(_) => {
                let builder = TimestampType::create_builder(result_size, &[]);
                Self::take_block_value_types::<TimestampType>(columns, builder, indices)
            }
            Column::Date(_) => {
                let builder = DateType::create_builder(result_size, &[]);
                Self::take_block_value_types::<DateType>(columns, builder, indices)
            }
            Column::Array(column) => {
                let mut offsets = Vec::with_capacity(result_size + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(&column.values.data_type(), result_size);
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_block_value_types::<ArrayType<AnyType>>(columns, builder, indices)
            }
            Column::Map(column) => {
                let mut offsets = Vec::with_capacity(result_size + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(&column.values.data_type(), result_size).build(),
                );
                let (key_builder, val_builder) = match builder {
                    ColumnBuilder::Tuple(fields) => (fields[0].clone(), fields[1].clone()),
                    _ => unreachable!(),
                };
                let builder = KvColumnBuilder {
                    keys: key_builder,
                    values: val_builder,
                };
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_block_value_types::<MapType<AnyType, AnyType>>(columns, builder, indices)
            }
            Column::Bitmap(_) => {
                let builder = BitmapType::create_builder(result_size, &[]);
                Self::take_block_value_types::<BitmapType>(columns, builder, indices)
            }
            Column::Nullable(_) => {
                let inner_ty = datatype.as_nullable().unwrap();
                let inner_columns = columns
                    .iter()
                    .map(|c| match c {
                        Column::Nullable(c) => c.column.clone(),
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>();

                let inner_bitmaps = columns
                    .iter()
                    .map(|c| match c {
                        Column::Nullable(c) => Column::Boolean(c.validity.clone()),
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>();

                let inner_column = Self::take_column_indices(
                    &inner_columns,
                    *inner_ty.clone(),
                    indices,
                    result_size,
                );

                let inner_bitmap = Self::take_column_indices(
                    &inner_bitmaps,
                    DataType::Boolean,
                    indices,
                    result_size,
                );

                Column::Nullable(Box::new(NullableColumn {
                    column: inner_column,
                    validity: BooleanType::try_downcast_column(&inner_bitmap).unwrap(),
                }))
            }
            Column::Tuple { .. } => {
                let inner_ty = datatype.as_tuple().unwrap();
                let inner_columns = columns
                    .iter()
                    .map(|c| match c {
                        Column::Tuple(fields) => fields.clone(),
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>();

                let fields: Vec<Column> = inner_ty
                    .iter()
                    .enumerate()
                    .map(|(idx, ty)| {
                        let sub_columns = inner_columns
                            .iter()
                            .map(|c| c[idx].clone())
                            .collect::<Vec<_>>();
                        Self::take_column_indices(&sub_columns, ty.clone(), indices, result_size)
                    })
                    .collect();

                Column::Tuple(fields)
            }
            Column::Variant(_) => {
                let builder = VariantType::create_builder(result_size, &[]);
                Self::take_block_value_types::<VariantType>(columns, builder, indices)
            }
        }
    }

    pub fn take_downcast_column_vec(columns: &[Column], datatype: DataType) -> ColumnVec {
        match &columns[0] {
            Column::Null { .. } => ColumnVec::Null,
            Column::EmptyArray { .. } => ColumnVec::EmptyArray,
            Column::EmptyMap { .. } => ColumnVec::EmptyMap,
            Column::Number(column) => match column {
                NumberColumn::UInt8(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<u8>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::UInt8(columns))
                }
                NumberColumn::UInt16(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<u16>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::UInt16(columns))
                }
                NumberColumn::UInt32(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<u32>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::UInt32(columns))
                }
                NumberColumn::UInt64(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<u64>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::UInt64(columns))
                }
                NumberColumn::Int8(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<i8>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::Int8(columns))
                }
                NumberColumn::Int16(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<i16>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::Int16(columns))
                }
                NumberColumn::Int32(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<i32>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::Int32(columns))
                }
                NumberColumn::Int64(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<i64>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::Int64(columns))
                }
                NumberColumn::Float32(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<F32>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::Float32(columns))
                }
                NumberColumn::Float64(_) => {
                    let columns = columns
                        .iter()
                        .map(|col| <NumberType<F64>>::try_downcast_column(col).unwrap())
                        .collect_vec();
                    ColumnVec::Number(NumberColumnVec::Float64(columns))
                }
            },
            Column::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumn::DECIMAL_TYPE(_, size) => {
                    let columns = columns
                        .iter()
                        .map(|col| match col {
                            Column::Decimal(DecimalColumn::DECIMAL_TYPE(col, _)) => col.clone(),
                            _ => unreachable!(),
                        })
                        .collect_vec();
                    ColumnVec::Decimal(DecimalColumnVec::DECIMAL_TYPE(columns, *size))
                }
            }),
            Column::Boolean(_) => {
                let columns = columns
                    .iter()
                    .map(|col| BooleanType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Boolean(columns)
            }
            Column::String(_) => {
                let columns = columns
                    .iter()
                    .map(|col| StringType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::String(columns)
            }
            Column::Timestamp(_) => {
                let columns = columns
                    .iter()
                    .map(|col| TimestampType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Timestamp(columns)
            }
            Column::Date(_) => {
                let columns = columns
                    .iter()
                    .map(|col| DateType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Date(columns)
            }
            Column::Array(_) => {
                let columns = columns
                    .iter()
                    .map(|col| <ArrayType<AnyType>>::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Array(columns)
            }
            Column::Map(_) => {
                let columns = columns
                    .iter()
                    .map(|col| <MapType<AnyType, AnyType>>::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Map(columns)
            }
            Column::Bitmap(_) => {
                let columns = columns
                    .iter()
                    .map(|col| BitmapType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Bitmap(columns)
            }
            Column::Nullable(_) => {
                let inner_ty = datatype.as_nullable().unwrap();
                let inner_columns = columns
                    .iter()
                    .map(|c| match c {
                        Column::Nullable(c) => c.column.clone(),
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>();

                let inner_bitmaps = columns
                    .iter()
                    .map(|c| match c {
                        Column::Nullable(c) => Column::Boolean(c.validity.clone()),
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>();

                let inner_column =
                    Self::take_downcast_column_vec(&inner_columns, *inner_ty.clone());

                let inner_bitmap =
                    Self::take_downcast_column_vec(&inner_bitmaps, DataType::Boolean);

                ColumnVec::Nullable(Box::new(NullableColumnVec {
                    column: inner_column,
                    validity: inner_bitmap,
                }))
            }
            Column::Tuple { .. } => {
                let inner_ty = datatype.as_tuple().unwrap();
                let inner_columns = columns
                    .iter()
                    .map(|c| match c {
                        Column::Tuple(fields) => fields.clone(),
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>();

                let fields: Vec<ColumnVec> = inner_ty
                    .iter()
                    .enumerate()
                    .map(|(idx, ty)| {
                        let sub_columns = inner_columns
                            .iter()
                            .map(|c| c[idx].clone())
                            .collect::<Vec<_>>();
                        Self::take_downcast_column_vec(&sub_columns, ty.clone())
                    })
                    .collect();

                ColumnVec::Tuple(fields)
            }
            Column::Variant(_) => {
                let columns = columns
                    .iter()
                    .map(|col| VariantType::try_downcast_column(col).unwrap())
                    .collect_vec();
                ColumnVec::Variant(columns)
            }
        }
    }

    pub fn take_column_vec_indices(
        columns: &ColumnVec,
        data_type: DataType,
        indices: &[RowPtr],
        result_size: usize,
        string_items_buf: &mut Option<Vec<(u64, usize)>>,
    ) -> Column {
        match &columns {
            ColumnVec::Null { .. } => Column::Null { len: result_size },
            ColumnVec::EmptyArray { .. } => Column::EmptyArray { len: result_size },
            ColumnVec::EmptyMap { .. } => Column::EmptyMap { len: result_size },
            ColumnVec::Number(column) => with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumnVec::NUM_TYPE(columns) => {
                    let builder = Self::take_block_vec_primitive_types(columns, indices);
                    <NumberType<NUM_TYPE>>::upcast_column(<NumberType<NUM_TYPE>>::column_from_vec(
                        builder,
                        &[],
                    ))
                }
            }),
            ColumnVec::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumnVec::DECIMAL_TYPE(columns, size) => {
                    let builder = Self::take_block_vec_primitive_types(columns, indices);
                    Column::Decimal(DecimalColumn::DECIMAL_TYPE(builder.into(), *size))
                }
            }),
            ColumnVec::Boolean(columns) => {
                Column::Boolean(Self::take_block_vec_boolean_types(columns, indices))
            }
            ColumnVec::String(columns) => StringType::upcast_column(
                Self::take_block_vec_string_types(columns, indices, string_items_buf.as_mut()),
            ),
            ColumnVec::Timestamp(columns) => {
                let builder = Self::take_block_vec_primitive_types(columns, indices);
                let ts = <NumberType<i64>>::upcast_column(<NumberType<i64>>::column_from_vec(
                    builder,
                    &[],
                ))
                .into_number()
                .unwrap()
                .into_int64()
                .unwrap();
                Column::Timestamp(ts)
            }
            ColumnVec::Date(columns) => {
                let builder = Self::take_block_vec_primitive_types(columns, indices);
                let d = <NumberType<i32>>::upcast_column(<NumberType<i32>>::column_from_vec(
                    builder,
                    &[],
                ))
                .into_number()
                .unwrap()
                .into_int32()
                .unwrap();
                Column::Date(d)
            }
            ColumnVec::Array(columns) => {
                let data_type = data_type.as_array().unwrap();
                let mut offsets = Vec::with_capacity(result_size + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(data_type, result_size);
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_block_vec_value_types::<ArrayType<AnyType>>(columns, builder, indices)
            }
            ColumnVec::Map(columns) => {
                let data_type = data_type.as_map().unwrap();
                let mut offsets = Vec::with_capacity(result_size + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(data_type, result_size).build(),
                );
                let (key_builder, val_builder) = match builder {
                    ColumnBuilder::Tuple(fields) => (fields[0].clone(), fields[1].clone()),
                    _ => unreachable!(),
                };
                let builder = KvColumnBuilder {
                    keys: key_builder,
                    values: val_builder,
                };
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_block_vec_value_types::<MapType<AnyType, AnyType>>(
                    columns, builder, indices,
                )
            }
            ColumnVec::Bitmap(columns) => BitmapType::upcast_column(
                Self::take_block_vec_string_types(columns, indices, string_items_buf.as_mut()),
            ),
            ColumnVec::Nullable(columns) => {
                let inner_data_type = data_type.as_nullable().unwrap();
                let inner_column = Self::take_column_vec_indices(
                    &columns.column,
                    *inner_data_type.clone(),
                    indices,
                    result_size,
                    string_items_buf,
                );

                let inner_bitmap = Self::take_column_vec_indices(
                    &columns.validity,
                    DataType::Boolean,
                    indices,
                    result_size,
                    string_items_buf,
                );

                Column::Nullable(Box::new(NullableColumn {
                    column: inner_column,
                    validity: BooleanType::try_downcast_column(&inner_bitmap).unwrap(),
                }))
            }
            ColumnVec::Tuple(columns) => {
                let inner_data_type = data_type.as_tuple().unwrap();
                let fields: Vec<Column> = inner_data_type
                    .iter()
                    .enumerate()
                    .map(|(idx, ty)| {
                        Self::take_column_vec_indices(
                            &columns[idx],
                            ty.clone(),
                            indices,
                            result_size,
                            string_items_buf,
                        )
                    })
                    .collect();

                Column::Tuple(fields)
            }
            ColumnVec::Variant(columns) => VariantType::upcast_column(
                Self::take_block_vec_string_types(columns, indices, string_items_buf.as_mut()),
            ),
        }
    }

    pub fn take_block_vec_primitive_types<T>(col: &[Buffer<T>], indices: &[RowPtr]) -> Vec<T>
    where T: Copy {
        let num_rows = indices.len();
        let mut builder: Vec<T> = Vec::with_capacity(num_rows);
        builder.extend(indices.iter().map(|row_ptr| unsafe {
            col.get_unchecked(row_ptr.chunk_index as usize)[row_ptr.row_index as usize]
        }));
        builder
    }

    pub fn take_block_vec_string_types<'a>(
        col: &'a [StringColumn],
        indices: &[RowPtr],
        string_items_buf: Option<&mut Vec<(u64, usize)>>,
    ) -> StringColumn {
        let num_rows = indices.len();

        // Each element of `items` is (string pointer(u64), string length), if `string_items_buf`
        // can be reused, we will not re-allocate memory.
        let mut items: Option<Vec<(u64, usize)>> = match &string_items_buf {
            Some(string_items_buf) if string_items_buf.capacity() >= num_rows => None,
            _ => Some(Vec::with_capacity(num_rows)),
        };
        let items = match items.is_some() {
            true => items.as_mut().unwrap(),
            false => string_items_buf.unwrap(),
        };

        // [`StringColumn`] consists of [`data`] and [`offset`], we build [`data`] and [`offset`] respectively,
        // and then call `StringColumn::new(data.into(), offsets.into())` to create [`StringColumn`].
        let mut offsets: Vec<u64> = Vec::with_capacity(num_rows + 1);
        let mut data_size = 0;

        // Build [`offset`] and calculate `data_size` required by [`data`].
        unsafe {
            *offsets.get_unchecked_mut(0) = 0;
            for (i, row_ptr) in indices.iter().enumerate() {
                let item =
                    col[row_ptr.chunk_index as usize].index_unchecked(row_ptr.row_index as usize);
                data_size += item.len() as u64;
                *items.get_unchecked_mut(i) = (item.as_ptr() as u64, item.len());
                *offsets.get_unchecked_mut(i + 1) = data_size;
            }
            items.set_len(num_rows);
            offsets.set_len(num_rows + 1);
        }

        // Build [`data`].
        let mut data: Vec<u8> = Vec::with_capacity(data_size as usize);
        let mut data_ptr = data.as_mut_ptr();

        unsafe {
            for (str_ptr, len) in items.iter() {
                copy_advance_aligned(*str_ptr as *const u8, &mut data_ptr, *len);
            }
            set_vec_len_by_ptr(&mut data, data_ptr);
        }

        StringColumn::new(data.into(), offsets.into())
    }

    pub fn take_block_vec_boolean_types(col: &[Bitmap], indices: &[RowPtr]) -> Bitmap {
        let num_rows = indices.len();
        // Fast path: avoid iterating column to generate a new bitmap.
        // If all [`Bitmap`] are all_true or all_false and `num_rows <= bitmap.len()`,
        // we can just slice it.
        let mut total_len = 0;
        let mut unset_bits = 0;
        for bitmap in col.iter() {
            unset_bits += bitmap.unset_bits();
            total_len += bitmap.len();
        }
        if unset_bits == total_len || unset_bits == 0 {
            // Goes fast path.
            for bitmap in col.iter() {
                if num_rows <= bitmap.len() {
                    let mut bitmap = bitmap.clone();
                    bitmap.slice(0, num_rows);
                    return bitmap;
                }
            }
        }

        let capacity = num_rows.saturating_add(7) / 8;
        let mut builder: Vec<u8> = Vec::with_capacity(capacity);
        let mut builder_len = 0;
        let mut unset_bits = 0;
        let mut value = 0;
        let mut i = 0;

        unsafe {
            for row_ptr in indices.iter() {
                if col[row_ptr.chunk_index as usize].get_bit_unchecked(row_ptr.row_index as usize) {
                    value |= BIT_MASK[i % 8];
                } else {
                    unset_bits += 1;
                }
                i += 1;
                if i % 8 == 0 {
                    *builder.get_unchecked_mut(builder_len) = value;
                    builder_len += 1;
                    value = 0;
                }
            }
            if i % 8 != 0 {
                *builder.get_unchecked_mut(builder_len) = value;
                builder_len += 1;
            }
            builder.set_len(builder_len);
            Bitmap::from_inner(Arc::new(builder.into()), 0, num_rows, unset_bits)
                .ok()
                .unwrap()
        }
    }

    fn take_block_vec_value_types<T: ValueType>(
        columns: &[T::Column],
        mut builder: T::ColumnBuilder,
        indices: &[RowPtr],
    ) -> Column {
        for row_ptr in indices {
            let val = unsafe {
                T::index_column_unchecked(
                    &columns[row_ptr.chunk_index as usize],
                    row_ptr.row_index as usize,
                )
            };
            T::push_item(&mut builder, val.clone());
        }
        T::upcast_column(T::build_column(builder))
    }

    fn take_block_value_types<T: ValueType>(
        columns: &[Column],
        mut builder: T::ColumnBuilder,
        indices: &[BlockRowIndex],
    ) -> Column {
        let columns = columns
            .iter()
            .map(|col| T::try_downcast_column(col).unwrap())
            .collect_vec();
        for &(block_index, row, times) in indices {
            let val =
                unsafe { T::index_column_unchecked(&columns[block_index as usize], row as usize) };
            for _ in 0..times {
                T::push_item(&mut builder, val.clone())
            }
        }
        T::upcast_column(T::build_column(builder))
    }
}
