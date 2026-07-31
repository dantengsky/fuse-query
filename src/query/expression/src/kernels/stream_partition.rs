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

use std::collections::HashSet;

use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_column::buffer::Buffer;
use databend_common_column::types::Index;

use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::types::AnyType;
use crate::types::ArrayColumn;
use crate::types::BinaryColumn;
use crate::types::DecimalColumn;
use crate::types::DecimalColumnBuilder;
use crate::types::NullableColumn;
use crate::types::NumberColumn;
use crate::types::NumberColumnBuilder;
use crate::types::OpaqueColumn;
use crate::types::OpaqueColumnBuilder;
use crate::types::StringColumn;
use crate::types::VectorColumn;
use crate::types::VectorColumnBuilder;
use crate::types::array::ArrayColumnBuilder;
use crate::types::binary::BinaryColumnBuilder;
use crate::types::nullable::NullableColumnBuilder;
use crate::types::string::StringColumnBuilder;
use crate::with_decimal_type;
use crate::with_number_mapped_type;

struct PartitionBlockBuilder {
    num_rows: usize,
    columns_builder: Vec<ColumnBuilder>,
}

pub struct BlockPartitionStream {
    initialize: bool,
    scatter_size: usize,
    preserve_string_views: bool,
    rows_threshold: usize,
    bytes_threshold: usize,
    partitions: Vec<PartitionBlockBuilder>,
}

impl BlockPartitionStream {
    pub fn create(
        rows_threshold: usize,
        bytes_threshold: usize,
        scatter_size: usize,
    ) -> BlockPartitionStream {
        Self::create_inner(rows_threshold, bytes_threshold, scatter_size, false)
    }

    /// Preserve Arc-backed string buffers while partitioning. Callers that send partitions over
    /// the network must compact retained buffers before serialization.
    pub fn create_with_string_views(
        rows_threshold: usize,
        bytes_threshold: usize,
        scatter_size: usize,
    ) -> BlockPartitionStream {
        Self::create_inner(rows_threshold, bytes_threshold, scatter_size, true)
    }

    fn create_inner(
        mut rows_threshold: usize,
        mut bytes_threshold: usize,
        scatter_size: usize,
        preserve_string_views: bool,
    ) -> BlockPartitionStream {
        if rows_threshold == 0 {
            rows_threshold = usize::MAX;
        }

        if bytes_threshold == 0 {
            bytes_threshold = usize::MAX;
        }

        BlockPartitionStream {
            scatter_size,
            preserve_string_views,
            rows_threshold,
            bytes_threshold,
            initialize: false,
            partitions: vec![],
        }
    }

    pub fn partition(
        &mut self,
        indices: Vec<u64>,
        block: DataBlock,
        out_ready: bool,
    ) -> Vec<(usize, DataBlock)> {
        if block.is_empty() {
            return vec![];
        }

        if !self.initialize {
            self.initialize = true;

            self.partitions.reserve(self.scatter_size);
            for _ in 0..self.scatter_size {
                let mut columns_builder = Vec::with_capacity(block.num_columns());

                for column in block.columns() {
                    let data_type = column.data_type();
                    columns_builder.push(ColumnBuilder::with_capacity(&data_type, 0));
                }

                let block_builder = PartitionBlockBuilder {
                    num_rows: 0,
                    columns_builder,
                };
                self.partitions.push(block_builder);
            }
        }

        let columns = block
            .take_columns()
            .into_iter()
            .map(|x| x.to_column())
            .collect::<Vec<_>>();

        let scatter_indices =
            DataBlock::divide_indices_by_scatter_size(&indices, self.scatter_size);

        for (partition_id, indices) in scatter_indices.iter().enumerate() {
            self.partitions[partition_id].num_rows += indices.len();
        }

        for (column_idx, column) in columns.into_iter().enumerate() {
            for (partition_id, indices) in scatter_indices.iter().enumerate() {
                if indices.is_empty() {
                    continue;
                }

                let partition = &mut self.partitions[partition_id];
                let column_builder = &mut partition.columns_builder[column_idx];
                copy_column_impl(indices, &column, column_builder, self.preserve_string_views);
            }

            drop(column);
        }

        if !out_ready {
            return vec![];
        }

        let mut ready_blocks = Vec::with_capacity(self.partitions.len());
        for (id, partition) in self.partitions.iter_mut().enumerate() {
            let memory_size = if self.preserve_string_views {
                partition
                    .columns_builder
                    .iter()
                    .map(partition_builder_memory_size)
                    .sum::<usize>()
            } else {
                partition
                    .columns_builder
                    .iter()
                    .map(|builder| builder.memory_size())
                    .sum::<usize>()
            };

            let rows = partition.num_rows;

            if memory_size >= self.bytes_threshold || rows >= self.rows_threshold {
                let mut columns = Vec::with_capacity(partition.columns_builder.len());
                let columns_builder = std::mem::take(&mut partition.columns_builder);
                partition.columns_builder.reserve(columns_builder.len());

                for column_builder in columns_builder {
                    let historical_size = column_builder.len();
                    let data_type = column_builder.data_type();
                    let new_builder = ColumnBuilder::with_capacity(&data_type, historical_size);
                    partition.columns_builder.push(new_builder);
                    columns.push(BlockEntry::from(column_builder.build()));
                }

                partition.num_rows = 0;
                ready_blocks.push((id, DataBlock::new(columns, rows)));
            }
        }

        ready_blocks
    }

    pub fn partition_ids(&self) -> Vec<usize> {
        let mut partition_ids = vec![];

        if !self.initialize {
            return partition_ids;
        }

        for (partition_id, data) in self.partitions.iter().enumerate() {
            if data.num_rows != 0 {
                partition_ids.push(partition_id);
            }
        }
        partition_ids
    }

    pub fn take_partitions(&mut self, excluded: &HashSet<usize>) -> Vec<(usize, DataBlock)> {
        if !self.initialize {
            return vec![];
        }

        let capacity = self.partitions.len() - excluded.len();

        let mut take_blocks = Vec::with_capacity(capacity);

        for (id, partition) in self.partitions.iter_mut().enumerate() {
            if excluded.contains(&id) {
                continue;
            }

            let mut columns = Vec::with_capacity(partition.columns_builder.len());
            let columns_builder = std::mem::take(&mut partition.columns_builder);
            partition.columns_builder.reserve(columns_builder.len());

            for column_builder in columns_builder {
                let historical_size = column_builder.len();
                let data_type = column_builder.data_type();
                let new_builder = ColumnBuilder::with_capacity(&data_type, historical_size);
                partition.columns_builder.push(new_builder);
                columns.push(BlockEntry::from(column_builder.build()));
            }

            let num_rows = partition.num_rows;
            partition.num_rows = 0;
            take_blocks.push((id, DataBlock::new(columns, num_rows)));
        }

        take_blocks
    }

    pub fn finalize_partition(&mut self, partition_id: usize) -> Option<DataBlock> {
        if !self.initialize {
            return None;
        }

        let partition = &mut self.partitions[partition_id];

        let num_rows = partition.num_rows;

        if num_rows == 0 {
            return None;
        }

        let mut columns = Vec::with_capacity(partition.columns_builder.len());
        let columns_builder = std::mem::take(&mut partition.columns_builder);
        partition.columns_builder.reserve(columns_builder.len());

        for column_builder in columns_builder {
            let data_type = column_builder.data_type();
            let new_builder = ColumnBuilder::with_capacity(&data_type, 0);
            partition.columns_builder.push(new_builder);
            columns.push(BlockEntry::from(column_builder.build()));
        }

        partition.num_rows = 0;
        Some(DataBlock::new(columns, num_rows))
    }
}

fn partition_builder_memory_size(builder: &ColumnBuilder) -> usize {
    match builder {
        ColumnBuilder::String(builder) => builder.len() * 16 + builder.data.total_bytes_len(),
        ColumnBuilder::Nullable(builder) => {
            partition_builder_memory_size(&builder.builder) + builder.validity.as_slice().len()
        }
        ColumnBuilder::Tuple(fields) => fields.iter().map(partition_builder_memory_size).sum(),
        _ => builder.memory_size(),
    }
}

pub fn copy_column<I: Index>(indices: &[I], from: &Column, to: &mut ColumnBuilder) {
    copy_column_impl(indices, from, to, false)
}

fn copy_column_impl<I: Index>(
    indices: &[I],
    from: &Column,
    to: &mut ColumnBuilder,
    preserve_string_views: bool,
) {
    match to {
        ColumnBuilder::EmptyArray { len } => match from {
            Column::EmptyArray { .. } => *len += indices.len(),
            Column::Array(column) => {
                let capacity = *len + column.len();
                match ColumnBuilder::with_capacity(&from.data_type(), capacity) {
                    ColumnBuilder::Array(mut builder) => {
                        builder.offsets.extend(vec![0; *len]);
                        copy_array(&mut builder, column, indices);
                        *to = ColumnBuilder::Array(builder);
                    }
                    _ => unreachable!(
                        "ColumnBuilder::with_capacity for Array type should return ColumnBuilder::Array, \
                     but got different variant. data_type: {}, capacity: {}",
                        from.data_type(),
                        capacity
                    ),
                }
            }
            _ => unreachable!(
                "EmptyArray builder can only copy from EmptyArray or Array, but got from type: {}",
                from.data_type()
            ),
        },
        ColumnBuilder::Array(builder) => match from {
            Column::EmptyArray { .. } => {
                for _ in 0..indices.len() {
                    builder.commit_row();
                }
            }
            Column::Array(column) => {
                copy_array(builder, column, indices);
            }
            _ => unreachable!(
                "Array builder can only copy from EmptyArray or Array, but got from type: {}",
                from.data_type()
            ),
        },
        ColumnBuilder::Null { len } => match from {
            Column::Null { .. } => *len += indices.len(),
            Column::Nullable(column) => {
                let capacity = *len + column.len();

                match ColumnBuilder::with_capacity(&from.data_type(), capacity) {
                    ColumnBuilder::Nullable(mut builder) => {
                        builder.push_repeat_null(*len);
                        copy_nullable(&mut builder, column, indices, preserve_string_views);
                        *to = ColumnBuilder::Nullable(builder);
                    }
                    _ => unreachable!(
                        "ColumnBuilder::with_capacity for Nullable type should return ColumnBuilder::Nullable, \
                     but got different variant. data_type: {}, capacity: {}",
                        from.data_type(),
                        capacity
                    ),
                }
            }
            _ => unreachable!(
                "Null builder can only copy from Null or Nullable, but got from type: {}",
                from.data_type()
            ),
        },
        ColumnBuilder::Nullable(builder) => match from {
            Column::Null { .. } => {
                builder.push_repeat_null(indices.len());
            }
            Column::Nullable(column) => {
                copy_nullable(builder, column, indices, preserve_string_views);
            }
            _ => unreachable!(
                "Nullable builder can only copy from Null or Nullable, but got from type: {}",
                from.data_type()
            ),
        },
        ColumnBuilder::EmptyMap { len } => match from {
            Column::EmptyMap { .. } => *len += indices.len(),
            Column::Map(column) => {
                let capacity = *len + indices.len();
                match ColumnBuilder::with_capacity(&from.data_type(), capacity) {
                    ColumnBuilder::Map(mut builder) => {
                        builder.offsets.extend(vec![0; *len]);
                        copy_array(&mut builder, column, indices);
                        *to = ColumnBuilder::Map(builder);
                    }
                    _ => unreachable!(
                        "ColumnBuilder::with_capacity for Map type should return ColumnBuilder::Map, \
                     but got different variant. data_type: {}, capacity: {}",
                        from.data_type(),
                        capacity
                    ),
                }
            }
            _ => unreachable!(
                "EmptyMap builder can only copy from EmptyMap or Map, but got from type: {}",
                from.data_type()
            ),
        },
        ColumnBuilder::Map(builder) => match from {
            Column::Map(column) => {
                copy_array(builder, column, indices);
            }
            Column::EmptyMap { .. } => {
                for _ in 0..indices.len() {
                    builder.commit_row();
                }
            }
            _ => unreachable!(
                "Map builder can only copy from EmptyMap or Map, but got from type: {}",
                from.data_type()
            ),
        },
        _ => match (to, from) {
            (ColumnBuilder::Number(builder), Column::Number(number_column)) => {
                with_number_mapped_type!(|NUM_TYPE| match (builder, number_column) {
                    (NumberColumnBuilder::NUM_TYPE(b), NumberColumn::NUM_TYPE(c)) => {
                        copy_primitive_type(b, c, indices);
                    }
                    _ => unreachable!(),
                })
            }
            (ColumnBuilder::Decimal(builder), Column::Decimal(column)) => {
                with_decimal_type!(|DECIMAL_TYPE| match (builder, column) {
                    (
                        DecimalColumnBuilder::DECIMAL_TYPE(builder, _),
                        DecimalColumn::DECIMAL_TYPE(column, _),
                    ) => {
                        copy_primitive_type(builder, column, indices);
                    }
                    _ => unreachable!(),
                });
            }
            (ColumnBuilder::Boolean(builder), Column::Boolean(column)) => {
                copy_boolean(builder, column, indices)
            }
            (ColumnBuilder::Date(builder), Column::Date(column)) => {
                copy_primitive_type(builder, column, indices);
            }
            (ColumnBuilder::Interval(builder), Column::Interval(column)) => {
                copy_primitive_type(builder, column, indices);
            }
            (ColumnBuilder::Timestamp(builder), Column::Timestamp(column)) => {
                copy_primitive_type(builder, column, indices);
            }
            (ColumnBuilder::Bitmap(builder), Column::Bitmap(column)) => {
                copy_binary(builder, column, indices);
            }
            (ColumnBuilder::Binary(builder), Column::Binary(column)) => {
                copy_binary(builder, column, indices);
            }
            (ColumnBuilder::Variant(builder), Column::Variant(column)) => {
                copy_binary(builder, column, indices);
            }
            (ColumnBuilder::Geometry(builder), Column::Geometry(column)) => {
                copy_binary(builder, column, indices);
            }
            (ColumnBuilder::Geography(builder), Column::Geography(column)) => {
                copy_binary(builder, &column.0, indices);
            }
            (ColumnBuilder::String(builder), Column::String(column)) => {
                if preserve_string_views {
                    copy_string_views(builder, column, indices);
                } else {
                    copy_string(builder, column, indices);
                }
            }
            (ColumnBuilder::Vector(builder), Column::Vector(column)) => {
                copy_vector(indices, builder, column);
            }
            (ColumnBuilder::Opaque(builder), Column::Opaque(column)) => {
                copy_opaque(indices, builder, column);
            }
            (ColumnBuilder::Tuple(builders), Column::Tuple(columns)) => {
                for (builder, column) in builders.iter_mut().zip(columns.iter()) {
                    copy_column_impl(indices, column, builder, preserve_string_views)
                }
            }
            (to, from) => unreachable!(
                "Unsupported column builder type for copy_column. to type: {:?}, from type: {}",
                to.data_type(),
                from.data_type()
            ),
        },
    };
}

fn copy_boolean<I: Index>(to: &mut MutableBitmap, from: &Bitmap, indices: &[I]) {
    let num_rows = indices.len();

    if num_rows == 0 {
        return;
    }

    // Fast path: avoid iterating column to generate a new bitmap.
    // If this [`Bitmap`] is all true or all false and `num_rows <= bitmap.len()``,
    // we can just slice it.
    if num_rows <= from.len() && (from.null_count() == 0 || from.null_count() == from.len()) {
        to.extend_constant(num_rows, from.get_bit(0));
        return;
    }

    to.extend_from_trusted_len_iter(indices.iter().map(|index| from.get_bit(index.to_usize())));
}

fn copy_primitive_type<T: Copy, I: Index>(to: &mut Vec<T>, from: &Buffer<T>, indices: &[I]) {
    to.extend(
        indices
            .iter()
            .map(|index| unsafe { *from.get_unchecked(index.to_usize()) }),
    );
}

fn copy_binary<I: Index>(to: &mut BinaryColumnBuilder, from: &BinaryColumn, indices: &[I]) {
    let num_rows = indices.len();

    let row_bytes = from.total_bytes_len() / from.len();
    let data_capacity = row_bytes * (indices.len() * 4).div_ceil(3);
    to.reserve(num_rows, data_capacity);

    for index in indices.iter() {
        unsafe {
            to.put_slice(from.index_unchecked(index.to_usize()));
            to.commit_row();
        }
    }
}

fn copy_string<I: Index>(to: &mut StringColumnBuilder, from: &StringColumn, indices: &[I]) {
    to.data.reserve(indices.len());

    for index in indices.iter() {
        unsafe {
            to.put_and_commit(from.index_unchecked(index.to_usize()));
        }
    }
}

fn copy_string_views<I: Index>(to: &mut StringColumnBuilder, from: &StringColumn, indices: &[I]) {
    // String columns store long values in shared buffers and rows as 16-byte views. Preserve
    // those buffers while partitioning instead of copying every selected string into a new one.
    // `append_views_unchecked` adjusts buffer indices when appending multiple input blocks.
    unsafe {
        to.data.append_views_unchecked(
            indices
                .iter()
                .map(|index| from.views().get_unchecked(index.to_usize())),
            from.data_buffers(),
        );
    }
}

fn copy_nullable<I: Index>(
    to: &mut NullableColumnBuilder<AnyType>,
    from: &NullableColumn<AnyType>,
    indices: &[I],
    preserve_string_views: bool,
) {
    copy_boolean(&mut to.validity, &from.validity, indices);
    copy_column_impl(
        indices,
        &from.column,
        &mut to.builder,
        preserve_string_views,
    )
}

fn copy_opaque<I: Index>(indices: &[I], builder: &mut OpaqueColumnBuilder, column: &OpaqueColumn) {
    match (builder, column) {
        (OpaqueColumnBuilder::Opaque1(builder), OpaqueColumn::Opaque1(column)) => {
            copy_primitive_type(builder, column, indices);
        }
        (OpaqueColumnBuilder::Opaque2(builder), OpaqueColumn::Opaque2(column)) => {
            copy_primitive_type(builder, column, indices);
        }
        (OpaqueColumnBuilder::Opaque3(builder), OpaqueColumn::Opaque3(column)) => {
            copy_primitive_type(builder, column, indices);
        }
        (OpaqueColumnBuilder::Opaque4(builder), OpaqueColumn::Opaque4(column)) => {
            copy_primitive_type(builder, column, indices);
        }
        (OpaqueColumnBuilder::Opaque5(builder), OpaqueColumn::Opaque5(column)) => {
            copy_primitive_type(builder, column, indices);
        }
        (OpaqueColumnBuilder::Opaque6(builder), OpaqueColumn::Opaque6(column)) => {
            copy_primitive_type(builder, column, indices);
        }
        _ => unreachable!(),
    }
}

fn copy_vector<I: Index>(indices: &[I], builder: &mut VectorColumnBuilder, column: &VectorColumn) {
    match (builder, column) {
        (VectorColumnBuilder::Int8((builder, _)), VectorColumn::Int8((column, _))) => {
            copy_primitive_type(builder, column, indices);
        }
        (VectorColumnBuilder::Float32((builder, _)), VectorColumn::Float32((column, _))) => {
            copy_primitive_type(builder, column, indices);
        }
        _ => unreachable!(),
    }
}

fn copy_array<I: Index>(
    to: &mut ArrayColumnBuilder<AnyType>,
    from: &ArrayColumn<AnyType>,
    indices: &[I],
) {
    // TODO:
    for index in indices {
        unsafe { to.push(from.index_unchecked(index.to_usize())) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_string_views_preserves_buffers_across_input_blocks() {
        let first = [
            "short",
            "a value long enough to use a backing buffer",
            "another buffered string value",
        ]
        .into_iter()
        .collect::<StringColumnBuilder>()
        .build();
        let second = ["second block also uses a backing buffer", "tiny"]
            .into_iter()
            .collect::<StringColumnBuilder>()
            .build();
        let first_buffer = first.data_buffers()[0].as_ptr();
        let second_buffer = second.data_buffers()[0].as_ptr();

        let mut builder = StringColumnBuilder::with_capacity(5);
        copy_string_views(&mut builder, &first, &[2_u32, 0, 1]);
        copy_string_views(&mut builder, &second, &[1_u32, 0]);
        let result = builder.build();

        assert_eq!(result.iter().collect::<Vec<_>>(), vec![
            "another buffered string value",
            "short",
            "a value long enough to use a backing buffer",
            "tiny",
            "second block also uses a backing buffer",
        ]);
        assert_eq!(result.data_buffers()[0].as_ptr(), first_buffer);
        assert_eq!(result.data_buffers()[1].as_ptr(), second_buffer);
    }

    #[test]
    fn partition_preserves_string_views_until_compacted() {
        let source = [
            "partition zero first buffered string",
            "partition one first buffered string",
            "partition zero second buffered string",
            "partition one second buffered string",
        ]
        .into_iter()
        .collect::<StringColumnBuilder>()
        .build();
        let source_buffer = source.data_buffers()[0].as_ptr();
        let block = DataBlock::new_from_columns(vec![Column::String(source)]);
        let mut stream = BlockPartitionStream::create_with_string_views(1, usize::MAX, 2);

        let blocks = stream.partition(vec![0, 1, 0, 1], block, true);
        assert_eq!(blocks.len(), 2);

        let local = blocks[0].1.get_by_offset(0).to_column();
        let remote = blocks[1].1.get_by_offset(0).to_column();
        let Column::String(remote_shared) = &remote else {
            unreachable!()
        };
        assert_eq!(remote_shared.data_buffers()[0].as_ptr(), source_buffer);
        let remote = remote.compact_string_buffers();
        let Column::String(local) = local else {
            unreachable!()
        };
        let Column::String(remote) = remote else {
            unreachable!()
        };

        assert_eq!(local.iter().collect::<Vec<_>>(), vec![
            "partition zero first buffered string",
            "partition zero second buffered string",
        ]);
        assert_eq!(remote.iter().collect::<Vec<_>>(), vec![
            "partition one first buffered string",
            "partition one second buffered string",
        ]);
        assert_eq!(local.data_buffers()[0].as_ptr(), source_buffer);
        assert_ne!(remote.data_buffers()[0].as_ptr(), source_buffer);
    }
}
