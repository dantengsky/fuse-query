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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_p2_reader::from_table_filed_type;
use databend_common_p2_reader::page_iter_to_columns;
use databend_common_p2_reader::ColumnIter;
use databend_common_storage::ColumnNode;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::SizedColumnArray;
use databend_storages_common_cache::TableDataCacheKey;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
use parquet2::compression::Compression as ParquetCompression;
use parquet2::metadata::Descriptor;
use parquet2::read::Decompressor;
use parquet2::read::PageMetaData;
use parquet2::read::PageReader;

use super::BlockReader;
use crate::io::read::block::block_reader_merge_io::DataItem;

pub struct FieldDeserializationContext<'a> {
    pub(crate) column_metas: &'a HashMap<ColumnId, ColumnMeta>,
    pub(crate) column_chunks: &'a HashMap<ColumnId, DataItem<'a>>,
    pub(crate) num_rows: usize,
    pub(crate) compression: &'a Compression,
}

enum DeserializedColumn<'a> {
    FromCache(&'a Arc<SizedColumnArray>),
    Column((ColumnId, Column, usize)),
}

impl BlockReader {
    pub(crate) fn deserialize_using_parquet2(
        &self,
        block_path: &str,
        num_rows: usize,
        compression: &Compression,
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
    ) -> Result<DataBlock> {
        if column_chunks.is_empty() {
            return self.build_default_values_block(num_rows);
        }

        let mut need_default_vals = Vec::with_capacity(self.project_column_nodes.len());
        let mut need_to_fill_default_val = false;
        let mut deserialized_column_arrays = Vec::with_capacity(self.projection.len());
        let field_deserialization_ctx = FieldDeserializationContext {
            column_metas,
            column_chunks: &column_chunks,
            num_rows,
            compression,
        };

        for column_node in &self.project_column_nodes {
            let deserialized_column = self
                .deserialize_field_using_parquet2(&field_deserialization_ctx, column_node)
                .map_err(|e| {
                    e.add_message(format!(
                        "failed to deserialize column: {:?}, location {} ",
                        column_node, block_path
                    ))
                })?;
            match deserialized_column {
                None => {
                    need_to_fill_default_val = true;
                    need_default_vals.push(true);
                }
                Some(v) => {
                    deserialized_column_arrays.push((v, column_node.table_field.data_type()));
                    need_default_vals.push(false);
                }
            }
        }

        let cache = if self.put_cache {
            CacheManager::instance().get_table_data_array_cache()
        } else {
            None
        };

        let mut block_entries = Vec::with_capacity(deserialized_column_arrays.len());
        for (col, table_data_type) in deserialized_column_arrays {
            let entry = match col {
                DeserializedColumn::FromCache(arrow_array) => {
                    BlockEntry::Column(Column::from_arrow_rs(
                        arrow_array.0.clone(),
                        &(&table_data_type.clone()).into(),
                    )?)
                }
                DeserializedColumn::Column((column_id, col, size)) => {
                    if let Some(cache) = &cache {
                        let meta = column_metas.get(&column_id).unwrap();
                        let (offset, len) = meta.offset_length();
                        let key = TableDataCacheKey::new(block_path, column_id, offset, len);
                        let array = col.clone().into_arrow_rs();
                        cache.insert(key.into(), (array, size));
                    };
                    BlockEntry::Column(col)
                }
            };
            block_entries.push(entry);
        }

        // build data block
        let data_block = if !need_to_fill_default_val {
            assert_eq!(block_entries.len(), self.projected_schema.num_fields());
            DataBlock::new(block_entries, num_rows)
        } else {
            let mut default_vals = Vec::with_capacity(need_default_vals.len());
            for (i, need_default_val) in need_default_vals.iter().enumerate() {
                if !need_default_val {
                    default_vals.push(None);
                } else {
                    default_vals.push(Some(self.default_vals[i].clone()));
                }
            }

            create_with_opt_default_value(
                block_entries,
                &self.data_schema(),
                &default_vals,
                num_rows,
            )?
        };
        Ok(data_block)
    }

    fn deserialize_field_using_parquet2<'a>(
        &self,
        deserialization_context: &'a FieldDeserializationContext,
        column_node: &ColumnNode,
    ) -> Result<Option<DeserializedColumn<'a>>> {
        let is_nested = column_node.is_nested;

        if is_nested {
            unimplemented!()
        }

        let indices = &column_node.leaf_indices;
        let column_chunks = deserialization_context.column_chunks;
        let compression = deserialization_context.compression;
        // column passed in may be a compound field (with sub leaves),
        // or a leaf column of compound field
        let estimated_cap = indices.len();
        let mut field_column_metas = Vec::with_capacity(estimated_cap);
        let mut field_column_data = Vec::with_capacity(estimated_cap);
        let mut field_column_descriptors = Vec::with_capacity(estimated_cap);
        let mut field_uncompressed_size = 0;

        let parquet_primitive_type = from_table_filed_type(
            column_node.table_field.name.clone(),
            &column_node.table_field.data_type,
        );

        // TODO calculate max_def_level and max_rep_level
        let column_descriptor = Descriptor {
            primitive_type: parquet_primitive_type,
            max_def_level: 0,
            max_rep_level: 0,
        };

        for (i, _leaf_index) in indices.iter().enumerate() {
            let column_id = column_node.leaf_column_ids[i];
            if let Some(column_meta) = deserialization_context.column_metas.get(&column_id) {
                if let Some(chunk) = column_chunks.get(&column_id) {
                    match chunk {
                        DataItem::RawData(data) => {
                            field_column_metas.push(column_meta);
                            field_column_data.push(data.as_ref());
                            field_column_descriptors.push(&column_descriptor);
                            field_uncompressed_size += data.len();
                        }
                        DataItem::ColumnArray(column_array) => {
                            if is_nested {
                                // TODO more context info for error message
                                return Err(ErrorCode::StorageOther(
                                    "unexpected nested field: nested leaf field hits cached",
                                ));
                            }
                            // since it is not nested, one column is enough
                            return Ok(Some(DeserializedColumn::FromCache(column_array)));
                        }
                    }
                } else {
                    // TODO review this further
                    // If the column is the source of virtual columns, it may be ignored.
                    return Ok(None);
                }
            } else {
                // TODO review this
                // no column meta of given column id
                break;
            }
        }

        let num_rows = deserialization_context.num_rows;
        if !field_column_metas.is_empty() {
            let field_name = column_node.field.name().to_owned();

            // TODO reuse decompression buffer
            let mut column_iter = Self::chunks_to_col_iter(
                field_column_metas,
                field_column_data,
                num_rows,
                field_column_descriptors,
                column_node.table_field.clone(),
                compression,
            )?;
            let column = column_iter
                .next()
                .transpose()
                .map_err(|e| {
                    ErrorCode::StorageOther(format!(
                        "unexpected deserialization error, while processing field {field_name}: {e} "
                    ))
                })?
                .ok_or_else(|| {
                    ErrorCode::StorageOther(format!(
                        "unexpected deserialization error, no array found for field {field_name} "
                    ))
                })?;
            assert!(column_iter.next().is_none());

            // mark the array
            if is_nested {
                //  // the array is not intended to be cached
                //  // currently, caching of compound field columns is not support
                //  Ok(Some(DeserializedArray::NoNeedToCache(array)))
                unreachable!()
            } else {
                // the array is deserialized from raw bytes, and intended to be cached
                let column_id = column_node.leaf_column_ids[0];
                Ok(Some(DeserializedColumn::Column((
                    column_id,
                    column,
                    field_uncompressed_size,
                ))))
            }
        } else {
            Ok(None)
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn chunks_to_col_iter<'a>(
        metas: Vec<&ColumnMeta>,
        chunks: Vec<&'a [u8]>,
        rows: usize,
        column_descriptors: Vec<&Descriptor>,
        field: TableField,
        compression: &Compression,
    ) -> Result<ColumnIter<'a>> {
        let columns = metas
            .iter()
            .zip(chunks.into_iter().zip(column_descriptors.iter()))
            .map(|(meta, (chunk, column_descriptor))| {
                let meta = meta.as_parquet().unwrap();

                let page_meta_data = PageMetaData {
                    column_start: meta.offset,
                    num_values: meta.num_values as i64,
                    compression: Self::to_parquet_compression(compression)?,
                    descriptor: (*column_descriptor).clone(),
                };
                // TODO reuse scratch and uncompressed_buffer
                let pages = PageReader::new_with_page_meta(
                    chunk,
                    page_meta_data,
                    Arc::new(|_, _| true),
                    vec![],
                    usize::MAX,
                );

                Ok(Decompressor::new(pages, vec![]))
            })
            .collect::<Result<Vec<_>>>()?;

        let types = column_descriptors
            .iter()
            .map(|column_descriptor| &column_descriptor.primitive_type)
            .collect::<Vec<_>>();

        page_iter_to_columns(columns, types, field, None, rows)
    }

    pub(crate) fn to_parquet_compression(
        meta_compression: &Compression,
    ) -> Result<ParquetCompression> {
        match meta_compression {
            Compression::Lz4 => {
                let err_msg = r#"Deprecated compression algorithm [Lz4] detected.

                                        The Legacy compression algorithm [Lz4] is no longer supported.
                                        To migrate data from old format, please consider re-create the table,
                                        by using an old compatible version [v0.8.25-nightly … v0.7.12-nightly].

                                        - Bring up the compatible version of databend-query
                                        - re-create the table
                                           Suppose the name of table is T
                                            ~~~
                                            create table tmp_t as select * from T;
                                            drop table T all;
                                            alter table tmp_t rename to T;
                                            ~~~
                                        Please note that the history of table T WILL BE LOST.
                                       "#;
                Err(ErrorCode::StorageOther(err_msg))
            }
            Compression::Lz4Raw => Ok(ParquetCompression::Lz4Raw),
            Compression::Snappy => Ok(ParquetCompression::Snappy),
            Compression::Zstd => Ok(ParquetCompression::Zstd),
            Compression::Gzip => Ok(ParquetCompression::Gzip),
            Compression::None => Ok(ParquetCompression::Uncompressed),
        }
    }
}

pub fn create_with_opt_default_value(
    block_entries: Vec<BlockEntry>,
    schema: &DataSchema,
    default_vals: &[Option<Scalar>],
    num_rows: usize,
) -> Result<DataBlock> {
    let schema_fields = schema.fields();
    let mut block_entries_iter = block_entries.into_iter();

    let mut entries = Vec::with_capacity(default_vals.len());
    for (i, default_val) in default_vals.iter().enumerate() {
        let field = &schema_fields[i];
        let data_type = field.data_type();

        let entry = match default_val {
            Some(default_val) => {
                BlockEntry::new_const_column(data_type.clone(), default_val.to_owned(), num_rows)
            }
            None => block_entries_iter
                .next()
                .expect("arrays should have enough elements"),
        };

        entries.push(entry);
    }

    Ok(DataBlock::new(entries, num_rows))
}
