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

use arrow_array::RecordBatch;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_parquet2_reader::arrow::chunk::Chunk;
use databend_common_parquet2_reader::arrow::datatypes::Field;
use databend_common_parquet2_reader::arrow::datatypes::Schema;
use databend_common_parquet2_reader::arrow::io::parquet::read::column_iter_to_arrays;
use databend_common_parquet2_reader::arrow::io::parquet::read::nested_column_iter_to_arrays;
use databend_common_parquet2_reader::arrow::io::parquet::read::ArrayIter;
use databend_common_parquet2_reader::arrow::io::parquet::read::InitNested;
use databend_common_parquet2_reader::arrow::io::parquet::write::to_parquet_schema;
use databend_common_parquet2_reader::arrow::io::parquet::write::to_parquet_type;
use databend_common_parquet2_reader::parquet::compression::Compression as ParquetCompression;
use databend_common_parquet2_reader::parquet::metadata::ColumnDescriptor;
use databend_common_parquet2_reader::parquet::metadata::SchemaDescriptor;
use databend_common_parquet2_reader::parquet::read::PageMetaData;
use databend_common_parquet2_reader::parquet::read::PageReader;
use databend_common_storage::ColumnNode;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::TableDataCacheKey;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;

use super::BlockReader;
use crate::io::read::block::block_reader_deserialize::DeserializedArray;
use crate::io::read::block::block_reader_merge_io::DataItem;
use crate::io::read::block::parquet::decompressor::BuffedBasicDecompressor;
use crate::io::read::block::parquet::decompressor::UncompressedBuffer;

pub struct FieldDeserializationContext<'a> {
    pub(crate) column_metas: &'a HashMap<ColumnId, ColumnMeta>,
    pub(crate) column_chunks: &'a HashMap<ColumnId, DataItem<'a>>,
    pub(crate) num_rows: usize,
    pub(crate) compression: &'a Compression,
    pub(crate) uncompressed_buffer: &'a Option<Arc<UncompressedBuffer>>,
    pub(crate) parquet_schema_descriptor: &'a Option<SchemaDescriptor>,
}
impl BlockReader {
    pub(crate) fn column_chunks_to_data_block_2(
        &self,
        block_path: &str,
        num_rows: usize,
        compression: &Compression,
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
        uncompressed_buffer: Option<Arc<UncompressedBuffer>>,
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
            uncompressed_buffer: &uncompressed_buffer,
            parquet_schema_descriptor: &None::<SchemaDescriptor>,
        };
        for column_node in &self.project_column_nodes {
            let deserialized_column = self
                .deserialize_field(&field_deserialization_ctx, column_node)
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
                    deserialized_column_arrays.push(v);
                    need_default_vals.push(false);
                }
            }
        }

        // assembly the arrays
        let mut chunk_arrays = vec![];
        for array in &deserialized_column_arrays {
            match array {
                DeserializedArray::Deserialized((_, array, ..)) => {
                    chunk_arrays.push(array);
                }
                DeserializedArray::NoNeedToCache(array) => {
                    chunk_arrays.push(array);
                }
                DeserializedArray::Cached(sized_column) => {
                    chunk_arrays.push(&sized_column.0);
                }
            }
        }

        // build data block
        // let chunk = Chunk::try_new(chunk_arrays)?;
        let data_block = if !need_to_fill_default_val {
            let record_batch = RecordBatch::try_new(
                self.arrow_schema(),
                chunk_arrays.into_iter().cloned().collect(),
            )?;
            let (block, _) = DataBlock::from_record_batch(&self.data_schema(), &record_batch)?;
            block
        } else {
            todo!()
            // let data_schema = self.data_schema();
            // let mut default_vals = Vec::with_capacity(need_default_vals.len());
            // for (i, need_default_val) in need_default_vals.iter().enumerate() {
            //    if !need_default_val {
            //        default_vals.push(None);
            //    } else {
            //        default_vals.push(Some(self.default_vals[i].clone()));
            //    }
            //}
            // DataBlock::create_with_default_value_and_chunk(
            //    &data_schema,
            //    &chunk,
            //    &default_vals,
            //    num_rows,
            //)?
        };

        // populate cache if necessary
        if self.put_cache {
            if let Some(cache) = CacheManager::instance().get_table_data_array_cache() {
                // populate array cache items
                for item in deserialized_column_arrays.into_iter() {
                    if let DeserializedArray::Deserialized((column_id, array, size)) = item {
                        let meta = column_metas.get(&column_id).unwrap();
                        let (offset, len) = meta.offset_length();
                        let key = TableDataCacheKey::new(block_path, column_id, offset, len);
                        cache.insert(key.into(), (array, size));
                    }
                }
            }
        }
        Ok(data_block)
    }

    pub fn deserialize_field<'a>(
        &self,
        deserialization_context: &'a FieldDeserializationContext,
        column: &ColumnNode,
    ) -> Result<Option<DeserializedArray<'a>>> {
        let indices = &column.leaf_indices;
        let column_chunks = deserialization_context.column_chunks;
        let compression = deserialization_context.compression;
        let uncompressed_buffer = deserialization_context.uncompressed_buffer;
        // column passed in may be a compound field (with sub leaves),
        // or a leaf column of compound field
        let is_nested = column.is_nested;
        let estimated_cap = indices.len();
        let mut field_column_metas = Vec::with_capacity(estimated_cap);
        let mut field_column_data = Vec::with_capacity(estimated_cap);
        let mut field_column_descriptors = Vec::with_capacity(estimated_cap);
        let mut field_uncompressed_size = 0;

        let arrow_schema = self.arrow_schema.as_ref();
        let fields = arrow_schema
            .fields()
            .iter()
            .map(|f| Field::from(f))
            .collect::<Vec<_>>();
        let arrow_schema = Schema::from(fields);
        let parquet_schema_descriptor = to_parquet_schema(&arrow_schema).map_err(|e| {
            ErrorCode::StorageOther(format!(
                "failed to convert arrow schema to parquet schema, error: {}",
                e
            ))
        })?;

        for (i, leaf_index) in indices.iter().enumerate() {
            let column_id = column.leaf_column_ids[i];
            if let Some(column_meta) = deserialization_context.column_metas.get(&column_id) {
                if let Some(chunk) = column_chunks.get(&column_id) {
                    match chunk {
                        DataItem::RawData(data) => {
                            let column_descriptor = if let Some(parquet_schema_descriptor) =
                                deserialization_context.parquet_schema_descriptor
                            {
                                &parquet_schema_descriptor.columns()[*leaf_index]
                            } else {
                                //   // TODO refactor this, put it somewhere else
                                //   // let arrow_schema = self.arrow_schema.as_ref().into();
                                //   let arrow_schema = self.schema().into();
                                //   let parquet_schema_descriptor = to_parquet_schema(&arrow_schema).map_err(|e|
                                //       ErrorCode::StorageOther(
                                //           format!("failed to convert arrow schema to parquet schema, error: {}", e)
                                //   ))?;

                                &parquet_schema_descriptor.columns()[*leaf_index]
                            };
                            field_column_metas.push(column_meta);
                            field_column_data.push(data.as_ref());
                            field_column_descriptors.push(column_descriptor);
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
                            return Ok(Some(DeserializedArray::Cached(column_array)));
                        }
                    }
                } else {
                    // If the column is the source of virtual columns, it may be ignored.
                    // TODO cover more case and add context info for error message
                    // no raw data of given column id, it is unexpected
                    return Ok(None);
                }
            } else {
                // no column meta of given column id
                break;
            }
        }

        let num_rows = deserialization_context.num_rows;
        if !field_column_metas.is_empty() {
            let field_name = column.field.name().to_owned();
            let mut array_iter = Self::chunks_to_parquet_array_iter(
                field_column_metas,
                field_column_data,
                num_rows,
                field_column_descriptors,
                column.field.clone().into(),
                // TODO
                // column.init.clone(),
                vec![],
                compression,
                uncompressed_buffer
                    .clone()
                    .unwrap_or_else(|| UncompressedBuffer::new(0)),
            )?;
            let array = array_iter
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
            assert!(array_iter.next().is_none());

            // mark the array
            if is_nested {
                // the array is not intended to be cached
                // currently, caching of compound field columns is not support
                Ok(Some(DeserializedArray::NoNeedToCache(array.into())))
            } else {
                // the array is deserialized from raw bytes, should be cached
                let column_id = column.leaf_column_ids[0];
                Ok(Some(DeserializedArray::Deserialized((
                    column_id,
                    array.into(),
                    field_uncompressed_size,
                ))))
            }
        } else {
            Ok(None)
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn chunks_to_parquet_array_iter<'a>(
        metas: Vec<&ColumnMeta>,
        chunks: Vec<&'a [u8]>,
        rows: usize,
        column_descriptors: Vec<&ColumnDescriptor>,
        field: Field,
        init: Vec<InitNested>,
        compression: &Compression,
        uncompressed_buffer: Arc<UncompressedBuffer>,
    ) -> Result<ArrayIter<'a>> {
        let columns = metas
            .iter()
            .zip(chunks.into_iter().zip(column_descriptors.iter()))
            .map(|(meta, (chunk, column_descriptor))| {
                let meta = meta.as_parquet().unwrap();

                let page_meta_data = PageMetaData {
                    column_start: meta.offset,
                    num_values: meta.num_values as i64,
                    compression: Self::to_parquet_compression(compression)?,
                    descriptor: column_descriptor.descriptor.clone(),
                };
                let pages = PageReader::new_with_page_meta(
                    chunk,
                    page_meta_data,
                    Arc::new(|_, _| true),
                    vec![],
                    usize::MAX,
                );

                Ok(BuffedBasicDecompressor::new(
                    pages,
                    uncompressed_buffer.clone(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let types = column_descriptors
            .iter()
            .map(|column_descriptor| &column_descriptor.descriptor.primitive_type)
            .collect::<Vec<_>>();

        let array_iter = if init.is_empty() {
            column_iter_to_arrays(columns, types, field, Some(rows), rows)
                .map_err(|e| ErrorCode::StorageOther(e.to_string()))?
        } else {
            nested_column_iter_to_arrays(columns, types, field, init, Some(rows), rows)
                .map_err(|e| ErrorCode::StorageOther(e.to_string()))?
        };
        Ok(array_iter)
    }

    fn to_parquet_compression(
        meta_compression: &Compression,
    ) -> databend_common_exception::Result<ParquetCompression> {
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
