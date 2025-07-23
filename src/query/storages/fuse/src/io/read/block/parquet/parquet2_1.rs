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
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_p2_reader::page_iter_to_columns;
use databend_common_p2_reader::BuffedBasicDecompressor;
use databend_common_p2_reader::ColumnIter;
use databend_common_p2_reader::UncompressedBuffer;
use databend_common_storage::ColumnNode;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
use parquet2::compression::Compression as ParquetCompression;
use parquet2::metadata::Descriptor;
use parquet2::read::PageMetaData;
use parquet2::read::PageReader;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveType;
use parquet2::schema::Repetition;

use super::BlockReader;
use crate::io::read::block::block_reader_merge_io::DataItem;

pub struct FieldDeserializationContext<'a> {
    pub(crate) column_metas: &'a HashMap<ColumnId, ColumnMeta>,
    pub(crate) column_chunks: &'a HashMap<ColumnId, DataItem<'a>>,
    pub(crate) num_rows: usize,
    pub(crate) compression: &'a Compression,
    pub(crate) uncompressed_buffer: &'a Option<Arc<UncompressedBuffer>>,
}
impl BlockReader {
    pub(crate) fn column_chunks_to_data_block_2_1(
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
        };
        for column_node in &self.project_column_nodes {
            let deserialized_column = self
                .deserialize_field_2_1(&field_deserialization_ctx, column_node)
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
        // let mut chunk_arrays = vec![];
        // for array in &deserialized_column_arrays {
        //    match array {
        //        DeserializedArray::Deserialized((_, array, ..)) => {
        //            chunk_arrays.push(array);
        //        }
        //        DeserializedArray::NoNeedToCache(array) => {
        //            chunk_arrays.push(array);
        //        }
        //        DeserializedArray::Cached(sized_column) => {
        //            chunk_arrays.push(&sized_column.0);
        //        }
        //    }
        //}

        // build data block
        // let chunk = Chunk::try_new(chunk_arrays)?;
        let data_block = if !need_to_fill_default_val {
            // let arrow_schema: arrow_schema::Schema = self.projected_schema.as_ref().into();
            // let record_batch = RecordBatch::try_new(
            //    Arc::new(arrow_schema),
            //    chunk_arrays.into_iter().cloned().collect(),
            //)?;
            // let (block, _) = DataBlock::from_record_batch(&self.data_schema(), &record_batch)?;
            let block = DataBlock::from_iter(
                deserialized_column_arrays
                    .into_iter()
                    .map(|col| BlockEntry::Column(col)),
                num_rows,
            );
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
        // if self.put_cache {
        //    if let Some(cache) = CacheManager::instance().get_table_data_array_cache() {
        //        // populate array cache items
        //        for item in deserialized_column_arrays.into_iter() {
        //            if let DeserializedArray::Deserialized((column_id, array, size)) = item {
        //                let meta = column_metas.get(&column_id).unwrap();
        //                let (offset, len) = meta.offset_length();
        //                let key = TableDataCacheKey::new(block_path, column_id, offset, len);
        //                cache.insert(key.into(), (array, size));
        //            }
        //        }
        //    }
        //}
        Ok(data_block)
    }

    pub fn deserialize_field_2_1<'a>(
        &self,
        deserialization_context: &'a FieldDeserializationContext,
        column: &ColumnNode,
    ) -> Result<Option<Column>> {
        let indices = &column.leaf_indices;
        let column_chunks = deserialization_context.column_chunks;
        let compression = deserialization_context.compression;
        let uncompressed_buffer = deserialization_context.uncompressed_buffer;
        // column passed in may be a compound field (with sub leaves),
        // or a leaf column of compound field
        let estimated_cap = indices.len();
        let mut field_column_metas = Vec::with_capacity(estimated_cap);
        let mut field_column_data = Vec::with_capacity(estimated_cap);
        let mut field_column_descriptors = Vec::with_capacity(estimated_cap);
        // let mut field_uncompressed_size = 0;
        let mut parquet_primitive_type = match column.table_field.data_type {
            TableDataType::String => PrimitiveType::from_physical(
                column.table_field.name.clone(),
                PhysicalType::ByteArray,
            ),
            TableDataType::Number(number_type) => match number_type {
                NumberDataType::Int8 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int32,
                ),
                NumberDataType::Int16 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int32,
                ),
                NumberDataType::Int32 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int32,
                ),
                NumberDataType::Int64 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int64,
                ),
                NumberDataType::UInt8 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int32,
                ),
                NumberDataType::UInt16 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int32,
                ),
                NumberDataType::UInt32 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int64,
                ),
                NumberDataType::UInt64 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int64,
                ),
                NumberDataType::Float32 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Float,
                ),
                NumberDataType::Float64 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Double,
                ),
            },
            TableDataType::Decimal(decimal_type) => {
                let precision = decimal_type.precision();
                let _scale = decimal_type.scale();
                if precision <= 9 {
                    PrimitiveType::from_physical(
                        column.table_field.name.clone(),
                        PhysicalType::Int32,
                    )
                } else if precision <= 18 {
                    PrimitiveType::from_physical(
                        column.table_field.name.clone(),
                        PhysicalType::Int64,
                    )
                } else {
                    let len = decimal_length_from_precision(precision as usize);
                    // For decimal256
                    PrimitiveType::from_physical(
                        column.table_field.name.clone(),
                        PhysicalType::FixedLenByteArray(len),
                    )
                }
            }
            TableDataType::Date => {
                PrimitiveType::from_physical(column.table_field.name.clone(), PhysicalType::Int32)
            }
            _ => unimplemented!(),
        };

        parquet_primitive_type.field_info.repetition = Repetition::Required;

        let column_descriptor = Descriptor {
            primitive_type: parquet_primitive_type,
            max_def_level: 0,
            max_rep_level: 0,
        };

        for (i, _leaf_index) in indices.iter().enumerate() {
            let column_id = column.leaf_column_ids[i];
            if let Some(column_meta) = deserialization_context.column_metas.get(&column_id) {
                if let Some(chunk) = column_chunks.get(&column_id) {
                    match chunk {
                        DataItem::RawData(data) => {
                            field_column_metas.push(column_meta);
                            field_column_data.push(data.as_ref());
                            field_column_descriptors.push(&column_descriptor);
                            // field_uncompressed_size += data.len();
                        }
                        DataItem::ColumnArray(_column_array) => {
                            unimplemented!()
                            // if is_nested {
                            //    // TODO more context info for error message
                            //    return Err(ErrorCode::StorageOther(
                            //        "unexpected nested field: nested leaf field hits cached",
                            //    ));
                            //}
                            //// since it is not nested, one column is enough
                            // return Ok(Some(DeserializedArray::Cached(column_array)));
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

            let mut array_iter = Self::chunks_to_col_iter(
                field_column_metas,
                field_column_data,
                num_rows,
                field_column_descriptors,
                column.table_field.clone(),
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
            Ok(Some(array))
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
        uncompressed_buffer: Arc<UncompressedBuffer>,
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

                Ok(BuffedBasicDecompressor::new(
                    pages,
                    uncompressed_buffer.clone(),
                ))
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

fn decimal_length_from_precision(precision: usize) -> usize {
    // digits = floor(log_10(2^(8*n - 1) - 1))
    // ceil(digits) = log10(2^(8*n - 1) - 1)
    // 10^ceil(digits) = 2^(8*n - 1) - 1
    // 10^ceil(digits) + 1 = 2^(8*n - 1)
    // log2(10^ceil(digits) + 1) = (8*n - 1)
    // log2(10^ceil(digits) + 1) + 1 = 8*n
    // (log2(10^ceil(a) + 1) + 1) / 8 = n
    (((10.0_f64.powi(precision as i32) + 1.0).log2() + 1.0) / 8.0).ceil() as usize
}
