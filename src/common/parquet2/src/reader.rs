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
use std::io::Cursor;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::TableSchema;
use parquet2::compression::Compression;
use parquet2::error::Error;
use parquet2::error::Result as ParquetResult;
use parquet2::metadata::ColumnChunkMetaData;
use parquet2::metadata::Descriptor;
use parquet2::metadata::SchemaDescriptor;
use parquet2::page::CompressedDataPage;
use parquet2::page::CompressedPage;
use parquet2::page::DataPage;
use parquet2::page::DataPageHeader;
use parquet2::page::Page;
use parquet2::read::PageReader;
use parquet2::schema::types::FieldInfo;
use parquet2::schema::types::ParquetType as Type;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveType;
use parquet2::schema::Repetition;
use parquet_format_safe::ConvertedType;

use crate::buffer::DecompressedBuffer;
use crate::create_data_block_from_columns;
use crate::deserialize::deserialize_page_to_column;

/// Convert a Parquet physical type to a Databend DataType
pub fn physical_to_databend_type(physical_type: PhysicalType) -> Result<DataType> {
    match physical_type {
        PhysicalType::Boolean => Ok(DataType::Boolean),
        PhysicalType::Int32 => Ok(DataType::Number(NumberDataType::Int32)),
        PhysicalType::Int64 => Ok(DataType::Number(NumberDataType::Int64)),
        PhysicalType::Float => Ok(DataType::Number(NumberDataType::Float32)),
        PhysicalType::Double => Ok(DataType::Number(NumberDataType::Float64)),
        PhysicalType::ByteArray => Ok(DataType::String),
        PhysicalType::FixedLenByteArray(_) => Ok(DataType::String),
        _ => Err(ErrorCode::Internal(format!(
            "Unsupported physical type: {:?}",
            physical_type
        ))),
    }
}

/// A reader for parquet data that directly deserializes to DataBlock
pub struct ParquetReader {
    schema: DataSchema,
    parquet_schema: SchemaDescriptor,
}

impl ParquetReader {
    /// Create a new ParquetReader
    pub fn new(schema: DataSchema) -> Result<Self> {
        // Convert the DataSchema to a ParquetType
        let schema_type = Self::schema_to_parquet_type(&schema)?;

        // Convert the Type to a GroupType
        let schema_root = match schema_type {
            Type::GroupType {
                field_info, fields, ..
            } => {
                // Create a schema descriptor from the fields
                let parquet_schema = SchemaDescriptor::new(field_info.name, fields);
                Ok(Self {
                    schema,
                    parquet_schema,
                })
            }
            _ => Err(ErrorCode::Internal(
                "Schema root is not a group type".to_string(),
            )),
        }?;

        Ok(schema_root)
    }

    /// Convert a DataSchema to a ParquetType
    fn schema_to_parquet_type(schema: &DataSchema) -> Result<Type> {
        // Create a group type for the schema
        let mut fields = Vec::new();

        for field in schema.fields() {
            fields.push(Self::field_to_parquet_type(field)?);
        }

        // Create a message type for the schema
        let schema_root = Type::GroupType {
            field_info: FieldInfo {
                name: "databend_schema".to_string(),
                repetition: Repetition::Optional,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            fields,
        };

        Ok(schema_root)
    }

    /// Convert a DataField to a ParquetType
    fn field_to_parquet_type(field: &DataField) -> Result<Type> {
        match field.data_type() {
            DataType::Boolean => {
                let repetition = if field.is_nullable() {
                    Repetition::Optional
                } else {
                    Repetition::Required
                };

                let primitive_type = PrimitiveType {
                    field_info: FieldInfo {
                        name: field.name().to_string(),
                        repetition,
                        id: None,
                    },
                    physical_type: PhysicalType::Boolean,
                    converted_type: None,
                    logical_type: None,
                };

                Ok(Type::PrimitiveType(primitive_type))
            }
            DataType::Number(NumberDataType::Int8)
            | DataType::Number(NumberDataType::UInt8)
            | DataType::Number(NumberDataType::Int16)
            | DataType::Number(NumberDataType::UInt16)
            | DataType::Number(NumberDataType::Int32)
            | DataType::Number(NumberDataType::UInt32) => {
                let repetition = if field.is_nullable() {
                    Repetition::Optional
                } else {
                    Repetition::Required
                };

                let primitive_type = PrimitiveType {
                    field_info: FieldInfo {
                        name: field.name().to_string(),
                        repetition,
                        id: None,
                    },
                    physical_type: PhysicalType::Int32,
                    converted_type: None,
                    logical_type: None,
                };

                Ok(Type::PrimitiveType(primitive_type))
            }
            DataType::Number(NumberDataType::Int64) | DataType::Number(NumberDataType::UInt64) => {
                let repetition = if field.is_nullable() {
                    Repetition::Optional
                } else {
                    Repetition::Required
                };

                let primitive_type = PrimitiveType {
                    field_info: FieldInfo {
                        name: field.name().to_string(),
                        repetition,
                        id: None,
                    },
                    physical_type: PhysicalType::Int64,
                    converted_type: None,
                    logical_type: None,
                };

                Ok(Type::PrimitiveType(primitive_type))
            }
            DataType::Number(NumberDataType::Float32) => {
                let repetition = if field.is_nullable() {
                    Repetition::Optional
                } else {
                    Repetition::Required
                };
                let primitive_type = PrimitiveType {
                    field_info: FieldInfo {
                        name: field.name().to_string(),
                        repetition,
                        id: None,
                    },
                    physical_type: PhysicalType::Float,
                    converted_type: None,
                    logical_type: None,
                };

                Ok(Type::PrimitiveType(primitive_type))
            }
            DataType::Number(NumberDataType::Float64) => {
                let repetition = if field.is_nullable() {
                    Repetition::Optional
                } else {
                    Repetition::Required
                };

                let primitive_type = PrimitiveType {
                    field_info: FieldInfo {
                        name: field.name().to_string(),
                        repetition,
                        id: None,
                    },
                    physical_type: PhysicalType::Double,
                    converted_type: None,
                    logical_type: None,
                };

                Ok(Type::PrimitiveType(primitive_type))
            }
            DataType::String => {
                let repetition = if field.is_nullable() {
                    Repetition::Optional
                } else {
                    Repetition::Required
                };

                let primitive_type = PrimitiveType {
                    field_info: FieldInfo {
                        name: field.name().to_string(),
                        repetition,
                        id: None,
                    },
                    physical_type: PhysicalType::ByteArray,
                    converted_type: None,
                    logical_type: None,
                };

                Ok(Type::PrimitiveType(primitive_type))
            }
            DataType::Date => {
                let repetition = if field.is_nullable() {
                    Repetition::Optional
                } else {
                    Repetition::Required
                };

                let primitive_type = PrimitiveType {
                    field_info: FieldInfo {
                        name: field.name().to_string(),
                        repetition,
                        id: None,
                    },
                    physical_type: PhysicalType::Int32,
                    converted_type: None,
                    logical_type: None,
                };

                Ok(Type::PrimitiveType(primitive_type))
            }
            DataType::Timestamp => {
                let repetition = if field.is_nullable() {
                    Repetition::Optional
                } else {
                    Repetition::Required
                };

                let primitive_type = PrimitiveType {
                    field_info: FieldInfo {
                        name: field.name().to_string(),
                        repetition,
                        id: None,
                    },
                    physical_type: PhysicalType::Int64,
                    converted_type: None,
                    logical_type: None,
                };

                Ok(Type::PrimitiveType(primitive_type))
            }
            _ => Err(ErrorCode::Internal(format!(
                "Unsupported data type for Parquet conversion: {:?}",
                field.data_type()
            ))),
        }
    }

    /// Find the index of a column in the schema by its path
    fn find_column_index_by_path(&self, column_path: &str) -> Option<usize> {
        self.schema
            .fields()
            .iter()
            .position(|field| field.name() == column_path)
    }

    /// Read a column chunk from raw bytes
    fn read_column_chunk(
        &self,
        column_index: usize,
        data: &[u8],
        meta: &ColumnChunkMetaData,
        num_rows: usize,
        decompressed_buffer: &Arc<DecompressedBuffer>,
    ) -> Result<Column> {
        todo!()

        //      // Get the column descriptor for this column index
        //      let column_descriptor = self.parquet_schema.columns()[column_index].clone();

        //      // Get the compression for this column
        //      let compression = meta.compression();

        //      // Create a page reader for the column chunk
        //      let reader = Cursor::new(data);

        //      // Create an Arc with a function that always returns true (no filtering)
        //      let filter = Arc::new(|_: &Descriptor, _: &DataPageHeader| -> bool { true });

        //      let mut page_reader = PageReader::new(
        //          reader,
        //          meta,   // Pass the ColumnChunkMetaData
        //          filter, // Filter function
        //          vec![], // No specific pages to read
        //          0,      // No limit on number of pages (0 means no limit)
        //      );

        //      // Read the first page (PageReader implements Iterator in v0.17)
        //      // PageReader::next returns Option<Result<CompressedPage>>
        //      let compressed_page = match page_reader.next() {
        //          Some(Ok(page)) => page,
        //          Some(Err(e)) => return Err(ErrorCode::Internal(format!("Failed to read page: {}", e))),
        //          None => return Err(ErrorCode::Internal("No pages in column chunk".to_string())),
        //      };

        //      // Handle the compressed page based on its type
        //      match compressed_page {
        //          CompressedPage::Data(compressed_data_page) => {
        //              // Get the header and compressed data
        //              let header = compressed_data_page.header().clone();

        //              // In parquet2 v0.17, CompressedDataPage doesn't have a buffer() method
        //              // Access the compressed data directly
        //              let compressed_data = &compressed_data_page.buffer;
        //              let uncompressed_size = compressed_data_page.uncompressed_size();

        //              // Decompress the buffer if needed
        //              let decompressed = if compression != Compression::Uncompressed {
        //                  decompress_helper(compressed_data, compression, uncompressed_size)?
        //              } else {
        //                  compressed_data.to_vec()
        //              };

        //              // Create a DataPage with the header and decompressed buffer
        //              // DataPage::new requires Descriptor as well
        //              let data_page = DataPage::new(
        //                  header,
        //                  decompressed,
        //                  column_descriptor.descriptor,
        //                  Some(num_rows),
        //              );

        //              // Deserialize the page to a column
        //              deserialize_page_to_column(&data_page)
        //          }
        //          _ => Err(ErrorCode::Internal("Unsupported page type".to_string())),
        //      }
    }

    /// Read column chunks and create a DataBlock
    pub fn read_column_chunks(
        &self,
        column_chunks: HashMap<ColumnId, &[u8]>,
        column_metas: &HashMap<ColumnId, ColumnChunkMetaData>,
        num_rows: usize,
        compression: Compression,
        decompressed_buffer: Arc<DecompressedBuffer>,
    ) -> Result<DataBlock> {
        let mut columns = Vec::with_capacity(column_chunks.len());
        let mut data_type_refs = Vec::with_capacity(column_chunks.len());
        let mut data_types = Vec::with_capacity(column_chunks.len());

        // Process each column chunk
        for (column_id, data) in column_chunks.iter() {
            // Find the metadata for this column
            if let Some(meta) = column_metas.get(column_id) {
                // Get the column field using the path from the descriptor
                // Note: path is path_in_schema in the parquet2 API
                let column_path = meta.descriptor().path_in_schema.join(".");

                // Find the index of the column in the schema
                let column_index =
                    self.find_column_index_by_path(&column_path)
                        .ok_or_else(|| {
                            ErrorCode::Internal(format!(
                                "Column not found in schema: {}",
                                column_path
                            ))
                        })?;

                // Read the column chunk
                let column = self.read_column_chunk(
                    column_index,
                    data,
                    meta,
                    num_rows,
                    &decompressed_buffer,
                )?;

                // Add column and data type to vectors
                columns.push(column);
                let dt = self.schema.field(column_index).data_type().clone();
                data_types.push(dt);
            }
        }

        // Create references to data types for create_data_block_from_columns
        data_type_refs = data_types.iter().collect();

        // Create a DataBlock from the columns
        create_data_block_from_columns(columns, data_type_refs)
    }
}

pub fn decompress_page(
    page_buf: &[u8],
    compression: Compression,
    decompressed_size: usize,
) -> Result<Arc<DecompressedBuffer>> {
    match compression {
        Compression::Uncompressed => {
            // No compression, just copy the buffer
            Ok(Arc::new(DecompressedBuffer::new(page_buf.to_vec())))
        }
        _ => {
            // Create a decompression context
            let decompressed = decompress_helper(page_buf, compression, decompressed_size)?;

            // Create a decompressed buffer
            Ok(Arc::new(DecompressedBuffer::new(decompressed)))
        }
    }
}

/// Helper function to decompress a buffer using Gzip compression
fn decompress_gzip(compressed: &[u8], decompressed_size: usize) -> Result<Vec<u8>> {
    // TODO: Add the flate2 dependency to Cargo.toml and enable this code
    // let mut decoder = flate2::read::GzDecoder::new(compressed);
    // let mut decompressed = vec![0; decompressed_size];
    // decoder.read_exact(&mut decompressed)?;
    // Ok(decompressed)

    // For now, since we don't have flate2 in dependencies, return an error
    Err(ErrorCode::Internal(
        "Gzip decompression support requires flate2 dependency".to_string(),
    ))
}

// Helper function for decompression
fn decompress_helper(
    compressed: &[u8],
    compression: Compression,
    decompressed_size: usize,
) -> Result<Vec<u8>> {
    // Convert to Arrow compression
    let mut output = Vec::with_capacity(decompressed_size);

    match compression {
        Compression::Uncompressed => {
            // Just copy the buffer
            output.extend_from_slice(compressed);
            Ok(output)
        }
        Compression::Snappy => {
            // Decompress using Snappy
            let mut decoder = snap::raw::Decoder::new();
            match decoder.decompress(compressed, &mut output) {
                Ok(_) => Ok(output),
                Err(e) => Err(ErrorCode::Internal(format!(
                    "Failed to decompress Snappy: {}",
                    e
                ))),
            }
        }
        Compression::Gzip => {
            // Decompress using Gzip
            decompress_gzip(compressed, decompressed_size)
        }
        // Add other compression methods as needed
        _ => Err(ErrorCode::Internal(format!(
            "Unsupported compression: {:?}",
            compression
        ))),
    }
}
