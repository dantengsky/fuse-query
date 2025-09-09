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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
use databend_common_expression::Column;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_storage::ColumnNode;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
use parquet2::compression::Compression as ParquetCompression;
use parquet2::metadata::Descriptor;
use parquet2::read::PageMetaData;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveType;

use crate::column::new_boolean_iter;
use crate::column::new_decimal128_iter;
use crate::column::new_decimal256_iter;
use crate::column::new_decimal64_iter;
use crate::column::new_float32_iter;
use crate::column::new_float64_iter;
use crate::column::new_int16_iter;
use crate::column::new_int32_iter;
use crate::column::new_int64_iter;
use crate::column::new_int8_iter;
use crate::column::new_uint16_iter;
use crate::column::new_uint32_iter;
use crate::column::new_uint64_iter;
use crate::column::new_uint8_iter;
use crate::column::ArrayColumnIterator;
use crate::column::GenericArrayColumnIterator;
use crate::column::BinaryIter;
use crate::column::BooleanMetadata;
use crate::column::DateIter;
use crate::column::IntegerMetadata;
use crate::column::StringIter;
use crate::column::GenericTupleColumnIterator;
use crate::reader::decompressor::Decompressor;
use crate::reader::page_reader::PageReader;

pub type ColumnIter<'a> = Box<dyn Iterator<Item = Result<Column>> + Send + Sync + 'a>;

pub fn data_chunk_to_col_iter<'a>(
    meta: &ColumnMeta,
    chunk: &'a [u8],
    rows: usize,
    column_descriptor: &Descriptor,
    field: TableField,
    compression: &Compression,
) -> Result<ColumnIter<'a>> {
    let pages = {
        let meta = meta.as_parquet().unwrap();
        let page_meta_data = PageMetaData {
            column_start: meta.offset,
            num_values: meta.num_values as i64,
            compression: to_parquet_compression(compression)?,
            descriptor: (*column_descriptor).clone(),
        };
        let pages = PageReader::new_with_page_meta(chunk, page_meta_data, usize::MAX);
        Decompressor::new(pages, vec![])
    };

    let typ = &column_descriptor.primitive_type;

    pages_to_column_iter(pages, typ, field, rows, None)
}

fn pages_to_column_iter<'a>(
    column: Decompressor<'a>,
    types: &PrimitiveType,
    field: TableField,
    num_rows: usize,
    chunk_size: Option<usize>,
) -> Result<ColumnIter<'a>> {
    let pages = column;
    let parquet_physical_type = &types.physical_type;

    let (inner_data_type, is_nullable) = match &field.data_type {
        TableDataType::Nullable(inner) => (inner.as_ref(), true),
        other => (other, false),
    };

    match (parquet_physical_type, inner_data_type) {
        (PhysicalType::Boolean, TableDataType::Boolean) => {
            Ok(Box::new(new_boolean_iter(pages, num_rows, is_nullable, chunk_size)))
        }

        // ===== Signed Integer Types =====
        (PhysicalType::Int32, TableDataType::Number(NumberDataType::Int8)) => {
            Ok(Box::new(new_int8_iter(pages, num_rows, is_nullable, chunk_size)))
        }
        (PhysicalType::Int32, TableDataType::Number(NumberDataType::Int16)) => {
            Ok(Box::new(new_int16_iter(pages, num_rows, is_nullable, chunk_size)))
        }
        (PhysicalType::Int32, TableDataType::Number(NumberDataType::Int32)) => {
            Ok(Box::new(new_int32_iter(pages, num_rows, is_nullable, chunk_size)))
        }
        (PhysicalType::Int64, TableDataType::Number(NumberDataType::Int64)) => {
            Ok(Box::new(new_int64_iter(pages, num_rows, is_nullable, chunk_size)))
        }

        // ===== Unsigned Integer Types =====
        (PhysicalType::Int32, TableDataType::Number(NumberDataType::UInt8)) => {
            Ok(Box::new(new_uint8_iter(pages, num_rows, is_nullable, chunk_size)))
        }
        (PhysicalType::Int32, TableDataType::Number(NumberDataType::UInt16)) => {
            Ok(Box::new(new_uint16_iter(pages, num_rows, is_nullable, chunk_size)))
        }
        (PhysicalType::Int32, TableDataType::Number(NumberDataType::UInt32)) => {
            Ok(Box::new(new_uint32_iter(pages, num_rows, is_nullable, chunk_size)))
        }
        (PhysicalType::Int64, TableDataType::Number(NumberDataType::UInt64)) => {
            Ok(Box::new(new_uint64_iter(pages, num_rows, is_nullable, chunk_size)))
        }

        // ===== Float Types =====
        (PhysicalType::Float, TableDataType::Number(NumberDataType::Float32)) => {
            Ok(Box::new(new_float32_iter(pages, num_rows, is_nullable, chunk_size)))
        }
        (PhysicalType::Double, TableDataType::Number(NumberDataType::Float64)) => {
            Ok(Box::new(new_float64_iter(pages, num_rows, is_nullable, chunk_size)))
        }

        // ===== String and Binary Types =====
        (PhysicalType::ByteArray, TableDataType::String) => {
            Ok(Box::new(StringIter::new(pages, num_rows, chunk_size)))
        }

        // ===== Binary Types =====
        (PhysicalType::ByteArray, TableDataType::Binary) => {
            Ok(Box::new(BinaryIter::new(pages, num_rows, is_nullable, inner_data_type.clone(), chunk_size)))
        }
        (PhysicalType::ByteArray, TableDataType::Variant) => {
            Ok(Box::new(BinaryIter::new(pages, num_rows, is_nullable, inner_data_type.clone(), chunk_size)))
        }
        (PhysicalType::ByteArray, TableDataType::Bitmap) => {
            Ok(Box::new(BinaryIter::new(pages, num_rows, is_nullable, inner_data_type.clone(), chunk_size)))
        }
        (PhysicalType::ByteArray, TableDataType::Geometry) => {
            Ok(Box::new(BinaryIter::new(pages, num_rows, is_nullable, inner_data_type.clone(), chunk_size)))
        }
        (PhysicalType::ByteArray, TableDataType::Geography) => {
            Ok(Box::new(BinaryIter::new(pages, num_rows, is_nullable, inner_data_type.clone(), chunk_size)))
        }

        // ===== Decimal Types =====
        (PhysicalType::Int32, TableDataType::Decimal(DecimalDataType::Decimal64(_))) => {
            unimplemented!("coming soon")
        }
        (PhysicalType::Int64, TableDataType::Decimal(DecimalDataType::Decimal64(decimal_size))) => {
            Ok(Box::new(new_decimal64_iter(
                pages,
                num_rows,
                decimal_size.precision(),
                decimal_size.scale(),
                is_nullable,
                chunk_size,
            )))
        }
        // TODO: arrow  55.1.0 does not support Decimal64 yet, so we use Decimal128, but the storage format is Int64
        (PhysicalType::Int64, TableDataType::Decimal(DecimalDataType::Decimal128(decimal_size))) => {
            Ok(Box::new(new_decimal64_iter(
                pages,
                num_rows,
                decimal_size.precision(),
                decimal_size.scale(),
                is_nullable,
                chunk_size,
            )))
        }
        (PhysicalType::FixedLenByteArray(_), TableDataType::Decimal(DecimalDataType::Decimal128(decimal_size))) => {
            Ok(Box::new(new_decimal128_iter(
                pages,
                num_rows,
                decimal_size.precision(),
                decimal_size.scale(),
                is_nullable,
                chunk_size,
            )))
        }
        (PhysicalType::FixedLenByteArray(_), TableDataType::Decimal(DecimalDataType::Decimal256(decimal_size))) => {
            Ok(Box::new(new_decimal256_iter(
                pages,
                num_rows,
                decimal_size.precision(),
                decimal_size.scale(),
                is_nullable,
                chunk_size,
            )))
        }

        // ===== Date and Timestamp Types =====
        (PhysicalType::Int32, TableDataType::Date) => {
            Ok(Box::new(DateIter::new(
                pages,
                num_rows,
                is_nullable,
                IntegerMetadata,
                chunk_size,
            )))
        }
        (PhysicalType::Int64, TableDataType::Timestamp) => {
            // Timestamp is stored as Int64 (microseconds since epoch)
            Ok(Box::new(new_int64_iter(pages, num_rows, is_nullable, chunk_size)))
        }

        // ===== Array Types =====
        // Arrays are handled using the new table-driven dispatch system
        (_, TableDataType::Array(inner_type)) => {
            use crate::column::{TypeId, create_array_iterator};
            
            let element_type_id = TypeId::from_table_data_type(inner_type);
            create_array_iterator(
                &element_type_id,
                pages,
                num_rows,
                is_nullable,
                chunk_size,
                2, // Default definition level for arrays
                1, // Default repetition level for arrays
            )
        }

        // ===== Tuple Types =====
        // Tuples are complex nested structures requiring coordination of multiple fields
        (_, TableDataType::Tuple { fields_name, fields_type }) => {
            // Basic tuple support for simple field combinations
            // This requires careful coordination but we can handle basic cases
            
            // Check if all fields are supported primitive types
            let mut supported_fields = true;
            for field_type in fields_type {
                match field_type {
                    TableDataType::Number(_) | TableDataType::Boolean | TableDataType::String => {
                        // These are supported
                    }
                    _ => {
                        supported_fields = false;
                        break;
                    }
                }
            }
            
            if !supported_fields {
                return Err(ErrorCode::StorageOther(
                    "Tuple with complex field types (Array, Tuple, etc.) not yet supported".to_string()
                ));
            }
            
            // For basic tuples, we need to create multiple decompressors from the same page data
            // This is a simplified implementation - real tuple support needs parquet schema analysis
            // to understand how fields are laid out in the parquet structure
            
            // For now, return a more helpful error that indicates basic structure is ready
            // but field parsing needs schema information not available in this context
            Err(ErrorCode::StorageOther(
                "Tuple support requires parquet schema field mapping - \
                 basic infrastructure ready but needs schema analysis integration".to_string()
            ))
        }

        // ===== Unsupported Combinations =====
        (physical_type, table_data_type) => Err(ErrorCode::StorageOther(format!(
            "Unsupported combination: parquet_physical_type={:?}, field_data_type={:?}, nullable={}",
            physical_type, table_data_type, is_nullable
        ))),
    }
}

fn to_parquet_compression(meta_compression: &Compression) -> Result<ParquetCompression> {
    match meta_compression {
        Compression::Lz4 => Err(ErrorCode::StorageOther(
            "Legacy compression algorithm [Lz4] is no longer supported.",
        )),
        Compression::Lz4Raw => Ok(ParquetCompression::Lz4Raw),
        Compression::Snappy => Ok(ParquetCompression::Snappy),
        Compression::Zstd => Ok(ParquetCompression::Zstd),
        Compression::Gzip => Ok(ParquetCompression::Gzip),
        Compression::None => Ok(ParquetCompression::Uncompressed),
    }
}

/// Schema-driven nested column iterator creation using ColumnNode
/// 
/// This function leverages the complete nested schema information available in ColumnNode
/// to recursively handle complex nested types like Array(Array(T)), Array(Tuple(...)), etc.
/// 
/// # Arguments
/// * `column_node` - Complete schema information with nested structure
/// * `meta` - Column metadata for page information
/// * `chunk` - Raw parquet data chunk
/// * `rows` - Number of rows to read
/// * `compression` - Compression type
pub fn create_nested_column_iter<'a>(
    column_node: &ColumnNode,
    meta: &ColumnMeta,
    chunk: &'a [u8],
    rows: usize,
    compression: &Compression,
) -> Result<ColumnIter<'a>> {
    // Create page decompressor from raw data
    let pages = create_page_decompressor(meta, chunk, compression)?;
    
    // Recursively resolve the nested type structure
    resolve_nested_column_type(column_node, pages, rows)
}

/// Recursively resolve nested column types based on ColumnNode structure
fn resolve_nested_column_type<'a>(
    column_node: &ColumnNode,
    pages: Decompressor<'a>,
    rows: usize,
) -> Result<ColumnIter<'a>> {
    match (&column_node.children, column_node.is_nested) {
        // Case 1: Leaf node (primitive type) - use existing logic
        (None, false) => {
            resolve_primitive_column_type(&column_node.table_field, pages, rows)
        }
        
        // Case 2: Array type - single child element type
        (Some(children), true) if children.len() == 1 => {
            let element_child = &children[0];
            
            // For arrays, we need to handle the element type recursively
            if element_child.is_nested {
                // Handle nested array elements: Array(Array(T)), Array(Tuple(...))
                // Create recursive nested column iterator for the element type
                let inner_iter = resolve_nested_column_type(element_child, pages, rows)?;
                
                // Wrap the inner iterator in a GenericArrayColumnIterator
                // This creates an array where each element is the result from inner_iter
                let array_iter = GenericArrayColumnIterator::new(
                    inner_iter,
                    true, // Array elements can be null
                    1,    // Simple definition level for now
                    1,    // Simple repetition level for now
                    rows
                )?;
                
                Ok(Box::new(array_iter))
            } else {
                // Handle primitive array elements
                resolve_primitive_array_type(&element_child.table_field, pages, rows, true)
            }
        }
        
        // Case 3: Tuple type - multiple children fields
        (Some(children), true) if children.len() > 1 => {
            // For tuples, each field needs its own page data stream
            // This requires parquet schema analysis to split the page data correctly
            // Currently not implemented as it requires understanding the parquet file structure
            Err(ErrorCode::StorageOther(format!(
                "Tuple type with {} fields requires parquet page data splitting - \
                 GenericTupleColumnIterator ready but needs page distribution logic",
                children.len()
            )))
        }
        
        // Case 4: Invalid combinations
        _ => Err(ErrorCode::Internal(format!(
            "Invalid ColumnNode structure: is_nested={}, children_count={}",
            column_node.is_nested,
            column_node.children.as_ref().map_or(0, |c| c.len())
        ))),
    }
}

/// Resolve primitive column types (non-nested)
fn resolve_primitive_column_type<'a>(
    table_field: &TableField,
    pages: Decompressor<'a>,
    rows: usize,
) -> Result<ColumnIter<'a>> {
    // Extract type information
    let (inner_data_type, is_nullable) = match &table_field.data_type {
        TableDataType::Nullable(inner) => (inner.as_ref(), true),
        other => (other, false),
    };
    
    // Use existing type matching logic for primitives
    match inner_data_type {
        TableDataType::Boolean => {
            Ok(Box::new(new_boolean_iter(pages, rows, is_nullable, None)))
        }
        
        TableDataType::Number(NumberDataType::Int8) => {
            Ok(Box::new(new_int8_iter(pages, rows, is_nullable, None)))
        }
        TableDataType::Number(NumberDataType::Int16) => {
            Ok(Box::new(new_int16_iter(pages, rows, is_nullable, None)))
        }
        TableDataType::Number(NumberDataType::Int32) => {
            Ok(Box::new(new_int32_iter(pages, rows, is_nullable, None)))
        }
        TableDataType::Number(NumberDataType::Int64) => {
            Ok(Box::new(new_int64_iter(pages, rows, is_nullable, None)))
        }
        
        TableDataType::Number(NumberDataType::UInt8) => {
            Ok(Box::new(new_uint8_iter(pages, rows, is_nullable, None)))
        }
        TableDataType::Number(NumberDataType::UInt16) => {
            Ok(Box::new(new_uint16_iter(pages, rows, is_nullable, None)))
        }
        TableDataType::Number(NumberDataType::UInt32) => {
            Ok(Box::new(new_uint32_iter(pages, rows, is_nullable, None)))
        }
        TableDataType::Number(NumberDataType::UInt64) => {
            Ok(Box::new(new_uint64_iter(pages, rows, is_nullable, None)))
        }
        
        TableDataType::Number(NumberDataType::Float32) => {
            Ok(Box::new(new_float32_iter(pages, rows, is_nullable, None)))
        }
        TableDataType::Number(NumberDataType::Float64) => {
            Ok(Box::new(new_float64_iter(pages, rows, is_nullable, None)))
        }
        
        TableDataType::String => {
            Ok(Box::new(StringIter::new(pages, rows, None)))
        }
        
        TableDataType::Binary => {
            Ok(Box::new(BinaryIter::new(pages, rows, is_nullable, inner_data_type.clone(), None)))
        }
        
        _ => Err(ErrorCode::StorageOther(format!(
            "Primitive type {:?} not supported in schema-driven resolver",
            inner_data_type
        ))),
    }
}

/// Resolve primitive array types using existing ArrayColumnIterator
fn resolve_primitive_array_type<'a>(
    element_field: &TableField,
    pages: Decompressor<'a>,
    rows: usize,
    is_array_nullable: bool,
) -> Result<ColumnIter<'a>> {
    let (element_type, _element_nullable) = match &element_field.data_type {
        TableDataType::Nullable(inner) => (inner.as_ref(), true),
        other => (other, false),
    };
    
    // Use existing ArrayColumnIterator for supported primitive types
    match element_type {
        TableDataType::Number(NumberDataType::Int8) => {
            Ok(Box::new(ArrayColumnIterator::<i8>::new(
                pages, rows, is_array_nullable, IntegerMetadata, None, 2, 1
            )))
        }
        TableDataType::Number(NumberDataType::Int16) => {
            Ok(Box::new(ArrayColumnIterator::<i16>::new(
                pages, rows, is_array_nullable, IntegerMetadata, None, 2, 1
            )))
        }
        TableDataType::Number(NumberDataType::Int32) => {
            Ok(Box::new(ArrayColumnIterator::<i32>::new(
                pages, rows, is_array_nullable, IntegerMetadata, None, 2, 1
            )))
        }
        TableDataType::Number(NumberDataType::Int64) => {
            Ok(Box::new(ArrayColumnIterator::<i64>::new(
                pages, rows, is_array_nullable, IntegerMetadata, None, 2, 1
            )))
        }
        
        TableDataType::Number(NumberDataType::UInt8) => {
            Ok(Box::new(ArrayColumnIterator::<u8>::new(
                pages, rows, is_array_nullable, IntegerMetadata, None, 2, 1
            )))
        }
        TableDataType::Number(NumberDataType::UInt16) => {
            Ok(Box::new(ArrayColumnIterator::<u16>::new(
                pages, rows, is_array_nullable, IntegerMetadata, None, 2, 1
            )))
        }
        TableDataType::Number(NumberDataType::UInt32) => {
            Ok(Box::new(ArrayColumnIterator::<u32>::new(
                pages, rows, is_array_nullable, IntegerMetadata, None, 2, 1
            )))
        }
        TableDataType::Number(NumberDataType::UInt64) => {
            Ok(Box::new(ArrayColumnIterator::<u64>::new(
                pages, rows, is_array_nullable, IntegerMetadata, None, 2, 1
            )))
        }
        
        TableDataType::Number(NumberDataType::Float32) => {
            Ok(Box::new(ArrayColumnIterator::<F32>::new(
                pages, rows, is_array_nullable, IntegerMetadata, None, 2, 1
            )))
        }
        TableDataType::Number(NumberDataType::Float64) => {
            Ok(Box::new(ArrayColumnIterator::<F64>::new(
                pages, rows, is_array_nullable, IntegerMetadata, None, 2, 1
            )))
        }
        
        TableDataType::Boolean => {
            Ok(Box::new(ArrayColumnIterator::<bool>::new(
                pages, rows, is_array_nullable, BooleanMetadata, None, 2, 1
            )))
        }
        
        _ => Err(ErrorCode::StorageOther(format!(
            "Array element type {:?} not yet supported in schema-driven resolver",
            element_type
        ))),
    }
}

/// Create page decompressor from raw data (helper function)
fn create_page_decompressor<'a>(
    meta: &ColumnMeta,
    chunk: &'a [u8],
    compression: &Compression,
) -> Result<Decompressor<'a>> {
    let meta = meta.as_parquet().unwrap();
    
    // Create a minimal descriptor for page reading
    // Note: This is a temporary approach - we'll need proper descriptor extraction
    let descriptor = Descriptor {
        primitive_type: PrimitiveType::from_physical("temp".to_string(), PhysicalType::Int32),
        max_def_level: 2, // Will be overridden based on actual schema
        max_rep_level: 1, // Will be overridden based on actual schema
    };
    
    let page_meta_data = PageMetaData {
        column_start: meta.offset,
        num_values: meta.num_values as i64,
        compression: to_parquet_compression(compression)?,
        descriptor,
    };
    
    let pages = PageReader::new_with_page_meta(chunk, page_meta_data, usize::MAX);
    Ok(Decompressor::new(pages, vec![]))
}
