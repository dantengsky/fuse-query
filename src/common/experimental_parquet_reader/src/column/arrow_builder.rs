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

//! Arrow-inspired builder for creating column array readers
//! 
//! This implements the elegant recursive construction pattern from Apache Arrow,
//! replacing our complex factory/dispatch systems with simple, recursive builders.
//! 
//! Key design principles:
//! - Type-driven recursive construction (not runtime dispatch)  
//! - Composition over type matching
//! - Level-aware reader creation

use databend_common_exception::{ErrorCode, Result};
use databend_common_expression::{TableDataType, TableField};
use crate::reader::decompressor::Decompressor;
use super::ColumnArrayReader;
use super::arrow_reader_trait::LevelInfo;
use super::ArrowListArrayReader;
use super::ArrowStructArrayReader;

/// Build a column array reader from field definition
/// 
/// This is the main entry point that recursively constructs the appropriate
/// reader hierarchy based on the field structure, following Arrow's pattern.
pub fn build_column_reader<'a>(
    field: &TableField,
    pages: Decompressor<'a>,
    rows: usize,
    chunk_size: Option<usize>,
) -> Result<Box<dyn ColumnArrayReader + 'a>> {
    let level_info = if field.data_type.is_nullable() {
        LevelInfo::optional()
    } else {
        LevelInfo::required()
    };
    
    build_reader_recursive(&field.data_type, level_info, pages, rows, chunk_size)
}

/// Recursive builder function - the heart of Arrow's design pattern
/// 
/// This function recursively constructs readers by analyzing the data type structure
/// and creating the appropriate reader hierarchy through composition.
fn build_reader_recursive<'a>(
    data_type: &TableDataType,
    level_info: LevelInfo,
    pages: Decompressor<'a>,
    rows: usize,
    chunk_size: Option<usize>,
) -> Result<Box<dyn ColumnArrayReader + 'a>> {
    match data_type {
        // Handle nullable wrapper
        TableDataType::Nullable(inner) => {
            let nullable_level = LevelInfo {
                def_level: level_info.def_level + 1,
                rep_level: level_info.rep_level,
                nullable: true,
            };
            build_reader_recursive(inner, nullable_level, pages, rows, chunk_size)
        }
        
        // Handle arrays - this is where Arrow's composition pattern shines
        TableDataType::Array(element_type) => {
            let element_level = LevelInfo::list_element(level_info, element_type.is_nullable());
            let element_reader = build_reader_recursive(element_type, element_level, pages, rows, chunk_size)?;
            
            Ok(Box::new(ArrowListArrayReader::new(
                element_reader,
                level_info,
                chunk_size,
            )))
        }
        
        // Handle tuples/structs  
        TableDataType::Tuple { fields_type, .. } => {
            let mut field_readers = Vec::with_capacity(fields_type.len());
            
            for field_type in fields_type {
                let field_level = LevelInfo::struct_field(level_info, field_type.is_nullable());
                let field_reader = build_reader_recursive(field_type, field_level, pages.clone(), rows, chunk_size)?;
                field_readers.push(field_reader);
            }
            
            Ok(Box::new(ArrowStructArrayReader::new(
                field_readers,
                level_info,
                chunk_size,
            )))
        }
        
        // Handle primitive types - the leaves of the tree
        _ => build_primitive_reader(data_type, level_info, pages, rows, chunk_size),
    }
}

/// Build primitive reader - this replaces all our complex type dispatch
/// 
/// Instead of massive match statements or factory registries, we use Arrow's
/// simple approach: delegate to type-specific constructors.
fn build_primitive_reader<'a>(
    data_type: &TableDataType,
    level_info: LevelInfo,
    pages: Decompressor<'a>,
    rows: usize,
    chunk_size: Option<usize>,
) -> Result<Box<dyn ColumnArrayReader + 'a>> {
    use databend_common_expression::types::NumberDataType;
    use super::{ArrowPrimitiveArrayReader, ArrowStringArrayReader, ArrowBinaryArrayReader};
    
    match data_type {
        TableDataType::Boolean => {
            Ok(Box::new(ArrowPrimitiveArrayReader::<bool>::new(
                pages, rows, level_info, chunk_size
            )))
        }
        TableDataType::Number(number_type) => match number_type {
            NumberDataType::Int8 => Ok(Box::new(ArrowPrimitiveArrayReader::<i8>::new(
                pages, rows, level_info, chunk_size
            ))),
            NumberDataType::Int16 => Ok(Box::new(ArrowPrimitiveArrayReader::<i16>::new(
                pages, rows, level_info, chunk_size
            ))),
            NumberDataType::Int32 => Ok(Box::new(ArrowPrimitiveArrayReader::<i32>::new(
                pages, rows, level_info, chunk_size
            ))),
            NumberDataType::Int64 => Ok(Box::new(ArrowPrimitiveArrayReader::<i64>::new(
                pages, rows, level_info, chunk_size
            ))),
            NumberDataType::UInt8 => Ok(Box::new(ArrowPrimitiveArrayReader::<u8>::new(
                pages, rows, level_info, chunk_size
            ))),
            NumberDataType::UInt16 => Ok(Box::new(ArrowPrimitiveArrayReader::<u16>::new(
                pages, rows, level_info, chunk_size
            ))),
            NumberDataType::UInt32 => Ok(Box::new(ArrowPrimitiveArrayReader::<u32>::new(
                pages, rows, level_info, chunk_size
            ))),
            NumberDataType::UInt64 => Ok(Box::new(ArrowPrimitiveArrayReader::<u64>::new(
                pages, rows, level_info, chunk_size
            ))),
            NumberDataType::Float32 => Ok(Box::new(ArrowPrimitiveArrayReader::<f32>::new(
                pages, rows, level_info, chunk_size
            ))),
            NumberDataType::Float64 => Ok(Box::new(ArrowPrimitiveArrayReader::<f64>::new(
                pages, rows, level_info, chunk_size
            ))),
        },
        TableDataType::String => {
            Ok(Box::new(ArrowStringArrayReader::new(
                pages, rows, level_info, chunk_size
            )))
        }
        TableDataType::Binary => {
            Ok(Box::new(ArrowBinaryArrayReader::new(
                pages, rows, level_info, chunk_size
            )))
        }
        _ => Err(ErrorCode::StorageOther(format!(
            "Unsupported primitive type: {:?}", data_type
        ))),
    }
}

// Forward declarations are replaced by imports from separate files