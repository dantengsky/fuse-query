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

//! Native-style macro-based dispatch system
//! 
//! This replaces the complex factory/registry system with simple, 
//! compile-time macro expansion following the excellent Native format pattern.

use databend_common_exception::Result;
use databend_common_expression::{Column, TableDataType, TableField};
use crate::reader::decompressor::Decompressor;
use super::TypeId;

/// Simplified iterator type - following Native pattern
pub type ParquetColumnIter<'a> = Box<dyn Iterator<Item = Result<Column>> + Send + Sync + 'a>;

/// Create column iterator using macro dispatch - main public interface
/// 
/// This completely replaces all the factory complexity with simple macros
pub fn create_column_iterator_macro<'a>(
    field: &TableField,
    pages: Decompressor<'a>,
    rows: usize,
    chunk_size: Option<usize>,
) -> Result<ParquetColumnIter<'a>> {
    // Extract nullable information
    let (inner_data_type, is_nullable) = match &field.data_type {
        TableDataType::Nullable(inner) => (inner.as_ref(), true),
        other => (other, false),
    };
    
    // Convert to TypeId for dispatch
    let type_id = TypeId::from_table_data_type(inner_data_type);
    
    match inner_data_type {
        // Handle array types using macro
        TableDataType::Array(element_type) => {
            let element_type_id = TypeId::from_table_data_type(element_type);
            create_array_column_iter!(
                &element_type_id,
                pages,
                rows, 
                is_nullable,
                chunk_size,
                2, // Default def level for arrays
                1  // Default rep level for arrays
            )
        }
        
        // Handle primitive types using macro
        _ => {
            create_primitive_column_iter!(
                &type_id,
                pages,
                rows,
                is_nullable, 
                chunk_size
            )
        }
    }
}

/// Create primitive iterator using macro dispatch
pub fn create_primitive_iterator_macro<'a>(
    type_id: &TypeId,
    pages: Decompressor<'a>,
    rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> Result<ParquetColumnIter<'a>> {
    create_primitive_column_iter!(type_id, pages, rows, is_nullable, chunk_size)
}

/// Create array iterator using macro dispatch  
pub fn create_array_iterator_macro<'a>(
    element_type_id: &TypeId,
    pages: Decompressor<'a>,
    rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
    max_def_level: u16,
    max_rep_level: u16,
) -> Result<ParquetColumnIter<'a>> {
    create_array_column_iter!(
        element_type_id,
        pages,
        rows,
        is_nullable,
        chunk_size,
        max_def_level,
        max_rep_level
    )
}