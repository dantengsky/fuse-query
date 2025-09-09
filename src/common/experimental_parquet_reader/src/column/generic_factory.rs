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

//! Generic factory system for all column iterator types
//! 
//! This provides a unified interface for creating both primitive and 
//! array column iterators with full type safety and extensibility.

use std::collections::HashMap;
use databend_common_exception::{ErrorCode, Result};
use databend_common_expression::{Column, TableDataType, TableField};
use crate::reader::decompressor::Decompressor;
use super::{TypeId, BooleanMetadata, IntegerMetadata, ArrayColumnIterator};
use super::{new_boolean_iter, new_int8_iter, new_int16_iter, new_int32_iter, new_int64_iter};
use super::{new_uint8_iter, new_uint16_iter, new_uint32_iter, new_uint64_iter};

pub type ColumnIter<'a> = Box<dyn Iterator<Item = Result<Column>> + Send + Sync + 'a>;

/// Function signature for creating primitive column iterators
type PrimitiveIteratorFactory = fn(
    pages: Decompressor<'_>,
    rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
) -> Result<ColumnIter<'_>>;

/// Function signature for creating array column iterators  
type ArrayIteratorFactory = fn(
    pages: Decompressor<'_>,
    rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
    max_def_level: u16,
    max_rep_level: u16,
) -> Result<ColumnIter<'_>>;

/// Generic column iterator factory that handles all column types
pub struct GenericColumnIteratorFactory {
    primitive_factories: HashMap<TypeId, PrimitiveIteratorFactory>,
    array_factories: HashMap<TypeId, ArrayIteratorFactory>,
}

impl GenericColumnIteratorFactory {
    /// Create a new generic factory with all supported types registered
    pub fn new() -> Self {
        let mut factory = Self {
            primitive_factories: HashMap::new(),
            array_factories: HashMap::new(),
        };
        
        factory.register_all_types();
        factory
    }
    
    /// Register all supported primitive and array types
    fn register_all_types(&mut self) {
        // Register primitive types
        self.primitive_factories.insert(TypeId::Boolean, Self::create_boolean_primitive);
        self.primitive_factories.insert(TypeId::Int8, Self::create_i8_primitive);
        self.primitive_factories.insert(TypeId::Int16, Self::create_i16_primitive);
        self.primitive_factories.insert(TypeId::Int32, Self::create_i32_primitive);
        self.primitive_factories.insert(TypeId::Int64, Self::create_i64_primitive);
        self.primitive_factories.insert(TypeId::UInt8, Self::create_u8_primitive);
        self.primitive_factories.insert(TypeId::UInt16, Self::create_u16_primitive);
        self.primitive_factories.insert(TypeId::UInt32, Self::create_u32_primitive);
        self.primitive_factories.insert(TypeId::UInt64, Self::create_u64_primitive);
        
        // Register array element types
        self.array_factories.insert(TypeId::Boolean, Self::create_bool_array);
        self.array_factories.insert(TypeId::Int8, Self::create_i8_array);
        self.array_factories.insert(TypeId::Int16, Self::create_i16_array);
        self.array_factories.insert(TypeId::Int32, Self::create_i32_array);
        self.array_factories.insert(TypeId::Int64, Self::create_i64_array);
        self.array_factories.insert(TypeId::UInt8, Self::create_u8_array);
        self.array_factories.insert(TypeId::UInt16, Self::create_u16_array);
        self.array_factories.insert(TypeId::UInt32, Self::create_u32_array);
        self.array_factories.insert(TypeId::UInt64, Self::create_u64_array);
    }
    
    /// Create column iterator from table field - main public interface
    pub fn create_column_iterator<'a>(
        &self,
        field: &TableField,
        pages: Decompressor<'a>,
        rows: usize,
        chunk_size: Option<usize>,
    ) -> Result<ColumnIter<'a>> {
        // Extract nullable information
        let (inner_data_type, is_nullable) = match &field.data_type {
            TableDataType::Nullable(inner) => (inner.as_ref(), true),
            other => (other, false),
        };
        
        // Convert to TypeId for dispatch
        let type_id = TypeId::from_table_data_type(inner_data_type);
        
        match inner_data_type {
            // Handle array types
            TableDataType::Array(element_type) => {
                let element_type_id = TypeId::from_table_data_type(element_type);
                self.create_array_iterator(
                    &element_type_id,
                    pages,
                    rows,
                    is_nullable,
                    chunk_size,
                    2, // Default def level for arrays
                    1, // Default rep level for arrays
                )
            }
            
            // Handle primitive types
            _ => {
                self.create_primitive_iterator(
                    &type_id,
                    pages,
                    rows,
                    is_nullable,
                    chunk_size,
                )
            }
        }
    }
    
    /// Create primitive column iterator
    pub fn create_primitive_iterator<'a>(
        &self,
        type_id: &TypeId,
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Result<ColumnIter<'a>> {
        if let Some(&factory) = self.primitive_factories.get(type_id) {
            factory(pages, rows, is_nullable, chunk_size)
        } else {
            Err(ErrorCode::StorageOther(format!(
                "Primitive type {:?} not supported in experimental reader",
                type_id
            )))
        }
    }
    
    /// Create array column iterator
    pub fn create_array_iterator<'a>(
        &self,
        element_type_id: &TypeId,
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
        max_def_level: u16,
        max_rep_level: u16,
    ) -> Result<ColumnIter<'a>> {
        if let Some(&factory) = self.array_factories.get(element_type_id) {
            factory(pages, rows, is_nullable, chunk_size, max_def_level, max_rep_level)
        } else {
            match element_type_id {
                TypeId::String | TypeId::Binary => {
                    Err(ErrorCode::StorageOther(format!(
                        "Array({:?}) not yet supported - requires specialized string/binary array iterator",
                        element_type_id
                    )))
                }
                TypeId::Array(_) | TypeId::Tuple(_) => {
                    Err(ErrorCode::StorageOther(format!(
                        "Nested array element type {:?} not yet supported - requires complex level processing",
                        element_type_id
                    )))
                }
                _ => {
                    Err(ErrorCode::StorageOther(format!(
                        "Array element type {:?} not supported in experimental reader",
                        element_type_id
                    )))
                }
            }
        }
    }
    
    /// Check if a primitive type is supported
    pub fn supports_primitive_type(&self, type_id: &TypeId) -> bool {
        self.primitive_factories.contains_key(type_id)
    }
    
    /// Check if an array element type is supported
    pub fn supports_array_type(&self, element_type_id: &TypeId) -> bool {
        self.array_factories.contains_key(element_type_id)
    }
    
    // Factory functions for primitive types
    fn create_boolean_primitive<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(new_boolean_iter(pages, rows, is_nullable, chunk_size)))
    }
    
    fn create_i8_primitive<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(new_int8_iter(pages, rows, is_nullable, chunk_size)))
    }
    
    fn create_i16_primitive<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(new_int16_iter(pages, rows, is_nullable, chunk_size)))
    }
    
    fn create_i32_primitive<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(new_int32_iter(pages, rows, is_nullable, chunk_size)))
    }
    
    fn create_i64_primitive<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(new_int64_iter(pages, rows, is_nullable, chunk_size)))
    }
    
    fn create_u8_primitive<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(new_uint8_iter(pages, rows, is_nullable, chunk_size)))
    }
    
    fn create_u16_primitive<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(new_uint16_iter(pages, rows, is_nullable, chunk_size)))
    }
    
    fn create_u32_primitive<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(new_uint32_iter(pages, rows, is_nullable, chunk_size)))
    }
    
    fn create_u64_primitive<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(new_uint64_iter(pages, rows, is_nullable, chunk_size)))
    }
    
    // Factory functions for array element types
    fn create_bool_array<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
        max_def_level: u16,
        max_rep_level: u16,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(ArrayColumnIterator::<bool>::new(
            pages, rows, is_nullable, BooleanMetadata,
            chunk_size, max_def_level, max_rep_level
        )))
    }
    
    fn create_i8_array<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
        max_def_level: u16,
        max_rep_level: u16,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(ArrayColumnIterator::<i8>::new(
            pages, rows, is_nullable, IntegerMetadata,
            chunk_size, max_def_level, max_rep_level
        )))
    }
    
    fn create_i16_array<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
        max_def_level: u16,
        max_rep_level: u16,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(ArrayColumnIterator::<i16>::new(
            pages, rows, is_nullable, IntegerMetadata,
            chunk_size, max_def_level, max_rep_level
        )))
    }
    
    fn create_i32_array<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
        max_def_level: u16,
        max_rep_level: u16,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(ArrayColumnIterator::<i32>::new(
            pages, rows, is_nullable, IntegerMetadata,
            chunk_size, max_def_level, max_rep_level
        )))
    }
    
    fn create_i64_array<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
        max_def_level: u16,
        max_rep_level: u16,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(ArrayColumnIterator::<i64>::new(
            pages, rows, is_nullable, IntegerMetadata,
            chunk_size, max_def_level, max_rep_level
        )))
    }
    
    fn create_u8_array<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
        max_def_level: u16,
        max_rep_level: u16,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(ArrayColumnIterator::<u8>::new(
            pages, rows, is_nullable, IntegerMetadata,
            chunk_size, max_def_level, max_rep_level
        )))
    }
    
    fn create_u16_array<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
        max_def_level: u16,
        max_rep_level: u16,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(ArrayColumnIterator::<u16>::new(
            pages, rows, is_nullable, IntegerMetadata,
            chunk_size, max_def_level, max_rep_level
        )))
    }
    
    fn create_u32_array<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
        max_def_level: u16,
        max_rep_level: u16,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(ArrayColumnIterator::<u32>::new(
            pages, rows, is_nullable, IntegerMetadata,
            chunk_size, max_def_level, max_rep_level
        )))
    }
    
    fn create_u64_array<'a>(
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
        max_def_level: u16,
        max_rep_level: u16,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(ArrayColumnIterator::<u64>::new(
            pages, rows, is_nullable, IntegerMetadata,
            chunk_size, max_def_level, max_rep_level
        )))
    }
}

/// Global factory instance for convenience
static mut GLOBAL_FACTORY: Option<GenericColumnIteratorFactory> = None;
static FACTORY_INIT: std::sync::Once = std::sync::Once::new();

/// Get the global factory instance
pub fn get_global_factory() -> &'static GenericColumnIteratorFactory {
    unsafe {
        FACTORY_INIT.call_once(|| {
            GLOBAL_FACTORY = Some(GenericColumnIteratorFactory::new());
        });
        GLOBAL_FACTORY.as_ref().unwrap()
    }
}

/// Convenience function using the global factory
pub fn create_column_iterator_with_factory<'a>(
    field: &TableField,
    pages: Decompressor<'a>,
    rows: usize,
    chunk_size: Option<usize>,
) -> Result<ColumnIter<'a>> {
    get_global_factory().create_column_iterator(field, pages, rows, chunk_size)
}