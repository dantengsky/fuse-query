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

//! Dynamic type registry for column iterator creation
//! 
//! This eliminates the remaining hardcoded match statements with a 
//! fully dynamic dispatch system using function pointers.

use std::collections::HashMap;
use databend_common_exception::{ErrorCode, Result};
use databend_common_expression::{Column, TableDataType};
use crate::reader::decompressor::Decompressor;
use super::{TypeId, BooleanMetadata, IntegerMetadata, ArrayColumnIterator};

pub type ColumnIter<'a> = Box<dyn Iterator<Item = Result<Column>> + Send + Sync + 'a>;

/// Function signature for creating array iterators
type ArrayIteratorFactory = fn(
    pages: Decompressor<'_>,
    rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
    max_def_level: u16,
    max_rep_level: u16,
) -> Result<ColumnIter<'_>>;

/// Dynamic registry for array iterator factories
pub struct ArrayIteratorRegistry {
    factories: HashMap<TypeId, ArrayIteratorFactory>,
}

impl ArrayIteratorRegistry {
    /// Create and initialize the registry
    pub fn new() -> Self {
        let mut registry = Self {
            factories: HashMap::new(),
        };
        
        // Register all supported primitive types
        registry.register_primitives();
        registry
    }
    
    /// Register all primitive type factories
    fn register_primitives(&mut self) {
        self.factories.insert(TypeId::Boolean, Self::create_bool_array_iter);
        self.factories.insert(TypeId::Int8, Self::create_i8_array_iter);
        self.factories.insert(TypeId::Int16, Self::create_i16_array_iter);
        self.factories.insert(TypeId::Int32, Self::create_i32_array_iter);
        self.factories.insert(TypeId::Int64, Self::create_i64_array_iter);
        self.factories.insert(TypeId::UInt8, Self::create_u8_array_iter);
        self.factories.insert(TypeId::UInt16, Self::create_u16_array_iter);
        self.factories.insert(TypeId::UInt32, Self::create_u32_array_iter);
        self.factories.insert(TypeId::UInt64, Self::create_u64_array_iter);
        // Note: Float types need proper metadata implementation
        // self.factories.insert(TypeId::Float32, Self::create_f32_array_iter);
        // self.factories.insert(TypeId::Float64, Self::create_f64_array_iter);
    }
    
    /// Create array iterator using registered factory
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
        if let Some(&factory) = self.factories.get(element_type_id) {
            // SAFETY: This is safe because we know the lifetime is correct
            // and we're just delegating to the factory function
            let result = factory(pages, rows, is_nullable, chunk_size, max_def_level, max_rep_level)?;
            Ok(result)
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
                        "Array element type {:?} not registered in factory",
                        element_type_id
                    )))
                }
            }
        }
    }
    
    /// Check if a type is supported
    pub fn supports_type(&self, type_id: &TypeId) -> bool {
        self.factories.contains_key(type_id)
    }
    
    // Factory functions for each type
    fn create_bool_array_iter<'a>(
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
    
    fn create_i8_array_iter<'a>(
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
    
    fn create_i16_array_iter<'a>(
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
    
    fn create_i32_array_iter<'a>(
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
    
    fn create_i64_array_iter<'a>(
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
    
    fn create_u8_array_iter<'a>(
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
    
    fn create_u16_array_iter<'a>(
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
    
    fn create_u32_array_iter<'a>(
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
    
    fn create_u64_array_iter<'a>(
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

/// Global registry instance
static mut GLOBAL_REGISTRY: Option<ArrayIteratorRegistry> = None;
static REGISTRY_INIT: std::sync::Once = std::sync::Once::new();

/// Get the global registry instance
pub fn get_registry() -> &'static ArrayIteratorRegistry {
    unsafe {
        REGISTRY_INIT.call_once(|| {
            GLOBAL_REGISTRY = Some(ArrayIteratorRegistry::new());
        });
        GLOBAL_REGISTRY.as_ref().unwrap()
    }
}

/// Public API function to create array iterator using global registry
pub fn create_array_iterator_with_registry<'a>(
    element_type_id: &TypeId,
    pages: Decompressor<'a>,
    rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
    max_def_level: u16,
    max_rep_level: u16,
) -> Result<ColumnIter<'a>> {
    get_registry().create_array_iterator(
        element_type_id,
        pages,
        rows,
        is_nullable,
        chunk_size,
        max_def_level,
        max_rep_level,
    )
}