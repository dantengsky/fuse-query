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

//! Type dispatch system for creating column iterators
//! 
//! This module provides a trait-based system to replace hardcoded type matching
//! with a more flexible and maintainable approach.

use std::collections::HashMap;
use databend_common_exception::{ErrorCode, Result};
use databend_common_expression::types::NumberDataType;
use databend_common_expression::{TableDataType, TableField};

use super::traits::{ParquetColumnType, DictionarySupport, ParquetPhysicalMapping};
use super::{IntegerMetadata, BooleanMetadata, ArrayColumnIterator};
use crate::reader::decompressor::Decompressor;

/// Column iterator creation result
pub type ColumnIter<'a> = Box<dyn Iterator<Item = Result<databend_common_expression::Column>> + Send + Sync + 'a>;

/// Trait for creating column iterators for specific types
pub trait ColumnIteratorFactory<'a> {
    /// Create a column iterator for primitive types
    fn create_primitive_iter(
        &self,
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Result<ColumnIter<'a>>;

    /// Create an array iterator for this element type
    fn create_array_iter(
        &self,
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
        max_def_level: u16,
        max_rep_level: u16,
    ) -> Result<ColumnIter<'a>>;

    /// Check if this factory supports the given data type
    fn supports_type(&self, data_type: &TableDataType) -> bool;
}

/// Factory for integer types
pub struct IntegerIteratorFactory<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> IntegerIteratorFactory<T> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, T> ColumnIteratorFactory<'a> for IntegerIteratorFactory<T>
where
    T: ParquetColumnType + DictionarySupport + ParquetPhysicalMapping + 'static,
{
    fn create_primitive_iter(
        &self,
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Result<ColumnIter<'a>> {
        use super::new_int8_iter;
        // This is where we'd call the appropriate iterator creation function
        // For now, we'll need to dispatch based on type T
        // This requires some type-level programming or dynamic dispatch
        Err(ErrorCode::Internal("IntegerIteratorFactory not fully implemented".to_string()))
    }

    fn create_array_iter(
        &self,
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
        max_def_level: u16,
        max_rep_level: u16,
    ) -> Result<ColumnIter<'a>> {
        Ok(Box::new(ArrayColumnIterator::<T>::new(
            pages, rows, is_nullable, IntegerMetadata,
            chunk_size, max_def_level, max_rep_level
        )))
    }

    fn supports_type(&self, data_type: &TableDataType) -> bool {
        matches!(data_type, TableDataType::Number(_))
    }
}

/// Factory for boolean types
pub struct BooleanIteratorFactory;

impl<'a> ColumnIteratorFactory<'a> for BooleanIteratorFactory {
    fn create_primitive_iter(
        &self,
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Result<ColumnIter<'a>> {
        use super::new_boolean_iter;
        Ok(Box::new(new_boolean_iter(pages, rows, is_nullable, chunk_size)))
    }

    fn create_array_iter(
        &self,
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

    fn supports_type(&self, data_type: &TableDataType) -> bool {
        matches!(data_type, TableDataType::Boolean)
    }
}

/// Type registry for managing column iterator factories
pub struct TypeRegistry<'a> {
    factories: HashMap<String, Box<dyn ColumnIteratorFactory<'a> + 'a>>,
}

impl<'a> TypeRegistry<'a> {
    pub fn new() -> Self {
        let mut registry = Self {
            factories: HashMap::new(),
        };
        
        // Register built-in types
        registry.register_boolean_factory();
        // TODO: Register integer factories for each type
        
        registry
    }

    fn register_boolean_factory(&mut self) {
        self.factories.insert(
            "Boolean".to_string(),
            Box::new(BooleanIteratorFactory),
        );
    }

    /// Create a primitive iterator for the given type
    pub fn create_primitive_iter(
        &self,
        data_type: &TableDataType,
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
    ) -> Result<ColumnIter<'a>> {
        let type_key = self.get_type_key(data_type);
        
        if let Some(factory) = self.factories.get(&type_key) {
            factory.create_primitive_iter(pages, rows, is_nullable, chunk_size)
        } else {
            Err(ErrorCode::StorageOther(format!(
                "No factory registered for type: {:?}",
                data_type
            )))
        }
    }

    /// Create an array iterator for the given element type
    pub fn create_array_iter(
        &self,
        element_type: &TableDataType,
        pages: Decompressor<'a>,
        rows: usize,
        is_nullable: bool,
        chunk_size: Option<usize>,
        max_def_level: u16,
        max_rep_level: u16,
    ) -> Result<ColumnIter<'a>> {
        let type_key = self.get_type_key(element_type);
        
        if let Some(factory) = self.factories.get(&type_key) {
            factory.create_array_iter(pages, rows, is_nullable, chunk_size, max_def_level, max_rep_level)
        } else {
            Err(ErrorCode::StorageOther(format!(
                "No array factory registered for element type: {:?}",
                element_type
            )))
        }
    }

    /// Get a string key for the data type
    fn get_type_key(&self, data_type: &TableDataType) -> String {
        match data_type {
            TableDataType::Boolean => "Boolean".to_string(),
            TableDataType::Number(NumberDataType::Int8) => "Int8".to_string(),
            TableDataType::Number(NumberDataType::Int16) => "Int16".to_string(),
            TableDataType::Number(NumberDataType::Int32) => "Int32".to_string(),
            TableDataType::Number(NumberDataType::Int64) => "Int64".to_string(),
            TableDataType::Number(NumberDataType::UInt8) => "UInt8".to_string(),
            TableDataType::Number(NumberDataType::UInt16) => "UInt16".to_string(),
            TableDataType::Number(NumberDataType::UInt32) => "UInt32".to_string(),
            TableDataType::Number(NumberDataType::UInt64) => "UInt64".to_string(),
            TableDataType::Number(NumberDataType::Float32) => "Float32".to_string(),
            TableDataType::Number(NumberDataType::Float64) => "Float64".to_string(),
            TableDataType::String => "String".to_string(),
            TableDataType::Binary => "Binary".to_string(),
            _ => format!("{:?}", data_type),
        }
    }
}

/// High-level function to replace the hardcoded type matching
pub fn create_column_iterator<'a>(
    field: &TableField,
    pages: Decompressor<'a>,
    rows: usize,
    chunk_size: Option<usize>,
) -> Result<ColumnIter<'a>> {
    let registry = TypeRegistry::new();
    
    // Extract nullable information
    let (inner_data_type, is_nullable) = match &field.data_type {
        TableDataType::Nullable(inner) => (inner.as_ref(), true),
        other => (other, false),
    };

    match inner_data_type {
        // Handle arrays using the registry
        TableDataType::Array(element_type) => {
            registry.create_array_iter(
                element_type,
                pages,
                rows,
                is_nullable,
                chunk_size,
                2, // Default def level for arrays
                1, // Default rep level for arrays
            )
        }
        
        // Handle primitives using the registry
        _ => {
            registry.create_primitive_iter(
                inner_data_type,
                pages,
                rows,
                is_nullable,
                chunk_size,
            )
        }
    }
}