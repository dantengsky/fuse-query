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

//! Clean dispatch system for column iterators
//! 
//! This replaces the hardcoded match statements with a cleaner approach.

use databend_common_exception::{ErrorCode, Result};
use databend_common_expression::types::{NumberDataType, F32, F64};
use databend_common_expression::{TableDataType, Column};
use crate::reader::decompressor::Decompressor;
use super::{IntegerMetadata, BooleanMetadata, ArrayColumnIterator};

pub type ColumnIter<'a> = Box<dyn Iterator<Item = Result<Column>> + Send + Sync + 'a>;

/// Type identifier for dispatch
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum TypeId {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    String,
    Binary,
    Array(Box<TypeId>),
    Tuple(Vec<TypeId>),
}

impl TypeId {
    /// Convert TableDataType to TypeId
    pub fn from_table_data_type(data_type: &TableDataType) -> Self {
        match data_type {
            TableDataType::Boolean => TypeId::Boolean,
            TableDataType::Number(NumberDataType::Int8) => TypeId::Int8,
            TableDataType::Number(NumberDataType::Int16) => TypeId::Int16,
            TableDataType::Number(NumberDataType::Int32) => TypeId::Int32,
            TableDataType::Number(NumberDataType::Int64) => TypeId::Int64,
            TableDataType::Number(NumberDataType::UInt8) => TypeId::UInt8,
            TableDataType::Number(NumberDataType::UInt16) => TypeId::UInt16,
            TableDataType::Number(NumberDataType::UInt32) => TypeId::UInt32,
            TableDataType::Number(NumberDataType::UInt64) => TypeId::UInt64,
            TableDataType::Number(NumberDataType::Float32) => TypeId::Float32,
            TableDataType::Number(NumberDataType::Float64) => TypeId::Float64,
            TableDataType::String => TypeId::String,
            TableDataType::Binary => TypeId::Binary,
            TableDataType::Array(inner) => TypeId::Array(Box::new(Self::from_table_data_type(inner))),
            TableDataType::Tuple { fields_type, .. } => {
                TypeId::Tuple(fields_type.iter().map(|t| Self::from_table_data_type(t)).collect())
            }
            TableDataType::Nullable(inner) => Self::from_table_data_type(inner), // Strip nullable wrapper
            _ => TypeId::String, // Fallback for unsupported types
        }
    }
}

/// Create an array iterator using clean dispatch
pub fn create_array_iterator<'a>(
    element_type_id: &TypeId,
    pages: Decompressor<'a>,
    rows: usize,
    is_nullable: bool,
    chunk_size: Option<usize>,
    max_def_level: u16,
    max_rep_level: u16,
) -> Result<ColumnIter<'a>> {
    match element_type_id {
        TypeId::Boolean => {
            Ok(Box::new(ArrayColumnIterator::<bool>::new(
                pages, rows, is_nullable, BooleanMetadata,
                chunk_size, max_def_level, max_rep_level
            )))
        }
        TypeId::Int8 => {
            Ok(Box::new(ArrayColumnIterator::<i8>::new(
                pages, rows, is_nullable, IntegerMetadata,
                chunk_size, max_def_level, max_rep_level
            )))
        }
        TypeId::Int16 => {
            Ok(Box::new(ArrayColumnIterator::<i16>::new(
                pages, rows, is_nullable, IntegerMetadata,
                chunk_size, max_def_level, max_rep_level
            )))
        }
        TypeId::Int32 => {
            Ok(Box::new(ArrayColumnIterator::<i32>::new(
                pages, rows, is_nullable, IntegerMetadata,
                chunk_size, max_def_level, max_rep_level
            )))
        }
        TypeId::Int64 => {
            Ok(Box::new(ArrayColumnIterator::<i64>::new(
                pages, rows, is_nullable, IntegerMetadata,
                chunk_size, max_def_level, max_rep_level
            )))
        }
        TypeId::UInt8 => {
            Ok(Box::new(ArrayColumnIterator::<u8>::new(
                pages, rows, is_nullable, IntegerMetadata,
                chunk_size, max_def_level, max_rep_level
            )))
        }
        TypeId::UInt16 => {
            Ok(Box::new(ArrayColumnIterator::<u16>::new(
                pages, rows, is_nullable, IntegerMetadata,
                chunk_size, max_def_level, max_rep_level
            )))
        }
        TypeId::UInt32 => {
            Ok(Box::new(ArrayColumnIterator::<u32>::new(
                pages, rows, is_nullable, IntegerMetadata,
                chunk_size, max_def_level, max_rep_level
            )))
        }
        TypeId::UInt64 => {
            Ok(Box::new(ArrayColumnIterator::<u64>::new(
                pages, rows, is_nullable, IntegerMetadata,
                chunk_size, max_def_level, max_rep_level
            )))
        }
        TypeId::Float32 => {
            Ok(Box::new(ArrayColumnIterator::<F32>::new(
                pages, rows, is_nullable, IntegerMetadata, // TODO: Float metadata
                chunk_size, max_def_level, max_rep_level
            )))
        }
        TypeId::Float64 => {
            Ok(Box::new(ArrayColumnIterator::<F64>::new(
                pages, rows, is_nullable, IntegerMetadata, // TODO: Float metadata
                chunk_size, max_def_level, max_rep_level
            )))
        }
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
    }
}