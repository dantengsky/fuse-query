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

//! Macro-driven type dispatch for parquet column readers
//! 
//! This follows the excellent pattern from Native format, using compile-time
//! macro expansion instead of runtime HashMap lookups for better performance.

/// Macro for dispatching parquet primitive types
/// 
/// This generates specialized code for each supported type, following the
/// Native format pattern. Zero runtime overhead, full compiler optimization.
#[macro_export]
macro_rules! with_match_parquet_primitive_type {
    (
        $type_id:expr, 
        |$_:tt $T:ident| $body_integer:tt,
        |$__:tt $F:ident| $body_float:tt,
        |$___:tt $B:ident| $body_boolean:tt
    ) => {{
        macro_rules! __with_integer_type__ {
            ($_ $T:ident) => {
                $body_integer
            };
        }
        macro_rules! __with_float_type__ {
            ($_ $F:ident) => {
                $body_float
            };
        }
        macro_rules! __with_boolean_type__ {
            ($_ $B:ident) => {
                $body_boolean
            };
        }
        
        use crate::column::TypeId;
        use databend_common_expression::types::{F32, F64};
        
        match $type_id {
            TypeId::Int8 => __with_integer_type__! { i8 },
            TypeId::Int16 => __with_integer_type__! { i16 },
            TypeId::Int32 => __with_integer_type__! { i32 },
            TypeId::Int64 => __with_integer_type__! { i64 },
            TypeId::UInt8 => __with_integer_type__! { u8 },
            TypeId::UInt16 => __with_integer_type__! { u16 },
            TypeId::UInt32 => __with_integer_type__! { u32 },
            TypeId::UInt64 => __with_integer_type__! { u64 },
            
            TypeId::Float32 => __with_float_type__! { F32 },
            TypeId::Float64 => __with_float_type__! { F64 },
            
            TypeId::Boolean => __with_boolean_type__! { bool },
            
            _ => {
                use databend_common_exception::ErrorCode;
                Err(ErrorCode::StorageOther(format!(
                    "Type {:?} not supported in primitive dispatch",
                    $type_id
                )))
            }
        }
    }};
}

/// Simplified macro for array element types (most common case)
#[macro_export]
macro_rules! with_match_parquet_array_type {
    ($type_id:expr, |$_:tt $T:ident| $body:tt) => {{
        macro_rules! __with_array_element_type__ {
            ($_ $T:ident) => {
                $body
            };
        }
        
        use crate::column::TypeId;
        
        match $type_id {
            TypeId::Int8 => __with_array_element_type__! { i8 },
            TypeId::Int16 => __with_array_element_type__! { i16 },
            TypeId::Int32 => __with_array_element_type__! { i32 },
            TypeId::Int64 => __with_array_element_type__! { i64 },
            TypeId::UInt8 => __with_array_element_type__! { u8 },
            TypeId::UInt16 => __with_array_element_type__! { u16 },
            TypeId::UInt32 => __with_array_element_type__! { u32 },
            TypeId::UInt64 => __with_array_element_type__! { u64 },
            TypeId::Boolean => __with_array_element_type__! { bool },
            
            _ => {
                use databend_common_exception::ErrorCode;
                match $type_id {
                    TypeId::Float32 | TypeId::Float64 => {
                        Err(ErrorCode::StorageOther(format!(
                            "Float array element type {:?} needs special metadata handling",
                            $type_id
                        )))
                    }
                    TypeId::String | TypeId::Binary => {
                        Err(ErrorCode::StorageOther(format!(
                            "Array({:?}) not yet supported - requires specialized string/binary array iterator",
                            $type_id
                        )))
                    }
                    TypeId::Array(_) | TypeId::Tuple(_) => {
                        Err(ErrorCode::StorageOther(format!(
                            "Nested array element type {:?} not yet supported - requires complex level processing",
                            $type_id
                        )))
                    }
                    _ => {
                        Err(ErrorCode::StorageOther(format!(
                            "Array element type {:?} not supported in experimental reader",
                            $type_id
                        )))
                    }
                }
            }
        }
    }};
}

/// Macro specifically for creating primitive column iterators
/// This replaces the complex factory system with simple, direct dispatch
#[macro_export] 
macro_rules! create_primitive_column_iter {
    ($type_id:expr, $pages:expr, $rows:expr, $is_nullable:expr, $chunk_size:expr) => {{
        use crate::column::{new_boolean_iter, new_int8_iter, new_int16_iter, new_int32_iter, new_int64_iter};
        use crate::column::{new_uint8_iter, new_uint16_iter, new_uint32_iter, new_uint64_iter};
        use crate::column::{new_float32_iter, new_float64_iter};
        
        with_match_parquet_primitive_type!($type_id,
            |$T| {
                match $type_id {
                    TypeId::Int8 => Ok(Box::new(new_int8_iter($pages, $rows, $is_nullable, $chunk_size))),
                    TypeId::Int16 => Ok(Box::new(new_int16_iter($pages, $rows, $is_nullable, $chunk_size))),
                    TypeId::Int32 => Ok(Box::new(new_int32_iter($pages, $rows, $is_nullable, $chunk_size))),
                    TypeId::Int64 => Ok(Box::new(new_int64_iter($pages, $rows, $is_nullable, $chunk_size))),
                    TypeId::UInt8 => Ok(Box::new(new_uint8_iter($pages, $rows, $is_nullable, $chunk_size))),
                    TypeId::UInt16 => Ok(Box::new(new_uint16_iter($pages, $rows, $is_nullable, $chunk_size))),
                    TypeId::UInt32 => Ok(Box::new(new_uint32_iter($pages, $rows, $is_nullable, $chunk_size))),
                    TypeId::UInt64 => Ok(Box::new(new_uint64_iter($pages, $rows, $is_nullable, $chunk_size))),
                    _ => unreachable!("Integer type dispatch failed")
                }
            },
            |$F| {
                match $type_id {
                    TypeId::Float32 => Ok(Box::new(new_float32_iter($pages, $rows, $is_nullable, $chunk_size))),
                    TypeId::Float64 => Ok(Box::new(new_float64_iter($pages, $rows, $is_nullable, $chunk_size))),
                    _ => unreachable!("Float type dispatch failed")
                }
            },
            |$B| {
                Ok(Box::new(new_boolean_iter($pages, $rows, $is_nullable, $chunk_size)))
            }
        )
    }};
}

/// Macro for creating array column iterators
/// This eliminates all the repetitive ArrayColumnIterator::<Type>::new calls
#[macro_export]
macro_rules! create_array_column_iter {
    ($element_type_id:expr, $pages:expr, $rows:expr, $is_nullable:expr, $chunk_size:expr, $max_def:expr, $max_rep:expr) => {{
        use crate::column::{ArrayColumnIterator, IntegerMetadata, BooleanMetadata};
        
        with_match_parquet_array_type!($element_type_id, |$T| {
            match $element_type_id {
                TypeId::Boolean => {
                    Ok(Box::new(ArrayColumnIterator::<bool>::new(
                        $pages, $rows, $is_nullable, BooleanMetadata,
                        $chunk_size, $max_def, $max_rep
                    )))
                }
                _ => {
                    Ok(Box::new(ArrayColumnIterator::<$T>::new(
                        $pages, $rows, $is_nullable, IntegerMetadata,
                        $chunk_size, $max_def, $max_rep
                    )))
                }
            }
        })
    }};
}