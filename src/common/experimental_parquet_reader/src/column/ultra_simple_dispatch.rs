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

//! Ultra-simple macro dispatch - following Native format's elegance
//! 
//! This eliminates ALL complex abstractions and provides the simplest
//! possible solution inspired by Native's excellent design.

/// The simplest possible array dispatch - no abstractions, just direct match
#[macro_export]
macro_rules! simple_array_dispatch {
    ($element_type_id:expr, $pages:expr, $rows:expr, $is_nullable:expr, $chunk_size:expr, $max_def:expr, $max_rep:expr) => {{
        use crate::column::{ArrayColumnIterator, IntegerMetadata, BooleanMetadata, TypeId};
        
        match $element_type_id {
            TypeId::Boolean => {
                Ok(Box::new(ArrayColumnIterator::<bool>::new(
                    $pages, $rows, $is_nullable, BooleanMetadata,
                    $chunk_size, $max_def, $max_rep
                )))
            }
            TypeId::Int8 => {
                Ok(Box::new(ArrayColumnIterator::<i8>::new(
                    $pages, $rows, $is_nullable, IntegerMetadata,
                    $chunk_size, $max_def, $max_rep
                )))
            }
            TypeId::Int16 => {
                Ok(Box::new(ArrayColumnIterator::<i16>::new(
                    $pages, $rows, $is_nullable, IntegerMetadata,
                    $chunk_size, $max_def, $max_rep
                )))
            }
            TypeId::Int32 => {
                Ok(Box::new(ArrayColumnIterator::<i32>::new(
                    $pages, $rows, $is_nullable, IntegerMetadata,
                    $chunk_size, $max_def, $max_rep
                )))
            }
            TypeId::Int64 => {
                Ok(Box::new(ArrayColumnIterator::<i64>::new(
                    $pages, $rows, $is_nullable, IntegerMetadata,
                    $chunk_size, $max_def, $max_rep
                )))
            }
            TypeId::UInt8 => {
                Ok(Box::new(ArrayColumnIterator::<u8>::new(
                    $pages, $rows, $is_nullable, IntegerMetadata,
                    $chunk_size, $max_def, $max_rep
                )))
            }
            TypeId::UInt16 => {
                Ok(Box::new(ArrayColumnIterator::<u16>::new(
                    $pages, $rows, $is_nullable, IntegerMetadata,
                    $chunk_size, $max_def, $max_rep
                )))
            }
            TypeId::UInt32 => {
                Ok(Box::new(ArrayColumnIterator::<u32>::new(
                    $pages, $rows, $is_nullable, IntegerMetadata,
                    $chunk_size, $max_def, $max_rep
                )))
            }
            TypeId::UInt64 => {
                Ok(Box::new(ArrayColumnIterator::<u64>::new(
                    $pages, $rows, $is_nullable, IntegerMetadata,
                    $chunk_size, $max_def, $max_rep
                )))
            }
            _ => {
                use databend_common_exception::ErrorCode;
                match $element_type_id {
                    TypeId::Float32 | TypeId::Float64 => {
                        Err(ErrorCode::StorageOther(format!(
                            "Float array element type {:?} needs special metadata handling",
                            $element_type_id
                        )))
                    }
                    TypeId::String | TypeId::Binary => {
                        Err(ErrorCode::StorageOther(format!(
                            "Array({:?}) not yet supported - requires specialized string/binary array iterator",
                            $element_type_id
                        )))
                    }
                    TypeId::Array(_) | TypeId::Tuple(_) => {
                        Err(ErrorCode::StorageOther(format!(
                            "Nested array element type {:?} not yet supported - requires complex level processing",
                            $element_type_id
                        )))
                    }
                    _ => {
                        Err(ErrorCode::StorageOther(format!(
                            "Array element type {:?} not supported in experimental reader",
                            $element_type_id
                        )))
                    }
                }
            }
        }
    }};
}

/// The simplest possible primitive dispatch - no abstractions, just direct match
#[macro_export]
macro_rules! simple_primitive_dispatch {
    ($type_id:expr, $pages:expr, $rows:expr, $is_nullable:expr, $chunk_size:expr) => {{
        use crate::column::{new_boolean_iter, new_int8_iter, new_int16_iter, new_int32_iter, new_int64_iter};
        use crate::column::{new_uint8_iter, new_uint16_iter, new_uint32_iter, new_uint64_iter};
        use crate::column::{new_float32_iter, new_float64_iter, TypeId};
        
        match $type_id {
            TypeId::Boolean => Ok(Box::new(new_boolean_iter($pages, $rows, $is_nullable, $chunk_size))),
            TypeId::Int8 => Ok(Box::new(new_int8_iter($pages, $rows, $is_nullable, $chunk_size))),
            TypeId::Int16 => Ok(Box::new(new_int16_iter($pages, $rows, $is_nullable, $chunk_size))),
            TypeId::Int32 => Ok(Box::new(new_int32_iter($pages, $rows, $is_nullable, $chunk_size))),
            TypeId::Int64 => Ok(Box::new(new_int64_iter($pages, $rows, $is_nullable, $chunk_size))),
            TypeId::UInt8 => Ok(Box::new(new_uint8_iter($pages, $rows, $is_nullable, $chunk_size))),
            TypeId::UInt16 => Ok(Box::new(new_uint16_iter($pages, $rows, $is_nullable, $chunk_size))),
            TypeId::UInt32 => Ok(Box::new(new_uint32_iter($pages, $rows, $is_nullable, $chunk_size))),
            TypeId::UInt64 => Ok(Box::new(new_uint64_iter($pages, $rows, $is_nullable, $chunk_size))),
            TypeId::Float32 => Ok(Box::new(new_float32_iter($pages, $rows, $is_nullable, $chunk_size))),
            TypeId::Float64 => Ok(Box::new(new_float64_iter($pages, $rows, $is_nullable, $chunk_size))),
            _ => {
                use databend_common_exception::ErrorCode;
                Err(ErrorCode::StorageOther(format!(
                    "Primitive type {:?} not supported in experimental reader",
                    $type_id
                )))
            }
        }
    }};
}