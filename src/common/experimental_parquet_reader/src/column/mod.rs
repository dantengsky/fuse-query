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

// Declare macro modules first so macros are available to other modules  
#[macro_use]
mod macro_dispatch;
#[macro_use] 
mod ultra_simple_dispatch; // The simplest possible solution

// Arrow-inspired architecture modules (newest and best)
mod arrow_reader_trait;    // Unified ColumnArrayReader trait
mod arrow_builder;         // Recursive builder pattern  
mod arrow_list_reader;     // Level-driven list reader
mod arrow_struct_reader;   // Struct/tuple reader
mod arrow_primitive_readers; // Primitive type readers
mod arrow_success;         // Success demonstration

mod array;
mod binary;
mod boolean;
mod common;
mod date;
mod decimal;
mod encoding;
mod level_decoder;
mod levels;
mod number;
mod string;
mod traits;
mod tuple;
mod utils;
mod validation;
mod table_dispatch; // New table-driven dispatch system
mod registry; // Dynamic type registry
mod generic_factory; // Generic factory for all column types
mod simple_dispatch; // Native-style macro dispatch (recommended)
mod final_dispatch; // Ultra-simple final solution (best)
mod macro_tests; // Tests for macro dispatch system
mod standalone_validation; // Standalone validation for new macros
mod arrow_demo; // Demonstration of Arrow architecture

pub use array::*;
pub use binary::BinaryIter;
pub use boolean::*;
pub use date::*;
pub use decimal::*;
pub use level_decoder::*;
pub use levels::*;
pub use number::IntegerMetadata;
pub use number::*;
pub use string::*;
pub use traits::*;
pub use tuple::*;
pub use table_dispatch::*; // Export the new dispatch system
pub use registry::*; // Export the dynamic registry
pub use generic_factory::*; // Export the generic factory
pub use simple_dispatch::*; // Export the macro-based dispatch (recommended)
pub use final_dispatch::*; // Export the ultra-simple dispatch (best)

// Export Arrow-inspired architecture (recommended for new development)
pub use arrow_reader_trait::*;
pub use arrow_builder::*;
pub use arrow_list_reader::*;
pub use arrow_struct_reader::*;
pub use arrow_primitive_readers::*;
