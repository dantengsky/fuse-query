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

//! Direct deserialization from parquet2 to DataBlock without Arrow intermediate representation
//!
//! This crate provides functionality to directly deserialize Parquet data into DataBlock
//! structures, bypassing the Arrow memory model for improved performance.

pub mod column;
pub mod column_reader;
pub mod util;

pub mod wip;

pub use column_reader::*;
pub use util::from_table_filed_type;
pub use wip::page_reader::PageReader;
