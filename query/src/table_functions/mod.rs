//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_table_functions::async_crash_me;
use common_table_functions::memory_block_part;
use common_table_functions::numbers_part;
use common_table_functions::numbers_table;
use common_table_functions::sync_crash_me;
use common_table_functions::table_function;

mod table_function_factory;

pub use memory_block_part::generate_numbers_parts;
pub use numbers_part::NumbersPartInfo;
pub use numbers_table::NumbersTable;
pub use table_function::TableFunction;
pub use table_function_factory::TableArgs;
pub use table_function_factory::TableFunctionFactory;
