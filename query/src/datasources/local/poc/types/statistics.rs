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

use std::collections::HashMap;

pub type Bytes = Vec<u8>;

pub struct TableSummary {
    pub chunks: u64,
    pub cols: u32,
    pub rows: u64,
    pub uncompressed_size: u64,
    pub persistent_size: u64,
    pub col_stats: HashMap<ColumnId, ColStats>,
}

pub type ColumnId = u64;

pub struct ColStats {
    min: Bytes,
    max: Bytes,
    null_count: u64,
    distinct_count: Option<u64>,
}
