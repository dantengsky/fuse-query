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

use std::collections::HashMap;

use arrow_array::RecordBatch;
use databend_common_expression::ColumnId;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::meta::Compression;

use crate::io::read::block::block_reader_merge_io::DataItem;

pub fn column_chunks_to_record_batch(
    original_schema: &TableSchema,
    num_rows: usize,
    column_chunks: &HashMap<ColumnId, DataItem>,
    compression: &Compression,
) -> databend_common_exception::Result<RecordBatch> {
    todo!()
}
