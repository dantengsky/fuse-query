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

mod builder_aggregate;
mod builder_append_table;
mod builder_commit;
mod builder_compact;
mod builder_copy_into;
mod builder_delete;
mod builder_distributed_insert_select;
mod builder_exchange;
mod builder_fill_missing_columns;
mod builder_filter;
mod builder_join;
mod builder_lambda;
mod builder_limit;
mod builder_merge_into;
mod builder_on_finished;
mod builder_project;
mod builder_recluster;
mod builder_replace_into;
mod builder_row_fetch;
mod builder_scalar;
mod builder_scan;
mod builder_sort;
mod builder_udf;
mod builder_union_all;
mod builder_window;

pub use builder_replace_into::ValueSource;
