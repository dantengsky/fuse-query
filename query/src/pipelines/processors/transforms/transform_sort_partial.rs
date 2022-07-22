// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datablocks::SortColumnDescription;
use common_exception::Result;
pub use common_pipeline::sort_util::get_sort_descriptions;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::transforms::transform::Transform;
use crate::pipelines::processors::transforms::transform::Transformer;

pub struct TransformSortPartial {
    limit: Option<usize>,
    sort_columns_descriptions: Vec<SortColumnDescription>,
}

impl TransformSortPartial {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        limit: Option<usize>,
        sort_columns_descriptions: Vec<SortColumnDescription>,
    ) -> Result<ProcessorPtr> {
        Ok(Transformer::create(input, output, TransformSortPartial {
            limit,
            sort_columns_descriptions,
        }))
    }
}

#[async_trait::async_trait]
impl Transform for TransformSortPartial {
    const NAME: &'static str = "SortPartialTransform";

    fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
        DataBlock::sort_block(&block, &self.sort_columns_descriptions, self.limit)
    }
}
