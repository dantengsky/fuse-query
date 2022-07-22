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

use common_pipeline_sources::sources::async_source;
mod blocks_source;
mod deserializer;
mod file_splitter;
mod multi_file_splitter;
mod stream_source;
mod stream_source_v2;
mod sync_ck_source_receiver;
mod sync_source_receiver;

pub use async_source::AsyncSource;
pub use async_source::AsyncSourcer;
pub use blocks_source::BlocksSource;
use common_pipeline_sources::sources::empty_source;
pub use common_pipeline_sources::sources::sync_source::SyncSource;
pub use common_pipeline_sources::sources::sync_source::SyncSourcer;
pub use deserializer::Deserializer;
pub use empty_source::EmptySource;
pub use multi_file_splitter::MultiFileSplitter;
pub use multi_file_splitter::OperatorInfo;
pub use stream_source::StreamSource;
pub use stream_source_v2::StreamSourceV2;
pub use sync_ck_source_receiver::SyncReceiverCkSource;
pub use sync_source_receiver::SyncReceiverSource;

#[allow(dead_code)]
mod source_example {
    use std::sync::Arc;

    use common_datablocks::DataBlock;
    use common_exception::Result;
    use common_pipeline_sources::sources::sync_source::SyncSource;
    use common_pipeline_sources::sources::sync_source::SyncSourcer;

    use crate::pipelines::processors::port::OutputPort;
    use crate::pipelines::processors::processor::ProcessorPtr;
    use crate::pipelines::processors::sources::async_source::AsyncSource;
    use crate::pipelines::processors::AsyncSourcer;
    use crate::sessions::QueryContext;

    struct ExampleSyncSource {
        pos: usize,
        data_blocks: Vec<DataBlock>,
    }

    impl ExampleSyncSource {
        pub fn create(
            ctx: Arc<QueryContext>,
            data_blocks: Vec<DataBlock>,
            outputs: Arc<OutputPort>,
        ) -> Result<ProcessorPtr> {
            SyncSourcer::create(ctx, outputs, ExampleSyncSource {
                pos: 0,
                data_blocks,
            })
        }
    }

    impl SyncSource for ExampleSyncSource {
        const NAME: &'static str = "Example";

        fn generate(&mut self) -> Result<Option<DataBlock>> {
            self.pos += 1;
            match self.data_blocks.len() >= self.pos {
                true => Ok(Some(self.data_blocks[self.pos - 1].clone())),
                false => Ok(None),
            }
        }
    }

    struct ExampleAsyncSource {
        pos: usize,
        data_blocks: Vec<DataBlock>,
    }

    impl ExampleAsyncSource {
        pub fn create(
            ctx: Arc<QueryContext>,
            data_blocks: Vec<DataBlock>,
            output: Arc<OutputPort>,
        ) -> Result<ProcessorPtr> {
            AsyncSourcer::create(ctx, output, ExampleAsyncSource {
                pos: 0,
                data_blocks,
            })
        }
    }

    #[async_trait::async_trait]
    impl AsyncSource for ExampleAsyncSource {
        const NAME: &'static str = "Async";

        #[async_trait::unboxed_simple]
        async fn generate(&mut self) -> Result<Option<DataBlock>> {
            self.pos += 1;
            match self.data_blocks.len() >= self.pos {
                true => Ok(Some(self.data_blocks[self.pos - 1].clone())),
                false => Ok(None),
            }
        }
    }
}
