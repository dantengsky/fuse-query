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

use std::any::Any;
use std::mem;
use std::sync::Arc;

use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_pipeline_core::processors::Event;
use common_pipeline_core::processors::InputPort;
use common_pipeline_core::processors::OutputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::processors::ProcessorPtr;

use crate::row_based_file::buffers::FileOutputBuffers;

pub(super) struct LimitFileSizeProcessor {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    threshold: usize,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
    buffers: Vec<Vec<u8>>,
}

impl LimitFileSizeProcessor {
    pub(super) fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        threshold: usize,
    ) -> Result<ProcessorPtr> {
        let p = Self {
            input,
            output,
            threshold,
            input_data: None,
            output_data: None,
            buffers: Vec::new(),
        };
        Ok(ProcessorPtr::create(Box::new(p)))
    }
}

impl Processor for LimitFileSizeProcessor {
    fn name(&self) -> String {
        String::from("ResizeProcessor")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> common_exception::Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            Ok(Event::Finished)
        } else if !self.output.can_push() {
            self.input.set_not_need_data();
            Ok(Event::NeedConsume)
        } else {
            match self.output_data.take() {
                Some(data) => {
                    self.output.push_data(Ok(data));
                    Ok(Event::NeedConsume)
                }
                None => {
                    if self.input_data.is_some() {
                        Ok(Event::Sync)
                    } else if self.input.has_data() {
                        self.input_data = Some(self.input.pull_data().unwrap()?);
                        Ok(Event::Sync)
                    } else if self.input.is_finished() {
                        if self.buffers.is_empty() {
                            self.output.finish();
                            Ok(Event::Finished)
                        } else {
                            let buffers = std::mem::take(&mut self.buffers);
                            self.output
                                .push_data(Ok(FileOutputBuffers::create_block(buffers)));
                            Ok(Event::NeedConsume)
                        }
                    } else {
                        self.input.set_need_data();
                        Ok(Event::NeedData)
                    }
                }
            }
        }
    }

    fn process(&mut self) -> common_exception::Result<()> {
        assert!(self.output_data.is_none());
        assert!(self.input_data.is_some());

        let block = self.input_data.take().unwrap();
        let block_meta = block.get_owned_meta().unwrap();
        let buffers = FileOutputBuffers::downcast_from(block_meta).unwrap();
        let buffers = buffers.buffers;

        self.buffers.extend(buffers);

        let mut size = 0;
        let mut buffers = mem::take(&mut self.buffers);
        let break_idx = buffers
            .iter()
            .enumerate()
            .find_map(|(idx, b)| {
                size += b.len();
                if size >= self.threshold {
                    Some(idx)
                } else {
                    None
                }
            })
            .unwrap_or(buffers.len());
        if break_idx == buffers.len() {
            self.buffers = buffers;
            Ok(())
        } else {
            let remain = buffers.split_off(break_idx + 1);
            self.output_data = Some(FileOutputBuffers::create_block(buffers));
            self.buffers = remain;
            Ok(())
        }
    }
}
