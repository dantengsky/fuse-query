use std::any::Any;
use std::sync::Arc;
use common_base::base::{Progress, ProgressValues};
use common_catalog::plan::PartInfoPtr;
use common_catalog::table_context::TableContext;
use common_datablocks::{BlockMetaInfo, DataBlock};
use common_exception::ErrorCode;
use common_pipeline_core::processors::processor::{Event, ProcessorPtr};
use common_pipeline_transforms::processors::transforms::{Transform, Transformer};
use crate::io::BlockReader;
use crate::operations::read::parquet_data_source::DataSourceMeta;
use common_exception::Result;
use common_pipeline_core::processors::port::{InputPort, OutputPort};
use common_pipeline_core::processors::Processor;

pub struct DeserializeDataTransform {
    scan_progress: Arc<Progress>,
    block_reader: Arc<BlockReader>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    parts: Vec<PartInfoPtr>,
    chunks: Vec<Vec<(usize, Vec<u8>)>>,
}

impl DeserializeDataTransform {
    pub fn create(ctx: Arc<dyn TableContext>, block_reader: Arc<BlockReader>, input: Arc<InputPort>, output: Arc<OutputPort>) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(
            DeserializeDataTransform {
                scan_progress,
                block_reader,
                input,
                output,
                output_data: None,
                parts: vec![],
                chunks: vec![],
            }
        )))
    }
}

#[async_trait::async_trait]
impl Processor for DeserializeDataTransform {
    fn name(&self) -> String {
        String::from("DeserializeDataTransform")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if !self.chunks.is_empty() {
            if !self.input.has_data() {
                self.input.set_need_data();
            }

            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            if let Some(mut source_meta) = data_block.take_meta() {
                if let Some(source_meta) = source_meta.as_mut_any().downcast_mut::<DataSourceMeta>() {
                    self.parts = source_meta.part.clone();
                    self.chunks = source_meta.data.take().unwrap();
                    return Ok(Event::Sync);
                }
            }

            unreachable!();
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let part = self.parts.pop();
        let chunks = self.chunks.pop();
        if let Some((part, chunks)) = part.zip(chunks) {
            let data_block = self.block_reader.deserialize(part, chunks)?;

            let progress_values = ProgressValues {
                rows: data_block.num_rows(),
                bytes: data_block.memory_size(),
            };
            self.scan_progress.incr(&progress_values);

            self.output_data = Some(data_block);
        }

        Ok(())
    }
}
