// Copyright 2021 Datafuse Labs.
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
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::Arc;

use common_catalog::catalog::StorageDescription;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::output_format::OutputFormatType;
use common_meta_app::schema::TableInfo;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::Pipeline;
use common_pipeline_core::SinkPipeBuilder;
use common_pipeline_core::SourcePipeBuilder;
use common_pipeline_sinks::processors::sinks::ContextSink;
use common_pipeline_transforms::processors::transforms::Transform;
use common_pipeline_transforms::processors::transforms::TransformLimit;
use common_pipeline_transforms::processors::transforms::Transformer;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::Projection;
use common_planners::ReadDataSourcePlan;
use common_planners::StageTableInfo;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_storages_util::storage_context::StorageContext;
use parking_lot::Mutex;
use tracing::info;

use super::StageSourceHelper;

pub struct StageTable {
    table_info: StageTableInfo,
    // This is no used but a placeholder.
    // But the Table trait need it:
    // fn get_table_info(&self) -> &TableInfo).
    table_info_placeholder: TableInfo,
}

impl StageTable {
    pub fn try_create(table_info: StageTableInfo) -> Result<Arc<dyn Table>> {
        let mut table_info_placeholder = TableInfo::default().set_schema(table_info.schema());
        table_info_placeholder.meta.engine = "STAGE".to_owned();

        Ok(Arc::new(Self {
            table_info,
            table_info_placeholder,
        }))
    }

    pub fn try_create_new(_ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>> {
        let stage_table_info = StageTableInfo {
            schema: table_info.schema(),
            stage_info: Default::default(),
            path: "".to_string(),
            files: vec![],
        };
        Ok(Box::new(Self {
            table_info: stage_table_info,
            table_info_placeholder: table_info,
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "STAGE".to_string(),
            comment: "Stage Storage Engine".to_string(),
            ..Default::default()
        }
    }
}

#[async_trait::async_trait]
impl Table for StageTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    // External stage has no table info yet.
    fn get_table_info(&self) -> &TableInfo {
        &self.table_info_placeholder
    }

    fn adhoc_schema(&self) -> bool {
        true
    }

    fn benefit_column_prune(&self) -> bool {
        true
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        Ok((Statistics::default(), vec![]))
    }

    fn read2(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &ReadDataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let settings = ctx.get_settings();
        let mut builder = SourcePipeBuilder::create();
        let table_info = &self.table_info;
        let schema = table_info.schema.clone();

        eprintln!("files are {:?}", table_info.files);
        eprintln!(
            "plan projection {:?}",
            plan.push_downs.as_ref().map(|e| &e.projection)
        );

        let projection = plan
            .push_downs
            .as_ref()
            .and_then(|e| e.projection.as_ref())
            .and_then(|p| match p {
                Projection::Columns(cols) => Some(cols),
                Projection::InnerColumns(_) => None,
            })
            .ok_or_else(|| ErrorCode::StorageOther("invalid projection"))?;

        let mut files_deque = VecDeque::with_capacity(table_info.files.len());
        for f in &table_info.files {
            files_deque.push_back(f.to_string());
        }

        files_deque.push_back("stage/test_stage/books.csv".to_owned());

        let files = Arc::new(Mutex::new(files_deque));

        let output_schema = Arc::new(schema.project(projection));

        // let stage_source =
        //    StageSourceHelper::try_create(ctx, stage_source_schema, table_info.clone(), files)?;
        let stage_source = StageSourceHelper::try_create(ctx, schema, table_info.clone(), files)?;

        for _index in 0..settings.get_max_threads()? {
            let output = OutputPort::create();
            builder.add_source(output.clone(), stage_source.get_splitter(output)?);
        }
        pipeline.add_pipe(builder.finalize());

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            stage_source.get_deserializer(transform_input_port, transform_output_port)
        })?;

        pipeline.add_transform(move |transform_input_port, transform_output_port| {
            Ok(Transformer::create(
                transform_input_port,
                transform_output_port,
                Appender {
                    output_schema: output_schema.clone(),
                },
            ))
        })?;

        let limit = self.table_info.stage_info.copy_options.size_limit;
        if limit > 0 {
            pipeline.resize(1)?;
            pipeline.add_transform(|transform_input_port, transform_output_port| {
                TransformLimit::try_create(
                    Some(limit),
                    0,
                    transform_input_port,
                    transform_output_port,
                )
            })?;
        }
        Ok(())
    }

    fn append2(&self, ctx: Arc<dyn TableContext>, pipeline: &mut Pipeline) -> Result<()> {
        let mut sink_pipeline_builder = SinkPipeBuilder::create();
        for _ in 0..pipeline.output_len() {
            let input_port = InputPort::create();
            sink_pipeline_builder.add_sink(
                input_port.clone(),
                ContextSink::create(input_port, ctx.clone()),
            );
        }
        pipeline.add_pipe(sink_pipeline_builder.finalize());
        Ok(())
    }

    // TODO use tmp file_name & rename to have atomic commit
    async fn commit_insertion(
        &self,
        ctx: Arc<dyn TableContext>,
        _catalog_name: &str,
        operations: Vec<DataBlock>,
        _overwrite: bool,
    ) -> Result<()> {
        let format_name = format!(
            "{:?}",
            self.table_info.stage_info.file_format_options.format
        );
        let path = format!(
            "{}{}.{}",
            self.table_info.path,
            uuid::Uuid::new_v4(),
            format_name.to_ascii_lowercase()
        );
        info!(
            "try commit stage table {} to file {path}",
            self.table_info.stage_info.stage_name
        );

        let op = StageSourceHelper::get_op(&ctx, &self.table_info.stage_info).await?;

        let fmt = OutputFormatType::from_str(format_name.as_str())?;
        let mut format_settings = ctx.get_format_settings()?;

        let format_options = &self.table_info.stage_info.file_format_options;
        {
            format_settings.skip_header = format_options.skip_header;
            if !format_options.field_delimiter.is_empty() {
                format_settings.field_delimiter =
                    format_options.field_delimiter.as_bytes().to_vec();
            }
            if !format_options.record_delimiter.is_empty() {
                format_settings.record_delimiter =
                    format_options.record_delimiter.as_bytes().to_vec();
            }
        }

        let mut output_format = fmt.create_format(self.table_info.schema(), format_settings);

        let prefix = output_format.serialize_prefix()?;
        let written_bytes: usize = operations.iter().map(|b| b.memory_size()).sum();
        let mut bytes = Vec::with_capacity(written_bytes + prefix.len());
        bytes.extend_from_slice(&prefix);
        for block in operations {
            let bs = output_format.serialize_block(&block)?;
            bytes.extend_from_slice(bs.as_slice());
        }

        let bs = output_format.finalize()?;
        bytes.extend_from_slice(bs.as_slice());

        ctx.get_dal_context()
            .get_metrics()
            .inc_write_bytes(bytes.len());

        let object = op.object(&path);
        object.write(bytes.as_slice()).await?;
        Ok(())
    }

    // Truncate the stage file.
    async fn truncate(
        &self,
        _ctx: Arc<dyn TableContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(
            "S3 external table truncate() unimplemented yet!",
        ))
    }
}

struct Appender {
    output_schema: DataSchemaRef,
}

impl Transform for Appender {
    const NAME: &'static str = "Appender";

    fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
        let num_rows = block.num_rows();
        let input_schema = block.schema().clone();

        eprintln!("input schema {:?}, rows {}", input_schema, num_rows);
        eprintln!("output schema {:?}", self.output_schema);
        use common_datavalues::DataType;
        let mut new_block = DataBlock::empty();

        for (i, f) in self.output_schema.fields().iter().enumerate() {
            if !input_schema.has_field(f.name()) {
                let default_value = f.data_type().default_value();
                let column = f
                    .data_type()
                    .create_constant_column(&default_value, num_rows)?;
                new_block = new_block.add_column(column, f.clone())?;
            } else {
                eprintln!("adding col from input block, len {}", block.column(i).len());
                new_block = new_block.add_column(block.column(i).clone(), f.clone())?;
            }
        }
        eprintln!("row of new_block is {}", new_block.num_rows());

        new_block.resort(self.output_schema.clone())
        //        eprintln!("row of r is {}", r.num_rows());
        //
        //        Ok(block)
    }
}
