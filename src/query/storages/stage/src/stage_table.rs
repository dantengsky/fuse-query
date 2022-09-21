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
use std::io;
use std::str::FromStr;
use std::sync::Arc;

use common_catalog::catalog::StorageDescription;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_datavalues::chrono::TimeZone;
use common_datavalues::chrono::Utc;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataTypeImpl;
use common_datavalues::StringType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::output_format::OutputFormatType;
use common_meta_app::schema::TableInfo;
use common_meta_types::StageFile;
use common_meta_types::StageFileFormatType;
use common_meta_types::UserStageInfo;
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
use common_planners::PartInfo;
use common_planners::Partitions;
use common_planners::Projection;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;
use common_planners::StageTableInfo;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_storages_util::storage_context::StorageContext;
use futures::TryStreamExt;
use parking_lot::Mutex;
use regex::Regex;
use tracing::info;
use tracing::warn;

use super::StageSourceHelper;

pub const ENGINE_STAGE: &str = "STAGE";
const STAGE_ENGINE_OPT_KEY_ARTIFICIAL_SCHEMA: &str = "ARTIFICIAL_SCHEMA";

pub enum StageMode {
    /// schema and the files to be scanned are all settled
    Settled(StageTableInfo),
    /// only know the stage to be scanned, no schema provided
    /// also the files under stage to be scanned need to be deduced
    UnSettled(UserStageInfo),
}
// TODO A enum to hold the StageTableInfo or the UserStageInfo (the later has no schema, which indicates a artificial schema)
pub struct StageTable {
    stage_table_info: StageTableInfo,
    // This is no used but a placeholder.
    // But the Table trait need it:
    // fn get_table_info(&self) -> &TableInfo).
    table_info_placeholder: TableInfo,
    artificial_schema: bool,
}

impl StageTable {
    /// Construct stage table instance by using StageTableInfo.
    ///
    /// Schema of StageTableInfo will be used
    pub fn with_stage_table_info(stage_info: StageTableInfo) -> Result<Arc<dyn Table>> {
        eprintln!("using with_stage_table_info");
        let mut table_info_placeholder = TableInfo::default().set_schema(stage_info.schema());
        table_info_placeholder.meta.engine = ENGINE_STAGE.to_owned();

        Ok(Arc::new(Self {
            stage_table_info: stage_info,
            table_info_placeholder,
            artificial_schema: false,
        }))
    }

    // Construct stage table instance by using UserStageInfo only.
    //
    // In this case, the schema of this table is not provided, an artificial schema will be
    // generated and used.
    pub fn with_stage_info(
        user_stage_info: UserStageInfo,
        path: &str,
        stage_files: &[String],
    ) -> Result<Arc<dyn Table>> {
        // generate artificial schema
        let artificial_schema = user_stage_info
            .file_format_options
            .format
            .artificial_schema()?;
        let mut table_info_placeholder = TableInfo::default();
        table_info_placeholder.meta.engine = ENGINE_STAGE.to_owned();
        table_info_placeholder = table_info_placeholder.set_schema(artificial_schema.clone());

        // table_info is supported to be serialized and passed to other nodes in distributed mode
        table_info_placeholder.meta.engine_options.insert(
            STAGE_ENGINE_OPT_KEY_ARTIFICIAL_SCHEMA.to_owned(),
            "".to_owned(),
        );

        let files = stage_files.to_vec();

        eprintln!("files during construction {:?}", &files);

        let stage_table_info = StageTableInfo {
            schema: artificial_schema,
            stage_info: user_stage_info,
            path: path.to_owned(),
            files,
        };

        Ok(Arc::new(Self {
            stage_table_info,
            table_info_placeholder,
            artificial_schema: true,
        }))
    }

    /// This is the constructor that being used by [StorageFactory]
    pub fn try_create(_ctx: StorageContext, mut table_info: TableInfo) -> Result<Box<dyn Table>> {
        eprintln!("using try_create");
        let artificial_schema = table_info
            .meta
            .engine_options
            .contains_key(STAGE_ENGINE_OPT_KEY_ARTIFICIAL_SCHEMA);
        eprintln!("is artificial schema {}", artificial_schema);

        // TODO re-consider this
        table_info
            .meta
            .engine_options
            .remove(STAGE_ENGINE_OPT_KEY_ARTIFICIAL_SCHEMA);

        let stage_table_info = StageTableInfo {
            schema: table_info.schema(),
            stage_info: Default::default(),
            path: "".to_string(),
            files: vec![],
        };
        Ok(Box::new(Self {
            stage_table_info: stage_table_info,
            table_info_placeholder: table_info,
            artificial_schema,
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: ENGINE_STAGE.to_string(),
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

    fn artificial_schema(&self) -> bool {
        true
    }

    fn benefit_column_prune(&self) -> bool {
        true
    }

    fn support_source_plan(&self) -> bool {
        true
    }

    async fn source_plan(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<Extras>,
    ) -> Result<ReadDataSourcePlan> {
        eprintln!("using read source plan");
        if self.artificial_schema {
            let stage_info = &self.stage_table_info.stage_info;
            let path = &self.stage_table_info.path;
            let prefix = stage_info.get_prefix();
            let path = format!("{prefix}{path}");
            // TODO pass pattern
            let files = list_files_from_dal(&ctx, stage_info, &path, "").await?;
            let parts = files
                .into_iter()
                .map(|staged_file| {
                    let part_info: Box<dyn PartInfo> = Box::new(StageTablePartInfo {
                        location: staged_file.path,
                    });
                    Arc::new(part_info)
                })
                .collect::<_>();
            Ok((Statistics::default(), parts))

            Ok(ReadDataSourcePlan {
                catalog,
                source_info: SourceInfo::TableSource(table_info.clone()),
                scan_fields,
                parts,
                statistics,
                description,
                tbl_args: self.table_args(),
                push_downs,
            })
        } else {
            Ok((Statistics::default(), vec![]))
        }
    }

    // TODO unify the logics while merging with PR https://github.com/datafuselabs/databend/pull/7613
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        // TODO use list_files after merge with current main
        eprintln!(
            "read partitions, self stage info {:?}",
            self.stage_table_info
        );
        if self.artificial_schema {
            let stage_info = &self.stage_table_info.stage_info;
            let path = &self.stage_table_info.path;
            let prefix = stage_info.get_prefix();
            let path = format!("{prefix}{path}");
            // TODO pass pattern
            let files = list_files_from_dal(&ctx, stage_info, &path, "").await?;
            let parts = files
                .into_iter()
                .map(|staged_file| {
                    let part_info: Box<dyn PartInfo> = Box::new(StageTablePartInfo {
                        location: staged_file.path,
                    });
                    Arc::new(part_info)
                })
                .collect::<_>();
            Ok((Statistics::default(), parts))
        } else {
            Ok((Statistics::default(), vec![]))
        }
    }

    fn read2(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &ReadDataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let settings = ctx.get_settings();
        let mut builder = SourcePipeBuilder::create();
        let table_info = &self.stage_table_info;
        let schema = table_info.schema.clone();

        let files = plan
            .parts
            .iter()
            .map(|part| {
                part.as_any()
                    .downcast_ref::<StageTablePartInfo>()
                    .ok_or_else(|| {
                        ErrorCode::LogicalError(
                            "Cannot downcast from PartInfo to StageTablePartInfo.",
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        // NOTE the plan.source_info is of type SourceInfo::TableSource in
        if let SourceInfo::StageSource(s) = &plan.source_info {
            eprintln!("source plan path {:?}, files {:?}", s.path, s.files);
        } else {
            eprintln!("shit plan is not stage source");
        }

        let mut files_deque = VecDeque::with_capacity(files.len());
        for f in files {
            files_deque.push_back(f.location.clone());
        }

        let transform_projection = if self.artificial_schema {
            let projection = plan
                .push_downs
                .as_ref()
                .and_then(|e| e.projection.as_ref())
                .and_then(|p| match p {
                    Projection::Columns(cols) => Some(cols),
                    Projection::InnerColumns(_) => None,
                })
                .ok_or_else(|| ErrorCode::StorageOther("invalid projection"))?;
            let output_schema = Arc::new(schema.project(projection));

            // TODO add another stage, to list all the files
            // files_deque.push_back("stage/test_stage/books.csv".to_owned());
            let reshape_artificial_schema = move |transform_input_port, transform_output_port| {
                Ok(Transformer::create(
                    transform_input_port,
                    transform_output_port,
                    ReshapeArtificialSchema {
                        output_schema: output_schema.clone(),
                    },
                ))
            };
            Some(reshape_artificial_schema)
        } else {
            None
        };

        let files = Arc::new(Mutex::new(files_deque));

        // let stage_source =
        //    StageSourceHelper::try_create(ctx, stage_source_schema, table_info.clone(), files)?;
        let artificial_schema = self.artificial_schema;
        let stage_source = StageSourceHelper::try_create(
            ctx,
            schema,
            artificial_schema,
            table_info.clone(),
            files,
        )?;

        for _index in 0..settings.get_max_threads()? {
            let output = OutputPort::create();
            builder.add_source(output.clone(), stage_source.get_splitter(output)?);
        }
        pipeline.add_pipe(builder.finalize());

        pipeline.add_transform(|transform_input_port, transform_output_port| {
            stage_source.get_deserializer(transform_input_port, transform_output_port)
        })?;

        if let Some(transform) = transform_projection {
            pipeline.add_transform(transform)?;
        };

        let limit = self.stage_table_info.stage_info.copy_options.size_limit;
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
            self.stage_table_info.stage_info.file_format_options.format
        );
        let path = format!(
            "{}{}.{}",
            self.stage_table_info.path,
            uuid::Uuid::new_v4(),
            format_name.to_ascii_lowercase()
        );
        info!(
            "try commit stage table {} to file {path}",
            self.stage_table_info.stage_info.stage_name
        );

        let op = StageSourceHelper::get_op(&ctx, &self.stage_table_info.stage_info).await?;

        let fmt = OutputFormatType::from_str(format_name.as_str())?;
        let mut format_settings = ctx.get_format_settings()?;

        let format_options = &self.stage_table_info.stage_info.file_format_options;
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

        let mut output_format = fmt.create_format(self.stage_table_info.schema(), format_settings);

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

struct ReshapeArtificialSchema {
    output_schema: DataSchemaRef,
}

// TODO note for json format, we do not need this transform
impl Transform for ReshapeArtificialSchema {
    const NAME: &'static str = "Appender";

    fn transform(&mut self, block: DataBlock) -> Result<DataBlock> {
        let num_rows = block.num_rows();
        let input_schema = block.schema().clone();

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
                new_block = new_block.add_column(block.column(i).clone(), f.clone())?;
            }
        }
        new_block.resort(self.output_schema.clone())
    }
}

trait ArtificialSchema {
    fn artificial_schema(&self) -> Result<DataSchemaRef>;
}

impl ArtificialSchema for StageFileFormatType {
    fn artificial_schema(&self) -> Result<DataSchemaRef> {
        match self {
            StageFileFormatType::Csv | StageFileFormatType::Tsv | StageFileFormatType::Parquet => {
                // for formats that likely to be multiple columns,
                // gives at most 1024 string virtual columns
                let fields = (1..=1024)
                    .into_iter()
                    .map(|i| {
                        DataField::new_nullable(
                            format!("${i}").as_str(),
                            DataTypeImpl::String(StringType {}),
                        )
                    })
                    .collect::<Vec<_>>();
                Ok(DataSchemaRefExt::create(fields))
            }
            StageFileFormatType::Json | StageFileFormatType::NdJson => {
                // one string columns only
                let field = DataField::new_nullable("$1", DataTypeImpl::String(StringType {}));
                Ok(DataSchemaRefExt::create(vec![field]))
            }
            f => Err(ErrorCode::UnImplement(format!(
                "Artificial schema of format {:?} , not implemented yet",
                f
            ))),
        }
    }
}

pub async fn list_files_from_dal(
    ctx: &Arc<dyn TableContext>,
    stage: &UserStageInfo,
    path: &str,
    pattern: &str,
) -> Result<Vec<StageFile>> {
    let rename_me_qry_ctx: Arc<dyn TableContext> = ctx.clone();
    let op = StageSourceHelper::get_op(&rename_me_qry_ctx, stage).await?;
    let mut files = Vec::new();

    // - If the path itself is a dir, return directly.
    // - Otherwise, return a path suffix by `/`
    // - If other errors happen, we will ignore them by returning None.
    let dir_path = match op.object(path).metadata().await {
        Ok(meta) if meta.mode().is_dir() => Some(path.to_string()),
        Ok(meta) if !meta.mode().is_dir() => {
            files.push((path.to_string(), meta));

            Some(format!("{path}/"))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Some(format!("{path}/")),
        Err(e) => return Err(e.into()),
        _ => None,
    };

    // Check the if this dir valid and list it recursively.
    if let Some(dir) = dir_path {
        match op.object(&dir).metadata().await {
            Ok(_) => {
                let mut ds = op.batch().walk_top_down(&dir)?;
                while let Some(de) = ds.try_next().await? {
                    if de.mode().is_file() {
                        let path = de.path().to_string();
                        let meta = de.metadata().await?;
                        files.push((path, meta));
                    }
                }
            }
            Err(e) => warn!("ignore listing {path}/, because: {:?}", e),
        };
    }

    let regex = if !pattern.is_empty() {
        Some(Regex::new(pattern).map_err(|e| {
            ErrorCode::SyntaxException(format!(
                "Pattern format invalid, got:{}, error:{:?}",
                pattern, e
            ))
        })?)
    } else {
        None
    };

    let matched_files = files
        .iter()
        .filter(|(name, _meta)| {
            if let Some(regex) = &regex {
                regex.is_match(name)
            } else {
                true
            }
        })
        .cloned()
        .map(|(name, meta)| StageFile {
            path: name,
            size: meta.content_length(),
            md5: meta.content_md5().map(str::to_string),
            last_modified: meta
                .last_modified()
                .map_or(Utc::now(), |t| Utc.timestamp(t.unix_timestamp(), 0)),
            creator: None,
        })
        .collect::<Vec<StageFile>>();
    Ok(matched_files)
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct StageTablePartInfo {
    pub location: String,
}

#[typetag::serde(name = "stage")]
impl PartInfo for StageTablePartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<StageTablePartInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }
}
