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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::LargeStringArray;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchOptions;
use arrow_array::cast::AsArray;
use arrow_buffer::NullBuffer;
use arrow_buffer::OffsetBuffer;
use arrow_cast::cast;
use arrow_flight::FlightData;
use arrow_flight::SchemaAsIpc;
use arrow_ipc::writer::DictionaryTracker;
use arrow_ipc::writer::IpcDataGenerator;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::ArrowError;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field;
use arrow_schema::Schema as ArrowSchema;
use bytes::Bytes;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_column::binview::View;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::local_block_meta_serde;
use databend_common_expression::types::StringColumn;
use databend_common_io::prelude::BinaryWrite;
use databend_common_io::prelude::bincode_serialize_into_buf;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransformer;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;
use databend_common_pipeline_transforms::processors::UnknownMode;
use databend_common_settings::FlightCompression;

use crate::servers::flight::v1::exchange::ExchangeShuffleMeta;
use crate::servers::flight::v1::ipc_compression::compress_record_batch_lz4;
use crate::servers::flight::v1::ipc_compression::make_ipc_options;
use crate::servers::flight::v1::exchange::MergeExchangeParams;
use crate::servers::flight::v1::exchange::ShuffleExchangeParams;
use crate::servers::flight::v1::packets::DataPacket;
use crate::servers::flight::v1::packets::FragmentData;

fn elapsed_nanos(started: Instant) -> usize {
    started.elapsed().as_nanos().min(usize::MAX as u128) as usize
}

pub struct ExchangeSerializeMeta {
    pub block_number: isize,
    pub packet: Vec<DataPacket>,
}

impl ExchangeSerializeMeta {
    pub fn create(block_number: isize, packet: Vec<DataPacket>) -> BlockMetaInfoPtr {
        Box::new(ExchangeSerializeMeta {
            packet,
            block_number,
        })
    }
}

impl Debug for ExchangeSerializeMeta {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("ExchangeSerializeMeta").finish()
    }
}

local_block_meta_serde!(ExchangeSerializeMeta);

#[typetag::serde(name = "exchange_serialize")]
impl BlockMetaInfo for ExchangeSerializeMeta {}

pub struct TransformExchangeSerializer {
    options: IpcWriteOptions,
    native_lz4: bool,
}

impl TransformExchangeSerializer {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        _params: &MergeExchangeParams,
        compression: Option<FlightCompression>,
    ) -> Result<ProcessorPtr> {
        let (options, native_lz4) = make_ipc_options(compression)?;

        Ok(ProcessorPtr::create(Transformer::create(
            input,
            output,
            TransformExchangeSerializer {
                options,
                native_lz4,
            },
        )))
    }
}

impl Transform for TransformExchangeSerializer {
    const NAME: &'static str = "ExchangeSerializerTransform";

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        Profile::record_usize_profile(ProfileStatisticsName::ExchangeRows, data_block.num_rows());
        serialize_block(0, data_block, &self.options, self.native_lz4)
    }
}

pub struct TransformScatterExchangeSerializer {
    local_pos: usize,
    options: IpcWriteOptions,
    native_lz4: bool,
}

impl TransformScatterExchangeSerializer {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        compression: Option<FlightCompression>,
        params: &ShuffleExchangeParams,
    ) -> Result<ProcessorPtr> {
        let local_id = &params.executor_id;
        let (options, native_lz4) = make_ipc_options(compression)?;

        Ok(ProcessorPtr::create(BlockMetaTransformer::create(
            input,
            output,
            TransformScatterExchangeSerializer {
                options,
                native_lz4,
                local_pos: params
                    .destination_ids
                    .iter()
                    .position(|x| x == local_id)
                    .unwrap(),
            },
        )))
    }
}

impl BlockMetaTransform<ExchangeShuffleMeta> for TransformScatterExchangeSerializer {
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Error;
    const NAME: &'static str = "TransformScatterExchangeSerializer";

    fn transform(&mut self, meta: ExchangeShuffleMeta) -> Result<Vec<DataBlock>> {
        let mut new_blocks = Vec::with_capacity(meta.blocks.len());
        for (index, block) in meta.blocks.into_iter().enumerate() {
            new_blocks.push(match self.local_pos == index {
                true => block,
                // Remote partitions are already compacted by scatter_with_local.
                false => serialize_block(0, block, &self.options, self.native_lz4)?,
            });
        }

        Ok(vec![DataBlock::empty_with_meta(
            ExchangeShuffleMeta::create(new_blocks),
        )])
    }
}

pub fn serialize_block(
    block_num: isize,
    data_block: DataBlock,
    options: &IpcWriteOptions,
    native_lz4: bool,
) -> Result<DataBlock> {
    if data_block.is_empty() && data_block.get_meta().is_none() {
        return Ok(DataBlock::empty_with_meta(ExchangeSerializeMeta::create(
            block_num,
            vec![],
        )));
    }

    let mut meta = vec![];
    meta.write_scalar_own(data_block.num_rows() as u32)?;
    bincode_serialize_into_buf(&mut meta, &data_block.get_meta())
        .map_err(|_| ErrorCode::BadBytes("block meta serialize error when exchange"))?;

    let (_, dict, values) = match data_block.is_empty() {
        true => batches_to_flight_data_with_options(
            &ArrowSchema::empty(),
            vec![
                RecordBatch::try_new_with_options(
                    Arc::new(ArrowSchema::empty()),
                    vec![],
                    &RecordBatchOptions::new().with_row_count(Some(0)),
                )
                .unwrap(),
            ],
            options,
            native_lz4,
        )?,
        false => {
            // Build Flight wire batches directly from DataBlock columns so String
            // becomes LargeUtf8 in one pass. Going through Utf8View Arrow arrays
            // first creates view headers that we immediately discard, and the
            // zero-copy reuse path fails whenever any short (<=12B) string is
            // inlined — common after compact remote take.
            let (wire_schema, wire_batch) = encode_block_for_flight(data_block)?;
            batches_to_flight_data_with_options(&wire_schema, vec![wire_batch], options, native_lz4)?
        }
    };

    let mut packet = Vec::with_capacity(dict.len() + values.len());
    for dict_flight in dict {
        packet.push(DataPacket::Dictionary(dict_flight));
    }

    let meta: Bytes = meta.into();
    for value in values {
        packet.push(DataPacket::FragmentData(FragmentData::create(
            meta.clone(),
            value,
        )));
    }

    Ok(DataBlock::empty_with_meta(ExchangeSerializeMeta::create(
        block_num, packet,
    )))
}

/// Remap Arrow view types to contiguous LargeUtf8/LargeBinary for Flight IPC.
pub(crate) fn flight_wire_schema(schema: &ArrowSchema) -> ArrowSchema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| Arc::new(flight_wire_field(field.as_ref())))
        .collect::<Vec<_>>();
    ArrowSchema::new_with_metadata(fields, schema.metadata().clone())
}

fn flight_wire_field(field: &Field) -> Field {
    let data_type = match field.data_type() {
        // String columns are Utf8View locally; remap only this hot path.
        // Binary already uses LargeBinary, and arrow-cast 56 cannot cast BinaryView
        // to LargeBinary, so leave BinaryView unchanged.
        ArrowDataType::Utf8View => ArrowDataType::LargeUtf8,
        ArrowDataType::Struct(children) => {
            let children = children
                .iter()
                .map(|child| Arc::new(flight_wire_field(child.as_ref())))
                .collect::<Vec<_>>();
            ArrowDataType::Struct(children.into())
        }
        ArrowDataType::LargeList(child) => {
            ArrowDataType::LargeList(Arc::new(flight_wire_field(child.as_ref())))
        }
        ArrowDataType::List(child) => {
            ArrowDataType::List(Arc::new(flight_wire_field(child.as_ref())))
        }
        ArrowDataType::FixedSizeList(child, size) => {
            ArrowDataType::FixedSizeList(Arc::new(flight_wire_field(child.as_ref())), *size)
        }
        ArrowDataType::Map(child, sorted) => {
            ArrowDataType::Map(Arc::new(flight_wire_field(child.as_ref())), *sorted)
        }
        other => other.clone(),
    };
    Field::new(field.name(), data_type, field.is_nullable()).with_metadata(field.metadata().clone())
}

fn encode_block_for_flight(data_block: DataBlock) -> Result<(ArrowSchema, RecordBatch)> {
    let schema = data_block.infer_schema();
    let arrow_schema = ArrowSchema::from(&schema);
    let wire_schema = flight_wire_schema(&arrow_schema);

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(data_block.columns().len());
    for (entry, field) in data_block
        .take_columns()
        .into_iter()
        .zip(wire_schema.fields().iter())
    {
        let column = entry.to_column();
        columns.push(column_to_flight_array(column, field.data_type())?);
    }

    let wire_batch = RecordBatch::try_new(Arc::new(wire_schema.clone()), columns)
        .map_err(|err| ErrorCode::BadBytes(format!("flight LargeUtf8 batch failed: {err}")))?;
    Ok((wire_schema, wire_batch))
}

fn column_to_flight_array(column: Column, wire_type: &ArrowDataType) -> Result<ArrayRef> {
    match (column, wire_type) {
        (Column::String(col), ArrowDataType::LargeUtf8) => {
            Ok(string_column_to_large_utf8(col, None))
        }
        (Column::Nullable(n), ArrowDataType::LargeUtf8) => {
            let (inner, validity) = n.destructure();
            match inner {
                Column::String(col) => {
                    Ok(string_column_to_large_utf8(col, Some(validity.into())))
                }
                other => {
                    // Unexpected nested non-string under LargeUtf8 wire type.
                    let array = other.into_arrow_rs();
                    cast(array.as_ref(), wire_type).map_err(|err| {
                        ErrorCode::BadBytes(format!("flight LargeUtf8 encode failed: {err}"))
                    })
                }
            }
        }
        (other, _) => {
            // Non-string columns keep the existing Arrow conversion. maybe_gc is
            // still useful for rare nested string payloads that do not hit the
            // direct LargeUtf8 path above.
            let array = other.maybe_gc().into_arrow_rs();
            if array.data_type() == wire_type {
                Ok(array)
            } else if matches!(wire_type, ArrowDataType::LargeUtf8) {
                if let Some(converted) = try_utf8view_to_large_utf8_reuse_buffer(array.as_ref()) {
                    Ok(converted)
                } else {
                    cast(array.as_ref(), wire_type).map_err(|err| {
                        ErrorCode::BadBytes(format!("flight LargeUtf8 encode failed: {err}"))
                    })
                }
            } else {
                cast(array.as_ref(), wire_type).map_err(|err| {
                    ErrorCode::BadBytes(format!("flight wire encode failed: {err}"))
                })
            }
        }
    }
}

/// Build LargeUtf8 from a local StringColumn without first materializing Utf8View
/// Arrow ArrayData (which would allocate a discarded 16B/row view buffer).
fn string_column_to_large_utf8(col: StringColumn, nulls: Option<NullBuffer>) -> ArrayRef {
    // Fast path: compact remote take already packed every non-inline value into a
    // single contiguous buffer in row order with no inlined short strings.
    if nulls.as_ref().map(|n| n.null_count()).unwrap_or(0) == 0 {
        if let Some(reused) = try_string_column_reuse_large_utf8(&col) {
            return reused;
        }
    }

    let views = col.views().as_slice();
    let buffers = col.data_buffers().as_ref();
    let mut offsets: Vec<i64> = Vec::with_capacity(views.len() + 1);
    offsets.push(0);
    let mut values: Vec<u8> = Vec::with_capacity(col.total_bytes_len());
    for view in views {
        let bytes = unsafe { view.get_slice_unchecked(buffers) };
        values.extend_from_slice(bytes);
        offsets.push(values.len() as i64);
    }

    let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };
    // Source views already validated as UTF-8 StringColumn contents.
    let array = unsafe { LargeStringArray::new_unchecked(offsets, values.into(), nulls) };
    Arc::new(array)
}

fn try_string_column_reuse_large_utf8(col: &StringColumn) -> Option<ArrayRef> {
    let buffers = col.data_buffers();
    if buffers.len() != 1 {
        return None;
    }
    let data = &buffers[0];
    let mut offsets: Vec<i64> = Vec::with_capacity(col.len() + 1);
    offsets.push(0);
    let mut expected_offset: u32 = 0;
    for view in col.views().as_slice() {
        if view.length <= View::MAX_INLINE_SIZE {
            return None;
        }
        if view.buffer_idx != 0 || view.offset != expected_offset {
            return None;
        }
        expected_offset = expected_offset.checked_add(view.length)?;
        offsets.push(expected_offset as i64);
    }
    if expected_offset as usize != data.len() {
        return None;
    }
    let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };
    let values: arrow_buffer::Buffer = data.clone().into();
    let array = unsafe { LargeStringArray::new_unchecked(offsets, values, None) };
    Some(Arc::new(array))
}

/// Convert compacted Utf8View Arrow arrays into LargeUtf8 by reusing the data
/// buffer when safe. Kept as a fallback for nested / unexpected Utf8View inputs.
///
/// Safety conditions (all must hold):
/// - no nulls
/// - exactly one data buffer
/// - every view is non-inline and points into buffer 0
/// - views are packed contiguously from offset 0 in row order
fn try_utf8view_to_large_utf8_reuse_buffer(array: &dyn arrow_array::Array) -> Option<ArrayRef> {
    if array.data_type() != &ArrowDataType::Utf8View || array.null_count() != 0 {
        return None;
    }

    let views = array.as_string_view();
    let buffers = views.data_buffers();
    if buffers.len() != 1 {
        return None;
    }

    let data = &buffers[0];
    let mut offsets: Vec<i64> = Vec::with_capacity(views.len() + 1);
    offsets.push(0);
    let mut expected_offset: u32 = 0;

    for view in views.views().iter() {
        let length = *view as u32;
        // Inline views keep payload inside the u128; cannot reuse data buffer.
        if length <= 12 {
            return None;
        }
        let buffer_index = (*view >> 64) as u32;
        let offset = (*view >> 96) as u32;
        if buffer_index != 0 || offset != expected_offset {
            return None;
        }
        expected_offset = expected_offset.checked_add(length)?;
        offsets.push(expected_offset as i64);
    }

    if expected_offset as usize != data.len() {
        return None;
    }

    let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };
    // Source was already Utf8View, so UTF-8 validation is redundant.
    let array = unsafe { LargeStringArray::new_unchecked(offsets, data.clone(), None) };
    Some(Arc::new(array))
}

/// Convert `RecordBatch`es to wire protocol `FlightData`s
/// Returns schema, dictionaries and flight data
pub fn batches_to_flight_data_with_options(
    schema: &ArrowSchema,
    batches: Vec<RecordBatch>,
    options: &IpcWriteOptions,
    native_lz4: bool,
) -> std::result::Result<(FlightData, Vec<FlightData>, Vec<FlightData>), ArrowError> {
    let schema_flight_data: FlightData = SchemaAsIpc::new(schema, options).into();
    let mut dictionaries = Vec::with_capacity(batches.len());
    let mut flight_data = Vec::with_capacity(batches.len());

    let data_gen = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(false);

    for batch in batches.iter() {
        let ipc_encode_started = Instant::now();
        let (encoded_dictionaries, mut encoded_batch) =
            data_gen.encoded_batch(batch, &mut dictionary_tracker, options)?;
        Profile::record_usize_profile(
            ProfileStatisticsName::ExchangeIpcEncodeTime,
            elapsed_nanos(ipc_encode_started),
        );
        if native_lz4 {
            let lz4_started = Instant::now();
            encoded_batch = compress_record_batch_lz4(encoded_batch)?;
            Profile::record_usize_profile(
                ProfileStatisticsName::ExchangeLz4CompressTime,
                elapsed_nanos(lz4_started),
            );
        }

        dictionaries.extend(encoded_dictionaries.into_iter().map(Into::into));
        flight_data.push(encoded_batch.into());
    }
    Ok((schema_flight_data, dictionaries, flight_data))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_flight::utils::flight_data_to_arrow_batch;
    use arrow_ipc::CompressionType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::UInt64Type;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;

    use super::*;
    use crate::servers::flight::v1::exchange::serde::exchange_deserializer::flight_data_to_arrow_batch_zero_copy;

    #[test]
    fn test_exchange_lz4_string_view_zero_copy_roundtrip() {
        let strings = StringType::from_data(vec![
            "a repeated string value long enough to use an external StringView buffer";
            256
        ]);
        let numbers = UInt64Type::from_data((0..256_u64).collect::<Vec<_>>());
        let block = DataBlock::new_from_columns(vec![strings, numbers]);
        let schema = block.infer_schema();
        let arrow_schema = ArrowSchema::from(&schema);
        let original = block.to_record_batch_with_dataschema(&schema).unwrap();
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))
            .unwrap();

        let (_, dictionaries, batches) =
            batches_to_flight_data_with_options(&arrow_schema, vec![original.clone()], &options, false)
                .unwrap();
        assert!(dictionaries.is_empty());
        assert_eq!(batches.len(), 1);

        let decoded = flight_data_to_arrow_batch(
            &batches[0],
            Arc::new(arrow_schema.clone()),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(decoded, original);

        let decoded = flight_data_to_arrow_batch_zero_copy(
            &batches[0],
            Arc::new(arrow_schema),
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_exchange_serialize_block_large_utf8_wire_roundtrip() {
        use crate::servers::flight::v1::exchange::serde::exchange_deserializer::deserialize_block;

        // Mix inline and non-inline strings so the direct StringColumn -> LargeUtf8
        // path must copy short values out of view headers.
        let values = vec![
            "short",
            "a repeated string value long enough to use an external StringView buffer",
            "",
            "another-non-inline-string-payload-xxxxxx",
        ];
        let strings = StringType::from_data(values.clone());
        let numbers = UInt64Type::from_data((0..values.len() as u64).collect::<Vec<_>>());
        let block = DataBlock::new_from_columns(vec![strings, numbers]);
        // Compact first so long values sit in a contiguous buffer; short values stay
        // inlined and exercise the general one-pass copy path.
        let block = block.compact_string_buffers();
        let schema = Arc::new(block.infer_schema());
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))
            .unwrap();

        let (wire_schema, wire_batch) = encode_block_for_flight(block.clone()).unwrap();
        assert_eq!(wire_schema.field(0).data_type(), &ArrowDataType::LargeUtf8);
        assert_eq!(wire_batch.column(0).data_type(), &ArrowDataType::LargeUtf8);
        assert_eq!(
            wire_batch
                .column(0)
                .as_string::<i64>()
                .value(0),
            "short"
        );

        let (_, dictionaries, batches) =
            batches_to_flight_data_with_options(&wire_schema, vec![wire_batch], &options, false)
                .unwrap();
        assert!(dictionaries.is_empty());
        assert_eq!(batches.len(), 1);

        let fragment = FragmentData::create(bytes::Bytes::from(vec![0u8; 5]), batches[0].clone());
        let decoded = deserialize_block(
            vec![],
            fragment,
            schema.as_ref(),
            Arc::new(wire_schema),
        )
        .unwrap();
        assert_eq!(decoded.num_rows(), block.num_rows());
        assert_eq!(decoded.columns().len(), block.columns().len());
        assert_eq!(
            decoded.columns()[0].to_column(),
            block.columns()[0].to_column()
        );
        assert_eq!(
            decoded.columns()[1].to_column(),
            block.columns()[1].to_column()
        );
    }

    #[test]
    fn test_string_column_to_large_utf8_reuses_compact_buffer() {
        let values = vec![
            "a repeated string value long enough to use an external StringView buffer";
            8
        ];
        let block = DataBlock::new_from_columns(vec![StringType::from_data(values)])
            .compact_string_buffers();
        let Column::String(col) = block.columns()[0].to_column() else {
            panic!("expected string column");
        };
        let reused = try_string_column_reuse_large_utf8(&col)
            .expect("compact non-inline StringColumn should reuse values buffer");
        assert_eq!(reused.data_type(), &ArrowDataType::LargeUtf8);
        assert_eq!(reused.len(), 8);
    }

    #[test]
    fn test_string_column_to_large_utf8_copies_mixed_inline() {
        let values = vec!["short", "a repeated string value long enough for external buffer"];
        let block = DataBlock::new_from_columns(vec![StringType::from_data(values.clone())])
            .compact_string_buffers();
        let (wire_schema, wire_batch) = encode_block_for_flight(block).unwrap();
        assert_eq!(wire_schema.field(0).data_type(), &ArrowDataType::LargeUtf8);
        let arr = wire_batch.column(0).as_string::<i64>();
        assert_eq!(arr.value(0), "short");
        assert_eq!(arr.value(1), "a repeated string value long enough for external buffer");
    }
}
