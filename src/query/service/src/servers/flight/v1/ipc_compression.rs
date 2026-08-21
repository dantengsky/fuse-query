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

use std::cmp::min;
use std::io::Read;
use std::io::Write;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_buffer::MutableBuffer;
use arrow_buffer::bit_util;
use arrow_data::ArrayData;
use arrow_ipc::BodyCompressionBuilder;
use arrow_ipc::BodyCompressionMethod;
use arrow_ipc::Buffer as IpcBuffer;
use arrow_ipc::CompressionType;
use arrow_ipc::FieldNode;
use arrow_ipc::MessageBuilder;
use arrow_ipc::MessageHeader;
use arrow_ipc::MetadataVersion;
use arrow_ipc::RecordBatchBuilder;
use arrow_ipc::root_as_message;
use arrow_ipc::writer::EncodedData;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use databend_common_settings::FlightCompression;
use flatbuffers::FlatBufferBuilder;

const IPC_ALIGNMENT: usize = 64;
const PADDING: [u8; 64] = [0; 64];

pub(crate) fn make_ipc_options(
    compression: Option<FlightCompression>,
) -> Result<(IpcWriteOptions, bool), ArrowError> {
    let (compression, native_lz4) = match compression {
        // Keep arrow compression off for LZ4 so we own the C-liblz4 path and
        // can emit the body in one pass (no lz4_flex, no uncompressed staging).
        Some(FlightCompression::Lz4) => (None, true),
        Some(FlightCompression::Zstd) => (Some(CompressionType::ZSTD), false),
        None => (None, false),
    };
    let options = IpcWriteOptions::default().try_with_compression(compression)?;
    Ok((options, native_lz4))
}

fn ipc_error(message: impl Into<String>) -> ArrowError {
    ArrowError::IpcError(message.into())
}

fn pad_to_alignment(alignment: usize, len: usize) -> usize {
    let a = alignment.saturating_sub(1);
    ((len + a) & !a) - len
}

fn store_uncompressed_lz4_buffer(input: &[u8], output: &mut Vec<u8>) -> Result<usize, ArrowError> {
    let start = output.len();
    output.extend_from_slice(&(-1_i64).to_le_bytes());
    output.extend_from_slice(input);
    Ok(output.len() - start)
}

fn compress_lz4_buffer(input: &[u8], output: &mut Vec<u8>) -> Result<usize, ArrowError> {
    if input.is_empty() {
        return Ok(0);
    }

    // Tiny buffers are not worth a frame round-trip on localhost / low-latency links.
    const MIN_COMPRESS_BYTES: usize = 4 * 1024;
    if input.len() < MIN_COMPRESS_BYTES {
        return store_uncompressed_lz4_buffer(input, output);
    }

    let start = output.len();
    let uncompressed_len = i64::try_from(input.len())
        .map_err(|error| ipc_error(format!("IPC buffer is too large: {error}")))?;
    output.extend_from_slice(&uncompressed_len.to_le_bytes());
    {
        let block_size = if input.len() > 256 * 1024 {
            lz4::BlockSize::Max4MB
        } else if input.len() > 64 * 1024 {
            lz4::BlockSize::Max256KB
        } else {
            lz4::BlockSize::Max64KB
        };
        let mut encoder = lz4::EncoderBuilder::new()
            .block_size(block_size)
            .block_checksum(lz4::liblz4::BlockChecksum::NoBlockChecksum)
            .checksum(lz4::ContentChecksum::NoChecksum)
            .build(&mut *output)?;
        encoder.write_all(input)?;
        let (_, result) = encoder.finish();
        result?;
    }

    // Including the 8-byte length prefix, keep uncompressed when compression does not shrink.
    if output.len() - start >= input.len() + 8 {
        output.truncate(start);
        return store_uncompressed_lz4_buffer(input, output);
    }
    Ok(output.len() - start)
}

fn write_lz4_ipc_buffer(
    buffer: &[u8],
    buffers: &mut Vec<IpcBuffer>,
    arrow_data: &mut Vec<u8>,
    offset: i64,
) -> Result<i64, ArrowError> {
    let start = arrow_data.len();
    let written = compress_lz4_buffer(buffer, arrow_data)?;
    let len = i64::try_from(written)
        .map_err(|error| ipc_error(format!("compressed IPC buffer is too large: {error}")))?;
    buffers.push(IpcBuffer::new(offset, len));
    let pad_len = pad_to_alignment(IPC_ALIGNMENT, arrow_data.len() - start);
    arrow_data.extend_from_slice(&PADDING[..pad_len]);
    Ok(offset + len + pad_len as i64)
}

fn has_validity_bitmap(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::Null | DataType::Union(_, _) | DataType::RunEndEncoded(_, _)
    )
}

fn append_variadic_buffer_counts(counts: &mut Vec<i64>, array: &ArrayData) {
    match array.data_type() {
        DataType::BinaryView | DataType::Utf8View => {
            // Spec counts only variadic data buffers, not the views/null buffers.
            counts.push(array.buffers().len().saturating_sub(1) as i64);
            for child in array.child_data() {
                append_variadic_buffer_counts(counts, child);
            }
        }
        _ => {
            for child in array.child_data() {
                append_variadic_buffer_counts(counts, child);
            }
        }
    }
}

fn large_utf8_buffers(data: &ArrayData) -> Result<(Buffer, Buffer), ArrowError> {
    if data.buffers().len() < 2 {
        return Err(ipc_error("LargeUtf8/LargeBinary array missing buffers"));
    }
    if data.is_empty() {
        return Ok((MutableBuffer::new(0).into(), MutableBuffer::new(0).into()));
    }

    let offsets = data.buffers()[0].clone();
    let offsets_slice = offsets.typed_data::<i64>();
    let start = data.offset();
    let end = start + data.len() + 1;
    if end > offsets_slice.len() {
        return Err(ipc_error("LargeUtf8/LargeBinary offsets out of bounds"));
    }
    let window = &offsets_slice[start..end];
    let first = window[0];
    let last = *window.last().unwrap();
    if first < 0 || last < first {
        return Err(ipc_error("LargeUtf8/LargeBinary offsets are invalid"));
    }
    let values_start = first as usize;
    let values_len = (last - first) as usize;
    let values = data.buffers()[1].slice_with_length(values_start, values_len);

    if first == 0 && start == 0 {
        let size = std::mem::size_of::<i64>();
        let sliced_offsets = offsets.slice_with_length(0, (data.len() + 1) * size);
        return Ok((sliced_offsets, values));
    }

    let rebased: Vec<i64> = window.iter().map(|x| *x - first).collect();
    Ok((Buffer::from_vec(rebased), values))
}

fn write_array_data_native_lz4(
    array_data: &ArrayData,
    buffers: &mut Vec<IpcBuffer>,
    arrow_data: &mut Vec<u8>,
    nodes: &mut Vec<FieldNode>,
    mut offset: i64,
    num_rows: usize,
    null_count: usize,
) -> Result<i64, ArrowError> {
    if !matches!(array_data.data_type(), DataType::Null) {
        nodes.push(FieldNode::new(num_rows as i64, null_count as i64));
    } else {
        nodes.push(FieldNode::new(num_rows as i64, num_rows as i64));
    }

    if has_validity_bitmap(array_data.data_type()) {
        let null_buffer = match array_data.nulls() {
            None => {
                let num_bytes = bit_util::ceil(num_rows, 8);
                let buffer = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
                buffer.into()
            }
            Some(buffer) => buffer.inner().sliced(),
        };
        offset = write_lz4_ipc_buffer(null_buffer.as_slice(), buffers, arrow_data, offset)?;
    }

    match array_data.data_type() {
        DataType::Null => {}
        DataType::Boolean => {
            if array_data.buffers().len() != 1 {
                return Err(ipc_error("Boolean array should have one buffer"));
            }
            let buffer = array_data.buffers()[0].bit_slice(array_data.offset(), array_data.len());
            offset = write_lz4_ipc_buffer(&buffer, buffers, arrow_data, offset)?;
        }
        DataType::Utf8 | DataType::Binary => {
            return Err(ipc_error(
                "Flight native LZ4 encoder expects LargeUtf8/LargeBinary, not Utf8/Binary",
            ));
        }
        DataType::LargeUtf8 | DataType::LargeBinary => {
            let (offsets, values) = large_utf8_buffers(array_data)?;
            offset = write_lz4_ipc_buffer(offsets.as_slice(), buffers, arrow_data, offset)?;
            offset = write_lz4_ipc_buffer(values.as_slice(), buffers, arrow_data, offset)?;
        }
        DataType::BinaryView | DataType::Utf8View => {
            for buffer in array_data.buffers() {
                offset = write_lz4_ipc_buffer(buffer.as_slice(), buffers, arrow_data, offset)?;
            }
        }
        DataType::FixedSizeBinary(width) => {
            if array_data.buffers().len() != 1 {
                return Err(ipc_error("FixedSizeBinary should have one buffer"));
            }
            let byte_width = *width as usize;
            let buffer = &array_data.buffers()[0];
            let min_length = array_data.len() * byte_width;
            let byte_offset = array_data.offset() * byte_width;
            let buffer_length = min(min_length, buffer.len().saturating_sub(byte_offset));
            let slice = &buffer.as_slice()[byte_offset..(byte_offset + buffer_length)];
            offset = write_lz4_ipc_buffer(slice, buffers, arrow_data, offset)?;
        }
        dt if DataType::is_primitive(dt) => {
            if array_data.buffers().len() != 1 {
                return Err(ipc_error("primitive array should have one buffer"));
            }
            let buffer = &array_data.buffers()[0];
            let byte_width = dt.primitive_width().ok_or_else(|| {
                ipc_error(format!("missing primitive width for data type {dt:?}"))
            })?;
            let min_length = array_data.len() * byte_width;
            let byte_offset = array_data.offset() * byte_width;
            let buffer_length = min(min_length, buffer.len().saturating_sub(byte_offset));
            let slice = &buffer.as_slice()[byte_offset..(byte_offset + buffer_length)];
            offset = write_lz4_ipc_buffer(slice, buffers, arrow_data, offset)?;
        }
        DataType::Dictionary(_, _) => {
            // Dictionary values travel as separate Flight dictionary batches.
            // Here we only write the keys buffer(s).
            for buffer in array_data.buffers() {
                offset = write_lz4_ipc_buffer(buffer.as_slice(), buffers, arrow_data, offset)?;
            }
        }
        _ => {
            // List/Struct/Map/Union/etc: write own buffers then recurse children.
            for buffer in array_data.buffers() {
                offset = write_lz4_ipc_buffer(buffer.as_slice(), buffers, arrow_data, offset)?;
            }
            for child in array_data.child_data() {
                offset = write_array_data_native_lz4(
                    child,
                    buffers,
                    arrow_data,
                    nodes,
                    offset,
                    child.len(),
                    child.null_count(),
                )?;
            }
        }
    }

    Ok(offset)
}

/// Encode a RecordBatch to Flight IPC bytes with native C-LZ4 in one pass.
///
/// Unlike the previous two-phase path (uncompressed `encoded_batch` then
/// `compress_record_batch_lz4`), each IPC buffer is compressed directly into
/// the final body — matching the v1.2.636 `common_arrow` write behaviour.
pub(crate) fn encode_record_batch_native_lz4(
    batch: &RecordBatch,
) -> Result<EncodedData, ArrowError> {
    let mut nodes: Vec<FieldNode> = Vec::with_capacity(batch.num_columns());
    let mut buffers: Vec<IpcBuffer> = Vec::new();
    // Lower bound: compressed payload is usually << uncompressed, but reserve
    // something sane to avoid tiny geometric growth on the first buffers.
    let mut estimated = 0usize;
    for array in batch.columns() {
        let data = array.to_data();
        for buffer in data.buffers() {
            estimated = estimated.saturating_add(buffer.len() / 2).saturating_add(64);
        }
    }
    let mut arrow_data = Vec::with_capacity(estimated.max(4096));
    let mut offset = 0_i64;
    let mut variadic_buffer_counts = Vec::new();

    for array in batch.columns() {
        let array_data = array.to_data();
        offset = write_array_data_native_lz4(
            &array_data,
            &mut buffers,
            &mut arrow_data,
            &mut nodes,
            offset,
            array.len(),
            array.null_count(),
        )?;
        append_variadic_buffer_counts(&mut variadic_buffer_counts, &array_data);
    }

    let pad_len = pad_to_alignment(IPC_ALIGNMENT, arrow_data.len());
    arrow_data.extend_from_slice(&PADDING[..pad_len]);

    let mut builder = FlatBufferBuilder::new();
    let nodes_vec = builder.create_vector(&nodes);
    let buffers_vec = builder.create_vector(&buffers);
    let variadic = if variadic_buffer_counts.is_empty() {
        None
    } else {
        Some(builder.create_vector(&variadic_buffer_counts))
    };
    let compression = {
        let mut compression = BodyCompressionBuilder::new(&mut builder);
        compression.add_method(BodyCompressionMethod::BUFFER);
        compression.add_codec(CompressionType::LZ4_FRAME);
        compression.finish()
    };
    let record_batch = {
        let mut record_batch = RecordBatchBuilder::new(&mut builder);
        record_batch.add_length(batch.num_rows() as i64);
        record_batch.add_nodes(nodes_vec);
        record_batch.add_buffers(buffers_vec);
        record_batch.add_compression(compression);
        if let Some(counts) = variadic {
            record_batch.add_variadicBufferCounts(counts);
        }
        record_batch.finish().as_union_value()
    };
    let message = {
        let mut new_message = MessageBuilder::new(&mut builder);
        new_message.add_version(MetadataVersion::V5);
        new_message.add_header_type(MessageHeader::RecordBatch);
        new_message.add_header(record_batch);
        let body_len = i64::try_from(arrow_data.len())
            .map_err(|error| ipc_error(format!("IPC body is too large: {error}")))?;
        new_message.add_bodyLength(body_len);
        new_message.finish()
    };
    builder.finish(message, None);

    Ok(EncodedData {
        ipc_message: builder.finished_data().to_vec(),
        arrow_data,
    })
}

fn lz4_buffer_declared_len(input: &[u8]) -> usize {
    if input.len() < std::mem::size_of::<i64>() {
        return 0;
    }
    match i64::from_le_bytes(input[..8].try_into().unwrap()) {
        -1 => input.len().saturating_sub(8),
        len if len > 0 => len as usize,
        _ => 0,
    }
}

fn decompress_lz4_buffer(input: &[u8], output: &mut Vec<u8>) -> Result<usize, ArrowError> {
    if input.is_empty() {
        return Ok(0);
    }
    if input.len() < std::mem::size_of::<i64>() {
        return Err(ipc_error("compressed IPC buffer has no length prefix"));
    }

    let start = output.len();
    let uncompressed_len = i64::from_le_bytes(input[..8].try_into().unwrap());
    match uncompressed_len {
        -1 => output.extend_from_slice(&input[8..]),
        0 => {}
        len if len > 0 => {
            let len = usize::try_from(len)
                .map_err(|error| ipc_error(format!("invalid IPC buffer length: {error}")))?;
            let end = start
                .checked_add(len)
                .ok_or_else(|| ipc_error("decompressed IPC buffer length overflowed"))?;
            // Avoid Vec::resize(..., 0): zero-filling multi-GiB string payloads before
            // LZ4 overwrite showed up as ~half of large_ralloc in Q3 Flight profiles.
            output.reserve(len);
            unsafe {
                output.set_len(end);
            }
            let mut decoder = lz4::Decoder::new(&input[8..])?;
            if let Err(error) = decoder.read_exact(&mut output[start..]) {
                unsafe {
                    output.set_len(start);
                }
                return Err(error.into());
            }
        }
        len => return Err(ipc_error(format!("invalid IPC buffer length: {len}"))),
    }
    Ok(output.len() - start)
}

/// Arrow-rs decodes IPC LZ4 buffers through the pure-Rust `lz4_flex` path. For
/// large exchange batches, rebuild an uncompressed IPC body with C liblz4 so
/// the regular Arrow reader can construct arrays without decompressing again.
pub(crate) fn decompress_record_batch_lz4(
    data_header: &[u8],
    data_body: &[u8],
) -> Result<Option<EncodedData>, ArrowError> {
    let message = root_as_message(data_header)
        .map_err(|error| ipc_error(format!("invalid Flight IPC message: {error}")))?;
    let batch = message
        .header_as_record_batch()
        .ok_or_else(|| ipc_error("Flight IPC message is not a RecordBatch"))?;
    let Some(compression) = batch.compression() else {
        return Ok(None);
    };
    if compression.codec() != CompressionType::LZ4_FRAME {
        return Ok(None);
    }
    if compression.method() != BodyCompressionMethod::BUFFER {
        return Err(ipc_error("unsupported Flight IPC compression method"));
    }

    let nodes = batch
        .nodes()
        .map(|nodes| nodes.iter().copied().collect::<Vec<_>>())
        .unwrap_or_default();
    let variadic_buffer_counts = batch
        .variadicBufferCounts()
        .map(|counts| counts.iter().collect::<Vec<_>>());
    let source_buffers = batch
        .buffers()
        .ok_or_else(|| ipc_error("Flight IPC RecordBatch has no buffers"))?;

    // Prefixes already declare uncompressed sizes; reserve once so decompress does not
    // repeatedly large-ralloc while growing from the compressed body length.
    let mut estimated_body = 0usize;
    for source_buffer in source_buffers.iter() {
        let source_offset = usize::try_from(source_buffer.offset())
            .map_err(|error| ipc_error(format!("invalid IPC buffer offset: {error}")))?;
        let source_len = usize::try_from(source_buffer.length())
            .map_err(|error| ipc_error(format!("invalid IPC buffer length: {error}")))?;
        let source_end = source_offset
            .checked_add(source_len)
            .ok_or_else(|| ipc_error("Flight IPC buffer range overflowed"))?;
        let source = data_body
            .get(source_offset..source_end)
            .ok_or_else(|| ipc_error("Flight IPC buffer is outside its message body"))?;
        estimated_body = estimated_body
            .saturating_add(lz4_buffer_declared_len(source))
            .saturating_add(IPC_ALIGNMENT);
    }

    let mut arrow_data = Vec::with_capacity(estimated_body.max(data_body.len()));
    let mut buffers = Vec::with_capacity(source_buffers.len());
    for source_buffer in source_buffers {
        let source_offset = usize::try_from(source_buffer.offset())
            .map_err(|error| ipc_error(format!("invalid IPC buffer offset: {error}")))?;
        let source_len = usize::try_from(source_buffer.length())
            .map_err(|error| ipc_error(format!("invalid IPC buffer length: {error}")))?;
        let source_end = source_offset
            .checked_add(source_len)
            .ok_or_else(|| ipc_error("Flight IPC buffer range overflowed"))?;
        let source = data_body
            .get(source_offset..source_end)
            .ok_or_else(|| ipc_error("Flight IPC buffer is outside its message body"))?;

        let offset = i64::try_from(arrow_data.len())
            .map_err(|error| ipc_error(format!("IPC body is too large: {error}")))?;
        let len = i64::try_from(decompress_lz4_buffer(source, &mut arrow_data)?)
            .map_err(|error| ipc_error(format!("decompressed IPC buffer is too large: {error}")))?;
        buffers.push(IpcBuffer::new(offset, len));

        let padding = (IPC_ALIGNMENT - arrow_data.len() % IPC_ALIGNMENT) % IPC_ALIGNMENT;
        arrow_data.resize(arrow_data.len() + padding, 0);
    }

    let mut builder = FlatBufferBuilder::new();
    let nodes = builder.create_vector(&nodes);
    let buffers = builder.create_vector(&buffers);
    let variadic_buffer_counts = variadic_buffer_counts
        .as_ref()
        .map(|counts| builder.create_vector(counts));
    let record_batch = {
        let mut record_batch = RecordBatchBuilder::new(&mut builder);
        record_batch.add_length(batch.length());
        record_batch.add_nodes(nodes);
        record_batch.add_buffers(buffers);
        if let Some(counts) = variadic_buffer_counts {
            record_batch.add_variadicBufferCounts(counts);
        }
        record_batch.finish().as_union_value()
    };
    let message = {
        let mut new_message = MessageBuilder::new(&mut builder);
        new_message.add_version(message.version());
        new_message.add_header_type(MessageHeader::RecordBatch);
        new_message.add_header(record_batch);
        let body_len = i64::try_from(arrow_data.len())
            .map_err(|error| ipc_error(format!("IPC body is too large: {error}")))?;
        new_message.add_bodyLength(body_len);
        new_message.finish()
    };
    builder.finish(message, None);

    Ok(Some(EncodedData {
        ipc_message: builder.finished_data().to_vec(),
        arrow_data,
    }))
}
