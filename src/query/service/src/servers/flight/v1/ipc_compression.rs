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

use std::io::Read;
use std::io::Write;

use arrow_ipc::BodyCompressionBuilder;
use arrow_ipc::BodyCompressionMethod;
use arrow_ipc::Buffer as IpcBuffer;
use arrow_ipc::CompressionType;
use arrow_ipc::MessageBuilder;
use arrow_ipc::MessageHeader;
use arrow_ipc::RecordBatchBuilder;
use arrow_ipc::root_as_message;
use arrow_ipc::writer::EncodedData;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::ArrowError;
use databend_common_settings::FlightCompression;
use flatbuffers::FlatBufferBuilder;

const IPC_ALIGNMENT: usize = 64;

pub(crate) fn make_ipc_options(
    compression: Option<FlightCompression>,
) -> Result<(IpcWriteOptions, bool), ArrowError> {
    let (compression, native_lz4) = match compression {
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

/// Arrow IPC compresses every body buffer independently. Rebuild the generated
/// RecordBatch metadata after using the C LZ4 implementation for those buffers.
pub(crate) fn compress_record_batch_lz4(encoded: EncodedData) -> Result<EncodedData, ArrowError> {
    let message = root_as_message(&encoded.ipc_message)
        .map_err(|error| ipc_error(format!("invalid generated IPC message: {error}")))?;
    let batch = message
        .header_as_record_batch()
        .ok_or_else(|| ipc_error("generated IPC message is not a RecordBatch"))?;

    let nodes = batch
        .nodes()
        .map(|nodes| nodes.iter().copied().collect::<Vec<_>>())
        .unwrap_or_default();
    let variadic_buffer_counts = batch
        .variadicBufferCounts()
        .map(|counts| counts.iter().collect::<Vec<_>>());

    let source_buffers = batch
        .buffers()
        .ok_or_else(|| ipc_error("generated IPC RecordBatch has no buffers"))?;
    let mut arrow_data = Vec::with_capacity(encoded.arrow_data.len());
    let mut buffers = Vec::with_capacity(source_buffers.len());

    for source_buffer in source_buffers {
        let source_offset = usize::try_from(source_buffer.offset())
            .map_err(|error| ipc_error(format!("invalid IPC buffer offset: {error}")))?;
        let source_len = usize::try_from(source_buffer.length())
            .map_err(|error| ipc_error(format!("invalid IPC buffer length: {error}")))?;
        let source_end = source_offset
            .checked_add(source_len)
            .ok_or_else(|| ipc_error("generated IPC buffer range overflowed"))?;
        let source = encoded
            .arrow_data
            .get(source_offset..source_end)
            .ok_or_else(|| ipc_error("generated IPC buffer is outside its message body"))?;

        let offset = i64::try_from(arrow_data.len())
            .map_err(|error| ipc_error(format!("IPC body is too large: {error}")))?;
        let len = i64::try_from(compress_lz4_buffer(source, &mut arrow_data)?)
            .map_err(|error| ipc_error(format!("compressed IPC buffer is too large: {error}")))?;
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
    let compression = {
        let mut compression = BodyCompressionBuilder::new(&mut builder);
        compression.add_method(BodyCompressionMethod::BUFFER);
        compression.add_codec(CompressionType::LZ4_FRAME);
        compression.finish()
    };
    let record_batch = {
        let mut record_batch = RecordBatchBuilder::new(&mut builder);
        record_batch.add_length(batch.length());
        record_batch.add_nodes(nodes);
        record_batch.add_buffers(buffers);
        record_batch.add_compression(compression);
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

    Ok(EncodedData {
        ipc_message: builder.finished_data().to_vec(),
        arrow_data,
    })
}
