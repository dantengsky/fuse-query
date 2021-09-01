//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

/*
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use common_arrow::parquet::error::ParquetError;
use common_arrow::parquet::error::Result;
use common_arrow::parquet::metadata::{KeyValue, ColumnDescriptor};
use common_arrow::parquet::metadata::SchemaDescriptor;
use common_arrow::parquet::schema::types::BasicTypeInfo;
use common_arrow::parquet::schema::types::ParquetType;
use common_arrow::parquet::schema::Repetition;
use common_arrow::parquet::write::row;
use common_arrow::parquet::write::RowGroupIter;
use common_arrow::parquet::write::WriteOptions;
use parquet_format_async_temp::thrift::protocol::TCompactOutputProtocol;
use parquet_format_async_temp::thrift::protocol::TOutputProtocol;
use parquet_format_async_temp::FileMetaData;
use parquet_format_async_temp::SchemaElement;

const FOOTER_SIZE: u64 = 8;
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

/// The number of bytes read at the end of the parquet file on first read
const DEFAULT_FOOTER_READ_SIZE: u64 = 64 * 1024;
pub fn start_file<W: Write>(writer: &mut W) -> Result<()> {
    Ok(writer.write_all(&PARQUET_MAGIC)?)
}

pub(super) fn end_file<W: Write + Seek>(mut writer: &mut W, metadata: FileMetaData) -> Result<()> {
    // Write file metadata
    let start_pos = writer.seek(SeekFrom::Current(0))?;
    {
        let mut protocol = TCompactOutputProtocol::new(&mut writer);
        metadata.write_to_out_protocol(&mut protocol)?;
        protocol.flush()?
    }
    let end_pos = writer.seek(SeekFrom::Current(0))?;
    let metadata_len = (end_pos - start_pos) as i32;

    // Write footer
    let metadata_len = metadata_len.to_le_bytes();
    let mut footer_buffer = [0u8; FOOTER_SIZE as usize];
    (0..4).for_each(|i| {
        footer_buffer[i] = metadata_len[i];
    });

    (&mut footer_buffer[4..]).write_all(&PARQUET_MAGIC)?;
    writer.write_all(&footer_buffer)?;
    Ok(())
}

pub fn write_file<'a, W, I, E>(
    writer: &mut W,
    row_groups: I,
    schema: SchemaDescriptor,
    options: WriteOptions,
    created_by: Option<String>,
    key_value_metadata: Option<Vec<KeyValue>>,
) -> Result<()>
where
    W: Write + Seek,
    I: Iterator<Item = std::result::Result<RowGroupIter<'a, E>, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    start_file(writer)?;

    let row_groups = row_groups
        .map(|row_group| {
            write_row_group(
                writer,
                schema.columns(),
                options.compression,
                row_group.map_err(ParquetError::from_external_error)?,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    // compute file stats
    let num_rows = row_groups.iter().map(|group| group.num_rows).sum();

    let metadata = FileMetaData::new(
        options.version.into(),
        into_thrift(schema)?,
        num_rows,
        row_groups,
        key_value_metadata,
        created_by,
        None,
        None,
        None,
    );

    end_file(writer, metadata)?;
    Ok(())
}

pub fn into_thrift(schema: SchemaDescriptor) -> Result<Vec<SchemaElement>> {
    ParquetType::GroupType {
        basic_info: BasicTypeInfo::new(schema.name().to_string(), Repetition::Optional, None, true),
        logical_type: None,
        converted_type: None,
        fields: schema.fields().to_vec(),
    }
    .to_thrift()
}
pub fn write_row_group<
    W,
    E, // external error any of the iterators may emit
>(
    writer: &mut W,
    descriptors: &[ColumnDescriptor],
    compression: Compression,
    columns: DynIter<std::result::Result<DynIter<std::result::Result<CompressedPage, E>>, E>>,
) -> Result<RowGroup>
where
    W: Write + Seek,
    E: Error + Send + Sync + 'static,
{
    let column_iter = descriptors.iter().zip(columns);

    let columns = column_iter
        .map(|(descriptor, page_iter)| {
            write_column_chunk(
                writer,
                descriptor,
                compression,
                page_iter.map_err(ParquetError::from_external_error)?,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    // compute row group stats
    let num_rows = columns
        .iter()
        .map(|c| c.meta_data.as_ref().unwrap().num_values)
        .collect::<Vec<_>>();
    let num_rows = match same_elements(&num_rows) {
        None => return Err(general_err!("Every column chunk in a row group MUST have the same number of rows. The columns have rows: {:?}", num_rows)),
        Some(None) => 0,
        Some(Some(v)) => v
    };

    let total_byte_size = columns
        .iter()
        .map(|c| c.meta_data.as_ref().unwrap().total_compressed_size)
        .sum();

    Ok(RowGroup {
        columns,
        total_byte_size,
        num_rows,
        sorting_columns: None,
        file_offset: None,
        total_compressed_size: None,
        ordinal: None,
    })
}


 */
