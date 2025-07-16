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

//! Integration example for direct Parquet deserialization

use std::collections::HashMap;
use std::sync::Arc;

// Fix import paths for Databend types
use databend_common_exception::{ErrorCode, Result};
use databend_common_expression::types::number::NumberDataType;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use parquet2::encoding::Encoding;
use parquet2::metadata::ColumnChunkMetaData;
use parquet2::metadata::ColumnDescriptor;
use parquet2::metadata::Descriptor;
use parquet2::page::DataPage;
use parquet2::page::DataPageHeader;
use parquet2::page::DataPageHeaderV1;
use parquet2::schema::types::ParquetType;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveLogicalType;
use parquet2::schema::types::PrimitiveType;
use parquet2::schema::Repetition;

use crate::deserialize::deserialize_page_to_column;
use crate::reader::ParquetReader;

/// Struct representing column chunk data for example purposes
pub struct ColumnChunkData {
    pub page: DataPage,
    pub num_rows: usize,
}

/// Example of how to convert Parquet column chunks directly to DataBlock
pub fn column_chunks_to_data_block_direct(
    data_schema: &Arc<DataSchema>,
    column_chunks: Vec<ColumnChunkData>,
) -> Result<DataBlock> {
    // Ensure we have a column for each field in the schema
    if data_schema.fields().len() != column_chunks.len() {
        return Err(ErrorCode::Internal(format!(
            "Schema field count ({}) doesn't match column chunk count ({})",
            data_schema.fields().len(),
            column_chunks.len()
        )));
    }

    // Create columns from column chunks
    let mut columns = Vec::with_capacity(column_chunks.len());
    let mut data_types = Vec::with_capacity(column_chunks.len());

    for (i, chunk) in column_chunks.iter().enumerate() {
        // Deserialize the page to a column
        let column = deserialize_page_to_column(&chunk.page)?;
        columns.push(column);
        data_types.push(data_schema.field(i).data_type());
    }

    // Create entries from columns
    let mut entries = Vec::with_capacity(columns.len());
    for column in columns {
        entries.push(BlockEntry::Column(column));
    }

    // Calculate row count
    let num_rows = if entries.is_empty() {
        0
    } else {
        entries[0].len()
    };

    // Create DataBlock from entries
    let block = DataBlock::new(entries, num_rows);

    Ok(block)
}

/// Convert Databend's DataType to parquet PhysicalType
fn data_type_to_physical_type(data_type: &DataType) -> Result<PhysicalType> {
    match data_type {
        DataType::Boolean => Ok(PhysicalType::Boolean),
        DataType::Number(NumberDataType::Int32) => Ok(PhysicalType::Int32),
        DataType::Number(NumberDataType::Int64) => Ok(PhysicalType::Int64),
        DataType::Number(NumberDataType::Float32) => Ok(PhysicalType::Float),
        DataType::Number(NumberDataType::Float64) => Ok(PhysicalType::Double),
        DataType::String => Ok(PhysicalType::ByteArray),
        DataType::Date => Ok(PhysicalType::Int32),
        DataType::Timestamp => Ok(PhysicalType::Int64),
        _ => Err(ErrorCode::Internal(format!(
            "Unsupported DataType: {:?}",
            data_type
        ))),
    }
}

/// Generate test data for a column based on physical type id
fn generate_test_data_for_type(type_id: u8, row_count: usize) -> Vec<u8> {
    match type_id {
        1 => {
            // Boolean - 1 bit per value, packed
            let bytes_needed = (row_count + 7) / 8;
            vec![0x55; bytes_needed] // Alternating 0101 0101 pattern
        }
        2 => {
            // Int32 - 4 bytes per value
            let bytes_needed = row_count * 4;
            let mut data = Vec::with_capacity(bytes_needed);
            for i in 0..row_count {
                let value = (i as i32).to_le_bytes();
                data.extend_from_slice(&value);
            }
            data
        }
        3 => {
            // Int64 - 8 bytes per value
            let bytes_needed = row_count * 8;
            let mut data = Vec::with_capacity(bytes_needed);
            for i in 0..row_count {
                let value = (i as i64).to_le_bytes();
                data.extend_from_slice(&value);
            }
            data
        }
        4 | 5 => {
            // Float32/Float64 - 4/8 bytes per value
            let bytes_per_value = if type_id == 4 { 4 } else { 8 };
            let bytes_needed = row_count * bytes_per_value;
            vec![0; bytes_needed] // Just zeros for simplicity
        }
        6 => {
            // String (ByteArray) - variable length
            // Format: for each string, 4 byte length + string data
            let mut data = Vec::with_capacity(row_count * 8); // Estimate
            for i in 0..row_count {
                let s = format!("str{}", i);
                let len = s.len() as u32;
                data.extend_from_slice(&len.to_le_bytes());
                data.extend_from_slice(s.as_bytes());
            }
            data
        }
        _ => vec![0; row_count], // Default empty data for unsupported types
    }
}

/// Create a column from DataType for testing
pub fn create_column_from_data_type(
    data_type: &DataType,
    column_name: &str,
    nullable: bool,
    row_count: usize,
) -> Result<ColumnChunkData> {
    // Convert DataType to PhysicalType
    let physical_type = data_type_to_physical_type(data_type)?;

    // Create a primitive type
    let primitive_type =
        PrimitiveType::from_physical(column_name.to_string(), physical_type.clone());

    // Generate test data
    let data = generate_test_data_for_type(physical_type_to_type_id(&physical_type), row_count);

    // Create descriptor
    let descriptor = Descriptor {
        primitive_type: primitive_type.clone(),
        max_def_level: if nullable { 1 } else { 0 },
        max_rep_level: 0,
    };

    // Create a page header
    let header = DataPageHeader::V1(DataPageHeaderV1 {
        num_values: row_count as i32,
        encoding: Encoding::Plain.into(),
        definition_level_encoding: Encoding::Rle.into(),
        repetition_level_encoding: Encoding::Rle.into(),
        statistics: None,
    });

    // Create the data page
    let page = DataPage::new(header, data, descriptor.clone(), None);

    Ok(ColumnChunkData {
        page,
        num_rows: row_count,
    })
}

/// Creates a DataBlock from parquet column chunks
pub fn create_data_block_from_parquet_chunks(
    column_chunks: Vec<ColumnChunkData>,
    data_schema: Arc<DataSchema>,
) -> Result<DataBlock> {
    let mut entries = Vec::with_capacity(column_chunks.len());
    let mut row_count = 0;

    for (i, chunk) in column_chunks.iter().enumerate() {
        // Deserialize the page to a column
        let column = deserialize_page_to_column(&chunk.page)?;
        entries.push(BlockEntry::Column(column));

        // Update row count - use the first chunk's row count
        if i == 0 {
            row_count = chunk.num_rows;
        }
    }

    // Create a DataBlock from the columns
    Ok(DataBlock::new(entries, row_count))
}

/// Create a DataSchema from field definitions
pub fn create_data_schema(fields: Vec<DataField>) -> Arc<DataSchema> {
    Arc::new(DataSchema::new(fields))
}

/// Map physical type to databend data type code
pub(crate) fn physical_type_to_type_id(physical_type: &PhysicalType) -> u8 {
    match physical_type {
        PhysicalType::Boolean => 1,
        PhysicalType::Int32 => 2,
        PhysicalType::Int64 => 3,
        PhysicalType::Float => 4,
        PhysicalType::Double => 5,
        PhysicalType::ByteArray => 6,
        PhysicalType::FixedLenByteArray(_) => 7,
        PhysicalType::Int96 => 8,
    }
}

/// Create a mock column descriptor for testing
pub fn create_mock_column_descriptor(
    physical_type: PhysicalType,
    max_def_level: i16,
) -> ColumnDescriptor {
    // Create primitive type
    let primitive_type = PrimitiveType::from_physical("field".to_string(), physical_type);

    // Create descriptor
    let descriptor = Descriptor {
        primitive_type: primitive_type.clone(),
        max_def_level,
        max_rep_level: 0,
    };

    // Create column descriptor
    ColumnDescriptor {
        descriptor,
        path_in_schema: vec!["field".to_string()],
        base_type: ParquetType::PrimitiveType(primitive_type),
    }
}

/// Generate a test Parquet column descriptor
pub fn create_test_column_descriptor(
    name: &str,
    physical_type: PhysicalType,
    repetition: Repetition,
) -> ColumnDescriptor {
    // Create a primitive type for the descriptor
    let primitive_type = PrimitiveType::from_physical(name.to_string(), physical_type);

    // Create the descriptor
    let descriptor = Descriptor {
        max_def_level: match repetition {
            Repetition::Required => 0,
            _ => 1,
        },
        max_rep_level: 0, // No nesting
        primitive_type: primitive_type.clone(),
    };

    // Create the column descriptor
    ColumnDescriptor {
        descriptor,
        path_in_schema: vec![name.to_string()],
        base_type: ParquetType::PrimitiveType(primitive_type),
    }
}

/// Convert a Databend DataType to a Parquet PhysicalType
pub fn databend_to_parquet_physical_type(data_type: &DataType) -> Result<PhysicalType> {
    match data_type {
        DataType::Boolean => Ok(PhysicalType::Boolean),
        DataType::Number(NumberDataType::Int32) => Ok(PhysicalType::Int32),
        DataType::Number(NumberDataType::Int64) => Ok(PhysicalType::Int64),
        DataType::Number(NumberDataType::Float32) => Ok(PhysicalType::Float),
        DataType::Number(NumberDataType::Float64) => Ok(PhysicalType::Double),
        DataType::String => Ok(PhysicalType::ByteArray),
        DataType::Date => Ok(PhysicalType::Int32),
        DataType::Timestamp => Ok(PhysicalType::Int64),
        _ => Err(ErrorCode::Internal(format!(
            "Unsupported data type: {:?}",
            data_type
        ))),
    }
}

/// Create a mock test data page
fn create_mock_data_page(
    row_count: usize,
    column_descriptor: &ColumnDescriptor,
    values_data: Vec<u8>,
) -> DataPage {
    // Create a page header
    let header = DataPageHeader::V1(DataPageHeaderV1 {
        num_values: row_count as i32,
        encoding: Encoding::Plain.into(),
        definition_level_encoding: Encoding::Rle.into(),
        repetition_level_encoding: Encoding::Rle.into(),
        statistics: None,
    });

    // Create the page
    DataPage::new(
        header,
        values_data,
        column_descriptor.descriptor.clone(),
        None, // selected_rows
    )
}

/// Create a DataPage from raw data for testing
pub fn create_test_data_page(
    data: Vec<u8>,
    num_values: i32,
    descriptor: &ColumnDescriptor,
) -> DataPage {
    // Create a V1 header
    let header = DataPageHeaderV1 {
        num_values,
        encoding: Encoding::Plain.into(),
        definition_level_encoding: Encoding::Rle.into(),
        repetition_level_encoding: Encoding::Rle.into(),
        statistics: None,
    };

    // Create the page
    DataPage::new(
        DataPageHeader::V1(header),
        data,
        descriptor.descriptor.clone(), // Use the inner descriptor
        None,                          // selected_rows
    )
}
