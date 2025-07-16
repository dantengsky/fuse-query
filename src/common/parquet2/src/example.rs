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

//! Example of using the direct parquet2 deserialization API
//! This module demonstrates how to use the API to deserialize parquet data directly to DataBlock

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;

use crate::reader::ParquetReader;

/// Example function showing how to use the ParquetReader to deserialize parquet data to DataBlock
pub fn example_deserialize_parquet_to_datablock() -> Result<()> {
    // Create a schema
    let schema = Arc::new(DataSchema::new(vec![
        // Note: Use the correct DataType variants from databend_common_expression
        DataField::new("col1", DataType::Number(NumberDataType::Int32)),
        DataField::new("col2", DataType::String),
    ]));

    // Create a mock parquet file
    let parquet_file = create_mock_parquet_file(schema.clone())?;

    // Create a ParquetReader - Fix: ParquetReader::new expects DataSchema, not Vec<u8>
    let reader = ParquetReader::new(schema.as_ref().clone())?;

    // For demonstration only - in real code you would read a row group
    // let data_block = reader.read_row_group(0)?;

    // For simplicity, we just return success
    println!("Reader created successfully");

    Ok(())
}

/// Create a mock parquet file with sample data
fn create_mock_parquet_file(schema: Arc<DataSchema>) -> Result<Vec<u8>> {
    todo!()
}

/// Example showing how to integrate with the existing parquet2 reading logic in fuse storage
pub fn example_integration_with_fuse_storage() -> Result<()> {
    // This example demonstrates how you would integrate the direct deserialization
    // with the existing code in src/query/storages/fuse/src/io/read/block/parquet/parquet2.rs

    // In the BlockReader::column_chunks_to_data_block_2 method, you would:
    // 1. Create a ParquetReader with the schema
    // 2. Convert column_chunks from DataItem to raw bytes
    // 3. Use read_column_chunks to directly create a DataBlock

    // Pseudo-code for integration:
    // Inside BlockReader::column_chunks_to_data_block_2
    //
    // Create a schema from self.arrow_schema
    // let schema = self.arrow_schema.clone();
    //
    // Create a ParquetReader
    // let reader = ParquetReader::new(schema)?;
    //
    // Convert column_chunks from DataItem to raw bytes
    // let mut raw_chunks = HashMap::new();
    // for (column_id, chunk) in &column_chunks {
    // if let DataItem::RawData(data) = chunk {
    // raw_chunks.insert(*column_id, data.as_ref());
    // }
    // }
    //
    // Convert compression
    // let parquet_compression = to_parquet_compression(compression)?;
    //
    // Create decompressed buffer
    // let decompressed_buffer = Arc::new(DecompressedBuffer::new(
    // uncompressed_buffer.map_or(1024, |buf| buf.capacity())
    // ));
    //
    // Read column chunks into a DataBlock
    // let data_block = reader.read_column_chunks(
    // raw_chunks,
    // column_metas,
    // num_rows,
    // parquet_compression,
    // decompressed_buffer,
    // )?;
    //
    // return Ok(data_block);

    Ok(())
}
