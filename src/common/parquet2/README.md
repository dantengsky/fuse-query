# Parquet2 Direct Deserialization

This crate provides functionality to directly deserialize Parquet data into DataBlock structures, bypassing the Arrow memory model for improved performance.

## Overview

The main goal of this crate is to optimize the deserialization process by:

1. Directly converting Parquet data to DataBlock without intermediate Arrow arrays
2. Using direct memory copies for numeric columns when possible
3. Replacing recursion with loops for better performance
4. Efficiently handling nullable and non-nullable columns

## Key Components

- `ParquetReader`: Main entry point for deserializing Parquet data to DataBlock
- `deserialize_page_to_column`: Core function for converting Parquet pages to Columns
- `DecompressedBuffer`: Reusable buffer for decompression operations

## Usage Example

```rust
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_expression::{DataField, DataSchema, DataSchemaRef, DataType};
use databend_common_parquet2::{ParquetReader, DecompressedBuffer, to_parquet_compression};
use databend_storages_common_table_meta::meta::Compression;

// Create a schema
let schema = Arc::new(DataSchema::new(vec![
    DataField::new("id", DataType::Int32, false).with_id(1),
    DataField::new("value", DataType::Int64, false).with_id(2),
    DataField::new("name", DataType::String, true).with_id(3),
]));

// Create a ParquetReader
let reader = ParquetReader::new(schema.clone())?;

// In a real scenario, these would come from your storage layer
let column_chunks = HashMap::new(); // HashMap<ColumnId, &[u8]>
let column_metas = HashMap::new();  // HashMap<ColumnId, ColumnChunkMetaData>
let num_rows = 0;
let compression = Compression::Uncompressed;
let decompressed_buffer = Arc::new(DecompressedBuffer::new(1024));

// Convert compression to parquet compression
let parquet_compression = to_parquet_compression(&compression)?;

// Read column chunks into a DataBlock
let data_block = reader.read_column_chunks(
    column_chunks,
    &column_metas,
    num_rows,
    parquet_compression,
    decompressed_buffer,
)?;
```

## Integration with Existing Code

To integrate this with the existing parquet2 reading logic in fuse storage, replace the Arrow-based deserialization in `BlockReader::column_chunks_to_data_block_2` with direct deserialization using this crate.

## Performance Considerations

- Direct memory copies are used for non-nullable numeric columns
- Loops are used instead of recursion for better performance
- Buffer reuse is employed to minimize allocations
- Type-specific optimizations are applied based on column data types

## Supported Types

Currently supported Parquet physical types:
- Boolean
- Int32
- Int64
- Float
- Double
- ByteArray (for String data)

## Future Improvements

- Support for more complex types (Maps, Lists, Structs)
- Dictionary encoding support
- Additional compression optimizations
- Parallel deserialization of columns
