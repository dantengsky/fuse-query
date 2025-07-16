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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_exception::Result;
    use databend_common_expression::types::{DataField, DataSchema, DataType};
    use databend_common_expression::types::number::NumberDataType;
    use databend_common_expression::{BlockEntry, Column, DataBlock, ScalarRef};
    use parquet2::encoding::Encoding;
    use parquet2::page::DataPage;
    use parquet2::schema::types::PrimitiveTypeBuilder;
    use parquet2::schema::Repetition;

    use crate::deserialize::deserialize_page_to_column;

    fn generate_int32_page(values: &[i32], nullable: bool) -> Result<DataPage> {
        // Create the buffer
        let mut buffer = Vec::with_capacity(values.len() * std::mem::size_of::<i32>());
        for value in values {
            buffer.extend_from_slice(&value.to_le_bytes());
        }
        
        // Create definition levels if nullable
        let definition_levels = if nullable {
            // For simplicity, all values are non-null in this test
            let mut levels = Vec::with_capacity((values.len() + 7) / 8);
            for _ in 0..((values.len() + 7) / 8) {
                levels.push(0xFF); // All bits set = all values non-null
            }
            Some(levels)
        } else {
            None
        };
        
        // Create the descriptor
        let primitive_type = PrimitiveTypeBuilder::new("test_int32", 
            if nullable { Repetition::Optional } else { Repetition::Required })
            .with_physical_type(parquet2::schema::types::PhysicalType::Int32)
            .build()?;
        
        let descriptor = parquet2::metadata::ColumnDescriptor {
            descriptor: parquet2::metadata::ColumnDescriptorPtr::new(primitive_type),
            max_definition_level: if nullable { 1 } else { 0 },
            max_repetition_level: 0,
        };
        
        // Create the page
        let page = DataPage::new(
            buffer,
            values.len(),
            0, // null_count
            0, // num_rows
            Encoding::Plain,
            definition_levels,
            None, // repetition_levels
            None, // statistics
            descriptor,
            None, // dictionary_page
            None, // selected_rows
        );
        
        Ok(page)
    }

    fn generate_int64_page(values: &[i64], nullable: bool) -> Result<DataPage> {
        // Create the buffer
        let mut buffer = Vec::with_capacity(values.len() * std::mem::size_of::<i64>());
        for value in values {
            buffer.extend_from_slice(&value.to_le_bytes());
        }
        
        // Create definition levels if nullable
        let definition_levels = if nullable {
            // For simplicity, all values are non-null in this test
            let mut levels = Vec::with_capacity((values.len() + 7) / 8);
            for _ in 0..((values.len() + 7) / 8) {
                levels.push(0xFF); // All bits set = all values non-null
            }
            Some(levels)
        } else {
            None
        };
        
        // Create the descriptor
        let primitive_type = PrimitiveTypeBuilder::new("test_int64", 
            if nullable { Repetition::Optional } else { Repetition::Required })
            .with_physical_type(parquet2::schema::types::PhysicalType::Int64)
            .build()?;
        
        let descriptor = parquet2::metadata::ColumnDescriptor {
            descriptor: parquet2::metadata::ColumnDescriptorPtr::new(primitive_type),
            max_definition_level: if nullable { 1 } else { 0 },
            max_repetition_level: 0,
        };
        
        // Create the page
        let page = DataPage::new(
            buffer,
            values.len(),
            0, // null_count
            0, // num_rows
            Encoding::Plain,
            definition_levels,
            None, // repetition_levels
            None, // statistics
            descriptor,
            None, // dictionary_page
            None, // selected_rows
        );
        
        Ok(page)
    }

    fn generate_string_page(values: &[&str], nullable: bool) -> Result<DataPage> {
        // Create the buffer with length-prefixed strings
        let mut buffer = Vec::new();
        for value in values {
            let bytes = value.as_bytes();
            buffer.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(bytes);
        }
        
        // Create definition levels if nullable
        let definition_levels = if nullable {
            // For simplicity, all values are non-null in this test
            let mut levels = Vec::with_capacity((values.len() + 7) / 8);
            for _ in 0..((values.len() + 7) / 8) {
                levels.push(0xFF); // All bits set = all values non-null
            }
            Some(levels)
        } else {
            None
        };
        
        // Create the descriptor
        let primitive_type = PrimitiveTypeBuilder::new("test_string", 
            if nullable { Repetition::Optional } else { Repetition::Required })
            .with_physical_type(parquet2::schema::types::PhysicalType::ByteArray)
            .build()?;
        
        let descriptor = parquet2::metadata::ColumnDescriptor {
            descriptor: parquet2::metadata::ColumnDescriptorPtr::new(primitive_type),
            max_definition_level: if nullable { 1 } else { 0 },
            max_repetition_level: 0,
        };
        
        // Create the page
        let page = DataPage::new(
            buffer,
            values.len(),
            0, // null_count
            0, // num_rows
            Encoding::Plain,
            definition_levels,
            None, // repetition_levels
            None, // statistics
            descriptor,
            None, // dictionary_page
            None, // selected_rows
        );
        
        Ok(page)
    }

    #[test]
    fn test_deserialize_int32_column() -> Result<()> {
        let values = [1, 2, 3, 4, 5];
        let page = generate_int32_page(&values, false)?;
        
        let arrow_array = deserialize_page_to_column(&page, values.len())?;
        let column = Column::from_arrow_rs(arrow_array, &DataType::Number(NumberDataType::Int32))?;
        
        assert_eq!(column.len(), values.len());
        for (i, &value) in values.iter().enumerate() {
            let scalar_ref = column.index(i).unwrap();
            assert_eq!(scalar_ref.as_number().unwrap().as_int32(), value);
        }
        
        Ok(())
    }

    #[test]
    fn test_deserialize_int64_column() -> Result<()> {
        let values = [100, 200, 300, 400, 500];
        let page = generate_int64_page(&values, false)?;
        
        let arrow_array = deserialize_page_to_column(&page, values.len())?;
        let column = Column::from_arrow_rs(arrow_array, &DataType::Number(NumberDataType::Int64))?;
        
        assert_eq!(column.len(), values.len());
        for (i, &value) in values.iter().enumerate() {
            let scalar_ref = column.index(i).unwrap();
            assert_eq!(scalar_ref.as_number().unwrap().as_int64(), value);
        }
        
        Ok(())
    }

    #[test]
    fn test_deserialize_string_column() -> Result<()> {
        let values = ["hello", "world", "test", "parquet", "direct"];
        let page = generate_string_page(&values, false)?;
        
        let arrow_array = deserialize_page_to_column(&page, values.len())?;
        let column = Column::from_arrow_rs(arrow_array, &DataType::String)?;
        
        assert_eq!(column.len(), values.len());
        for (i, &value) in values.iter().enumerate() {
            let scalar_ref = column.index(i).unwrap();
            assert_eq!(scalar_ref.as_string().unwrap(), value);
        }
        
        Ok(())
    }

    #[test]
    fn test_create_data_block() -> Result<()> {
        // Create test columns
        let int32_values = [1, 2, 3, 4, 5];
        let int32_page = generate_int32_page(&int32_values, false)?;
        let int32_arrow_array = deserialize_page_to_column(&int32_page, int32_values.len())?;
        let int32_column = Column::from_arrow_rs(int32_arrow_array, &DataType::Number(NumberDataType::Int32))?;
        
        let int64_values = [100, 200, 300, 400, 500];
        let int64_page = generate_int64_page(&int64_values, false)?;
        let int64_arrow_array = deserialize_page_to_column(&int64_page, int64_values.len())?;
        let int64_column = Column::from_arrow_rs(int64_arrow_array, &DataType::Number(NumberDataType::Int64))?;
        
        let string_values = ["hello", "world", "test", "parquet", "direct"];
        let string_page = generate_string_page(&string_values, false)?;
        let string_arrow_array = deserialize_page_to_column(&string_page, string_values.len())?;
        let string_column = Column::from_arrow_rs(string_arrow_array, &DataType::String)?;
        
        // Create a DataBlock
        let schema = Arc::new(DataSchema::new(vec![
            DataField::new("col0", DataType::Number(NumberDataType::Int32)),
            DataField::new("col1", DataType::Number(NumberDataType::Int64)),
            DataField::new("col2", DataType::String),
        ]));
        
        // 将Column转换为BlockEntry，并使用正确的DataBlock构造函数
        let entries: Vec<BlockEntry> = vec![
            BlockEntry::Column(int32_column),
            BlockEntry::Column(int64_column),
            BlockEntry::Column(string_column),
        ];
        
        let block = DataBlock::new(entries, 5);
        
        // Verify column access
        assert_eq!(block.num_columns(), 3);
        assert_eq!(block.num_rows(), 5);
        
        let col0 = block.get_by_offset(0);
        let col1 = block.get_by_offset(1);
        let col2 = block.get_by_offset(2);
        
        for (i, &value) in int32_values.iter().enumerate() {
            let scalar_ref = col0.index(i).unwrap();
            assert_eq!(scalar_ref.as_number().unwrap().as_int32(), value);
        }
        
        for (i, &value) in int64_values.iter().enumerate() {
            let scalar_ref = col1.index(i).unwrap();
            assert_eq!(scalar_ref.as_number().unwrap().as_int64(), value);
        }
        
        for (i, &value) in string_values.iter().enumerate() {
            let scalar_ref = col2.index(i).unwrap();
            assert_eq!(scalar_ref.as_string().unwrap(), value);
        }
        
        Ok(())
    }
}
