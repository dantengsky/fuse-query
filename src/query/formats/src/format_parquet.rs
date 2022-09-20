// Copyright 2022 Datafuse Labs.
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
use std::borrow::Cow;
use std::io::Cursor;
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::io::parquet::read;
use common_arrow::arrow::io::parquet::read::read_columns_many;
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::parquet::metadata::FileMetaData;
use common_arrow::parquet::metadata::RowGroupMetaData;
use common_arrow::parquet::read::read_metadata;
use common_datablocks::DataBlock;
use common_datavalues::remove_nullable;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::IntoColumn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::default_column_cast;
use common_io::prelude::FileSplit;
use common_io::prelude::FormatSettings;
use similar_asserts::traits::MakeDiff;

use crate::FormatFactory;
use crate::InputFormat;
use crate::InputState;

pub struct ParquetInputState {
    pub memory: Vec<u8>,
}

impl InputState for ParquetInputState {
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}

pub struct ParquetInputFormat {
    schema: DataSchemaRef,
    is_artificial_schema: bool,
}

impl ParquetInputFormat {
    pub fn register(factory: &mut FormatFactory) {
        factory.register_input(
            "parquet",
            Box::new(
                |name: &str,
                 schema: DataSchemaRef,
                 is_artificial_schema: bool,
                 _settings: FormatSettings| {
                    ParquetInputFormat::try_create(name, schema, is_artificial_schema)
                },
            ),
        )
    }

    pub fn try_create(
        _name: &str,
        schema: DataSchemaRef,
        is_artificial_schema: bool,
    ) -> Result<Arc<dyn InputFormat>> {
        Ok(Arc::new(ParquetInputFormat {
            schema,
            is_artificial_schema,
        }))
    }

    fn read_meta_data(cursor: &mut Cursor<&Vec<u8>>) -> Result<FileMetaData> {
        match read_metadata(cursor) {
            Ok(metadata) => Ok(metadata),
            Err(cause) => Err(ErrorCode::ParquetError(cause.to_string())),
        }
    }

    fn read_columns(
        fields: &[Field],
        row_group: &RowGroupMetaData,
        cursor: &mut Cursor<&Vec<u8>>,
    ) -> Result<Vec<ArrayIter<'static>>> {
        match read_columns_many(cursor, row_group, fields.to_vec(), None) {
            Ok(array) => Ok(array),
            Err(e) => Err(ErrorCode::ParquetError(e.to_string())),
        }
    }

    fn deserialize(
        num_rows: usize,
        arrays: Vec<ArrayIter<'static>>,
    ) -> Result<Chunk<Box<dyn Array>>> {
        match RowGroupDeserializer::new(arrays, num_rows, None).next() {
            None => Err(ErrorCode::ParquetError("fail to get a chunk")),
            Some(Ok(chunk)) => Ok(chunk),
            Some(Err(e)) => Err(ErrorCode::ParquetError(e.to_string())),
        }
    }

    fn fields_to_read<'a>(&'a self, infer_schema: &'a Schema) -> Result<Cow<[Field]>> {
        if !self.is_artificial_schema {
            let mut fields = Vec::with_capacity(self.schema.num_fields());
            for f in self.schema.fields().iter() {
                if let Some(m) = infer_schema
                    .fields
                    .iter()
                    .filter(|c| c.name.eq_ignore_ascii_case(f.name()))
                    .last()
                {
                    let tf = DataField::from(m);
                    if remove_nullable(tf.data_type()) != remove_nullable(f.data_type()) {
                        let pair = (f, m);
                        let diff = pair.make_diff("expected_field", "infer_field");
                        return Err(ErrorCode::ParquetError(format!(
                            "parquet schema mismatch, differ: {}",
                            diff
                        )));
                    }

                    fields.push(m.clone());
                } else {
                    return Err(ErrorCode::ParquetError(format!(
                        "schema field size mismatch, expected to find column: {}",
                        f.name()
                    )));
                }
            }
            Ok(Cow::from(fields))
        } else {
            let artificial_schema_field_len = self.schema.num_fields();
            let parquet_fields = &infer_schema.fields;
            Ok(if artificial_schema_field_len < parquet_fields.len() {
                // trim unnecessary fields (unlikely, since number of artificial columns is large)
                Cow::from(&parquet_fields.as_slice()[0..artificial_schema_field_len])
            } else {
                Cow::from(parquet_fields.as_slice())
            })
        }
    }

    fn chunk_to_block(
        &self,
        fields_to_read: &[ArrowField],
        chunk: &Chunk<Box<dyn Array>>,
    ) -> Result<DataBlock> {
        if self.is_artificial_schema {
            let parquet_fields = fields_to_read.iter().map(DataField::from);
            let r = chunk
                .columns()
                .iter()
                .zip(parquet_fields)
                .zip(self.schema.fields().iter())
                .map(|((column_ref, parquet_field), data_field)| {
                    let col = match data_field.is_nullable() {
                        true => column_ref.into_nullable_column(),
                        false => column_ref.into_column(),
                    };
                    if parquet_field.data_type() == data_field.data_type() {
                        Ok((col, data_field))
                    } else {
                        let casted = default_column_cast(&col, data_field.data_type())?;
                        Ok((casted, data_field))
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            let mut columns = Vec::with_capacity(fields_to_read.len());
            let mut fields = Vec::with_capacity(fields_to_read.len());
            for (c, d) in r {
                columns.push(c);
                fields.push(d.clone())
            }
            let schema = DataSchemaRefExt::create(fields);
            Ok(DataBlock::create(schema, columns))
        } else {
            DataBlock::from_chunk(&self.schema, chunk)
        }
    }
}

impl InputFormat for ParquetInputFormat {
    fn create_state(&self) -> Box<dyn InputState> {
        Box::new(ParquetInputState { memory: vec![] })
    }

    fn deserialize_data(&self, state: &mut Box<dyn InputState>) -> Result<Vec<DataBlock>> {
        let mut state = std::mem::replace(state, self.create_state());
        let state = state.as_any().downcast_mut::<ParquetInputState>().unwrap();
        let memory = std::mem::take(&mut state.memory);
        if memory.is_empty() {
            return Ok(vec![]);
        }
        self.deserialize_complete_split(FileSplit {
            path: None,
            start_offset: 0,
            start_row: 0,
            buf: memory,
        })
    }

    fn deserialize_complete_split(&self, split: FileSplit) -> Result<Vec<DataBlock>> {
        let mut cursor = Cursor::new(&split.buf);
        let parquet_metadata = Self::read_meta_data(&mut cursor)?;
        let infer_schema = read::infer_schema(&parquet_metadata)?;

        let fields_to_read = self.fields_to_read(&infer_schema)?;
        let mut data_blocks = Vec::with_capacity(parquet_metadata.row_groups.len());

        for row_group in &parquet_metadata.row_groups {
            let arrays = Self::read_columns(&fields_to_read, row_group, &mut cursor)?;
            let chunk = Self::deserialize(row_group.num_rows() as usize, arrays)?;
            let data_block = self.chunk_to_block(&fields_to_read, &chunk)?;
            data_blocks.push(data_block);
        }

        Ok(data_blocks)
    }

    fn read_buf(&self, buf: &[u8], state: &mut Box<dyn InputState>) -> Result<(usize, bool)> {
        let state = state.as_any().downcast_mut::<ParquetInputState>().unwrap();
        state.memory.extend_from_slice(buf);
        Ok((buf.len(), false))
    }

    fn take_buf(&self, state: &mut Box<dyn InputState>) -> Vec<u8> {
        let state = state.as_any().downcast_mut::<ParquetInputState>().unwrap();
        std::mem::take(&mut state.memory)
    }

    fn skip_header(&self, _: &[u8], _: &mut Box<dyn InputState>, _: usize) -> Result<usize> {
        Ok(0)
    }
}
