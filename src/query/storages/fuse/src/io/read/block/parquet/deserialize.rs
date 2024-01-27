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

use std::collections::HashMap;

use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use databend_common_expression::converts::arrow::table_schema_to_arrow_schema_ignore_inside_nullable;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::meta::Compression;
use parquet_rs::arrow::array_reader::build_array_reader;
use parquet_rs::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet_rs::arrow::arrow_to_parquet_schema;
use parquet_rs::arrow::parquet_to_arrow_field_levels;
use parquet_rs::arrow::parquet_to_arrow_schema_by_columns;
use parquet_rs::arrow::schema::ParquetField;
use parquet_rs::arrow::ProjectionMask;
use parquet_rs::basic::Compression as ParquetCompression;

use crate::io::read::block::block_reader_merge_io::DataItem;
use crate::io::read::block::parquet::adapter::RowGroupImplBuilder;

pub fn deserialize_column_chunks(
    original_schema: &TableSchema,
    num_rows: usize,
    column_chunks: &HashMap<ColumnId, DataItem>,
    compression: &Compression,
    //) -> databend_common_exception::Result<RecordBatch> {
) -> databend_common_exception::Result<HashMap<ColumnId, ArrayRef>> {
    // TODO projection not handled correctly (leaf nodes)
    let filtered_fields = original_schema
        .fields
        .iter()
        .filter(|id| column_chunks.contains_key(&id.column_id))
        .map(|c| c.clone())
        .collect::<Vec<_>>();

    let filtered_schema = TableSchema::new_from(filtered_fields, original_schema.metadata.clone());

    // let arrow_schema = table_schema_to_arrow_schema_ignore_inside_nullable(original_schema);
    let arrow_schema = table_schema_to_arrow_schema_ignore_inside_nullable(&filtered_schema);
    let parquet_schema = arrow_to_parquet_schema(&arrow_schema)?;

    // let column_id_to_dfs_id = original_schema
    //    .to_leaf_column_ids()
    //    .iter()
    //    .enumerate()
    //    .map(|(dfs_id, column_id)| (*column_id, dfs_id))
    //    .collect::<HashMap<_, _>>();
    // let mut projection_mask = Vec::with_capacity(column_chunks.len());
    let mut builder = RowGroupImplBuilder::new(
        num_rows,
        &parquet_schema,
        ParquetCompression::from(*compression),
    );

    for table_field in &filtered_schema.fields {
        let data_item = column_chunks.get(&table_field.column_id).unwrap();
        match data_item {
            DataItem::RawData(bytes) => {
                builder.add_column_chunk(table_field.column_id as usize, bytes.clone());
            }
            DataItem::ColumnArray(_) => {}
        }
    }

    let mut res = HashMap::new();
    let row_group = Box::new(builder.build());
    for (col_desc, table_field) in parquet_schema
        .columns()
        .iter()
        .zip(filtered_schema.fields.iter())
    {
        use parquet_rs::arrow::schema::complex::convert_type;
        let mut parquet_field = convert_type(&col_desc.self_type_ptr())?;
        let projection_mask = ProjectionMask::all();

        use parquet_rs::arrow::schema::complex::ParquetFieldType;

        if let ParquetFieldType::Primitive { col_idx, .. } = &mut parquet_field.field_type {
            *col_idx = table_field.column_id as usize;
        }

        let mut reader =
            build_array_reader(Some(&parquet_field), &projection_mask, row_group.as_ref())?;
        let array = reader.next_batch(num_rows)?;
        res.insert(table_field.column_id, array);
    }

    // for (column_id, data_item) in column_chunks.iter() {
    //    match data_item {
    //        DataItem::RawData(bytes) => {
    //            let dfs_id = column_id_to_dfs_id.get(column_id).cloned().unwrap();
    //            projection_mask.push(dfs_id);
    //            builder.add_column_chunk(dfs_id, bytes.clone());
    //        }
    //        DataItem::ColumnArray(_) => {}
    //    }
    //}
    // let row_group = Box::new(builder.build());
    // let field_levels = parquet_to_arrow_field_levels(
    //    &parquet_schema,
    //    ProjectionMask::leaves(&parquet_schema, projection_mask),
    //    None,
    //)?;

    use parquet_rs::arrow::schema::complex;
    // use parquet_rs::arrow::schema::complex::convert_type;
    // let sth = parquet_schema.columns().get(0).unwrap().clone();
    // let parquet_field = convert_type(&sth.self_type_ptr())?;
    //// reading leaf column
    // let projection_mask = ProjectionMask::all();
    // let mut reader =
    //    build_array_reader(Some(&parquet_field), &projection_mask, row_group.as_ref())?;

    //// let mut record_reader = ParquetRecordBatchReader::try_new_with_row_groups(
    ////    &field_levels,
    ////    row_group.as_ref(),
    ////    num_rows,
    ////    None,
    //// )?;
    // let record = record_reader.next().unwrap()?;
    // assert!(record_reader.next().is_none());
    // Ok(record)
    Ok(res)
}
