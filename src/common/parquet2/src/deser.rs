use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::{Column, TableDataType};
use databend_common_expression::TableField;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::read::PageReader;
use parquet2::schema::types::{PhysicalType, PrimitiveType};
use parquet2::FallibleStreamingIterator;

use crate::decompressor::BuffedBasicDecompressor;

pub type ColumnIter<'a> = Box<dyn Iterator<Item = Result<Column>> + Send + Sync + 'a>;
pub fn column_iter_to_columns<'a, I: 'a>(
    mut columns: Vec<BuffedBasicDecompressor<PageReader<&[u8]>>>,
    mut types: Vec<&PrimitiveType>,
    field: TableField,
    chunk_size: Option<usize>,
    num_rows: usize,
) -> Result<ColumnIter<'a>> {
    let mut pages = columns.pop().unwrap();
    let parquet_physical_type = &types.pop().unwrap().physical_type;

    match (parquet_physical_type, field.data_type) {
        (PhysicalType::Int64, _) => {}
    }

    let mut dict_page = None;
    while let Some(page) = pages
        .next()
        .map_err(|e| ErrorCode::StorageOther(e.to_string()))?
    {
        match page {
            Page::Data(data_page) => {
                match data_page.encoding() {
                    // let's focus on plain
                    Encoding::Plain => {
                        let data = data_page.values();
                    }
                    _ => {
                        unimplemented!()
                    }
                }
            }
            Page::Dict(dp) => {
                if dict_page.is_none() {
                    dict_page = Some(dp);
                } else {
                    return Err(ErrorCode::StorageOther(format!(
                        "multiple dictionary pages in one column {}",
                        field.name
                    )));
                }
            }
        }
    }

    todo!()
    // Ok(Box::new(
    //    columns_to_iter_recursive(columns, types, field, vec![], num_rows, chunk_size)?
    //        .map(|x| x.map(|x| x.1)),
    //))
}
