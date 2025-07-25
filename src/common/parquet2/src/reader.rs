use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Column;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use parquet2::read::Decompressor;
use parquet2::read::PageReader;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveType;

use crate::column::DateIter;
use crate::column::DecimalIter;
use crate::column::Int32Iter;
use crate::column::Int64Iter;
use crate::column::StringIter;

pub type ColumnIter<'a> = Box<dyn Iterator<Item = Result<Column>> + Send + Sync + 'a>;

pub fn page_iter_to_columns<'a>(
    mut columns: Vec<Decompressor<PageReader<&'a [u8]>>>,
    mut types: Vec<&PrimitiveType>,
    field: TableField,
    chunk_size: Option<usize>,
    num_rows: usize,
) -> Result<ColumnIter<'a>> {
    let pages = columns.pop().unwrap();
    let parquet_physical_type = &types.pop().unwrap().physical_type;

    match (parquet_physical_type, field.data_type) {
        (PhysicalType::Int32, TableDataType::Number(NumberDataType::Int32)) => {
            Ok(Box::new(Int32Iter::new(pages, num_rows, chunk_size)))
        }
        (PhysicalType::Int64, TableDataType::Number(NumberDataType::Int64)) => {
            Ok(Box::new(Int64Iter::new(pages, num_rows, chunk_size)))
        }
        (PhysicalType::ByteArray, TableDataType::String) => {
            Ok(Box::new(StringIter::new(pages, num_rows, chunk_size)))
        }
        (PhysicalType::Int64, TableDataType::Decimal(DecimalDataType::Decimal64(decimal_size))) => {
            // Handle DECIMAL(15, 2) stored as Int64
            Ok(Box::new(DecimalIter::new(
                pages,
                num_rows,
                chunk_size,
                decimal_size.precision(),
                decimal_size.scale(),
            )))
        }
        (PhysicalType::Int32, TableDataType::Date) => {
            Ok(Box::new(DateIter::new(pages, num_rows, chunk_size)))
        }
        (physical_type, table_data_type) => Err(ErrorCode::StorageOther(format!(
            "Unsupported combination: parquet_physical_type={:?}, field_data_type={:?}",
            physical_type, table_data_type
        ))),
    }
}
