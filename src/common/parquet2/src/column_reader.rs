use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Column;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
use parquet2::compression::Compression as ParquetCompression;
use parquet2::metadata::Descriptor;
use parquet2::read::PageMetaData;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveType;

use crate::column::DateIter;
use crate::column::DecimalIter;
use crate::column::Int32Iter;
use crate::column::Int64Iter;
use crate::column::StringIter;
use crate::wip::decompressor::Decompressor;
use crate::PageReader;

pub type ColumnIter<'a> = Box<dyn Iterator<Item = Result<Column>> + Send + Sync + 'a>;

pub fn page_iter_to_columns<'a>(
    mut columns: Vec<Decompressor<'a>>,
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

fn chunks_to_col_iter<'a>(
    meta: &ColumnMeta,
    chunk: &'a [u8],
    rows: usize,
    column_descriptor: &Descriptor,
    field: TableField,
    compression: &Compression,
) -> Result<ColumnIter<'a>> {
    let pages = {
        let meta = meta.as_parquet().unwrap();

        let page_meta_data = PageMetaData {
            column_start: meta.offset,
            num_values: meta.num_values as i64,
            compression: to_parquet_compression(compression)?,
            descriptor: (*column_descriptor).clone(),
        };
        // TODO reuse scratch and uncompressed_buffer
        let pages = PageReader::new_with_page_meta(chunk, page_meta_data, usize::MAX);
        Decompressor::new(pages, vec![])
    };

    let typ = &column_descriptor.primitive_type;

    pages_to_column_iter(pages, &typ, field, rows, None)
}
fn pages_to_column_iter<'a>(
    column: Decompressor<'a>,
    types: &PrimitiveType,
    field: TableField,
    num_rows: usize,
    chunk_size: Option<usize>,
) -> Result<ColumnIter<'a>> {
    let pages = column;
    let parquet_physical_type = &types.physical_type;

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
pub(crate) fn to_parquet_compression(meta_compression: &Compression) -> Result<ParquetCompression> {
    match meta_compression {
        Compression::Lz4 => {
            let err_msg = r#"Deprecated compression algorithm [Lz4] detected.

                                        The Legacy compression algorithm [Lz4] is no longer supported.
                                        To migrate data from old format, please consider re-create the table,
                                        by using an old compatible version [v0.8.25-nightly … v0.7.12-nightly].

                                        - Bring up the compatible version of databend-query
                                        - re-create the table
                                           Suppose the name of table is T
                                            ~~~
                                            create table tmp_t as select * from T;
                                            drop table T all;
                                            alter table tmp_t rename to T;
                                            ~~~
                                        Please note that the history of table T WILL BE LOST.
                                       "#;
            Err(ErrorCode::StorageOther(err_msg))
        }
        Compression::Lz4Raw => Ok(ParquetCompression::Lz4Raw),
        Compression::Snappy => Ok(ParquetCompression::Snappy),
        Compression::Zstd => Ok(ParquetCompression::Zstd),
        Compression::Gzip => Ok(ParquetCompression::Gzip),
        Compression::None => Ok(ParquetCompression::Uncompressed),
    }
}
// pub fn create_with_opt_default_value(
//    block_entries: Vec<BlockEntry>,
//    schema: &DataSchema,
//    default_vals: &[Option<Scalar>],
//    num_rows: usize,
//) -> Result<DataBlock> {
//    let schema_fields = schema.fields();
//    let mut block_entries_iter = block_entries.into_iter();
//
//    let mut entries = Vec::with_capacity(default_vals.len());
//    for (i, default_val) in default_vals.iter().enumerate() {
//        let field = &schema_fields[i];
//        let data_type = field.data_type();
//
//        let entry = match default_val {
//            Some(default_val) => {
//                BlockEntry::new_const_column(data_type.clone(), default_val.to_owned(), num_rows)
//            }
//            None => block_entries_iter
//                .next()
//                .expect("arrays should have enough elements"),
//        };
//
//        entries.push(entry);
//    }
//
//    Ok(DataBlock::new(entries, num_rows))
//}
