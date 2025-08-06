use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveType;
use parquet2::schema::Repetition;

pub fn from_table_field_type(field_name: String, field_type: &TableDataType) -> PrimitiveType {
    let (inner_type, is_nullable) = match field_type {
        TableDataType::Nullable(inner) => (inner.as_ref(), true),
        other => (other, false),
    };

    let mut parquet_primitive_type = match inner_type {
        TableDataType::String => PrimitiveType::from_physical(field_name, PhysicalType::ByteArray),
        TableDataType::Number(number_type) => match number_type {
            NumberDataType::Int8 => PrimitiveType::from_physical(field_name, PhysicalType::Int32),
            NumberDataType::Int16 => PrimitiveType::from_physical(field_name, PhysicalType::Int32),
            NumberDataType::Int32 => PrimitiveType::from_physical(field_name, PhysicalType::Int32),
            NumberDataType::Int64 => PrimitiveType::from_physical(field_name, PhysicalType::Int64),
            NumberDataType::UInt8 => PrimitiveType::from_physical(field_name, PhysicalType::Int32),
            NumberDataType::UInt16 => PrimitiveType::from_physical(field_name, PhysicalType::Int32),
            NumberDataType::UInt32 => PrimitiveType::from_physical(field_name, PhysicalType::Int64),
            NumberDataType::UInt64 => PrimitiveType::from_physical(field_name, PhysicalType::Int64),
            NumberDataType::Float32 => {
                PrimitiveType::from_physical(field_name, PhysicalType::Float)
            }
            NumberDataType::Float64 => {
                PrimitiveType::from_physical(field_name, PhysicalType::Double)
            }
        },
        TableDataType::Decimal(decimal_type) => {
            let precision = decimal_type.precision();
            let _scale = decimal_type.scale();
            if precision <= 9 {
                PrimitiveType::from_physical(field_name, PhysicalType::Int32)
            } else if precision <= 18 {
                PrimitiveType::from_physical(field_name, PhysicalType::Int64)
            } else {
                let len = decimal_length_from_precision(precision as usize);
                // For decimal256
                PrimitiveType::from_physical(field_name, PhysicalType::FixedLenByteArray(len))
            }
        }
        TableDataType::Date => PrimitiveType::from_physical(field_name, PhysicalType::Int32),
        TableDataType::Nullable(_) => {
            // This should not happen due to our unwrapping logic above, but handle it safely
            return from_table_field_type(field_name, inner_type);
        }
        t => unimplemented!("Unsupported type: {:?} ", t),
    };

    // Set repetition based on nullability
    parquet_primitive_type.field_info.repetition = if is_nullable {
        Repetition::Optional
    } else {
        Repetition::Required
    };

    parquet_primitive_type
}

fn decimal_length_from_precision(precision: usize) -> usize {
    // digits = floor(log_10(2^(8*n - 1) - 1))
    // ceil(digits) = log10(2^(8*n - 1) - 1)
    // 10^ceil(digits) = 2^(8*n - 1) - 1
    // 10^ceil(digits) + 1 = 2^(8*n - 1)
    // log2(10^ceil(digits) + 1) = (8*n - 1)
    // log2(10^ceil(digits) + 1) + 1 = 8*n
    // (log2(10^ceil(digits) + 1) + 1) / 8 = n
    (((10.0_f64.powi(precision as i32) + 1.0).log2() + 1.0) / 8.0).ceil() as usize
}

pub fn get_bit_width(max_level: i16) -> u32 {
    16 - max_level.leading_zeros()
}

// mod page_util {
//
//    use parquet2::encoding::get_length;
//    use parquet2::error::Error;
//    use parquet2::page::{DataPage, DataPageHeader, Page};
//
//    /// Splits the page buffer into 3 slices corresponding to (encoded rep levels, encoded def levels, encoded values) for v1 pages.
//    #[inline]
//    pub fn split_buffer_v1(
//        buffer: &[u8],
//        has_rep: bool,
//        has_def: bool,
//    ) -> parquet2::error::Result<(&[u8], &[u8], &[u8])> {
//        let (rep, buffer) = if has_rep {
//            let level_buffer_length = get_length(buffer).ok_or_else(|| {
//                Error::OutOfSpec("The number of bytes declared in v1 rep levels is higher than the page size".to_string())
//            })?;
//            (
//                buffer.get(4..4 + level_buffer_length).ok_or_else(|| {
//                    Error::OutOfSpec(
//                        "The number of bytes declared in v1 rep levels is higher than the page size".to_string(),
//                    )
//                })?,
//                buffer.get(4 + level_buffer_length..).ok_or_else(|| {
//                    Error::OutOfSpec(
//                        "The number of bytes declared in v1 rep levels is higher than the page size".to_string(),
//                    )
//                })?,
//            )
//        } else {
//            (&[] as &[u8], buffer)
//        };
//
//        let (def, buffer) = if has_def {
//            let level_buffer_length = get_length(buffer).ok_or_else(|| {
//                Error::OutOfSpec("The number of bytes declared in v1 rep levels is higher than the page size".to_string())
//            })?;
//            (
//                buffer.get(4..4 + level_buffer_length).ok_or_else(|| {
//                    Error::OutOfSpec(
//                        "The number of bytes declared in v1 def levels is higher than the page size".to_string(),
//                    )
//                })?,
//                buffer.get(4 + level_buffer_length..).ok_or_else(|| {
//                    Error::OutOfSpec(
//                        "The number of bytes declared in v1 def levels is higher than the page size".to_string(),
//                    )
//                })?,
//            )
//        } else {
//            (&[] as &[u8], buffer)
//        };
//
//        Ok((rep, def, buffer))
//    }
//
//    /// Splits the page buffer into 3 slices corresponding to (encoded rep levels, encoded def levels, encoded values) for v2 pages.
//    pub fn split_buffer_v2(
//        buffer: &[u8],
//        rep_level_buffer_length: usize,
//        def_level_buffer_length: usize,
//    ) -> parquet2::error::Result<(&[u8], &[u8], &[u8])> {
//        Ok((
//            &buffer[..rep_level_buffer_length],
//            &buffer[rep_level_buffer_length..rep_level_buffer_length + def_level_buffer_length],
//            &buffer[rep_level_buffer_length + def_level_buffer_length..],
//        ))
//    }
//
//    /// Splits the page buffer into 3 slices corresponding to (encoded rep levels, encoded def levels, encoded values).
//    pub fn split_buffer(page: &DataPage) -> parquet2::error::Result<(&[u8], &[u8], &[u8])> {
//        match page.header() {
//            DataPageHeader::V1(_) => parquet2::page::split_buffer_v1(
//                page.buffer(),
//                page.descriptor.max_rep_level > 0,
//                page.descriptor.max_def_level > 0,
//            ),
//            DataPageHeader::V2(header) => {
//                let def_level_buffer_length: usize = header.definition_levels_byte_length.try_into()?;
//                let rep_level_buffer_length: usize = header.repetition_levels_byte_length.try_into()?;
//                parquet2::page::split_buffer_v2(
//                    page.buffer(),
//                    rep_level_buffer_length,
//                    def_level_buffer_length,
//                )
//            }
//        }
//    }
//
//}
