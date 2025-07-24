use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveType;
use parquet2::schema::Repetition;

pub fn from_table_filed_type(field_name: String, field_type: &TableDataType) -> PrimitiveType {
    let mut parquet_primitive_type = match field_type {
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
        t => unimplemented!("Unsupported type: {:?} ", t),
    };

    parquet_primitive_type.field_info.repetition = Repetition::Required;

    parquet_primitive_type
}

fn decimal_length_from_precision(precision: usize) -> usize {
    // digits = floor(log_10(2^(8*n - 1) - 1))
    // ceil(digits) = log10(2^(8*n - 1) - 1)
    // 10^ceil(digits) = 2^(8*n - 1) - 1
    // 10^ceil(digits) + 1 = 2^(8*n - 1)
    // log2(10^ceil(digits) + 1) = (8*n - 1)
    // log2(10^ceil(digits) + 1) + 1 = 8*n
    // (log2(10^ceil(a) + 1) + 1) / 8 = n
    (((10.0_f64.powi(precision as i32) + 1.0).log2() + 1.0) / 8.0).ceil() as usize
}
