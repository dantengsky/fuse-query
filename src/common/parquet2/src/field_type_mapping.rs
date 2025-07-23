use databend_common_expression::TableDataType;
use parquet2::schema::types::{PhysicalType, PrimitiveType};


fn from_table_filed_type(field_tyep: TableDataType) -> PrimitiveType {
        let mut parquet_primitive_type = match column.table_field.data_type {
            TableDataType::String => PrimitiveType::from_physical(
                column.table_field.name.clone(),
                PhysicalType::ByteArray,
            ),
            TableDataType::Number(number_type) => match number_type {
                NumberDataType::Int8 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int32,
                ),
                NumberDataType::Int16 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int32,
                ),
                NumberDataType::Int32 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int32,
                ),
                NumberDataType::Int64 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int64,
                ),
                NumberDataType::UInt8 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int32,
                ),
                NumberDataType::UInt16 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int32,
                ),
                NumberDataType::UInt32 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int64,
                ),
                NumberDataType::UInt64 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Int64,
                ),
                NumberDataType::Float32 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Float,
                ),
                NumberDataType::Float64 => PrimitiveType::from_physical(
                    column.table_field.name.clone(),
                    PhysicalType::Double,
                ),
            },
            TableDataType::Decimal(decimal_type) => {
                let precision = decimal_type.precision();
                let _scale = decimal_type.scale();
                if precision <= 9 {
                    PrimitiveType::from_physical(
                        column.table_field.name.clone(),
                        PhysicalType::Int32,
                    )
                } else if precision <= 18 {
                    PrimitiveType::from_physical(
                        column.table_field.name.clone(),
                        PhysicalType::Int64,
                    )
                } else {
                    let len = decimal_length_from_precision(precision as usize);
                    // For decimal256
                    PrimitiveType::from_physical(
                        column.table_field.name.clone(),
                        PhysicalType::FixedLenByteArray(len),
                    )
                }
            }
            TableDataType::Date => {
                PrimitiveType::from_physical(column.table_field.name.clone(), PhysicalType::Int32)
            }
            _ => unimplemented!(),
        };

}
