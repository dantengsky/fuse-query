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

// TODO doc this module, and refactor it: we no no ArrowError

use std::sync::Arc;

use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Schema as ArrowSchema;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use arrow_schema::TimeUnit;
use deltalake::kernel::error::Error;
use deltalake::kernel::ArrayType;
use deltalake::kernel::DataType;
use deltalake::kernel::MapType;
use deltalake::kernel::MetadataValue;
use deltalake::kernel::PrimitiveType;
use deltalake::kernel::StructField;
use deltalake::kernel::StructType;
use itertools::Itertools;

pub(crate) const LIST_ARRAY_ROOT: &str = "element";
pub(crate) const MAP_ROOT_DEFAULT: &str = "key_value";
pub(crate) const MAP_KEY_DEFAULT: &str = "key";
pub(crate) const MAP_VALUE_DEFAULT: &str = "value";

pub(crate) trait TryFromValue<T>: Sized {
    /// The type returned in the event of a conversion error.
    type Error;

    /// Performs the conversion.
    fn try_from_value(value: T) -> std::result::Result<Self, Self::Error>;
}

pub(crate) trait TryIntoValue<T>: Sized {
    /// The type returned in the event of a conversion error.
    type Error;

    /// Performs the conversion.
    fn try_into_value(self) -> std::result::Result<T, Self::Error>;
}

impl<T, U> TryIntoValue<U> for T
where U: TryFromValue<T>
{
    type Error = U::Error;

    #[inline]
    fn try_into_value(self) -> std::result::Result<U, U::Error> {
        U::try_from_value(self)
    }
}

impl TryFromValue<&StructType> for ArrowSchema {
    type Error = deltalake::arrow::error::ArrowError;

    fn try_from_value(
        s: &StructType,
    ) -> std::result::Result<Self, deltalake::arrow::error::ArrowError> {
        let fields: Vec<ArrowField> = s.fields().map(TryIntoValue::try_into_value).try_collect()?;
        Ok(ArrowSchema::new(fields))
    }
}

impl TryFromValue<&StructField> for ArrowField {
    type Error = deltalake::arrow::error::ArrowError;

    fn try_from_value(
        f: &StructField,
    ) -> std::result::Result<Self, deltalake::arrow::error::ArrowError> {
        let metadata = f
            .metadata()
            .iter()
            .map(|(key, val)| match &val {
                &MetadataValue::String(val) => Ok((key.clone(), val.clone())),
                _ => Ok((key.clone(), serde_json::to_string(val)?)),
            })
            .collect::<std::result::Result<_, serde_json::Error>>()
            .map_err(|err| deltalake::arrow::error::ArrowError::JsonError(err.to_string()))?;

        let field = ArrowField::new(
            f.name(),
            ArrowDataType::try_from_value(f.data_type())?,
            f.is_nullable(),
        )
        .with_metadata(metadata);

        Ok(field)
    }
}

impl TryFromValue<&ArrayType> for ArrowField {
    type Error = deltalake::arrow::error::ArrowError;

    fn try_from_value(
        a: &ArrayType,
    ) -> std::result::Result<Self, deltalake::arrow::error::ArrowError> {
        Ok(ArrowField::new(
            LIST_ARRAY_ROOT,
            ArrowDataType::try_from_value(a.element_type())?,
            a.contains_null(),
        ))
    }
}

impl TryFromValue<&MapType> for ArrowField {
    type Error = deltalake::arrow::error::ArrowError;

    fn try_from_value(
        a: &MapType,
    ) -> std::result::Result<Self, deltalake::arrow::error::ArrowError> {
        Ok(ArrowField::new(
            MAP_ROOT_DEFAULT,
            ArrowDataType::Struct(
                vec![
                    ArrowField::new(
                        MAP_KEY_DEFAULT,
                        ArrowDataType::try_from_value(a.key_type())?,
                        false,
                    ),
                    ArrowField::new(
                        MAP_VALUE_DEFAULT,
                        ArrowDataType::try_from_value(a.value_type())?,
                        a.value_contains_null(),
                    ),
                ]
                .into(),
            ),
            false, // always non-null
        ))
    }
}

impl TryFromValue<&DataType> for ArrowDataType {
    type Error = deltalake::arrow::error::ArrowError;

    fn try_from_value(
        t: &DataType,
    ) -> std::result::Result<Self, deltalake::arrow::error::ArrowError> {
        match t {
            DataType::Primitive(p) => {
                match p {
                    PrimitiveType::String => Ok(ArrowDataType::Utf8),
                    PrimitiveType::Long => Ok(ArrowDataType::Int64), // undocumented type
                    PrimitiveType::Integer => Ok(ArrowDataType::Int32),
                    PrimitiveType::Short => Ok(ArrowDataType::Int16),
                    PrimitiveType::Byte => Ok(ArrowDataType::Int8),
                    PrimitiveType::Float => Ok(ArrowDataType::Float32),
                    PrimitiveType::Double => Ok(ArrowDataType::Float64),
                    PrimitiveType::Boolean => Ok(ArrowDataType::Boolean),
                    PrimitiveType::Binary => Ok(ArrowDataType::Binary),
                    PrimitiveType::Decimal(dtype) => Ok(ArrowDataType::Decimal128(
                        dtype.precision(),
                        dtype.scale() as i8, // 0..=38
                    )),
                    PrimitiveType::Date => {
                        // A calendar date, represented as a year-month-day triple without a
                        // timezone. Stored as 4 bytes integer representing days since 1970-01-01
                        Ok(ArrowDataType::Date32)
                    }
                    // TODO: https://github.com/delta-io/delta/issues/643
                    PrimitiveType::Timestamp => Ok(ArrowDataType::Timestamp(
                        TimeUnit::Microsecond,
                        Some("UTC".into()),
                    )),
                    PrimitiveType::TimestampNtz => {
                        Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
                    }
                }
            }
            DataType::Struct(s) => {
                Ok(
                    ArrowDataType::Struct(
                        s.fields()
                            .map(TryIntoValue::try_into_value)
                            .collect::<std::result::Result<
                                Vec<ArrowField>,
                                deltalake::arrow::error::ArrowError,
                            >>()?
                            .into(),
                    ),
                )
            }
            DataType::Array(a) => Ok(ArrowDataType::List(Arc::new(a.as_ref().try_into_value()?))),
            DataType::Map(m) => Ok(ArrowDataType::Map(
                Arc::new(m.as_ref().try_into_value()?),
                false,
            )),
        }
    }
}

impl TryFromValue<&ArrowSchema> for StructType {
    type Error = deltalake::arrow::error::ArrowError;

    fn try_from_value(
        arrow_schema: &ArrowSchema,
    ) -> std::result::Result<Self, deltalake::arrow::error::ArrowError> {
        StructType::try_new(
            arrow_schema
                .fields()
                .iter()
                .map(|field| field.as_ref().try_into_value()),
        )
    }
}

impl TryFromValue<ArrowSchemaRef> for StructType {
    type Error = deltalake::arrow::error::ArrowError;

    fn try_from_value(
        arrow_schema: ArrowSchemaRef,
    ) -> std::result::Result<Self, deltalake::arrow::error::ArrowError> {
        arrow_schema.as_ref().try_into_value()
    }
}

impl TryFromValue<&ArrowField> for StructField {
    type Error = deltalake::arrow::error::ArrowError;

    fn try_from_value(
        arrow_field: &ArrowField,
    ) -> std::result::Result<Self, deltalake::arrow::error::ArrowError> {
        Ok(StructField::new(
            arrow_field.name().clone(),
            DataType::try_from_value(arrow_field.data_type())?,
            arrow_field.is_nullable(),
        )
        .with_metadata(arrow_field.metadata().iter().map(|(k, v)| (k.clone(), v))))
    }
}

impl TryFromValue<&ArrowDataType> for DataType {
    type Error = deltalake::arrow::error::ArrowError;

    fn try_from_value(
        arrow_datatype: &ArrowDataType,
    ) -> std::result::Result<Self, deltalake::arrow::error::ArrowError> {
        match arrow_datatype {
            ArrowDataType::Utf8 => Ok(DataType::STRING),
            ArrowDataType::LargeUtf8 => Ok(DataType::STRING),
            ArrowDataType::Utf8View => Ok(DataType::STRING),
            ArrowDataType::Int64 => Ok(DataType::LONG), // undocumented type
            ArrowDataType::Int32 => Ok(DataType::INTEGER),
            ArrowDataType::Int16 => Ok(DataType::SHORT),
            ArrowDataType::Int8 => Ok(DataType::BYTE),
            ArrowDataType::UInt64 => Ok(DataType::LONG), // undocumented type
            ArrowDataType::UInt32 => Ok(DataType::INTEGER),
            ArrowDataType::UInt16 => Ok(DataType::SHORT),
            ArrowDataType::UInt8 => Ok(DataType::BYTE),
            ArrowDataType::Float32 => Ok(DataType::FLOAT),
            ArrowDataType::Float64 => Ok(DataType::DOUBLE),
            ArrowDataType::Boolean => Ok(DataType::BOOLEAN),
            ArrowDataType::Binary => Ok(DataType::BINARY),
            ArrowDataType::FixedSizeBinary(_) => Ok(DataType::BINARY),
            ArrowDataType::LargeBinary => Ok(DataType::BINARY),
            ArrowDataType::BinaryView => Ok(DataType::BINARY),
            ArrowDataType::Decimal128(p, s) => {
                if *s < 0 {
                    return Err(deltalake::arrow::error::ArrowError::from_external_error(
                        // TODO
                        // Error::invalid_decimal("Negative scales are not supported in Delta").into(),
                        Error::Generic("Negative scales are not supported in Delta".to_owned())
                            .into(),
                    ));
                };
                DataType::decimal(*p, *s as u8)
                    .map_err(|e| deltalake::arrow::error::ArrowError::from_external_error(e.into()))
            }
            ArrowDataType::Date32 => Ok(DataType::DATE),
            ArrowDataType::Date64 => Ok(DataType::DATE),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => Ok(DataType::TIMESTAMP_NTZ),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(tz))
                if tz.eq_ignore_ascii_case("utc") =>
            {
                Ok(DataType::TIMESTAMP)
            }
            ArrowDataType::Struct(fields) => DataType::try_struct_type(
                fields.iter().map(|field| field.as_ref().try_into_value()),
            ),
            ArrowDataType::List(field) => Ok(ArrayType::new(
                (*field).data_type().try_into_value()?,
                (*field).is_nullable(),
            )
            .into()),
            ArrowDataType::ListView(field) => Ok(ArrayType::new(
                (*field).data_type().try_into_value()?,
                (*field).is_nullable(),
            )
            .into()),
            ArrowDataType::LargeList(field) => Ok(ArrayType::new(
                (*field).data_type().try_into_value()?,
                (*field).is_nullable(),
            )
            .into()),
            ArrowDataType::LargeListView(field) => Ok(ArrayType::new(
                (*field).data_type().try_into_value()?,
                (*field).is_nullable(),
            )
            .into()),
            ArrowDataType::FixedSizeList(field, _) => Ok(ArrayType::new(
                (*field).data_type().try_into_value()?,
                (*field).is_nullable(),
            )
            .into()),
            ArrowDataType::Map(field, _) => {
                if let ArrowDataType::Struct(struct_fields) = field.data_type() {
                    let key_type = DataType::try_from_value(struct_fields[0].data_type())?;
                    let value_type = DataType::try_from_value(struct_fields[1].data_type())?;
                    let value_type_nullable = struct_fields[1].is_nullable();
                    Ok(MapType::new(key_type, value_type, value_type_nullable).into())
                } else {
                    panic!("DataType::Map should contain a struct field child");
                }
            }
            // Dictionary types are just an optimized in-memory representation of an array.
            // Schema-wise, they are the same as the value type.
            ArrowDataType::Dictionary(_, value_type) => Ok(value_type.as_ref().try_into_value()?),
            s => Err(deltalake::arrow::error::ArrowError::SchemaError(format!(
                "Invalid data type for Delta Lake: {s}"
            ))),
        }
    }
}
