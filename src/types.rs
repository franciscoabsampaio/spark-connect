use crate::spark;
use crate::{SparkError, error::SparkErrorKind};

use api_parity_rs::{parity, parity_impl};
use serde_json;
use std::collections::HashMap;


#[parity(
    path = "pyspark.sql.types.DataType",
    status = Implemented,
)]
pub type DataType = arrow_schema::DataType;


pub struct StructField {
    name: String,
    data_type: DataType,
    nullable: bool,
    metadata: Option<HashMap<String, String>>,
}

#[parity_impl(
    path = "pyspark.sql.types.StructField",
    status = Implemented,
)]
impl StructField {
    pub fn new(
        name: String,
        data_type: DataType,
        nullable: bool,
        metadata: Option<HashMap<String, String>>,
    ) -> Self {
        StructField { name, data_type, nullable, metadata }
    }

    #[parity(
        path = ".fromJson",
        status = Implemented,
    )]
    pub fn from_json(json: serde_json::Value) -> Result<Self, SparkError> {
        let str_field = |key: &str| {
            json[key]
                .as_str()
                .ok_or_else(|| SparkError::new(SparkErrorKind::InvalidField(key.into())))
        };

        let name = str_field("name")?.to_string();
        let data_type = str_field("type")?
            .parse::<DataType>()
            .map_err(|_| SparkError::new(SparkErrorKind::InvalidField("type".into())))?;
        let nullable = json["nullable"]
            .as_bool()
            .ok_or_else(|| SparkError::new(SparkErrorKind::InvalidField("nullable".into())))?;

        let metadata = json["metadata"]
            .as_object()
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| {
                        v.as_str()
                            .map(|s| (k.clone(), s.to_string()))
                            .ok_or_else(|| {
                                SparkError::new(SparkErrorKind::InvalidField("metadata".into()))
                            })
                    })
                    .collect::<Result<HashMap<_, _>, _>>()
            })
            .transpose()?;

        Ok(StructField { name, data_type, nullable, metadata })
    }

    #[parity(
        path = ".simpleString",
        status = Implemented,
    )]
    pub fn simple_string(&self) -> String {
        format!("{}:{}", self.name, self.data_type)
    }

    /// Renders the field as Spark DDL, e.g. `"id INT"`.
    ///
    /// Unlike [`simple_string`](Self::simple_string), which prints the Arrow
    /// type name, this prints the Spark SQL one the server can parse.
    fn ddl(&self) -> Result<String, SparkError> {
        let nullable = if self.nullable { "" } else { " NOT NULL" };

        Ok(format!("{} {}{}", self.name, spark_type_name(&self.data_type)?, nullable))
    }

    #[parity(
        path = ".jsonValue",
        status = Implemented,
    )]
    pub fn json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "type": self.data_type.to_string(),
            "nullable": self.nullable,
            "metadata": self.metadata,
        })
    }
}

pub struct StructType {
    fields: Vec<StructField>,
}

#[parity_impl(
    path = "pyspark.sql.types.StructType",
    status = Implemented,
)]
impl StructType {
    pub fn new(fields: Vec<StructField>) -> Self {
        StructType { fields }
    }

    /// Renders the schema as Spark DDL, e.g. `"id INT, label STRING NOT NULL"`.
    #[parity(
        path = ".simpleString",
        status = Partial,
        comment = "Renders DDL for the server to parse; PySpark's form is lowercase `struct<...>`.",
    )]
    pub fn ddl(&self) -> Result<String, SparkError> {
        let fields: Result<Vec<String>, SparkError> =
            self.fields.iter().map(StructField::ddl).collect();

        Ok(fields?.join(", "))
    }

    /// Converts to the wire type Spark Connect expects for a schema.
    ///
    /// Spark Connect's `DataType` is a full structural type tree, but it
    /// carries an `Unparsed` arm that takes DDL, so the server's own parser
    /// does the work rather than a second type mapping living here.
    pub(crate) fn to_proto(&self) -> Result<spark::DataType, SparkError> {
        Ok(spark::DataType {
            kind: Some(spark::data_type::Kind::Unparsed(spark::data_type::Unparsed {
                data_type_string: self.ddl()?,
            })),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_schema::{Field, Fields, TimeUnit};
    use std::sync::Arc;

    fn field(name: &str, data_type: DataType, nullable: bool) -> StructField {
        StructField::new(name.into(), data_type, nullable, None)
    }

    #[test]
    fn test_ddl_renders_spark_type_names() {
        let schema = StructType::new(vec![
            field("id", DataType::Int32, true),
            field("label", DataType::Utf8, false),
            field("score", DataType::Float64, true),
        ]);

        assert_eq!(schema.ddl().unwrap(), "id int, label string NOT NULL, score double");
    }

    /// Arrow's own `Display` is not Spark DDL — `Int32` is not `int` — so the
    /// mapping has to be explicit.
    #[test]
    fn test_ddl_differs_from_arrow_display() {
        let schema = StructType::new(vec![field("id", DataType::Int32, true)]);

        assert_eq!(DataType::Int32.to_string(), "Int32");
        assert_eq!(schema.ddl().unwrap(), "id int");
    }

    #[test]
    fn test_nested_types() {
        let inner = Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let schema = StructType::new(vec![
            field("tags", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
            field("nested", DataType::Struct(inner), true),
            field("amount", DataType::Decimal128(10, 2), true),
            field("at", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            field("at_tz", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
        ]);

        assert_eq!(
            schema.ddl().unwrap(),
            "tags array<string>, nested struct<a:bigint,b:string>, \
             amount decimal(10,2), at timestamp_ntz, at_tz timestamp"
        );
    }

    /// Spark has no unsigned integers; widening silently would corrupt data,
    /// so the conversion refuses instead.
    #[test]
    fn test_unsupported_arrow_type_is_rejected() {
        let schema = StructType::new(vec![field("count", DataType::UInt64, true)]);

        assert!(schema.ddl().is_err());
    }

    #[test]
    fn test_to_proto_wraps_ddl_as_unparsed() {
        let schema = StructType::new(vec![field("id", DataType::Int32, true)]);

        match schema.to_proto().unwrap().kind {
            Some(spark::data_type::Kind::Unparsed(unparsed)) => {
                assert_eq!(unparsed.data_type_string, "id int");
            }
            other => panic!("unexpected kind: {other:?}"),
        }
    }
}

/// The Spark SQL name for an Arrow type.
///
/// Arrow types that Spark has no equivalent for (unsigned integers, 32-bit
/// times, ...) are rejected rather than silently widened.
fn spark_type_name(data_type: &DataType) -> Result<String, SparkError> {
    let unsupported =
        || SparkError::new(SparkErrorKind::Unimplemented(format!("Arrow type {data_type}")));

    Ok(match data_type {
        DataType::Null => "void".into(),
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "tinyint".into(),
        DataType::Int16 => "smallint".into(),
        DataType::Int32 => "int".into(),
        DataType::Int64 => "bigint".into(),
        DataType::Float16 | DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".into(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "binary".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        // Spark distinguishes the two timestamp types by whether a zone is set.
        DataType::Timestamp(_, None) => "timestamp_ntz".into(),
        DataType::Timestamp(_, Some(_)) => "timestamp".into(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("decimal({precision},{scale})")
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            format!("array<{}>", spark_type_name(field.data_type())?)
        }
        DataType::Struct(fields) => {
            let rendered: Result<Vec<String>, SparkError> = fields
                .iter()
                .map(|field| {
                    Ok(format!("{}:{}", field.name(), spark_type_name(field.data_type())?))
                })
                .collect();
            format!("struct<{}>", rendered?.join(","))
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(unsupported());
            };
            let [key, value] = fields.iter().collect::<Vec<_>>()[..] else {
                return Err(unsupported());
            };
            format!(
                "map<{},{}>",
                spark_type_name(key.data_type())?,
                spark_type_name(value.data_type())?
            )
        }
        _ => return Err(unsupported()),
    })
}
