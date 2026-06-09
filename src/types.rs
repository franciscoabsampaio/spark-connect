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
impl StructType {}
