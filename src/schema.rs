use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::types::{Db, DbValue, SchemaCache};

use std::fmt;

pub const SCHEMA_PREFIX: &str = "_internal:schemas:";

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataType {
    Integer,
    Text,
    Boolean,
    Timestamp,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
        }
    }
}


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColumnDefinition {
    #[serde(rename = "type")]
    pub data_type: DataType,
    pub nullable: bool,
    #[serde(default)]
    pub unique: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VirtualSchema {
    pub table_name: String,
    pub columns: BTreeMap<String, ColumnDefinition>,
    pub primary_key: Option<String>,
}

pub async fn load_schemas_from_db(db: &Db, schema_cache: &SchemaCache) -> Result<()> {
    for item in db.iter() {
        let key = item.key();
        if key.starts_with(SCHEMA_PREFIX) {
            let schema_result: Result<VirtualSchema, _> = match item.value() {
                DbValue::Bytes(bytes) => serde_json::from_slice(bytes),
                DbValue::Json(json_value) => serde_json::from_value(json_value.clone()),
                _ => {
                    eprintln!(
                        "Warning: Schema key '{}' has non-JSON/Bytes value type. Skipping.",
                        key
                    );
                    continue;
                }
            };

            match schema_result {
                Ok(schema) => {
                    let table_name = schema.table_name.clone();
                    schema_cache.insert(table_name, Arc::new(schema));
                }
                Err(e) => {
                    eprintln!("Failed to parse schema for key '{}': {}", key, e);
                }
            }
        }
    }
    Ok(())
}









