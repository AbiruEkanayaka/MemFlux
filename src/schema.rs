use anyhow::{anyhow, Result};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::types::{
    Db, DbValue, SchemaCache, TransactionIdManager, TransactionStatusManager, VersionedValue,
};
use crate::query_engine::logical_plan::Expression;
use crate::query_engine::ast::{TableConstraint};

use std::fmt;

pub const SCHEMA_PREFIX: &str = "_internal:schemas:";
pub const VIEW_PREFIX: &str = "_internal:views:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    SmallInt,
    Integer,
    BigInt,
    Numeric {
        precision: Option<u32>,
        scale: Option<u32>,
    },
    Real,
    DoublePrecision,
    Text,
    Varchar(u32),
    Char(u32),
    Bytea,
    JsonB,
    Uuid,
    Boolean,
    Timestamp,
    TimestampTz,
    Date,
    Time,
    Array(Box<DataType>),
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::SmallInt => write!(f, "SMALLINT"),
            DataType::Integer => write!(f, "INTEGER"),
            DataType::BigInt => write!(f, "BIGINT"),
            DataType::Numeric { precision, scale } => {
                if let (Some(p), Some(s)) = (precision, scale) {
                    write!(f, "NUMERIC({}, {})", p, s)
                } else if let Some(p) = precision {
                    write!(f, "NUMERIC({})", p)
                } else {
                    write!(f, "NUMERIC")
                }
            }
            DataType::Real => write!(f, "REAL"),
            DataType::DoublePrecision => write!(f, "DOUBLE PRECISION"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Varchar(n) => write!(f, "VARCHAR({})", n),
            DataType::Char(n) => write!(f, "CHAR({})", n),
            DataType::Bytea => write!(f, "BYTEA"),
            DataType::JsonB => write!(f, "JSONB"),
            DataType::Uuid => write!(f, "UUID"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::TimestampTz => write!(f, "TIMESTAMPTZ"),
            DataType::Date => write!(f, "DATE"),
            DataType::Time => write!(f, "TIME"),
            DataType::Array(inner) => write!(f, "{}[]", inner),
        }
    }
}

impl DataType {
    pub fn from_str(s: &str) -> Result<DataType> {
        let upper = s.to_uppercase();
        if upper.ends_with("[]") {
            let inner_type_str = &upper[..upper.len() - 2];
            let inner_type = DataType::from_str(inner_type_str)?;
            return Ok(DataType::Array(Box::new(inner_type)));
        }
        if upper.starts_with("NUMERIC") {
            let mut precision = None;
            let mut scale = None;
            if let Some(rest) = upper.strip_prefix("NUMERIC") {
                let trimmed = rest.trim();
                if trimmed.starts_with('(') && trimmed.ends_with(')') {
                    let parts: Vec<&str> =
                        trimmed[1..trimmed.len() - 1].split(',').map(|p| p.trim()).collect();
                    if let Some(p_str) = parts.get(0) {
                        if !p_str.is_empty() {
                            precision = Some(p_str.parse::<u32>()?);
                        }
                    }
                    if let Some(s_str) = parts.get(1) {
                        if !s_str.is_empty() {
                            scale = Some(s_str.parse::<u32>()?);
                        }
                    }
                }
            }
            Ok(DataType::Numeric { precision, scale })
        } else if upper.starts_with("VARCHAR") {
            if let Some(rest) = upper.strip_prefix("VARCHAR") {
                let trimmed = rest.trim();
                if trimmed.starts_with('(') && trimmed.ends_with(')') {
                    let n_str = &trimmed[1..trimmed.len() - 1];
                    let n = n_str.parse::<u32>()?;
                    return Ok(DataType::Varchar(n));
                }
            }
            Err(anyhow!("Invalid VARCHAR format. Expected VARCHAR(n)"))
        } else if upper.starts_with("CHAR") {
            if let Some(rest) = upper.strip_prefix("CHAR") {
                let trimmed = rest.trim();
                if trimmed.starts_with('(') && trimmed.ends_with(')') {
                    let n_str = &trimmed[1..trimmed.len() - 1];
                    let n = n_str.parse::<u32>()?;
                    return Ok(DataType::Char(n));
                }
            }
            Err(anyhow!("Invalid CHAR format. Expected CHAR(n)"))
        } else if upper == "DOUBLE PRECISION" {
            Ok(DataType::DoublePrecision)
        } else {
            match upper.as_str() {
                "SMALLINT" => Ok(DataType::SmallInt),
                "INTEGER" | "INT" => Ok(DataType::Integer),
                "BIGINT" => Ok(DataType::BigInt),
                "REAL" => Ok(DataType::Real),
                "TEXT" => Ok(DataType::Text),
                "BYTEA" => Ok(DataType::Bytea),
                "JSONB" => Ok(DataType::JsonB),
                "UUID" => Ok(DataType::Uuid),
                "BOOLEAN" => Ok(DataType::Boolean),
                "TIMESTAMP" => Ok(DataType::Timestamp),
                "TIMESTAMPTZ" => Ok(DataType::TimestampTz),
                "DATE" => Ok(DataType::Date),
                "TIME" => Ok(DataType::Time),
                _ => Err(anyhow!("Unsupported data type: {}", s)),
            }
        }
    }
}

impl Serialize for DataType {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        DataType::from_str(&s).map_err(de::Error::custom)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColumnDefinition {
    #[serde(rename = "type")]
    pub data_type: DataType,
    pub nullable: bool,
    #[serde(default)]
    pub default: Option<Expression>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VirtualSchema {
    pub table_name: String,
    pub columns: BTreeMap<String, ColumnDefinition>,
    #[serde(default)]
    pub column_order: Vec<String>,
    #[serde(default)]
    pub constraints: Vec<TableConstraint>,
}

pub async fn load_schemas_from_db(
    db: &Db,
    schema_cache: &SchemaCache,
    tx_status_manager: &TransactionStatusManager,
    tx_id_manager: &TransactionIdManager,
) -> Result<()> {
    let startup_snapshot = crate::types::Snapshot::new(0, tx_status_manager, tx_id_manager);
    let items_to_process: Vec<(String, VersionedValue)> = db
        .iter()
        .filter(|item| item.key().starts_with(SCHEMA_PREFIX))
        .filter_map(|item| {
            item.value()
                .try_read()
                .ok()
                .and_then(|guard| {
                    guard
                        .iter()
                        .rev()
                        .find(|version| startup_snapshot.is_visible(version, tx_status_manager))
                        .cloned()
                })
                .map(|version| (item.key().clone(), version))
        })
        .collect();

    for (key, latest_version) in items_to_process {
        let schema_result: Result<VirtualSchema, _> = match &latest_version.value {
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
    Ok(())
}