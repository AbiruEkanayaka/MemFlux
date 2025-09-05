use anyhow::Result;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, RwLock};

use crate::config::Config;
use crate::indexing::IndexManager;
use crate::memory::MemoryManager;
use crate::query_engine::ast::SelectStatement;
use crate::schema::VirtualSchema;

// --- Core Data Structures ---

pub enum DbValue {
    Json(Value),
    JsonB(Vec<u8>),
    List(RwLock<VecDeque<Vec<u8>>>),
    Set(RwLock<HashSet<Vec<u8>>>),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
}

impl fmt::Debug for DbValue {
    /// Formats a DbValue for debug output.
    ///
    /// Produces concise, human-readable representations for each DbValue variant:
    /// - `Json(...)` prints the debug representation of the contained `Value`.
    /// - `JsonB(...)` prints the contained bytes in debug form.
    /// - `List(<RwLock>)` and `Set(<RwLock>)` print placeholder text to avoid locking during debug.
    /// - `Bytes(...)` prints a UTF-8 lossy string representation of the bytes.
    /// - `Array(...)` prints the debug representation of the contained array.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::fmt;
    /// // assume DbValue is in scope
    /// let v = DbValue::Bytes(b"hello".to_vec());
    /// let s = format!("{:?}", v);
    /// assert_eq!(s, "Bytes(\"hello\")");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbValue::Json(v) => write!(f, "Json({:?})", v),
            DbValue::JsonB(b) => write!(f, "JsonB({:?})", b),
            DbValue::List(_) => write!(f, "List(<RwLock>)"),
            DbValue::Set(_) => write!(f, "Set(<RwLock>)"),
            DbValue::Bytes(b) => write!(f, "Bytes({:?})", String::from_utf8_lossy(b)),
            DbValue::Array(a) => write!(f, "Array({:?})", a),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum SerializableDbValue {
    Json(Value),
    JsonB(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Set(Vec<Vec<u8>>),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
}

impl SerializableDbValue {
    /// Convert a `DbValue` into its serializable `SerializableDbValue` form.
    ///
    /// For `List` and `Set` variants this asynchronously acquires a read lock to clone the
    /// contained collection; other variants are cloned directly.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use your_crate::types::{DbValue, SerializableDbValue};
    /// # tokio_test::block_on(async {
    /// let v = DbValue::Bytes(b"hello".to_vec());
    /// let s = SerializableDbValue::from_db_value(&v).await;
    /// match s {
    ///     SerializableDbValue::Bytes(b) => assert_eq!(b, b"hello".to_vec()),
    ///     _ => panic!("unexpected variant"),
    /// }
    /// # });
    /// ```
    pub async fn from_db_value(db_value: &DbValue) -> Self {
        match db_value {
            DbValue::Json(v) => SerializableDbValue::Json(v.clone()),
            DbValue::JsonB(b) => SerializableDbValue::JsonB(b.clone()),
            DbValue::Bytes(b) => SerializableDbValue::Bytes(b.clone()),
            DbValue::List(lock) => {
                let list = lock.read().await;
                SerializableDbValue::List(list.clone())
            }
            DbValue::Set(lock) => {
                let set = lock.read().await;
                SerializableDbValue::Set(set.iter().cloned().collect())
            }
            DbValue::Array(a) => SerializableDbValue::Array(a.clone()),
        }
    }

    /// Convert a SerializableDbValue into an owned in-memory DbValue.
    ///
    /// This consumes the serializable representation and produces the corresponding
    /// runtime `DbValue`. Containers that require interior mutability are rebuilt:
    /// - `List` becomes `DbValue::List(RwLock::new(...))`
    /// - `Set` becomes `DbValue::Set(RwLock::new(...))` (the vector is converted into a `HashSet`)
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use tokio::sync::RwLock;
    /// // example: convert a JSON-serializable value into a runtime DbValue
    /// let s = SerializableDbValue::Json(serde_json::json!({"a": 1}));
    /// let dbv = s.into_db_value();
    /// match dbv {
    ///     DbValue::Json(v) => assert_eq!(v["a"], 1),
    ///     _ => panic!("expected Json variant"),
    /// }
    /// ```
    pub fn into_db_value(self) -> DbValue {
        match self {
            SerializableDbValue::Json(v) => DbValue::Json(v),
            SerializableDbValue::JsonB(b) => DbValue::JsonB(b),
            SerializableDbValue::Bytes(b) => DbValue::Bytes(b),
            SerializableDbValue::List(v) => DbValue::List(RwLock::new(v)),
            SerializableDbValue::Set(v) => DbValue::Set(RwLock::new(v.into_iter().collect())),
            SerializableDbValue::Array(a) => DbValue::Array(a),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum LogEntry {
    SetBytes { key: String, value: Vec<u8> },
    SetJsonB { key: String, value: Vec<u8> },
    Delete { key: String },
    JsonSet { path: String, value: String },
    JsonDelete { path: String },
    LPush { key: String, values: Vec<Vec<u8>> },
    RPush { key: String, values: Vec<Vec<u8>> },
    LPop { key: String, count: usize },
    RPop { key: String, count: usize },
    SAdd { key: String, members: Vec<Vec<u8>> },
    SRem { key: String, members: Vec<Vec<u8>> },
    RenameTable { old_name: String, new_name: String },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SnapshotEntry {
    pub key: String,
    pub value: SerializableDbValue,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ViewDefinition {
    pub name: String,
    pub query: SelectStatement,
}

pub struct LogRequest {
    pub entry: LogEntry,
    pub ack: oneshot::Sender<Result<(), String>>,
}

// --- Type Aliases ---

pub type Logger = mpsc::Sender<LogRequest>;
pub type Db = Arc<DashMap<String, DbValue>>;
pub type JsonCache = Arc<DashMap<String, Arc<Vec<u8>>>>;
pub type SchemaCache = Arc<DashMap<String, Arc<VirtualSchema>>>;
pub type ViewCache = Arc<DashMap<String, Arc<ViewDefinition>>>;
pub type ScalarFunction = Box<dyn Fn(Vec<Value>) -> Result<Value> + Send + Sync>;

// --- Function Registry ---

#[derive(Default)]
pub struct FunctionRegistry {
    pub functions: HashMap<String, ScalarFunction>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, name: &str, func: ScalarFunction) {
        self.functions.insert(name.to_uppercase(), func);
    }


    pub fn get(&self, name: &str) -> Option<&ScalarFunction> {
        self.functions.get(&name.to_uppercase())
    }
}

// --- Application Context & Command Structures ---

#[derive(Clone)]
pub struct AppContext {
    pub db: Db,
    pub logger: Logger,
    pub index_manager: Arc<IndexManager>,
    pub json_cache: JsonCache,
    pub schema_cache: SchemaCache,
    pub view_cache: ViewCache,
    pub function_registry: Arc<FunctionRegistry>,
    pub config: Arc<Config>,
    pub memory: Arc<MemoryManager>,
}

pub struct Command {
    pub name: String,
    pub args: Vec<Vec<u8>>,
}

pub enum Response {
    Ok,
    SimpleString(String),
    Bytes(Vec<u8>),
    MultiBytes(Vec<Vec<u8>>),
    Integer(i64),
    Nil,
    Error(String),
}

impl Response {
    pub fn into_protocol_format(self) -> Vec<u8> {
        match self {
            Response::Ok => b"+OK\r\n".to_vec(),
            Response::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            Response::Bytes(b) => {
                let mut response = format!("${}\r\n", b.len()).into_bytes();
                response.extend_from_slice(&b);
                response.extend_from_slice(b"\r\n");
                response
            }

            Response::MultiBytes(vals) => {
                let mut response = format!("*{}\r\n", vals.len()).into_bytes();
                for v in vals {
                    let mut bulk_string = format!("${}\r\n", v.len()).into_bytes();
                    bulk_string.extend_from_slice(&v);
                    bulk_string.extend_from_slice(b"\r\n");
                    response.extend(bulk_string);
                }
                response
            }
            Response::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            Response::Nil => b"$-1\r\n".to_vec(),
            Response::Error(e) => format!("-ERR {}\r\n", e).into_bytes(),
        }
    }
}









