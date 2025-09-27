use anyhow::Result;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, RwLock};
use uuid::Uuid;

use crate::config::{Config, DurabilityLevel};
use crate::indexing::IndexManager;
use crate::memory::MemoryManager;
use crate::query_engine::ast::SelectStatement;
use crate::schema::VirtualSchema;

// --- Core Data Structures ---

// NEW: MVCC Types
pub type TxId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    Active,
    Committed,
    Aborted,
}

#[derive(Debug)]
pub struct TransactionIdManager {
    next_txid: AtomicU64,
}

impl Default for TransactionIdManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionIdManager {
    pub fn new() -> Self {
        // TXID 0 is reserved for pre-existing data from snapshots.
        Self {
            next_txid: AtomicU64::new(1),
        }
    }

    pub fn new_txid(&self) -> TxId {
        self.next_txid.fetch_add(1, Ordering::Relaxed)
    }

    pub fn get_current_txid(&self) -> TxId {
        self.next_txid.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.next_txid.store(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
pub struct TransactionStatusManager {
    // This map can grow indefinitely. A cleanup mechanism will be needed in Phase 3 (Vacuum).
    statuses: DashMap<TxId, TransactionStatus>,
}

impl TransactionStatusManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin(&self, txid: TxId) {
        self.statuses.insert(txid, TransactionStatus::Active);
    }

    pub fn commit(&self, txid: TxId) {
        self.statuses.insert(txid, TransactionStatus::Committed);
    }

    pub fn abort(&self, txid: TxId) {
        self.statuses.insert(txid, TransactionStatus::Aborted);
    }

    pub fn get_status(&self, txid: TxId) -> Option<TransactionStatus> {
        if txid == 0 {
            return Some(TransactionStatus::Committed);
        }
        self.statuses.get(&txid).map(|s| *s.value())
    }

    pub fn get_active_txids(&self) -> HashSet<TxId> {
        self.statuses
            .iter()
            .filter(|entry| *entry.value() == TransactionStatus::Active)
            .map(|entry| *entry.key())
            .collect()
    }

    pub fn reset(&self) {
        self.statuses.clear();
    }
}

// This will be the new value in the main DB map
#[derive(Debug, Clone)]
pub struct VersionedValue {
    pub value: DbValue,
    pub creator_txid: TxId,
    pub expirer_txid: TxId, // 0 means not expired
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub txid: TxId,
    pub xmin: TxId,          // The oldest active transaction ID.
    pub xmax: TxId,          // The next transaction ID to be handed out.
    pub xip: HashSet<TxId>, // The set of in-progress transaction IDs.
}

impl Snapshot {
    pub fn new(
        txid: TxId,
        tx_manager: &TransactionStatusManager,
        id_manager: &TransactionIdManager,
    ) -> Self {
        let active_txids = tx_manager.get_active_txids();
        let xmin = active_txids
            .iter()
            .min()
            .copied()
            .unwrap_or_else(|| id_manager.get_current_txid());
        Self {
            txid,
            xmin,
            xmax: id_manager.get_current_txid(),
            xip: active_txids,
        }
    }

    /// Checks if a given version is visible to the transaction owning this snapshot.
    pub fn is_visible(&self, version: &VersionedValue, status_manager: &TransactionStatusManager) -> bool {
        // Rule 1: The creating transaction is the current transaction.
        // The version is visible if it hasn't also been expired by the same transaction.
        if version.creator_txid == self.txid {
            return version.expirer_txid == 0 || version.expirer_txid != self.txid;
        }

        // Rule 2: The creating transaction must have been committed.
        let creator_status = status_manager.get_status(version.creator_txid);
        if creator_status != Some(TransactionStatus::Committed) {
            return false;
        }

        // Rule 3: The creating transaction must have committed *before* this snapshot was taken.
        // It cannot be an in-progress transaction from our point of view.
        if version.creator_txid >= self.xmax || self.xip.contains(&version.creator_txid) {
            return false;
        }

        // Rule 4: The expiring transaction (if any) must not be visible to us.
        // If a version is expired, the expiration is only visible if the expiring transaction
        // was already committed before our snapshot.
        if version.expirer_txid != 0 {
            if version.expirer_txid == self.txid {
                return false; // Expired by our own transaction.
            }

            let expirer_status = status_manager.get_status(version.expirer_txid);
            if expirer_status == Some(TransactionStatus::Committed) {
                if version.expirer_txid < self.xmax && !self.xip.contains(&version.expirer_txid) {
                    return false; // The deletion is visible to us, so the version is not.
                }
            }
        }

        true
    }
}

// END NEW
// END NEW

pub enum DbValue {
    Json(Value),
    JsonB(Vec<u8>),
    List(RwLock<VecDeque<Vec<u8>>>),
    Set(RwLock<HashSet<Vec<u8>>>),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
}

impl Clone for DbValue {
    fn clone(&self) -> Self {
        match self {
            DbValue::Json(v) => DbValue::Json(v.clone()),
            DbValue::JsonB(b) => DbValue::JsonB(b.clone()),
            DbValue::Bytes(b) => DbValue::Bytes(b.clone()),
            DbValue::Array(a) => DbValue::Array(a.clone()),
            DbValue::List(lock) => {
                let list = lock.try_read().expect("Cloning a List failed due to a write lock being held.").clone();
                DbValue::List(RwLock::new(list))
            }
            DbValue::Set(lock) => {
                let set = lock.try_read().expect("Cloning a Set failed due to a write lock being held.").clone();
                DbValue::Set(RwLock::new(set))
            }
        }
    }
}

impl fmt::Debug for DbValue {
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

#[derive(Serialize, Deserialize, Debug, Clone)]
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
    BeginTransaction { id: Uuid },
    CommitTransaction { id: Uuid },
    RollbackTransaction { id: Uuid },
    Savepoint { name: String },
    RollbackToSavepoint { name: String },
    ReleaseSavepoint { name: String },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SnapshotEntry {
    pub key: String,
    pub value: SerializableDbValue,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ViewDefinition {
    pub name: String,
    #[serde(default)]
    pub columns: Vec<String>,
    pub query: SelectStatement,
}

#[derive(Debug)]
pub enum PersistenceRequest {
    Log(LogRequest),
    Sync(oneshot::Sender<Result<(), String>>),
}

#[derive(Debug)]
pub struct LogRequest {
    pub entry: LogEntry,
    pub ack: oneshot::Sender<Result<(), String>>,
    pub durability: DurabilityLevel,
}

// --- Type Aliases ---

pub type Logger = mpsc::Sender<PersistenceRequest>;
pub type Db = Arc<DashMap<String, RwLock<Vec<VersionedValue>>>>;
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
    pub tx_id_manager: Arc<TransactionIdManager>,
    pub tx_status_manager: Arc<TransactionStatusManager>,
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
