use crate::types::DbValue;
use dashmap::DashMap;
use uuid::Uuid;
use crate::types::LogEntry;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionState {
    Active,
}

/// Represents a single, isolated transaction.
pub struct Transaction {
    pub id: Uuid,
    pub state: TransactionState,
    /// A log of operations performed within this transaction, to be written to the WAL on commit.
    pub log_entries: Vec<LogEntry>,
    /// The transaction's private workspace. Stores pending writes and deletions.
    pub writes: DashMap<String, Option<DbValue>>,
    /// A cache of data read from the main database to ensure repeatable reads within the transaction.
    pub read_cache: DashMap<String, Option<DbValue>>,
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            state: TransactionState::Active,
            log_entries: Vec::new(),
            writes: DashMap::new(),
            read_cache: DashMap::new(),
        }
    }
}

/// A handle to a transaction, managed per-connection.
pub type TransactionHandle = Arc<RwLock<Option<Transaction>>>;
