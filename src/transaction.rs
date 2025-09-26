use crate::types::{
    DbValue, LogEntry, Snapshot, TransactionIdManager, TransactionStatus, TransactionStatusManager, TxId,
};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Represents a single, isolated transaction.
#[derive(Debug)]
pub struct Transaction {
    pub id: Uuid, // This is for logging/debugging, txid is the real ID.
    pub txid: TxId,
    pub state: TransactionStatus,
    pub snapshot: Snapshot,
    /// A log of operations performed within this transaction, to be written to the WAL on commit.
    pub log_entries: Vec<LogEntry>,
    /// The transaction's private workspace. Stores pending writes and deletions.
    pub writes: DashMap<String, Option<DbValue>>,
    /// A cache of data read from the main database to ensure repeatable reads within the transaction.
    pub read_cache: DashMap<String, Option<DbValue>>,
}

impl Transaction {
    pub fn new(
        tx_id_manager: &TransactionIdManager,
        tx_status_manager: &TransactionStatusManager,
    ) -> Self {
        let txid = tx_id_manager.new_txid();
        tx_status_manager.begin(txid);
        let snapshot = Snapshot::new(txid, tx_status_manager, tx_id_manager);
        Self {
            id: Uuid::new_v4(),
            txid,
            state: TransactionStatus::Active,
            snapshot,
            log_entries: Vec::new(),
            writes: DashMap::new(),
            read_cache: DashMap::new(),
        }
    }
}

/// A handle to a transaction, managed per-connection.
pub type TransactionHandle = Arc<RwLock<Option<Transaction>>>;
