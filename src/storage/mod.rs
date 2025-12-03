use async_trait::async_trait;
use crate::types::{DbValue, TxId};
use anyhow::Result;

pub mod flux;

// Abstract the concept of a Transaction
#[async_trait]
pub trait StorageTransaction: Send + Sync {
    fn id(&self) -> TxId;
    async fn commit(&mut self) -> Result<()>;
    async fn rollback(&mut self) -> Result<()>;
    
    // Staging writes
    async fn set(&mut self, key: String, value: DbValue) -> Result<()>;
    async fn delete(&mut self, key: String) -> Result<bool>;
    async fn get(&self, key: &str) -> Option<DbValue>;
    
    // Scans within transaction (RYOW)
    async fn prefix_scan(&self, prefix: &str) -> Vec<(String, DbValue)>;
    async fn range_scan(&self, start: &str, end: &str) -> Vec<(String, DbValue)>;

    // Savepoints
    async fn savepoint(&mut self, name: &str) -> Result<()>;
    async fn rollback_to(&mut self, name: &str) -> Result<()>;
    async fn release_savepoint(&mut self, name: &str) -> Result<()>;
}

// Abstract the Database Engine
#[async_trait]
pub trait StorageEngine: Send + Sync {
    // Basic K/V (Auto-commit / Snapshot read)
    async fn get(&self, key: &str) -> Option<DbValue>;
    async fn set(&self, key: String, value: DbValue) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<bool>;

    // Scans (Crucial for SQL/Graph)
    async fn prefix_scan(&self, prefix: &str) -> Vec<(String, DbValue)>;
    async fn range_scan(&self, start: &str, end: &str) -> Vec<(String, DbValue)>;

    // Transaction Management
    async fn begin_transaction(&self) -> Box<dyn StorageTransaction>;
    
    // Flush/Checkpoint (for WAL persistence)
    async fn flush(&self) -> Result<()>;

    // Maintenance (e.g., GC)
    async fn vacuum(&self) -> Result<(usize, usize)>;

    // Resets the database (FLUSHDB)
    async fn clear(&self) -> Result<()>;
}
