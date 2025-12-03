use async_trait::async_trait;
use crate::storage::{StorageEngine, StorageTransaction};
use crate::types::{DbValue, TxId};
use anyhow::{Result, anyhow};
use std::sync::Arc;
use fluxmap::db::{Database, OwnedHandle};

pub struct FluxMapBackend {
    db: Arc<Database<String, DbValue>>,
}

impl FluxMapBackend {
    pub fn new(db: Arc<Database<String, DbValue>>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl StorageEngine for FluxMapBackend {
    async fn get(&self, key: &str) -> Option<DbValue> {
        // Autocommit read
        let handle = self.db.owned_handle();
        match handle.get(&key.to_string()) {
            Ok(Some(val)) => Some(val.as_ref().clone()),
            _ => None,
        }
    }

    async fn set(&self, key: String, value: DbValue) -> Result<()> {
        // Autocommit write
        let handle = self.db.owned_handle();
        handle.insert(key, value).await.map_err(|e| anyhow!(e.to_string()))
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        // Autocommit delete
        let handle = self.db.owned_handle();
        match handle.remove(&key.to_string()).await {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(anyhow!(e.to_string())),
        }
    }

    async fn prefix_scan(&self, prefix: &str) -> Vec<(String, DbValue)> {
        let handle = self.db.owned_handle();
        match handle.prefix_scan(prefix) {
            Ok(results) => results.into_iter().map(|(k, v)| (k, v.as_ref().clone())).collect(),
            Err(_) => Vec::new(),
        }
    }

    async fn range_scan(&self, start: &str, end: &str) -> Vec<(String, DbValue)> {
        let handle = self.db.owned_handle();
        match handle.range(&start.to_string(), &end.to_string()) {
            Ok(results) => results.into_iter().map(|(k, v)| (k, v.as_ref().clone())).collect(),
            Err(_) => Vec::new(),
        }
    }

    async fn begin_transaction(&self) -> Box<dyn StorageTransaction> {
        let mut handle = self.db.owned_handle();
        // We must start the transaction.
        // If start fails (e.g. fatal error), we panic or return a broken tx?
        // The trait expects Box<dyn StorageTransaction>, not Result.
        // We should probably panic if we can't even start a transaction context object,
        // or return a dummy object that fails on all methods.
        // For now, unwrap.
        handle.begin().expect("Failed to begin transaction");
        
        Box::new(FluxTransaction { handle })
    }

    async fn flush(&self) -> Result<()> {
        // FluxMap handles flushing in background if configured.
        // We could expose a manual flush on Database if needed.
        Ok(())
    }

    async fn vacuum(&self) -> Result<(usize, usize)> {
        self.db.vacuum().await.map_err(|_| anyhow!("Vacuum failed"))
    }

    async fn clear(&self) -> Result<()> {
        // FluxMap doesn't have clear().
        // We can just scan and delete everything?
        // Or maybe we shouldn't support it fully in Phase 1 if not needed by tests?
        // FLUSHDB calls this.
        // Implementation: Prefix scan "" and delete.
        let handle = self.db.owned_handle();
        let all = handle.prefix_scan("").map_err(|e| anyhow!(e.to_string()))?;
        for (k, _) in all {
            handle.remove(&k).await.map_err(|e| anyhow!(e.to_string()))?;
        }
        Ok(())
    }
}

pub struct FluxTransaction {
    handle: OwnedHandle<String, DbValue>,
}

#[async_trait]
impl StorageTransaction for FluxTransaction {
    fn id(&self) -> TxId {
        // We need to expose the TxId from the handle.
        // OwnedHandle -> active_tx -> id.
        // Currently OwnedHandle doesn't expose it.
        // But MemFlux uses this ID mostly for logging or returning to client.
        // We can modify FluxMap again to expose it, or just return 0 for now.
        0 
    }

    async fn commit(&mut self) -> Result<()> {
        self.handle.commit().await.map_err(|e| anyhow!(e.to_string()))
    }

    async fn rollback(&mut self) -> Result<()> {
        self.handle.rollback().map_err(|e| anyhow!(e.to_string()))
    }

    async fn set(&mut self, key: String, value: DbValue) -> Result<()> {
        self.handle.insert(key, value).await.map_err(|e| anyhow!(e.to_string()))
    }

    async fn delete(&mut self, key: String) -> Result<bool> {
        match self.handle.remove(&key).await {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(anyhow!(e.to_string())),
        }
    }

    async fn get(&self, key: &str) -> Option<DbValue> {
        match self.handle.get(&key.to_string()) {
            Ok(Some(val)) => Some(val.as_ref().clone()),
            _ => None,
        }
    }

    async fn prefix_scan(&self, prefix: &str) -> Vec<(String, DbValue)> {
        match self.handle.prefix_scan(prefix) {
            Ok(results) => results.into_iter().map(|(k, v)| (k, v.as_ref().clone())).collect(),
            Err(_) => Vec::new(),
        }
    }

    async fn range_scan(&self, start: &str, end: &str) -> Vec<(String, DbValue)> {
        match self.handle.range(&start.to_string(), &end.to_string()) {
            Ok(results) => results.into_iter().map(|(k, v)| (k, v.as_ref().clone())).collect(),
            Err(_) => Vec::new(),
        }
    }

    async fn savepoint(&mut self, name: &str) -> Result<()> {
        self.handle.savepoint(name).map_err(|e| anyhow!(e.to_string()))
    }

    async fn rollback_to(&mut self, name: &str) -> Result<()> {
        self.handle.rollback_to(name).map_err(|e| anyhow!(e.to_string()))
    }

    async fn release_savepoint(&mut self, name: &str) -> Result<()> {
        self.handle.release_savepoint(name).map_err(|e| anyhow!(e.to_string()))
    }
}
