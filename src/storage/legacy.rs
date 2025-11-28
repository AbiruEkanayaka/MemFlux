use async_trait::async_trait;
use crate::storage::{StorageEngine, StorageTransaction};
use crate::types::{
    Db, DbValue, TransactionIdManager, TransactionStatusManager, TxId, VersionedValue,
    TransactionStatus, LogEntry, LogRequest, PersistenceRequest, Logger,
};
use crate::transaction::Transaction;
use crate::config::DurabilityLevel;
use anyhow::{Result, anyhow};
use std::sync::Arc;
use tokio::sync::{RwLock, oneshot};
use std::collections::{HashSet, VecDeque, HashMap};
use dashmap::DashMap;
use crate::memory::MemoryManager;

// Legacy Backend that wraps the existing DashMap and manual MVCC logic
pub struct LegacyDashMapBackend {
    db: Db,
    tx_id_manager: Arc<TransactionIdManager>,
    tx_status_manager: Arc<TransactionStatusManager>,
    logger: Logger,
    durability: DurabilityLevel,
    active_transactions: Arc<DashMap<TxId, Arc<Transaction>>>,
    memory_manager: Arc<MemoryManager>,
}

impl LegacyDashMapBackend {
    pub fn new(
        db: Db, 
        tx_id_manager: Arc<TransactionIdManager>,
        tx_status_manager: Arc<TransactionStatusManager>,
        logger: Logger,
        durability: DurabilityLevel,
        active_transactions: Arc<DashMap<TxId, Arc<Transaction>>>,
        memory_manager: Arc<MemoryManager>,
    ) -> Self {
        Self {
            db,
            tx_id_manager,
            tx_status_manager,
            logger,
            durability,
            active_transactions,
            memory_manager,
        }
    }

    async fn log_to_wal(&self, log_entry: LogEntry) -> Result<()> {
        if self.durability == DurabilityLevel::None {
             // Fire and forget for None durability
        }
        
        let (ack_tx, ack_rx) = oneshot::channel();
        let log_req = LogRequest {
            entry: log_entry,
            ack: ack_tx,
            durability: self.durability.clone(),
        };
        
        self.logger.send(PersistenceRequest::Log(log_req)).await
            .map_err(|_| anyhow!("Persistence engine is down"))?;
            
        ack_rx.await
            .map_err(|_| anyhow!("Persistence engine dropped ACK channel"))?
            .map_err(|e| anyhow!("WAL write error: {}", e))
    }

    // Internal helper to read a visible value from the DB (snapshot read)
    async fn get_from_db(&self, key: &str, _snapshot_txid: TxId, snapshot: &crate::types::Snapshot) -> Option<DbValue> {
        if let Some(version_chain_lock) = self.db.get(key) {
            let version_chain = version_chain_lock.read().await;
            for version in version_chain.iter().rev() {
                if snapshot.is_visible(version, &self.tx_status_manager) {
                    return Some(version.value.clone());
                }
            }
        }
        None
    }
}

#[async_trait]
impl StorageEngine for LegacyDashMapBackend {
    async fn get(&self, key: &str) -> Option<DbValue> {
        // Auto-commit read
        let txid = 0; 
        let snapshot = crate::types::Snapshot::new(txid, &self.tx_status_manager, &self.tx_id_manager);
        self.get_from_db(key, txid, &snapshot).await
    }

    async fn set(&self, key: String, value: DbValue) -> Result<()> {
        let mut old_size = 0;
        // Check old size
        let snapshot = crate::types::Snapshot::new(0, &self.tx_status_manager, &self.tx_id_manager);
        if let Some(version_chain_lock) = self.db.get(&key) {
            let version_chain = version_chain_lock.read().await;
            if let Some(latest) = version_chain.iter().rev().find(|v| snapshot.is_visible(v, &*self.tx_status_manager)) {
                old_size = key.len() as u64 + crate::memory::estimate_db_value_size(&latest.value).await;
            }
        }

        // Calculate new size
        let new_size = key.len() as u64 + crate::memory::estimate_db_value_size(&value).await;
        let _needed = new_size.saturating_sub(old_size);

        if self.memory_manager.is_enabled() {
            self.memory_manager.increase_memory(new_size);
            self.memory_manager.decrease_memory(old_size);
        }

        // Auto-commit write
        let txid = self.tx_id_manager.new_txid();
        self.tx_status_manager.begin(txid);
        
        // 1. Log
        let entry = match &value {
            DbValue::Bytes(b) => LogEntry::SetBytes { key: key.clone(), value: b.clone() },
            DbValue::JsonB(b) => LogEntry::SetJsonB { key: key.clone(), value: b.clone() },
            DbValue::List(lock) => {
                let list = lock.read().await;
                LogEntry::SetList { key: key.clone(), value: list.clone() }
            },
            DbValue::Set(lock) => {
                let set = lock.read().await;
                LogEntry::SetSet { key: key.clone(), value: set.clone() }
            },
            _ => return Err(anyhow!("Unsupported DbValue type for raw set in LegacyBackend")),
        };

        self.log_to_wal(entry).await?;

        // 2. Apply
        let version_chain_arc = self.db.entry(key.clone()).or_default().clone();
        let mut version_chain = version_chain_arc.write().await;
        
        // Expire old
        if let Some(latest) = version_chain.iter_mut().rev().find(|v| snapshot.is_visible(v, &*self.tx_status_manager)) {
            latest.expirer_txid = txid;
        }
        
        // Push new
        version_chain.push(VersionedValue {
            value,
            creator_txid: txid,
            expirer_txid: 0,
        });
        
        self.tx_status_manager.commit(txid);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        let mut old_size = 0;
        let mut found = false;
        let snapshot = crate::types::Snapshot::new(0, &self.tx_status_manager, &self.tx_id_manager);
        
        if let Some(version_chain_lock) = self.db.get(key) {
            let version_chain = version_chain_lock.read().await;
            if let Some(latest) = version_chain.iter().rev().find(|v| snapshot.is_visible(v, &*self.tx_status_manager)) {
                old_size = key.len() as u64 + crate::memory::estimate_db_value_size(&latest.value).await;
                found = true;
            }
        }

        if !found {
            return Ok(false);
        }

        if self.memory_manager.is_enabled() {
            self.memory_manager.decrease_memory(old_size);
        }

        let txid = self.tx_id_manager.new_txid();
        self.tx_status_manager.begin(txid);
        
        self.log_to_wal(LogEntry::Delete { key: key.to_string() }).await?;
        
        if let Some(version_chain_lock) = self.db.get(key) {
            let version_chain_arc = version_chain_lock.clone();
            let mut version_chain = version_chain_arc.write().await;
            if let Some(latest) = version_chain.iter_mut().rev().find(|v| snapshot.is_visible(v, &*self.tx_status_manager)) {
                latest.expirer_txid = txid;
            }
        }
        
        self.tx_status_manager.commit(txid);
        Ok(true)
    }

    async fn prefix_scan(&self, prefix: &str) -> Vec<(String, DbValue)> {
        let mut results = Vec::new();
        let snapshot = crate::types::Snapshot::new(0, &self.tx_status_manager, &self.tx_id_manager);
        
        for entry in self.db.iter() {
            if entry.key().starts_with(prefix) {
                let chain = entry.value().read().await;
                if let Some(version) = chain.iter().rev().find(|v| snapshot.is_visible(v, &*self.tx_status_manager)) {
                    results.push((entry.key().clone(), version.value.clone()));
                }
            }
        }
        results
    }

    async fn range_scan(&self, start: &str, end: &str) -> Vec<(String, DbValue)> {
        let mut results = Vec::new();
        let snapshot = crate::types::Snapshot::new(0, &self.tx_status_manager, &self.tx_id_manager);
        
        for entry in self.db.iter() {
            let k = entry.key();
            if k.as_str() >= start && k.as_str() <= end {
                let chain = entry.value().read().await;
                if let Some(version) = chain.iter().rev().find(|v| snapshot.is_visible(v, &*self.tx_status_manager)) {
                    results.push((k.clone(), version.value.clone()));
                }
            }
        }
        results.sort_by(|a, b| a.0.cmp(&b.0));
        results
    }

    async fn begin_transaction(&self) -> Box<dyn StorageTransaction> {
        let txid = self.tx_id_manager.new_txid();
        self.tx_status_manager.begin(txid);
        let snapshot = crate::types::Snapshot::new(txid, &self.tx_status_manager, &self.tx_id_manager);
        
        let tx = Arc::new(Transaction {
            id: uuid::Uuid::new_v4(),
            txid,
            state: tokio::sync::RwLock::new(TransactionStatus::Active),
            snapshot,
            log_entries: tokio::sync::RwLock::new(Vec::new()),
            writes: DashMap::new(),
            read_cache: DashMap::new(),
            reads: DashMap::new(),
            ssi_in_conflict: std::sync::atomic::AtomicBool::new(false),
            savepoints: tokio::sync::RwLock::new(std::collections::HashMap::new()),
            reserved_memory: std::sync::atomic::AtomicI64::new(0),
        });
        
        self.active_transactions.insert(txid, tx.clone());
        
        Box::new(LegacyTransaction {
            tx,
            backend: self.db.clone(),
            tx_status_manager: self.tx_status_manager.clone(),
            active_transactions: self.active_transactions.clone(),
            logger: self.logger.clone(),
            durability: self.durability.clone(),
            memory_manager: self.memory_manager.clone(),
        })
    }

    async fn flush(&self) -> Result<()> {
        Ok(()) // Legacy engine has auto-background flush
    }

    async fn vacuum(&self) -> Result<(usize, usize)> {
        crate::vacuum::vacuum_inner(
            &self.db,
            &self.tx_status_manager,
            &self.tx_id_manager
        ).await
    }

    async fn clear(&self) -> Result<()> {
        self.db.clear();
        // Also reset managers? Legacy handle_wipedb did. handle_flushdb only cleared db.
        // Let's match FLUSHDB behavior: clear data.
        Ok(())
    }
}

pub struct LegacyTransaction {
    tx: Arc<Transaction>,
    backend: Db,
    tx_status_manager: Arc<TransactionStatusManager>,
    active_transactions: Arc<DashMap<TxId, Arc<Transaction>>>,
    logger: Logger,
    durability: DurabilityLevel,
    memory_manager: Arc<MemoryManager>,
}

#[async_trait]
impl StorageTransaction for LegacyTransaction {
    fn id(&self) -> TxId {
        self.tx.txid
    }

    async fn commit(&mut self) -> Result<()> {
        // SSI Checks
        if self.tx.ssi_in_conflict.load(std::sync::atomic::Ordering::Relaxed) {
            self.abort().await;
            return Err(anyhow!("ABORT: Serialization failure"));
        }
        
        // Outgoing conflicts
        for item in self.tx.writes.iter() {
            let key = item.key();
            for entry in self.active_transactions.iter() {
                let other = entry.value();
                if self.tx.snapshot.xip.contains(&other.txid) && other.reads.contains_key(key) {
                    other.ssi_in_conflict.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

        // Persistence
        let log_entries = self.tx.log_entries.read().await;
        // Simplified batch log
        for (i, entry) in log_entries.iter().enumerate() {
            let is_last = i == log_entries.len() - 1;
            let dur = if is_last { self.durability.clone() } else { DurabilityLevel::None };
            let (tx, rx) = oneshot::channel();
            self.logger.send(PersistenceRequest::Log(LogRequest {
                entry: entry.clone(),
                ack: tx,
                durability: dur,
            })).await.map_err(|_| anyhow!("Persistence down"))?;
            
            if is_last {
                rx.await.map_err(|_| anyhow!("Persistence down"))?.map_err(|e| anyhow!(e))?;
            }
        }

        // Apply to DB
        for item in self.tx.writes.iter() {
            let key = item.key();
            let val_opt = item.value();
            
            let chain_arc = self.backend.entry(key.clone()).or_default().clone();
            let mut chain = chain_arc.write().await;
            
            // expire old
            if let Some(v) = chain.iter_mut().rev().find(|v| self.tx.snapshot.is_visible(v, &*self.tx_status_manager)) {
                if v.expirer_txid == 0 {
                    v.expirer_txid = self.tx.txid;
                }
            }
            
            if let Some(new_val) = val_opt {
                chain.push(VersionedValue {
                    value: new_val.clone(),
                    creator_txid: self.tx.txid,
                    expirer_txid: 0,
                });
            }
        }

        self.tx_status_manager.commit(self.tx.txid);
        self.active_transactions.remove(&self.tx.txid);
        Ok(())
    }

    async fn rollback(&mut self) -> Result<()> {
        self.abort().await;
        
        // Revert memory usage for uncommitted writes
        if self.memory_manager.is_enabled() {
            let reserved = self.tx.reserved_memory.load(std::sync::atomic::Ordering::Relaxed);
            if reserved > 0 {
                self.memory_manager.decrease_memory(reserved as u64);
            } else {
                self.memory_manager.increase_memory((-reserved) as u64);
            }
        }
        Ok(())
    }

    async fn set(&mut self, key: String, value: DbValue) -> Result<()> {
        // Memory tracking for transaction
        if self.memory_manager.is_enabled() {
            // Estimate old size (visible to THIS transaction)
            let mut old_size = 0;
            // Check writes first
            if let Some(entry) = self.tx.writes.get(&key) {
                if let Some(v) = entry.value() {
                    old_size = key.len() as u64 + crate::memory::estimate_db_value_size(v).await;
                }
            } else {
                // Check DB
                if let Some(chain_lock) = self.backend.get(&key) {
                    let chain = chain_lock.read().await;
                    if let Some(v) = chain.iter().rev().find(|v| self.tx.snapshot.is_visible(v, &*self.tx_status_manager)) {
                        old_size = key.len() as u64 + crate::memory::estimate_db_value_size(&v.value).await;
                    }
                }
            }
            
            let new_size = key.len() as u64 + crate::memory::estimate_db_value_size(&value).await;
            let diff = new_size as i64 - old_size as i64;
            
            if diff > 0 {
                self.memory_manager.increase_memory(diff as u64);
            } else {
                self.memory_manager.decrease_memory((-diff) as u64);
            }
            
            self.tx.reserved_memory.fetch_add(diff, std::sync::atomic::Ordering::Relaxed);
        }

        let entry = match &value {
            DbValue::Bytes(b) => LogEntry::SetBytes { key: key.clone(), value: b.clone() },
            DbValue::JsonB(b) => LogEntry::SetJsonB { key: key.clone(), value: b.clone() },
            DbValue::List(lock) => {
                let list = lock.read().await;
                LogEntry::SetList { key: key.clone(), value: list.clone() }
            },
            DbValue::Set(lock) => {
                let set = lock.read().await;
                LogEntry::SetSet { key: key.clone(), value: set.clone() }
            },
            _ => LogEntry::SetBytes { key: key.clone(), value: vec![] }, // Fallback
        };
        self.tx.log_entries.write().await.push(entry);
        self.tx.writes.insert(key, Some(value));
        Ok(())
    }

    async fn delete(&mut self, key: String) -> Result<bool> {
        // Memory tracking
        let mut old_size = 0;
        let mut found = false;
        
        if self.memory_manager.is_enabled() {
            // Check writes first
            if let Some(entry) = self.tx.writes.get(&key) {
                if let Some(v) = entry.value() {
                    old_size = key.len() as u64 + crate::memory::estimate_db_value_size(v).await;
                    found = true;
                } else {
                    // Already deleted in this tx
                    return Ok(false);
                }
            } else {
                // Check DB
                if let Some(chain_lock) = self.backend.get(&key) {
                    let chain = chain_lock.read().await;
                    if let Some(v) = chain.iter().rev().find(|v| self.tx.snapshot.is_visible(v, &*self.tx_status_manager)) {
                        old_size = key.len() as u64 + crate::memory::estimate_db_value_size(&v.value).await;
                        found = true;
                    }
                }
            }
            
            if found && old_size > 0 {
                self.memory_manager.decrease_memory(old_size);
                self.tx.reserved_memory.fetch_sub(old_size as i64, std::sync::atomic::Ordering::Relaxed);
            }
        } else {
             // Check existence if memory manager disabled to return correct bool
             // Check writes first
            if let Some(entry) = self.tx.writes.get(&key) {
                if entry.value().is_some() {
                    found = true;
                } else {
                    return Ok(false);
                }
            } else {
                // Check DB
                if let Some(chain_lock) = self.backend.get(&key) {
                    let chain = chain_lock.read().await;
                    if chain.iter().rev().any(|v| self.tx.snapshot.is_visible(v, &*self.tx_status_manager)) {
                        found = true;
                    }
                }
            }
        }

        if !found {
            return Ok(false);
        }

        self.tx.log_entries.write().await.push(LogEntry::Delete { key: key.clone() });
        self.tx.writes.insert(key, None);
        Ok(true)
    }

    async fn get(&self, key: &str) -> Option<DbValue> {
        // 1. Check writes
        if let Some(entry) = self.tx.writes.get(key) {
            return entry.value().clone();
        }
        // 2. Check DB
        if let Some(chain_lock) = self.backend.get(key) {
            let chain = chain_lock.read().await;
            for v in chain.iter().rev() {
                if self.tx.snapshot.is_visible(v, &*self.tx_status_manager) {
                    // Track read for SSI
                    self.tx.reads.insert(key.to_string(), v.creator_txid);
                    return Some(v.value.clone());
                }
            }
        }
        None
    }

    async fn prefix_scan(&self, prefix: &str) -> Vec<(String, DbValue)> {
        let mut results = HashMap::new();
        
        // 1. Scan DB
        for entry in self.backend.iter() {
            if entry.key().starts_with(prefix) {
                let chain = entry.value().read().await;
                if let Some(v) = chain.iter().rev().find(|v| self.tx.snapshot.is_visible(v, &*self.tx_status_manager)) {
                    self.tx.reads.insert(entry.key().clone(), v.creator_txid);
                    results.insert(entry.key().clone(), v.value.clone());
                }
            }
        }
        
        // 2. Overlay writes
        for item in self.tx.writes.iter() {
            if item.key().starts_with(prefix) {
                if let Some(val) = item.value() {
                    results.insert(item.key().clone(), val.clone());
                } else {
                    results.remove(item.key());
                }
            }
        }
        
        let mut vec: Vec<_> = results.into_iter().collect();
        vec.sort_by(|a, b| a.0.cmp(&b.0));
        vec
    }

    async fn range_scan(&self, start: &str, end: &str) -> Vec<(String, DbValue)> {
        let mut results = HashMap::new();
        
        for entry in self.backend.iter() {
            let k = entry.key();
            if k.as_str() >= start && k.as_str() <= end {
                let chain = entry.value().read().await;
                if let Some(v) = chain.iter().rev().find(|v| self.tx.snapshot.is_visible(v, &*self.tx_status_manager)) {
                    self.tx.reads.insert(k.clone(), v.creator_txid);
                    results.insert(k.clone(), v.value.clone());
                }
            }
        }
        
        for item in self.tx.writes.iter() {
            let k = item.key();
            if k.as_str() >= start && k.as_str() <= end {
                if let Some(val) = item.value() {
                    results.insert(k.clone(), val.clone());
                } else {
                    results.remove(k);
                }
            }
        }
        
        let mut vec: Vec<_> = results.into_iter().collect();
        vec.sort_by(|a, b| a.0.cmp(&b.0));
        vec
    }

    async fn savepoint(&mut self, name: &str) -> Result<()> {
        let mut savepoints = self.tx.savepoints.write().await;
        let log_entries = self.tx.log_entries.read().await.clone();
        let writes = self.tx.writes.clone();
        let reserved_memory = self.tx.reserved_memory.load(std::sync::atomic::Ordering::Relaxed);
        savepoints.insert(name.to_string(), (log_entries, writes, reserved_memory));
        Ok(())
    }

    async fn rollback_to(&mut self, name: &str) -> Result<()> {
        let mut savepoints = self.tx.savepoints.write().await;
        if let Some((log_entries, writes, reserved_memory)) = savepoints.remove(name) {
            let mut current_log = self.tx.log_entries.write().await;
            *current_log = log_entries;
            
            self.tx.writes.clear();
            for item in writes.iter() {
                self.tx.writes.insert(item.key().clone(), item.value().clone());
            }

            let old_reserved = self.tx.reserved_memory.swap(reserved_memory, std::sync::atomic::Ordering::Relaxed);
            let diff = old_reserved - reserved_memory;
            if diff > 0 {
                self.memory_manager.decrease_memory(diff as u64);
            } else {
                self.memory_manager.increase_memory((-diff) as u64);
            }

            // Invalidate any savepoints created after this one.
            savepoints.retain(|_, (sp_log, _, _)| sp_log.len() < current_log.len());

            Ok(())
        } else {
            Err(anyhow!("Savepoint '{}' not found", name))
        }
    }

    async fn release_savepoint(&mut self, name: &str) -> Result<()> {
        let mut savepoints = self.tx.savepoints.write().await;
        if savepoints.remove(name).is_some() {
            Ok(())
        } else {
            Err(anyhow!("Savepoint '{}' not found", name))
        }
    }
}

impl LegacyTransaction {
    async fn abort(&self) {
        self.tx_status_manager.abort(self.tx.txid);
        self.active_transactions.remove(&self.tx.txid);
    }
}