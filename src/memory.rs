use anyhow::{anyhow, Result};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{oneshot, RwLock};

use crate::types::{AppContext, DbValue, LogEntry, LogRequest};

// A rough estimation of the memory used by a DbValue.
// It's not perfect but gives us a baseline for memory management.
pub async fn estimate_db_value_size(value: &DbValue) -> u64 {
    match value {
        DbValue::Json(v) => v.to_string().len() as u64,
        DbValue::Bytes(b) => b.len() as u64,
        DbValue::List(lock) => {
            let list = lock.read().await;
            list.iter().map(|v| v.len() as u64).sum()
        }
        DbValue::Set(lock) => {
            let set = lock.read().await;
            set.iter().map(|v| v.len() as u64).sum()
        }
    }
}

pub struct MemoryManager {
    max_memory_bytes: u64,
    estimated_memory: AtomicU64,
    // This is a simple LRU implementation using VecDeque.
    // For very high-throughput systems, a more optimized structure
    // (like a HashMap + Doubly-Linked List) would be better to avoid
    // the O(n) cost of moving an element to the front on access.
    lru_keys: RwLock<VecDeque<String>>,
}

impl MemoryManager {
    pub fn new(maxmemory_mb: u64) -> Self {
        Self {
            max_memory_bytes: maxmemory_mb * 1024 * 1024,
            estimated_memory: AtomicU64::new(0),
            lru_keys: RwLock::new(VecDeque::new()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.max_memory_bytes > 0
    }

    pub fn current_memory(&self) -> u64 {
        self.estimated_memory.load(Ordering::Relaxed)
    }

    pub fn increase_memory(&self, amount: u64) {
        self.estimated_memory.fetch_add(amount, Ordering::Relaxed);
    }

    pub fn decrease_memory(&self, amount: u64) {
        self.estimated_memory.fetch_sub(amount, Ordering::Relaxed);
    }

    pub async fn prime_lru(&self, keys: Vec<String>) {
        let mut lru = self.lru_keys.write().await;
        for key in keys {
            lru.push_back(key); // Oldest keys from snapshot are at the back
        }
    }

    // Moves a key to the front of the LRU list (most recently used).
    pub async fn track_access(&self, key: &str) {
        if !self.is_enabled() {
            return;
        }
        let mut lru = self.lru_keys.write().await;
        if let Some(pos) = lru.iter().position(|k| k == key) {
            if let Some(k) = lru.remove(pos) {
                lru.push_front(k);
            }
        } else {
            // Key was not tracked, add it now.
            lru.push_front(key.to_string());
        }
    }

    // Removes a key from LRU tracking.
    pub async fn forget_key(&self, key: &str) {
        if !self.is_enabled() {
            return;
        }
        let mut lru = self.lru_keys.write().await;
        if let Some(pos) = lru.iter().position(|k| k == key) {
            lru.remove(pos);
        }
    }

    pub fn max_memory(&self) -> u64 {
        self.max_memory_bytes
    }

    // Evicts keys until there is enough space for `needed_size`.
    pub async fn ensure_memory_for(&self, needed_size: u64, ctx: &AppContext) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        // Check if the single new item is larger than the total memory
        if needed_size > self.max_memory_bytes && needed_size > self.current_memory() {
            return Err(anyhow!(
                "OOM: The item is larger than the maxmemory limit"
            ));
        }

        while self.current_memory() + needed_size > self.max_memory_bytes {
            let key_to_evict = self.lru_keys.write().await.pop_back();

            if let Some(key) = key_to_evict {
                let old_value_for_index =
                    ctx.db.get(&key).and_then(|entry| match entry.value() {
                        DbValue::Json(v) => Some(v.clone()),
                        _ => None,
                    });

                // Log the deletion for persistence
                let log_entry = LogEntry::Delete { key: key.clone() };
                let (ack_tx, ack_rx) = oneshot::channel();
                if ctx.logger
                    .send(LogRequest {
                        entry: log_entry,
                        ack: ack_tx,
                    })
                    .await
                    .is_err()
                {
                    // If persistence is down, we probably shouldn't evict.
                    // Put the key back and return an error.
                    self.lru_keys.write().await.push_back(key);
                    return Err(anyhow!("Persistence engine is down, cannot evict"));
                }
                match ack_rx.await {
                    Ok(Ok(())) => {
                        // WAL write successful, now evict from memory
                        if let Some(entry) = ctx.db.remove(&key) {
                            let size = estimate_db_value_size(&entry.1).await;
                            self.decrease_memory(size + key.len() as u64);

                            if let Some(ref old_val) = old_value_for_index {
                                ctx.index_manager
                                    .remove_key_from_indexes(&key, old_val)
                                    .await;
                            }
                            println!("Evicted key to free memory: {}", key);
                        }
                    }
                    Ok(Err(e)) => {
                        self.lru_keys.write().await.push_back(key);
                        return Err(anyhow!("WAL write error during eviction: {}", e));
                    }
                    Err(_) => {
                        self.lru_keys.write().await.push_back(key);
                        return Err(anyhow!(
                            "Persistence engine dropped ACK channel during eviction"
                        ));
                    }
                }
            } else {
                // No more keys to evict
                return Err(anyhow!(
                    "OOM: No more keys to evict, but still not enough memory"
                ));
            }
        }

        Ok(())
    }
}
