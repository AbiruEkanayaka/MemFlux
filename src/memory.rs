use anyhow::{anyhow, Result};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{oneshot, RwLock};

use crate::config::EvictionPolicy;
use crate::types::{AppContext, DbValue, LogEntry, LogRequest};

// A rough estimation of the memory used by a DbValue.
// It's not perfect but gives us a baseline for memory management.
pub async fn estimate_db_value_size(value: &DbValue) -> u64 {
    match value {
        DbValue::Json(v) => v.to_string().len() as u64,
        DbValue::Bytes(b) => b.len() as u64,
        DbValue::List(lock) => {
            let list = lock.read().await;
            list.iter()
                .fold(0u64, |acc, v| acc.saturating_add(v.len() as u64))
        }
        DbValue::Set(lock) => {
            let set = lock.read().await;
            set.iter()
                .fold(0u64, |acc, v| acc.saturating_add(v.len() as u64))
        }
    }
}

pub struct MemoryManager {
    max_memory_bytes: u64,
    estimated_memory: AtomicU64,
    policy: EvictionPolicy,
    // For LRU: A VecDeque where the front is the most recently used.
    lru_keys: RwLock<VecDeque<String>>,
    // For LFU: A HashMap tracking access frequency.
    lfu_freqs: RwLock<HashMap<String, u64>>,
}

impl MemoryManager {
    pub fn new(maxmemory_mb: u64, policy: EvictionPolicy) -> Self {
        Self {
            max_memory_bytes: maxmemory_mb * 1024 * 1024,
            estimated_memory: AtomicU64::new(0),
            policy,
            lru_keys: RwLock::new(VecDeque::new()),
            lfu_freqs: RwLock::new(HashMap::new()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.max_memory_bytes > 0
    }

    pub fn current_memory(&self) -> u64 {
        self.estimated_memory.load(Ordering::Relaxed)
    }

    pub fn increase_memory(&self, amount: u64) {
        // Use a loop to ensure saturating behavior
        loop {
            let current = self.estimated_memory.load(Ordering::Relaxed);
            let new = current.saturating_add(amount);
            if self
                .estimated_memory
                .compare_exchange_weak(current, new, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    pub fn decrease_memory(&self, amount: u64) {
        // Use a loop to ensure saturating behavior
        loop {
            let current = self.estimated_memory.load(Ordering::Relaxed);
            let new = current.saturating_sub(amount);
            if self
                .estimated_memory
                .compare_exchange_weak(current, new, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    pub async fn prime(&self, keys: Vec<String>) {
        if !self.is_enabled() {
            return;
        }
        match self.policy {
            EvictionPolicy::LRU => {
                let mut lru = self.lru_keys.write().await;
                for key in keys {
                    lru.push_back(key); // Oldest keys from snapshot are at the back
                }
            }
            EvictionPolicy::LFU => {
                let mut lfu = self.lfu_freqs.write().await;
                for key in keys {
                    lfu.insert(key, 1); // Start with a frequency of 1
                }
            }
        }
    }

    // Moves a key to the front of the LRU list or increments its LFU count.
    pub async fn track_access(&self, key: &str) {
        if !self.is_enabled() {
            return;
        }
        match self.policy {
            EvictionPolicy::LRU => {
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
            EvictionPolicy::LFU => {
                let mut lfu = self.lfu_freqs.write().await;
                *lfu.entry(key.to_string()).or_insert(0) += 1;
            }
        }
    }

    // Removes a key from eviction tracking.
    pub async fn forget_key(&self, key: &str) {
        if !self.is_enabled() {
            return;
        }
        match self.policy {
            EvictionPolicy::LRU => {
                let mut lru = self.lru_keys.write().await;
                if let Some(pos) = lru.iter().position(|k| k == key) {
                    lru.remove(pos);
                }
            }
            EvictionPolicy::LFU => {
                let mut lfu = self.lfu_freqs.write().await;
                lfu.remove(key);
            }
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
        if needed_size > self.max_memory_bytes {
            return Err(anyhow!(
                "OOM: The item is larger than the maxmemory limit"
            ));
        }

        while self.current_memory() + needed_size > self.max_memory_bytes {
            let key_to_evict_info: Option<(String, Option<u64>)> = match self.policy {
                EvictionPolicy::LRU => self.lru_keys.write().await.pop_back().map(|k| (k, None)),
                EvictionPolicy::LFU => {
                    let mut freqs = self.lfu_freqs.write().await;
                    // This is inefficient (O(n)), but simple. A real LFU uses more complex data structures.
                    let key_to_evict = freqs.iter().min_by_key(|&(_, v)| v).map(|(k, _)| k.clone());

                    if let Some(key) = key_to_evict {
                        let freq = freqs.remove(&key);
                        Some((key, freq))
                    } else {
                        None
                    }
                }
            };

            if let Some((key, old_freq)) = key_to_evict_info {
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
                    match self.policy {
                        EvictionPolicy::LRU => self.lru_keys.write().await.push_back(key),
                        EvictionPolicy::LFU => {
                            self.lfu_freqs
                                .write()
                                .await
                                .insert(key, old_freq.unwrap_or(1));
                        }
                    }
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
                        match self.policy {
                            EvictionPolicy::LRU => self.lru_keys.write().await.push_back(key),
                            EvictionPolicy::LFU => {
                                self.lfu_freqs
                                    .write()
                                    .await
                                    .insert(key, old_freq.unwrap_or(1));
                            }
                        }
                        return Err(anyhow!("WAL write error during eviction: {}", e));
                    }
                    Err(_) => {
                        match self.policy {
                            EvictionPolicy::LRU => self.lru_keys.write().await.push_back(key),
                            EvictionPolicy::LFU => {
                                self.lfu_freqs
                                    .write()
                                    .await
                                    .insert(key, old_freq.unwrap_or(1));
                            }
                        }
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