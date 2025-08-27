use anyhow::{anyhow, Result};
use rand::Rng;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{oneshot, RwLock};

use crate::arc::ArcCache;
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
    // For LFU: A HashMap mapping frequency to a list of keys.
    lfu_freq_keys: RwLock<HashMap<u64, VecDeque<String>>>,
    // For LFU: The minimum frequency currently in the cache.
    lfu_min_freq: RwLock<u64>,
    // For LFRU: Probationary and protected segments.
    lfru_probationary: RwLock<VecDeque<String>>,
    lfru_protected: RwLock<VecDeque<String>>,
    lfru_protected_ratio: f64, // Ratio of total capacity for the protected segment
    // For ARC
    arc_cache: RwLock<ArcCache<String>>,
    // For Random
    random_keys: RwLock<Vec<String>>,
}

impl MemoryManager {
    pub fn new(maxmemory_mb: u64, policy: EvictionPolicy) -> Self {
        let max_memory_bytes = maxmemory_mb * 1024 * 1024;
        Self {
            max_memory_bytes,
            estimated_memory: AtomicU64::new(0),
            policy,
            lru_keys: RwLock::new(VecDeque::new()),
            lfu_freqs: RwLock::new(HashMap::new()),
            lfu_freq_keys: RwLock::new(HashMap::new()),
            lfu_min_freq: RwLock::new(0),
            lfru_probationary: RwLock::new(VecDeque::new()),
            lfru_protected: RwLock::new(VecDeque::new()),
            lfru_protected_ratio: 0.8, // Default 80% for protected
            arc_cache: RwLock::new(ArcCache::new(0)), // Start with 0 capacity
            random_keys: RwLock::new(Vec::new()),
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
                let mut freq_keys = self.lfu_freq_keys.write().await;
                let mut min_freq = self.lfu_min_freq.write().await;
                for key in keys {
                    lfu.insert(key.clone(), 1); // Start with a frequency of 1
                    freq_keys.entry(1).or_default().push_back(key);
                }
                if !lfu.is_empty() {
                    *min_freq = 1;
                }
            }
            EvictionPolicy::LFRU => {
                let mut probationary = self.lfru_probationary.write().await;
                for key in keys {
                    probationary.push_back(key);
                }
            }
            EvictionPolicy::ARC => {
                let mut arc = self.arc_cache.write().await;
                // Only re-initialize if capacity needs to change significantly
                if arc.capacity == 0 || arc.capacity < keys.len() / 2 {
                    *arc = ArcCache::new(keys.len());
                }
                arc.prime(keys);
            }
            EvictionPolicy::Random => {
                let mut random = self.random_keys.write().await;
                *random = keys;
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
                let mut freqs = self.lfu_freqs.write().await;
                let mut freq_keys = self.lfu_freq_keys.write().await;
                let mut min_freq = self.lfu_min_freq.write().await;

                let old_freq = freqs.get(key).copied().unwrap_or(0);
                let new_freq = old_freq + 1;

                freqs.insert(key.to_string(), new_freq);

                if old_freq > 0 {
                    if let Some(keys) = freq_keys.get_mut(&old_freq) {
                        if let Some(pos) = keys.iter().position(|k| k == key) {
                            keys.remove(pos);
                        }
                        if keys.is_empty() {
                            freq_keys.remove(&old_freq);
                            if old_freq == *min_freq {
                                *min_freq = new_freq;
                            }
                        }
                    }
                } else {
                    *min_freq = 1;
                }

                freq_keys
                    .entry(new_freq)
                    .or_default()
                    .push_back(key.to_string());
            }
            EvictionPolicy::LFRU => {
                let mut probationary = self.lfru_probationary.write().await;
                let mut protected = self.lfru_protected.write().await;

                if let Some(pos) = probationary.iter().position(|k| k == key) {
                    // Move from probationary to protected
                    if let Some(k) = probationary.remove(pos) {
                        // Check protected segment capacity
                        let total_capacity = probationary.len() + protected.len();
                        let max_protected = ((total_capacity as f64) * self.lfru_protected_ratio) as usize;
                        if protected.len() >= max_protected && !protected.is_empty() {
                            // Evict from protected to probationary
                            if let Some(evicted) = protected.pop_back() {
                                probationary.push_back(evicted);
                            }
                        }
                        protected.push_front(k);
                    }
                } else if let Some(pos) = protected.iter().position(|k| k == key) {
                    // Move to front of protected
                    if let Some(k) = protected.remove(pos) {
                        protected.push_front(k);
                    }
                } else {
                    // New key, add to probationary
                    probationary.push_front(key.to_string());
                }
            }
            EvictionPolicy::ARC => {
                self.arc_cache.write().await.track_access(&key.to_string());
            }
            EvictionPolicy::Random => {
                let mut keys = self.random_keys.write().await;
                if !keys.iter().any(|k| k == key) {
                    keys.push(key.to_string());
                }
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
                let mut freqs = self.lfu_freqs.write().await;
                if let Some(freq) = freqs.remove(key) {
                    let mut freq_keys = self.lfu_freq_keys.write().await;
                    if let Some(keys) = freq_keys.get_mut(&freq) {
                        if let Some(pos) = keys.iter().position(|k| k == key) {
                            keys.remove(pos);
                        }
                        if keys.is_empty() {
                            freq_keys.remove(&freq);
                        }
                    }
                }
            }
            EvictionPolicy::LFRU => {
                let mut probationary = self.lfru_probationary.write().await;
                if let Some(pos) = probationary.iter().position(|k| k == key) {
                    probationary.remove(pos);
                    return;
                }
                let mut protected = self.lfru_protected.write().await;
                if let Some(pos) = protected.iter().position(|k| k == key) {
                    protected.remove(pos);
                }
            }
            EvictionPolicy::ARC => {
                self.arc_cache.write().await.forget_key(&key.to_string());
            }
            EvictionPolicy::Random => {
                let mut keys = self.random_keys.write().await;
                if let Some(pos) = keys.iter().position(|k| k == key) {
                    keys.remove(pos);
                }
            }
        }
    }

    pub fn max_memory(&self) -> u64 {
        self.max_memory_bytes
    }

    /// Restores an evicted key back into the cache tracking structures.
    pub async fn restore_evicted_key(&self, key: String, old_freq: Option<u64>) {
        match self.policy {
            EvictionPolicy::LRU => {
                self.lru_keys.write().await.push_back(key);
            }
            EvictionPolicy::LFU => {
                let freq = old_freq.unwrap_or(1);
                let mut freqs = self.lfu_freqs.write().await;
                let mut freq_keys = self.lfu_freq_keys.write().await;
                let mut min_freq = self.lfu_min_freq.write().await;

                freqs.insert(key.clone(), freq);
                freq_keys.entry(freq).or_default().push_back(key);
                if freq < *min_freq || *min_freq == 0 {
                    *min_freq = freq;
                }
            }
            EvictionPolicy::LFRU => {
                // Restore to probationary
                self.lfru_probationary.write().await.push_back(key);
            }
            EvictionPolicy::ARC => {
                self.arc_cache.write().await.restore_evicted_key(key);
            }
            EvictionPolicy::Random => {
                self.random_keys.write().await.push(key);
            }
        }
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
                    if freqs.is_empty() {
                        None
                    } else {
                        let mut freq_keys = self.lfu_freq_keys.write().await;

                        // Acquire and prepare to scan starting from the current min frequency
                        let mut min_freq = self.lfu_min_freq.write().await;
                        let mut iterations = 0;
                        const MAX_ITERATIONS: u32 = 10000;
                        loop {
                            iterations += 1;
                            // Safety check: if min_freq exceeds a reasonable threshold, or
                            // we've looped too many times, give up
                            if *min_freq > 1_000_000 || iterations > MAX_ITERATIONS {
                                eprintln!("WARNING: LFU eviction safety limit reached - min_freq: {}, iterations: {}", *min_freq, iterations);
                                break None;
                            }
                            match freq_keys.get_mut(&min_freq) {
                                Some(keys) if !keys.is_empty() => {
                                    let key = keys.pop_front().unwrap();
                                    if keys.is_empty() {
                                        freq_keys.remove(&min_freq);
                                    }
                                    let freq = freqs.remove(&key);
                                    break Some((key, freq));
                                }
                                _ => {
                                    freq_keys.remove(&min_freq);
                                    *min_freq += 1;
                                }
                            }
                        }
                    }
                }
                EvictionPolicy::LFRU => {
                    let mut probationary = self.lfru_probationary.write().await;
                    if let Some(key) = probationary.pop_back() {
                        Some((key, None))
                    } else {
                        // Probationary is empty, move from protected to probationary
                        let mut protected = self.lfru_protected.write().await;
                        if let Some(key) = protected.pop_back() {
                            probationary.push_front(key);
                            // Now probationary has one item, so pop it
                            probationary.pop_back().map(|k| (k, None))
                        } else {
                            None // Both are empty
                        }
                    }
                }
                EvictionPolicy::ARC => self.arc_cache.write().await.evict().map(|k| (k, None)),
                EvictionPolicy::Random => {
                    let mut keys = self.random_keys.write().await;
                    if keys.is_empty() {
                        None
                    } else {
                        let index = rand::thread_rng().gen_range(0..keys.len());
                        Some((keys.remove(index), None))
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
                    self.restore_evicted_key(key, old_freq).await;
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
                        self.restore_evicted_key(key, old_freq).await;
                        return Err(anyhow!("WAL write error during eviction: {}", e));
                    }
                    Err(_) => {
                        self.restore_evicted_key(key, old_freq).await;
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