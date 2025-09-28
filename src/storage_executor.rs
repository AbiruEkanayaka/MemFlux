use crate::config::DurabilityLevel;
use crate::memory;
use crate::transaction::{Transaction, TransactionHandle};
use crate::types::{
    AppContext, DbValue, LogEntry, LogRequest, PersistenceRequest, Response, TransactionStatus,
};
use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};

pub async fn log_to_wal(log_entry: LogEntry, ctx: &AppContext) -> Response {
    if ctx.config.durability == DurabilityLevel::None && ctx.config.persistence {
        let logger = ctx.logger.clone();
        tokio::spawn(async move {
            let (ack_tx, _ack_rx) = oneshot::channel();
            let log_req = LogRequest {
                entry: log_entry,
                ack: ack_tx,
                durability: DurabilityLevel::None,
            };
            if logger.send(PersistenceRequest::Log(log_req)).await.is_err() {
                eprintln!("Error sending to persistence engine with 'none' durability.");
            }
        });
        return Response::Ok;
    }
    let (ack_tx, ack_rx) = oneshot::channel();
    let log_req = LogRequest {
        entry: log_entry,
        ack: ack_tx,
        durability: ctx.config.durability.clone(),
    };
    if ctx.logger.send(PersistenceRequest::Log(log_req)).await.is_err() {
        return Response::Error("Persistence engine is down".to_string());
    }
    match ack_rx.await {
        Ok(Ok(())) => Response::Ok,
        Ok(Err(e)) => Response::Error(format!("WAL write error: {}", e)),
        Err(_) => Response::Error("Persistence engine dropped ACK channel".to_string()),
    }
}

pub fn json_path_to_pointer(path: &str) -> String {
    if path == "." || path.is_empty() {
        return "".to_string();
    }
    let p = path.strip_prefix('.').unwrap_or(path);
    format!("/{}", p.replace('.', "/"))
}

pub async fn get_visible_db_value<'a>(
    key: &str,
    ctx: &'a AppContext,
    tx: Option<&Arc<Transaction>>,
) -> Option<DbValue> {
    if let Some(tx) = tx {
        if let Some(entry) = tx.writes.get(key) {
            return entry.value().clone();
        }
        if let Some(version_chain_lock) = ctx.db.get(key) {
            let version_chain = version_chain_lock.read().await;
            for version in version_chain.iter().rev() {
                if tx.snapshot
                    .is_visible(version, &ctx.tx_status_manager)
                {
                    if ctx.memory.is_enabled() {
                        ctx.memory.track_access(key).await;
                    }
                    // For SSI, track the version of the key that was read.
                    if ctx.config.isolation_level == crate::config::IsolationLevel::Serializable {
                        tx.reads.insert(key.to_string(), version.creator_txid);
                    }
                    return Some(version.value.clone());
                }
            }
        }
        return None;
    }
    if let Some(version_chain_lock) = ctx.db.get(key) {
        let version_chain = version_chain_lock.read().await;
        for version in version_chain.iter().rev() {
            if ctx.tx_status_manager.get_status(version.creator_txid) == Some(TransactionStatus::Committed) {
                if version.expirer_txid == 0 || ctx.tx_status_manager.get_status(version.expirer_txid) != Some(TransactionStatus::Committed) {
                    if ctx.memory.is_enabled() {
                        ctx.memory.track_access(key).await;
                    }
                    return Some(version.value.clone());
                } else {
                    return None;
                }
            }
        }
    }
    None
}

pub struct StorageExecutor {
    ctx: Arc<AppContext>,
    transaction_handle: TransactionHandle,
}

impl StorageExecutor {
    pub fn new(ctx: Arc<AppContext>, transaction_handle: TransactionHandle) -> Self {
        Self { ctx, transaction_handle }
    }

    pub async fn set(&self, key: String, value: Vec<u8>) -> Response {
        let log_entry = LogEntry::SetBytes { key: key.clone(), value: value.clone() };
        let mut tx_guard = self.transaction_handle.write().await;
        if let Some(tx) = tx_guard.as_mut() {
            tx.log_entries.write().await.push(log_entry);
            tx.writes.insert(key.clone(), Some(DbValue::Bytes(value.clone())));
            return Response::Ok;
        }
        drop(tx_guard);
        let txid = self.ctx.tx_id_manager.new_txid();
        self.ctx.tx_status_manager.begin(txid);
        let mut old_size = 0;
        if let Some(version_chain_lock) = self.ctx.db.get(&key) {
            let version_chain = version_chain_lock.read().await;
            if let Some(latest_version) = version_chain.iter().rev().find(|v| self.ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed) && v.expirer_txid == 0) {
                old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
            }
        }
        if self.ctx.memory.is_enabled() {
            let new_size = key.len() as u64 + value.len() as u64;
            let needed = new_size.saturating_sub(old_size);
            if let Err(e) = self.ctx.memory.ensure_memory_for(needed, &self.ctx).await {
                self.ctx.tx_status_manager.abort(txid);
                return Response::Error(e.to_string());
            }
        }
        let ack_response = log_to_wal(log_entry, &self.ctx).await;
        if !matches!(ack_response, Response::Ok) {
            self.ctx.tx_status_manager.abort(txid);
            return ack_response;
        }
        let mut version_chain_lock = self.ctx.db.entry(key.clone()).or_default();
        let mut version_chain = version_chain_lock.value_mut().write().await;
        if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| v.expirer_txid == 0 && self.ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)) {
            latest_version.expirer_txid = txid;
        }
        let new_version = crate::types::VersionedValue { value: DbValue::Bytes(value.clone()), creator_txid: txid, expirer_txid: 0 };
        version_chain.push(new_version);
        let new_size = key.len() as u64 + value.len() as u64;
        self.ctx.memory.decrease_memory(old_size);
        self.ctx.memory.increase_memory(new_size);
        if self.ctx.memory.is_enabled() {
            self.ctx.memory.track_access(&key).await;
        }
        self.ctx.tx_status_manager.commit(txid);
        Response::Ok
    }

    pub async fn delete(&self, keys: Vec<String>) -> Response {
        let mut tx_guard = self.transaction_handle.write().await;
        if let Some(tx) = tx_guard.as_mut() {
            let mut deleted_count = 0;
            for key in keys {
                tx.log_entries.write().await.push(LogEntry::Delete { key: key.clone() });
                tx.writes.insert(key, None);
                deleted_count += 1;
            }
            return Response::Integer(deleted_count);
        }
        drop(tx_guard);
        let mut deleted_count = 0;
        for key in keys {
            let txid = self.ctx.tx_id_manager.new_txid();
            self.ctx.tx_status_manager.begin(txid);
            let log_entry = LogEntry::Delete { key: key.clone() };
            let ack_response = log_to_wal(log_entry, &self.ctx).await;
            if !matches!(ack_response, Response::Ok) {
                self.ctx.tx_status_manager.abort(txid);
                continue;
            }
            let mut old_size = 0;
            let mut old_value_for_index: Option<Value> = None;
            if let Some(version_chain_lock) = self.ctx.db.get(&key) {
                let mut version_chain = version_chain_lock.value().write().await;
                let mut expired_something = false;
                if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| v.expirer_txid == 0 && self.ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)) {
                    latest_version.expirer_txid = txid;
                    expired_something = true;
                    old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
                    old_value_for_index = match &latest_version.value {
                        DbValue::Json(v) => Some(v.clone()),
                        DbValue::JsonB(b) => serde_json::from_slice(b).ok(),
                        _ => None,
                    };
                }
                if expired_something {
                    self.ctx.memory.decrease_memory(old_size);
                    if self.ctx.memory.is_enabled() {
                        self.ctx.memory.forget_key(&key).await;
                    }
                    if let Some(ref old_val) = old_value_for_index {
                        self.ctx.index_manager.remove_key_from_indexes(&key, old_val).await;
                    }
                    deleted_count += 1;
                }
            }
            self.ctx.tx_status_manager.commit(txid);
        }
        Response::Integer(deleted_count)
    }

    pub async fn json_set(&self, key: String, path: &str, value: Value) -> Response {
        let mut tx_guard = self.transaction_handle.write().await;
        if let Some(tx) = tx_guard.as_mut() {
            let log_entry = LogEntry::JsonSet { path: format!("{}.{}", key, path), value: value.to_string() };
            tx.log_entries.write().await.push(log_entry);
            let current_db_val = get_visible_db_value(&key, &self.ctx, Some(tx)).await;
            let mut current_val = match current_db_val {
                Some(DbValue::Json(v)) => v.clone(),
                Some(DbValue::JsonB(b)) => serde_json::from_slice(&b).unwrap_or(json!({})),
                Some(_) => return Response::Error("WRONGTYPE Operation against a non-JSON value".to_string()),
                _ => json!({}),
            };
            let pointer = if path == "." || path.is_empty() { "".to_string() } else { json_path_to_pointer(path) };
            if pointer.is_empty() {
                current_val = value;
            } else if let Some(target) = current_val.pointer_mut(&pointer) {
                *target = value;
            } else {
                let mut current = &mut current_val;
                for part in path.split('.') {
                    if part.is_empty() { continue; }
                    if current.is_object() {
                        current = current.as_object_mut().unwrap().entry(part).or_insert(json!({}));
                    } else {
                        return Response::Error("Path creation failed: part is not an object".to_string());
                    }
                }
                *current = value;
            }
            let new_value_bytes = match serde_json::to_vec(&current_val) {
                Ok(b) => b,
                Err(_) => return Response::Error("Failed to serialize new JSON value".to_string()),
            };
            tx.writes.insert(key, Some(DbValue::JsonB(new_value_bytes)));
            return Response::Ok;
        }
        drop(tx_guard);
        let txid = self.ctx.tx_id_manager.new_txid();
        self.ctx.tx_status_manager.begin(txid);
        let mut version_chain_lock = self.ctx.db.entry(key.clone()).or_default();
        let mut version_chain = version_chain_lock.value_mut().write().await;
        let mut old_size = 0;
        let mut old_val_for_index = json!({});
        let mut current_val = if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| v.expirer_txid == 0 && self.ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)) {
            latest_version.expirer_txid = txid;
            old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
            match &latest_version.value {
                DbValue::Json(v) => { old_val_for_index = v.clone(); v.clone() }
                DbValue::JsonB(b) => { let v: Value = serde_json::from_slice(b).unwrap_or_default(); old_val_for_index = v.clone(); v }
                _ => return Response::Error("WRONGTYPE Operation against a non-JSON value".to_string()),
            }
        } else {
            json!({})
        };
        let pointer = if path == "." || path.is_empty() { "".to_string() } else { json_path_to_pointer(path) };
        if pointer.is_empty() {
            current_val = value;
        } else if let Some(target) = current_val.pointer_mut(&pointer) {
            *target = value;
        } else {
            let mut current = &mut current_val;
            for part in path.split('.') {
                if part.is_empty() { continue; }
                if current.is_object() {
                    current = current.as_object_mut().unwrap().entry(part).or_insert(json!({}));
                } else {
                    return Response::Error("Path creation failed: part is not an object".to_string());
                }
            }
            *current = value;
        }
        let new_value_bytes = match serde_json::to_vec(&current_val) {
            Ok(b) => b,
            Err(_) => return Response::Error("Failed to serialize new JSON value".to_string()),
        };
        let log_entry = LogEntry::SetJsonB { key: key.clone(), value: new_value_bytes.clone() };
        if self.ctx.memory.is_enabled() {
            let new_size = key.len() as u64 + new_value_bytes.len() as u64;
            let needed = new_size.saturating_sub(old_size);
            if let Err(e) = self.ctx.memory.ensure_memory_for(needed, &self.ctx).await {
                self.ctx.tx_status_manager.abort(txid);
                return Response::Error(e.to_string());
            }
        }
        let ack_response = log_to_wal(log_entry, &self.ctx).await;
        if !matches!(ack_response, Response::Ok) {
            self.ctx.tx_status_manager.abort(txid);
            return ack_response;
        }
        let new_version = crate::types::VersionedValue { value: DbValue::JsonB(new_value_bytes.clone()), creator_txid: txid, expirer_txid: 0 };
        version_chain.push(new_version);
        let new_size = key.len() as u64 + new_value_bytes.len() as u64;
        self.ctx.memory.decrease_memory(old_size);
        self.ctx.memory.increase_memory(new_size);
        if self.ctx.memory.is_enabled() {
            self.ctx.memory.track_access(&key).await;
        }
        self.ctx.index_manager.remove_key_from_indexes(&key, &old_val_for_index).await;
        self.ctx.index_manager.add_key_to_indexes(&key, &current_val).await;
        self.ctx.tx_status_manager.commit(txid);
        Response::Ok
    }

    pub async fn json_del(&self, key: String, path: &str) -> Response {
        let mut tx_guard = self.transaction_handle.write().await;
        if let Some(tx) = tx_guard.as_mut() {
            let log_entry = if path.is_empty() || path == "." { LogEntry::Delete { key: key.clone() } } else { LogEntry::JsonDelete { path: format!("{}.{}", key, path) } };
            tx.log_entries.write().await.push(log_entry);
            if path.is_empty() || path == "." {
                tx.writes.insert(key.clone(), None);
            } else {
                let current_db_val = get_visible_db_value(&key, &self.ctx, Some(tx)).await;
                let mut current_val = match current_db_val {
                    Some(DbValue::Json(v)) => v.clone(),
                    Some(DbValue::JsonB(b)) => serde_json::from_slice(&b).unwrap_or(json!({})),
                    Some(_) => return Response::Error("WRONGTYPE Operation against a non-JSON value".to_string()),
                    _ => json!({}),
                };
                let mut pointer_parts: Vec<&str> = path.split('.').collect();
                let final_key = pointer_parts.pop().unwrap();
                let parent_pointer = json_path_to_pointer(&pointer_parts.join("."));
                let mut modified = false;
                if let Some(target) = current_val.pointer_mut(&parent_pointer) {
                    if let Some(obj) = target.as_object_mut() {
                        if obj.remove(final_key).is_some() { modified = true; }
                    }
                }
                if modified {
                    let new_value_bytes = match serde_json::to_vec(&current_val) {
                        Ok(b) => b,
                        Err(_) => return Response::Error("Failed to serialize new JSON value".to_string()),
                    };
                    tx.writes.insert(key.clone(), Some(DbValue::JsonB(new_value_bytes)));
                }
            }
            return Response::Integer(1);
        }
        drop(tx_guard);
        let txid = self.ctx.tx_id_manager.new_txid();
        self.ctx.tx_status_manager.begin(txid);
        let mut version_chain_lock = self.ctx.db.entry(key.clone()).or_default();
        let mut version_chain = version_chain_lock.value_mut().write().await;
        let old_size;
        let old_val_for_index;
        let mut modified = false;
        let new_val = if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| v.expirer_txid == 0 && self.ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)) {
            old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
            let current_val = match &latest_version.value {
                DbValue::Json(v) => { old_val_for_index = v.clone(); v.clone() }
                DbValue::JsonB(b) => { let v: Value = serde_json::from_slice(b).unwrap_or_default(); old_val_for_index = v.clone(); v }
                _ => return Response::Error("WRONGTYPE Operation against a non-JSON value".to_string()),
            };
            if path.is_empty() || path == "." {
                latest_version.expirer_txid = txid;
                modified = true;
                current_val
            } else {
                let mut temp_val = current_val;
                let mut pointer_parts: Vec<&str> = path.split('.').collect();
                let final_key = pointer_parts.pop().unwrap();
                let parent_pointer = json_path_to_pointer(&pointer_parts.join("."));
                if let Some(target) = temp_val.pointer_mut(&parent_pointer) {
                    if let Some(obj) = target.as_object_mut() {
                        if obj.remove(final_key).is_some() { latest_version.expirer_txid = txid; modified = true; }
                    }
                }
                temp_val
            }
        } else {
            self.ctx.tx_status_manager.abort(txid);
            return Response::Integer(0);
        };
        if !modified {
            self.ctx.tx_status_manager.abort(txid);
            return Response::Integer(0);
        }
        if path.is_empty() || path == "." {
            let log_entry = LogEntry::Delete { key: key.clone() };
            let ack_response = log_to_wal(log_entry, &self.ctx).await;
            if !matches!(ack_response, Response::Ok) {
                self.ctx.tx_status_manager.abort(txid);
                return ack_response;
            }
            self.ctx.memory.decrease_memory(old_size);
            if self.ctx.memory.is_enabled() { self.ctx.memory.forget_key(&key).await; }
            self.ctx.index_manager.remove_key_from_indexes(&key, &old_val_for_index).await;
        } else {
            let new_value_bytes = match serde_json::to_vec(&new_val) {
                Ok(b) => b,
                Err(_) => return Response::Error("Failed to serialize new JSON value".to_string()),
            };
            let log_entry = LogEntry::SetJsonB { key: key.clone(), value: new_value_bytes.clone() };
            let ack_response = log_to_wal(log_entry, &self.ctx).await;
            if !matches!(ack_response, Response::Ok) {
                self.ctx.tx_status_manager.abort(txid);
                return ack_response;
            }
            let new_version = crate::types::VersionedValue { value: DbValue::JsonB(new_value_bytes.clone()), creator_txid: txid, expirer_txid: 0 };
            version_chain.push(new_version);
            let new_size = key.len() as u64 + new_value_bytes.len() as u64;
            self.ctx.memory.decrease_memory(old_size);
            self.ctx.memory.increase_memory(new_size);
            if self.ctx.memory.is_enabled() { self.ctx.memory.track_access(&key).await; }
            self.ctx.index_manager.remove_key_from_indexes(&key, &old_val_for_index).await;
            self.ctx.index_manager.add_key_to_indexes(&key, &new_val).await;
        }
        self.ctx.tx_status_manager.commit(txid);
        Response::Integer(1)
    }

    pub async fn lpush(&self, key: String, values: Vec<Vec<u8>>) -> Response {
        let log_entry = LogEntry::LPush { key: key.clone(), values: values.clone() };
        let mut tx_guard = self.transaction_handle.write().await;
        if let Some(tx) = tx_guard.as_mut() {
            tx.log_entries.write().await.push(log_entry);
            let current_db_val = get_visible_db_value(&key, &self.ctx, Some(tx)).await;
            let mut current_list = match current_db_val {
                Some(DbValue::List(list_lock)) => list_lock.read().await.clone(),
                Some(_) => return Response::Error("WRONGTYPE Operation against a non-list value".to_string()),
                _ => VecDeque::new(),
            };
            for v in values.into_iter().rev() { current_list.push_front(v); }
            let new_len = current_list.len() as i64;
            tx.writes.insert(key, Some(DbValue::List(RwLock::new(current_list))));
            return Response::Integer(new_len);
        }
        drop(tx_guard);
        let txid = self.ctx.tx_id_manager.new_txid();
        self.ctx.tx_status_manager.begin(txid);
        let ack_response = log_to_wal(log_entry, &self.ctx).await;
        if !matches!(ack_response, Response::Ok) {
            self.ctx.tx_status_manager.abort(txid);
            return ack_response;
        }
        let mut version_chain_lock = self.ctx.db.entry(key.clone()).or_default();
        let mut version_chain = version_chain_lock.value_mut().write().await;
        let mut old_size = 0;
        let mut new_list = if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| v.expirer_txid == 0 && self.ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)) {
            latest_version.expirer_txid = txid;
            old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
            match &latest_version.value {
                DbValue::List(list_lock) => list_lock.read().await.clone(),
                _ => return Response::Error("WRONGTYPE Operation against a non-list value".to_string()),
            }
        } else { VecDeque::new() };
        for v in values { new_list.push_front(v); }
        let new_len = new_list.len() as i64;
        let new_db_value = DbValue::List(RwLock::new(new_list));
        let new_size = key.len() as u64 + memory::estimate_db_value_size(&new_db_value).await;
        if self.ctx.memory.is_enabled() {
            let needed = new_size.saturating_sub(old_size);
            if let Err(e) = self.ctx.memory.ensure_memory_for(needed, &self.ctx).await {
                self.ctx.tx_status_manager.abort(txid);
                return Response::Error(e.to_string());
            }
        }
        let new_version = crate::types::VersionedValue { value: new_db_value, creator_txid: txid, expirer_txid: 0 };
        version_chain.push(new_version);
        self.ctx.memory.decrease_memory(old_size);
        self.ctx.memory.increase_memory(new_size);
        if self.ctx.memory.is_enabled() { self.ctx.memory.track_access(&key).await; }
        self.ctx.tx_status_manager.commit(txid);
        Response::Integer(new_len)
    }

    pub async fn rpush(&self, key: String, values: Vec<Vec<u8>>) -> Response {
        let log_entry = LogEntry::RPush { key: key.clone(), values: values.clone() };
        let mut tx_guard = self.transaction_handle.write().await;
        if let Some(tx) = tx_guard.as_mut() {
            tx.log_entries.write().await.push(log_entry);
            let current_db_val = get_visible_db_value(&key, &self.ctx, Some(tx)).await;
            let mut current_list = match current_db_val {
                Some(DbValue::List(list_lock)) => list_lock.read().await.clone(),
                Some(_) => return Response::Error("WRONGTYPE Operation against a non-list value".to_string()),
                _ => VecDeque::new(),
            };
            for v in values { current_list.push_back(v); }
            let new_len = current_list.len() as i64;
            tx.writes.insert(key, Some(DbValue::List(RwLock::new(current_list))));
            return Response::Integer(new_len);
        }
        drop(tx_guard);
        let txid = self.ctx.tx_id_manager.new_txid();
        self.ctx.tx_status_manager.begin(txid);
        let ack_response = log_to_wal(log_entry, &self.ctx).await;
        if !matches!(ack_response, Response::Ok) {
            self.ctx.tx_status_manager.abort(txid);
            return ack_response;
        }
        let mut version_chain_lock = self.ctx.db.entry(key.clone()).or_default();
        let mut version_chain = version_chain_lock.value_mut().write().await;
        let mut old_size = 0;
        let mut new_list = if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| v.expirer_txid == 0 && self.ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)) {
            latest_version.expirer_txid = txid;
            old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
            match &latest_version.value {
                DbValue::List(list_lock) => list_lock.read().await.clone(),
                _ => return Response::Error("WRONGTYPE Operation against a non-list value".to_string()),
            }
        } else { VecDeque::new() };
        for v in values { new_list.push_back(v); }
        let new_len = new_list.len() as i64;
        let new_db_value = DbValue::List(RwLock::new(new_list));
        let new_size = key.len() as u64 + memory::estimate_db_value_size(&new_db_value).await;
        if self.ctx.memory.is_enabled() {
            let needed = new_size.saturating_sub(old_size);
            if let Err(e) = self.ctx.memory.ensure_memory_for(needed, &self.ctx).await {
                self.ctx.tx_status_manager.abort(txid);
                return Response::Error(e.to_string());
            }
        }
        let new_version = crate::types::VersionedValue { value: new_db_value, creator_txid: txid, expirer_txid: 0 };
        version_chain.push(new_version);
        self.ctx.memory.decrease_memory(old_size);
        self.ctx.memory.increase_memory(new_size);
        if self.ctx.memory.is_enabled() { self.ctx.memory.track_access(&key).await; }
        self.ctx.tx_status_manager.commit(txid);
        Response::Integer(new_len)
    }

    pub async fn lpop(&self, key: String, count: usize) -> Response {
        let log_entry = LogEntry::LPop { key: key.clone(), count };
        let mut tx_guard = self.transaction_handle.write().await;
        if let Some(tx) = tx_guard.as_mut() {
            tx.log_entries.write().await.push(log_entry);
            let current_db_val = get_visible_db_value(&key, &self.ctx, Some(tx)).await;
            let mut current_list = match current_db_val {
                Some(DbValue::List(list_lock)) => list_lock.read().await.clone(),
                Some(_) => return Response::Error("WRONGTYPE Operation against a non-list value".to_string()),
                _ => VecDeque::new(),
            };
            let mut popped = Vec::new();
            for _ in 0..count { if let Some(val) = current_list.pop_front() { popped.push(val); } else { break; } }
            tx.writes.insert(key, Some(DbValue::List(RwLock::new(current_list))));
            if popped.is_empty() { return Response::Nil; }
            return Response::MultiBytes(popped);
        }
        drop(tx_guard);
        let txid = self.ctx.tx_id_manager.new_txid();
        self.ctx.tx_status_manager.begin(txid);
        let ack_response = log_to_wal(log_entry, &self.ctx).await;
        if !matches!(ack_response, Response::Ok) {
            self.ctx.tx_status_manager.abort(txid);
            return ack_response;
        }
        let mut version_chain_lock = self.ctx.db.entry(key.clone()).or_default();
        let mut version_chain = version_chain_lock.value_mut().write().await;
        let mut old_size = 0;
        let mut popped = Vec::new();
        let mut list_exists = false;
        let mut new_list = if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| v.expirer_txid == 0 && self.ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)) {
            latest_version.expirer_txid = txid;
            list_exists = true;
            old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
            match &latest_version.value {
                DbValue::List(list_lock) => list_lock.read().await.clone(),
                _ => return Response::Error("WRONGTYPE Operation against a non-list value".to_string()),
            }
        } else { VecDeque::new() };
        if list_exists { for _ in 0..count { if let Some(val) = new_list.pop_front() { popped.push(val); } else { break; } } }
        let new_db_value = DbValue::List(RwLock::new(new_list));
        let new_size = key.len() as u64 + memory::estimate_db_value_size(&new_db_value).await;
        if self.ctx.memory.is_enabled() {
            let needed = new_size.saturating_sub(old_size);
            if let Err(e) = self.ctx.memory.ensure_memory_for(needed, &self.ctx).await {
                self.ctx.tx_status_manager.abort(txid);
                return Response::Error(e.to_string());
            }
        }
        let new_version = crate::types::VersionedValue { value: new_db_value, creator_txid: txid, expirer_txid: 0 };
        version_chain.push(new_version);
        self.ctx.memory.decrease_memory(old_size);
        self.ctx.memory.increase_memory(new_size);
        if self.ctx.memory.is_enabled() { self.ctx.memory.track_access(&key).await; }
        self.ctx.tx_status_manager.commit(txid);
        if popped.is_empty() { Response::Nil } else { Response::MultiBytes(popped) }
    }

    pub async fn rpop(&self, key: String, count: usize) -> Response {
        let log_entry = LogEntry::RPop { key: key.clone(), count };
        let mut tx_guard = self.transaction_handle.write().await;
        if let Some(tx) = tx_guard.as_mut() {
            tx.log_entries.write().await.push(log_entry);
            let current_db_val = get_visible_db_value(&key, &self.ctx, Some(tx)).await;
            let mut current_list = match current_db_val {
                Some(DbValue::List(list_lock)) => list_lock.read().await.clone(),
                Some(_) => return Response::Error("WRONGTYPE Operation against a non-list value".to_string()),
                _ => VecDeque::new(),
            };
            let mut popped = Vec::new();
            for _ in 0..count { if let Some(val) = current_list.pop_back() { popped.push(val); } else { break; } }
            tx.writes.insert(key, Some(DbValue::List(RwLock::new(current_list))));
            if popped.is_empty() { return Response::Nil; }
            return Response::MultiBytes(popped);
        }
        drop(tx_guard);
        let txid = self.ctx.tx_id_manager.new_txid();
        self.ctx.tx_status_manager.begin(txid);
        let ack_response = log_to_wal(log_entry, &self.ctx).await;
        if !matches!(ack_response, Response::Ok) {
            self.ctx.tx_status_manager.abort(txid);
            return ack_response;
        }
        let mut version_chain_lock = self.ctx.db.entry(key.clone()).or_default();
        let mut version_chain = version_chain_lock.value_mut().write().await;
        let mut old_size = 0;
        let mut popped = Vec::new();
        let mut list_exists = false;
        let mut new_list = if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| v.expirer_txid == 0 && self.ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)) {
            latest_version.expirer_txid = txid;
            list_exists = true;
            old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
            match &latest_version.value {
                DbValue::List(list_lock) => list_lock.read().await.clone(),
                _ => return Response::Error("WRONGTYPE Operation against a non-list value".to_string()),
            }
        } else { VecDeque::new() };
        if list_exists { for _ in 0..count { if let Some(val) = new_list.pop_back() { popped.push(val); } else { break; } } }
        let new_db_value = DbValue::List(RwLock::new(new_list));
        let new_size = key.len() as u64 + memory::estimate_db_value_size(&new_db_value).await;
        if self.ctx.memory.is_enabled() {
            let needed = new_size.saturating_sub(old_size);
            if let Err(e) = self.ctx.memory.ensure_memory_for(needed, &self.ctx).await {
                self.ctx.tx_status_manager.abort(txid);
                return Response::Error(e.to_string());
            }
        }
        let new_version = crate::types::VersionedValue { value: new_db_value, creator_txid: txid, expirer_txid: 0 };
        version_chain.push(new_version);
        self.ctx.memory.decrease_memory(old_size);
        self.ctx.memory.increase_memory(new_size);
        if self.ctx.memory.is_enabled() { self.ctx.memory.track_access(&key).await; }
        self.ctx.tx_status_manager.commit(txid);
        if popped.is_empty() { Response::Nil } else { Response::MultiBytes(popped) }
    }

    pub async fn sadd(&self, key: String, members: Vec<Vec<u8>>) -> Response {
        let log_entry = LogEntry::SAdd { key: key.clone(), members: members.clone() };
        let mut tx_guard = self.transaction_handle.write().await;
        if let Some(tx) = tx_guard.as_mut() {
            tx.log_entries.write().await.push(log_entry);
            let current_db_val = get_visible_db_value(&key, &self.ctx, Some(tx)).await;
            let mut current_set = match current_db_val {
                Some(DbValue::Set(set_lock)) => set_lock.read().await.clone(),
                Some(_) => return Response::Error("WRONGTYPE Operation against a non-set value".to_string()),
                _ => HashSet::new(),
            };
            let mut added_count = 0;
            for m in members { if current_set.insert(m) { added_count += 1; } }
            tx.writes.insert(key, Some(DbValue::Set(RwLock::new(current_set))));
            return Response::Integer(added_count);
        }
        drop(tx_guard);
        let txid = self.ctx.tx_id_manager.new_txid();
        self.ctx.tx_status_manager.begin(txid);
        let existing_value = get_visible_db_value(&key, &self.ctx, None).await;
        let mut old_size = 0;
        let mut new_set = match existing_value {
            Some(DbValue::Set(set_lock)) => {
                let s = set_lock.read().await;
                old_size = key.len() as u64 + memory::estimate_db_value_size(&DbValue::Set(RwLock::new(s.clone()))).await;
                s.clone()
            }
            Some(_) => { self.ctx.tx_status_manager.abort(txid); return Response::Error("WRONGTYPE Operation against a non-set value".to_string()); }
            None => HashSet::new(),
        };
        let ack_response = log_to_wal(log_entry, &self.ctx).await;
        if !matches!(ack_response, Response::Ok) {
            self.ctx.tx_status_manager.abort(txid);
            return ack_response;
        }
        let mut version_chain_lock = self.ctx.db.entry(key.clone()).or_default();
        let mut version_chain = version_chain_lock.value_mut().write().await;
        if old_size > 0 {
            if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| v.expirer_txid == 0 && self.ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)) {
                latest_version.expirer_txid = txid;
            }
        }
        let mut added_count = 0;
        for m in members { if new_set.insert(m) { added_count += 1; } }
        let new_db_value = DbValue::Set(RwLock::new(new_set));
        let new_size = key.len() as u64 + memory::estimate_db_value_size(&new_db_value).await;
        if self.ctx.memory.is_enabled() {
            let needed = new_size.saturating_sub(old_size);
            if let Err(e) = self.ctx.memory.ensure_memory_for(needed, &self.ctx).await {
                self.ctx.tx_status_manager.abort(txid);
                return Response::Error(e.to_string());
            }
        }
        let new_version = crate::types::VersionedValue { value: new_db_value, creator_txid: txid, expirer_txid: 0 };
        version_chain.push(new_version);
        self.ctx.memory.decrease_memory(old_size);
        self.ctx.memory.increase_memory(new_size);
        if self.ctx.memory.is_enabled() { self.ctx.memory.track_access(&key).await; }
        self.ctx.tx_status_manager.commit(txid);
        Response::Integer(added_count)
    }

    pub async fn srem(&self, key: String, members: Vec<Vec<u8>>) -> Response {
        let log_entry = LogEntry::SRem { key: key.clone(), members: members.clone() };
        let mut tx_guard = self.transaction_handle.write().await;
        if let Some(tx) = tx_guard.as_mut() {
            tx.log_entries.write().await.push(log_entry);
            let current_db_val = get_visible_db_value(&key, &self.ctx, Some(tx)).await;
            let mut current_set = match current_db_val {
                Some(DbValue::Set(set_lock)) => set_lock.read().await.clone(),
                Some(_) => return Response::Error("WRONGTYPE Operation against a non-set value".to_string()),
                _ => HashSet::new(),
            };
            let mut removed_count = 0;
            for m in members { if current_set.remove(&m) { removed_count += 1; } }
            tx.writes.insert(key, Some(DbValue::Set(RwLock::new(current_set))));
            return Response::Integer(removed_count);
        }
        drop(tx_guard);
        let txid = self.ctx.tx_id_manager.new_txid();
        self.ctx.tx_status_manager.begin(txid);
        let ack_response = log_to_wal(log_entry, &self.ctx).await;
        if !matches!(ack_response, Response::Ok) {
            self.ctx.tx_status_manager.abort(txid);
            return ack_response;
        }
        let mut version_chain_lock = self.ctx.db.entry(key.clone()).or_default();
        let mut version_chain = version_chain_lock.value_mut().write().await;
        let mut old_size = 0;
        let mut removed_count = 0;
        let mut new_set = if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| v.expirer_txid == 0 && self.ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)) {
            latest_version.expirer_txid = txid;
            old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
            match &latest_version.value {
                DbValue::Set(set_lock) => set_lock.read().await.clone(),
                _ => return Response::Error("WRONGTYPE Operation against a non-set value".to_string()),
            }
        } else { HashSet::new() };
        for m in members { if new_set.remove(&m) { removed_count += 1; } }
        let new_db_value = DbValue::Set(RwLock::new(new_set));
        let new_size = key.len() as u64 + memory::estimate_db_value_size(&new_db_value).await;
        if self.ctx.memory.is_enabled() {
            let needed = new_size.saturating_sub(old_size);
            if let Err(e) = self.ctx.memory.ensure_memory_for(needed, &self.ctx).await {
                self.ctx.tx_status_manager.abort(txid);
                return Response::Error(e.to_string());
            }
        }
        let new_version = crate::types::VersionedValue { value: new_db_value, creator_txid: txid, expirer_txid: 0 };
        version_chain.push(new_version);
        self.ctx.memory.decrease_memory(old_size);
        self.ctx.memory.increase_memory(new_size);
        if self.ctx.memory.is_enabled() { self.ctx.memory.track_access(&key).await; }
        self.ctx.tx_status_manager.commit(txid);
        Response::Integer(removed_count)
    }

    pub async fn delete_rows(&self, table_name: &str, rows: Vec<Value>) -> Result<u64> {
        let mut tx_guard = self.transaction_handle.write().await;
        if let Some(tx) = tx_guard.as_mut() {
            let mut deleted_count = 0;
            for row in rows {
                let table_part = match row.get(table_name) { Some(part) => part, None => continue };
                let key = match table_part.get("_key").and_then(|k| k.as_str()) { Some(k) => k.to_string(), None => continue };
                let log_entry = LogEntry::Delete { key: key.clone() };
                tx.log_entries.write().await.push(log_entry);
                tx.writes.insert(key, None);
                deleted_count += 1;
            }
            return Ok(deleted_count);
        }
        drop(tx_guard);
        let mut deleted_count = 0;
        for row in rows {
            let table_part = match row.get(table_name) { Some(part) => part, None => continue };
            let key = match table_part.get("_key").and_then(|k| k.as_str()) { Some(k) => k.to_string(), None => continue };
            let txid = self.ctx.tx_id_manager.new_txid();
            self.ctx.tx_status_manager.begin(txid);
            let log_entry = LogEntry::Delete { key: key.clone() };
            let ack_response = log_to_wal(log_entry, &self.ctx).await;
            if !matches!(ack_response, Response::Ok) {
                self.ctx.tx_status_manager.abort(txid);
                continue;
            }
            let mut old_size = 0;
            if let Some(version_chain_lock) = self.ctx.db.get(&key) {
                let mut version_chain = version_chain_lock.value().write().await;
                let mut expired_something = false;
                if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| v.expirer_txid == 0 && self.ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)) {
                    latest_version.expirer_txid = txid;
                    expired_something = true;
                    old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
                    self.ctx.index_manager.remove_key_from_indexes(&key, &table_part).await;
                }
                if expired_something {
                    self.ctx.memory.decrease_memory(old_size);
                    if self.ctx.memory.is_enabled() { self.ctx.memory.forget_key(&key).await; }
                    deleted_count += 1;
                }
            }
            self.ctx.tx_status_manager.commit(txid);
        }
        Ok(deleted_count)
    }

    pub async fn update_rows(&self, table_name: &str, rows_to_update: Vec<Value>, set_clauses: &[(String, crate::query_engine::logical_plan::Expression)]) -> Result<Vec<Value>> {
        let mut updated_rows = Vec::new();
        let schema = self.ctx.schema_cache.get(table_name);
        let mut tx_guard = self.transaction_handle.write().await;
        if let Some(tx) = tx_guard.as_mut() {
            for row in rows_to_update {
                let table_part = match row.get(table_name) { Some(part) => part, None => continue };
                let key = match table_part.get("_key").and_then(|k| k.as_str()) { Some(k) => k.to_string(), None => continue };
                let mut new_val = table_part.clone();
                for (col, expr) in set_clauses {
                    let mut val = expr.evaluate_with_context(&row, None, self.ctx.clone(), Some(self.transaction_handle.clone())).await?;
                    if let Some(s) = &schema {
                        if let Some(col_def) = s.columns.get(col) {
                            val = crate::query_engine::logical_plan::cast_value_to_type(val, &col_def.data_type)?;
                        }
                    }
                    new_val[col] = val;
                }
                let new_val_bytes = serde_json::to_vec(&new_val)?;
                let log_entry = LogEntry::SetJsonB { key: key.clone(), value: new_val_bytes.clone() };
                tx.log_entries.write().await.push(log_entry);
                            tx.writes.insert(key, Some(DbValue::JsonB(new_val_bytes)));
                            updated_rows.push(new_val);
                        }
                        return Ok(updated_rows);        }
        drop(tx_guard);
        for row in rows_to_update {
            let table_part = match row.get(table_name) { Some(part) => part, None => continue };
            let key = match table_part.get("_key").and_then(|k| k.as_str()) { Some(k) => k.to_string(), None => continue };
            let txid = self.ctx.tx_id_manager.new_txid();
            self.ctx.tx_status_manager.begin(txid);
            let (old_val_for_index, new_val) = {
                let version_chain_lock = self.ctx.db.entry(key.clone()).or_default();
                let version_chain = version_chain_lock.read().await;
                let mut old_val = json!({});
                let mut found_visible = false;
                for version in version_chain.iter().rev() {
                    if self.ctx.tx_status_manager.get_status(version.creator_txid) == Some(TransactionStatus::Committed) && (version.expirer_txid == 0 || self.ctx.tx_status_manager.get_status(version.expirer_txid) != Some(TransactionStatus::Committed)) {
                        old_val = match &version.value {
                            DbValue::Json(v) => v.clone(),
                            DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_default(),
                            _ => return Err(anyhow!("WRONGTYPE Operation against a non-JSON value")),
                        };
                        found_visible = true;
                        break;
                    }
                }
                if !found_visible { continue; }
                            let mut new_val = old_val.clone();
                            for (col, expr) in set_clauses {
                                let mut val = expr.evaluate_with_context(&row, Some(&old_val), self.ctx.clone(), None).await?;
                                if let Some(s) = &schema {
                                    if let Some(col_def) = s.columns.get(col) {
                                        val = crate::query_engine::logical_plan::cast_value_to_type(val, &col_def.data_type)?;
                                    }
                                }
                                new_val[col] = val;
                            }
                (old_val, new_val)
            };
            let new_val_bytes = serde_json::to_vec(&new_val)?;
            let log_entry = LogEntry::SetJsonB { key: key.clone(), value: new_val_bytes.clone() };
            if let Response::Error(e) = log_to_wal(log_entry, &self.ctx).await {
                self.ctx.tx_status_manager.abort(txid);
                return Err(anyhow!(e));
            }
            let mut version_chain_lock = self.ctx.db.entry(key.clone()).or_default();
            let mut version_chain = version_chain_lock.value_mut().write().await;
            if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| v.expirer_txid == 0 && self.ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)) {
                latest_version.expirer_txid = txid;
            }
            let new_version = crate::types::VersionedValue { value: DbValue::JsonB(new_val_bytes), creator_txid: txid, expirer_txid: 0 };
            version_chain.push(new_version);
            self.ctx.index_manager.remove_key_from_indexes(&key, &old_val_for_index).await;
                    self.ctx.index_manager.add_key_to_indexes(&key, &new_val).await;
                    self.ctx.tx_status_manager.commit(txid);
                    updated_rows.push(new_val);
                }
                Ok(updated_rows)    }

    pub async fn insert_rows(&self, table_name: &str, columns: &[String], source_rows: Vec<Value>, source_column_names: &[String], on_conflict: &Option<(Vec<String>, crate::query_engine::logical_plan::OnConflictAction)>) -> Result<Vec<Value>> {
        let mut inserted_rows = Vec::new();
        let schema = self.ctx.schema_cache.get(table_name);
        let mut tx_guard = self.transaction_handle.write().await;
        if let Some(tx) = tx_guard.as_mut() {
            for source_row in source_rows {
                let source_row_obj = source_row.as_object().ok_or_else(|| anyhow!("INSERT source did not produce an object"))?;
                let insert_columns = if columns.is_empty() {
                    if let Some(s) = &schema {
                        if !s.column_order.is_empty() { s.column_order.clone() } else { s.columns.keys().cloned().collect::<Vec<String>>() }
                    } else {
                        return Err(anyhow!("Cannot INSERT without column list into a table with no schema"));
                    }
                } else {
                    columns.to_vec()
                };
                let mut row_data = json!({});
                if !source_column_names.is_empty() {
                    if insert_columns.len() != source_column_names.len() {
                        return Err(anyhow!("INSERT has mismatch between number of columns ({}) and values from source ({})", insert_columns.len(), source_column_names.len()));
                    }
                    for (i, target_col_name) in insert_columns.iter().enumerate() {
                        let source_col_name = &source_column_names[i];
                        let mut val = source_row_obj.get(source_col_name).cloned().unwrap_or(Value::Null);
                        if let Some(s) = &schema {
                            if let Some(col_def) = s.columns.get(target_col_name) {
                                val = crate::query_engine::logical_plan::cast_value_to_type(val, &col_def.data_type)?;
                            }
                        }
                        row_data[target_col_name] = val;
                    }
                }

                if let Some(s) = &schema {
                    for (col_name, col_def) in &s.columns {
                        if !row_data.get(col_name).is_some() {
                            if let Some(default_expr) = &col_def.default {
                                let mut val = default_expr.evaluate_with_context(&json!({}), None, self.ctx.clone(), Some(self.transaction_handle.clone())).await?;
                                val = crate::query_engine::logical_plan::cast_value_to_type(val, &col_def.data_type)?;
                                row_data[col_name.clone()] = val;
                            }
                        }
                    }
                }

                let pk_col = if let Some(s) = &schema { s.constraints.iter().find_map(|c| if let crate::query_engine::ast::TableConstraint::PrimaryKey { columns, .. } = c { columns.first().cloned() } else { None }).unwrap_or_else(|| "id".to_string()) } else { "id".to_string() };
                let pk = match row_data.get(&pk_col) { Some(Value::String(s)) => s.clone(), Some(Value::Number(n)) => n.to_string(), _ => uuid::Uuid::new_v4().to_string() };
                let key = format!("{}:{}", table_name, pk);

                let key_exists = tx.writes.contains_key(&key) || self.ctx.db.contains_key(&key);
                if key_exists {
                    if let Some((_target, action)) = on_conflict {
                        match action {
                            crate::query_engine::logical_plan::OnConflictAction::DoNothing => {
                                continue; // Skip insertion
                            }
                            crate::query_engine::logical_plan::OnConflictAction::DoUpdate(set_clauses) => {
                                let old_val = match get_visible_db_value(&key, &self.ctx, Some(tx)).await {
                                    Some(DbValue::JsonB(b)) => serde_json::from_slice(&b)?,
                                    Some(DbValue::Json(v)) => v.clone(),
                                    _ => json!({}),
                                };

                                let mut new_val = old_val.clone();
                                let excluded_row = json!({ "excluded": row_data.clone() });

                                for (col, expr) in set_clauses {
                                    let val = expr.evaluate_with_context(&excluded_row, Some(&old_val), self.ctx.clone(), Some(self.transaction_handle.clone())).await?;
                                    new_val[col] = val;
                                }

                                let value_bytes = serde_json::to_vec(&new_val)?;
                                let log_entry = LogEntry::SetJsonB { key: key.clone(), value: value_bytes.clone() };
                                tx.log_entries.write().await.push(log_entry);
                                tx.writes.insert(key, Some(DbValue::JsonB(value_bytes)));
                                
                                inserted_rows.push(new_val);
                                continue;
                            }
                        }
                    } else {
                        return Err(anyhow!("PRIMARY KEY constraint failed. Key '{}' already exists.", key));
                    }
                }

                let value_bytes = serde_json::to_vec(&row_data)?;
                let log_entry = LogEntry::SetJsonB { key: key.clone(), value: value_bytes.clone() };
                tx.log_entries.write().await.push(log_entry);
                tx.writes.insert(key, Some(DbValue::JsonB(value_bytes)));
                inserted_rows.push(row_data);
            }
            return Ok(inserted_rows);
        }
        drop(tx_guard);
        for source_row in source_rows {
            let source_row_obj = source_row.as_object().ok_or_else(|| anyhow!("INSERT source did not produce an object"))?;
            let insert_columns = if columns.is_empty() {
                if let Some(s) = &schema {
                    if !s.column_order.is_empty() { s.column_order.clone() } else { s.columns.keys().cloned().collect::<Vec<String>>() }
                } else {
                    return Err(anyhow!("Cannot INSERT without column list into a table with no schema"));
                }
            } else {
                columns.to_vec()
            };
            let mut row_data = json!({});
            for (i, target_col_name) in insert_columns.iter().enumerate() {
                let source_col_name = &source_column_names[i];
                let mut val = source_row_obj.get(source_col_name).cloned().unwrap_or(Value::Null);
                if let Some(s) = &schema {
                    if let Some(col_def) = s.columns.get(target_col_name) {
                        val = crate::query_engine::logical_plan::cast_value_to_type(val, &col_def.data_type)?;
                    }
                }
                row_data[target_col_name] = val;
            }
            if let Some(s) = &schema {
            for (col_name, col_def) in &s.columns {
                if !row_data.get(col_name).is_some() {
                    if let Some(default_expr) = &col_def.default {
                        let mut val = default_expr.evaluate_with_context(&json!({}), None, self.ctx.clone(), None).await?;
                        val = crate::query_engine::logical_plan::cast_value_to_type(val, &col_def.data_type)?;
                        row_data[col_name.clone()] = val;
                    }
                }
            }
        }

        if let Some(s) = &schema {
            // NOT NULL and CHECK constraints
            for (col_name, col_def) in &s.columns {
                let val = row_data.get(col_name).unwrap_or(&Value::Null);
                if !col_def.nullable && val.is_null() {
                    return Err(anyhow!("NULL value in column '{}' violates not-null constraint", col_name));
                }
            }
            for constraint in &s.constraints {
                if let crate::query_engine::ast::TableConstraint::Check { name: _, expression } = constraint {
                    let check_row = json!({ table_name.to_string(): row_data.clone() });
                    if !crate::query_engine::logical_plan::simple_expr_to_expression(expression.clone(), &self.ctx.schema_cache, &self.ctx.view_cache, &self.ctx.function_registry, None)?.evaluate_with_context(&check_row, None, self.ctx.clone(), None).await?.as_bool().unwrap_or(false)
                    {
                        return Err(anyhow!("CHECK constraint failed for table '{}'", table_name));
                    }
                }
            }
        }

        // UNIQUE and PRIMARY KEY constraints
        if let Some(s) = &schema {
            for constraint in &s.constraints {
                if let crate::query_engine::ast::TableConstraint::Unique { name, columns } = constraint {
                    if columns.len() == 1 { // Only single-column unique constraints are handled here
                        let col_name = &columns[0];
                        if let Some(val) = row_data.get(col_name) {
                            let index_name = name.clone().unwrap_or_else(|| format!("unique_{}_{}", table_name, col_name));

                            if let Some(internal_name_entry) = self.ctx.index_manager.name_to_internal_name.get(&index_name) {
                                let internal_name = internal_name_entry.value();
                                if let Some(index) = self.ctx.index_manager.indexes.get(internal_name) {
                                    let index_key = serde_json::to_string(val)?;
                                    let index_data = index.read().await;
                                    if index_data.contains_key(&index_key) {
                                        return Err(anyhow!("UNIQUE constraint '{}' failed for column '{}'", index_name, col_name));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // FOREIGN KEY constraints
        if let Some(s) = &schema {
            for constraint in &s.constraints {
                if let crate::query_engine::ast::TableConstraint::ForeignKey(fk) = constraint {
                    let child_key_values: Vec<Value> = fk
                        .columns
                        .iter()
                        .map(|col_name| row_data.get(col_name).cloned().unwrap_or(Value::Null))
                        .collect();

                    if child_key_values.iter().any(|v| v.is_null()) {
                        continue;
                    }

                    // This is a simplified check assuming single-column FK to PK
                    if fk.columns.len() == 1 && fk.references_columns.len() == 1 {
                        let child_fk_val = &child_key_values[0];
                        let parent_key_val = match child_fk_val {
                            Value::String(s) => s.clone(),
                            Value::Number(n) => n.to_string(),
                            _ => continue, // Cannot build key from this value
                        };

                        let parent_key = format!("{}:{}", fk.references_table, parent_key_val);
                        
                        let parent_exists = get_visible_db_value(&parent_key, &self.ctx, None).await.is_some();

                        if !parent_exists {
                            return Err(anyhow!(
                                "Insert or update on table '{}' violates foreign key constraint. A matching key was not found in table '{}'.",
                                table_name,
                                fk.references_table,
                            ));
                        }
                    }
                }
            }
        }

        let pk_col = if let Some(s) = &schema { s.constraints.iter().find_map(|c| if let crate::query_engine::ast::TableConstraint::PrimaryKey { columns, .. } = c { columns.first().cloned() } else { None }).unwrap_or_else(|| "id".to_string()) } else { "id".to_string() };
                    let pk = match row_data.get(&pk_col) { Some(Value::String(s)) => s.clone(), Some(Value::Number(n)) => n.to_string(), _ => uuid::Uuid::new_v4().to_string() };
                    let key = format!("{}:{}", table_name, pk);
                    if self.ctx.db.contains_key(&key) {
                        if let Some((_target, action)) = on_conflict {
                            match action {
                                crate::query_engine::logical_plan::OnConflictAction::DoNothing => {
                                    continue; // Skip insertion
                                }
                                crate::query_engine::logical_plan::OnConflictAction::DoUpdate(set_clauses) => {
                                let txid = self.ctx.tx_id_manager.new_txid();
                                self.ctx.tx_status_manager.begin(txid);
                                let (old_val_for_index, new_val) = {
                                    let old_val = match get_visible_db_value(&key, &self.ctx, None).await {
                                        Some(DbValue::JsonB(b)) => serde_json::from_slice(&b)?,
                                        Some(DbValue::Json(v)) => v.clone(),
                                        _ => json!({}),
                                    };
                                    let mut new_val = old_val.clone();
                                    let excluded_row = json!({ "excluded": row_data.clone() });
                                    for (col, expr) in set_clauses {
                                        let val = expr.evaluate_with_context(&excluded_row, Some(&old_val), self.ctx.clone(), None).await?;
                                        new_val[col] = val;
                                    }
                                    (old_val, new_val)
                                };
                                let new_val_bytes = serde_json::to_vec(&new_val)?;
                                let log_entry = LogEntry::SetJsonB { key: key.clone(), value: new_val_bytes.clone() };
                                if let Response::Error(e) = log_to_wal(log_entry, &self.ctx).await {
                                    self.ctx.tx_status_manager.abort(txid);
                                    return Err(anyhow!(e));
                                }
                                let mut version_chain_lock = self.ctx.db.entry(key.clone()).or_default();
                                let mut version_chain = version_chain_lock.value_mut().write().await;
                                if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| v.expirer_txid == 0 && self.ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)) {
                                    latest_version.expirer_txid = txid;
                                }
                                let new_version = crate::types::VersionedValue { value: DbValue::JsonB(new_val_bytes), creator_txid: txid, expirer_txid: 0 };
                                version_chain.push(new_version);
                                self.ctx.index_manager.remove_key_from_indexes(&key, &old_val_for_index).await;
                                self.ctx.index_manager.add_key_to_indexes(&key, &new_val).await;
                                self.ctx.tx_status_manager.commit(txid);
                                inserted_rows.push(new_val);
                                continue;
                            }
                            }
                        } else {
                            return Err(anyhow!("PRIMARY KEY constraint failed. Key '{}' already exists.", key));
                        }
                    }            let value_bytes = serde_json::to_vec(&row_data)?;
            let log_entry = LogEntry::SetJsonB { key: key.clone(), value: value_bytes.clone() };
            if let Response::Error(e) = log_to_wal(log_entry, &self.ctx).await {
                return Err(anyhow!(e));
            }
            self.ctx.index_manager.add_key_to_indexes(&key, &row_data).await;
            let txid = self.ctx.tx_id_manager.new_txid();
            self.ctx.tx_status_manager.begin(txid);
            let new_version = crate::types::VersionedValue { value: DbValue::JsonB(value_bytes), creator_txid: txid, expirer_txid: 0 };
            let mut version_chain_lock = self.ctx.db.entry(key.clone()).or_default();
            let mut version_chain = version_chain_lock.value_mut().write().await;
            version_chain.push(new_version);
            self.ctx.tx_status_manager.commit(txid);
            inserted_rows.push(row_data);
        }
        Ok(inserted_rows)
    }
}