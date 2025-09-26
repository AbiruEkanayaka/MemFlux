use anyhow::{bail, Result};
use serde_json::{json, Value};
use std::collections::{HashSet, VecDeque};
use std::str;
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};
use crate::config::DurabilityLevel;
use crate::indexing::Index;
use crate::memory;
use crate::transaction::{Transaction, TransactionHandle};
use crate::types::*;

pub async fn apply_log_entry(entry: &LogEntry, ctx: &AppContext) -> Result<()> {
    match entry {
        LogEntry::SetBytes { key, value } => {
            let mut old_size = 0;
            if let Some(entry) = ctx.db.get(key) {
                old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
            }

            if ctx.memory.is_enabled() {
                let new_size = key.len() as u64 + value.len() as u64;
                let needed = new_size.saturating_sub(old_size);
                if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
                    bail!(e);
                }
            }

            ctx.db.insert(key.clone(), DbValue::Bytes(value.clone()));
            let new_size = key.len() as u64 + value.len() as u64;
            ctx.memory.decrease_memory(old_size);
            ctx.memory.increase_memory(new_size);
            if ctx.memory.is_enabled() {
                ctx.memory.track_access(key).await;
            }
        }
        LogEntry::SetJsonB { key, value } => {
            let (old_size, old_val_for_index) = if let Some(entry) = ctx.db.get(key) {
                let size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
                let val = match entry.value() {
                    DbValue::Json(v) => v.clone(),
                    DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or(json!({})),
                    _ => json!({}),
                };
                (size, val)
            } else {
                (0, json!({}))
            };

            if ctx.memory.is_enabled() {
                let new_size = key.len() as u64 + value.len() as u64;
                let needed = new_size.saturating_sub(old_size);
                if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
                    bail!(e);
                }
            }

            ctx.db.insert(key.clone(), DbValue::JsonB(value.clone()));

            let new_size = key.len() as u64 + value.len() as u64;
            ctx.memory.decrease_memory(old_size);
            ctx.memory.increase_memory(new_size);
            if ctx.memory.is_enabled() {
                ctx.memory.track_access(key).await;
            }

            let new_val: serde_json::Value = serde_json::from_slice(value)?;
            ctx.index_manager
                .remove_key_from_indexes(key, &old_val_for_index)
                .await;
            ctx.index_manager.add_key_to_indexes(key, &new_val).await;
        }
        LogEntry::Delete { key } => {
            let mut old_size = 0;
            if let Some(entry) = ctx.db.get(key) {
                old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
            }

            let old_value: Option<Value> = ctx.db.get(key).and_then(|entry| match entry.value() {
                DbValue::Json(v) => Some(v.clone()),
                DbValue::JsonB(b) => serde_json::from_slice(b).ok(),
                _ => None,
            });

            if ctx.db.remove(key).is_some() {
                ctx.memory.decrease_memory(old_size);
                if ctx.memory.is_enabled() {
                    ctx.memory.forget_key(key).await;
                }
                if let Some(ref old_val) = old_value {
                    ctx.index_manager
                        .remove_key_from_indexes(key, old_val)
                        .await;
                }
            }
        }
        LogEntry::JsonSet { path, value } => {
            let mut parts = path.splitn(2, '.');
            let key = match parts.next() {
                Some(k) if !k.is_empty() => k.to_string(),
                _ => bail!("Invalid path for JsonSet: missing key"),
            };
            let inner_path = parts.next().unwrap_or("");

            let (old_size, mut current_val, old_val_for_index) = if let Some(entry) = ctx.db.get(&key) {
                let size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
                let val = match entry.value() {
                    DbValue::Json(v) => v.clone(),
                    DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or(json!({})),
                    _ => json!({}),
                };
                (size, val.clone(), val)
            } else {
                (0, json!({}), json!({}))
            };

            let value: serde_json::Value = serde_json::from_str(value)?;

            let pointer = json_path_to_pointer(inner_path);
            if pointer.is_empty() {
                current_val = value;
            } else if let Some(target) = current_val.pointer_mut(&pointer) {
                *target = value;
            } else {
                let mut current = &mut current_val;
                for part in inner_path.split('.') {
                    if part.is_empty() { continue; }
                    if current.is_object() {
                        current = current.as_object_mut().unwrap().entry(part).or_insert(json!({}));
                    }
                    else {
                        bail!("Path creation failed: part is not an object");
                    }
                }
                *current = value;
            }

            let new_value_bytes = serde_json::to_vec(&current_val)?;
            let new_size = key.len() as u64 + new_value_bytes.len() as u64;

            if ctx.memory.is_enabled() {
                let needed = new_size.saturating_sub(old_size);
                if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
                    bail!(e);
                }
            }

            ctx.db.insert(key.clone(), DbValue::JsonB(new_value_bytes));
            ctx.memory.decrease_memory(old_size);
            ctx.memory.increase_memory(new_size);
            if ctx.memory.is_enabled() {
                ctx.memory.track_access(&key).await;
            }

            ctx.index_manager
                .remove_key_from_indexes(&key, &old_val_for_index)
                .await;
            ctx.index_manager.add_key_to_indexes(&key, &current_val).await;
        }
        LogEntry::JsonDelete { path } => {
            let _ = apply_json_delete_to_db(&ctx.db, path);
        }
        LogEntry::LPush { key, values } => {
            let mut old_size = 0;
            if let Some(entry) = ctx.db.get(key) {
                old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
            }

            if ctx.memory.is_enabled() {
                let added_size: u64 = values.iter().map(|v| v.len() as u64).sum();
                let needed = if old_size == 0 {
                    key.len() as u64 + added_size
                } else {
                    added_size
                };
                if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
                    bail!(e);
                }
            }

            let entry = ctx
                .db
                .entry(key.clone())
                .or_insert_with(|| DbValue::List(RwLock::new(VecDeque::new())));
            if let DbValue::List(list_lock) = entry.value() {
                let mut list = list_lock.write().await;
                for v in values {
                    list.push_front(v.clone());
                }
                let new_size =
                    key.len() as u64 + list.iter().map(|v| v.len() as u64).sum::<u64>();
                ctx.memory.decrease_memory(old_size);
                ctx.memory.increase_memory(new_size);
                if ctx.memory.is_enabled() {
                    ctx.memory.track_access(key).await;
                }
            }
        }
        LogEntry::RPush { key, values } => {
            let mut old_size = 0;
            if let Some(entry) = ctx.db.get(key) {
                old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
            }

            if ctx.memory.is_enabled() {
                let added_size: u64 = values.iter().map(|v| v.len() as u64).sum();
                let needed = if old_size == 0 {
                    key.len() as u64 + added_size
                } else {
                    added_size
                };
                if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
                    bail!(e);
                }
            }

            let entry = ctx
                .db
                .entry(key.clone())
                .or_insert_with(|| DbValue::List(RwLock::new(VecDeque::new())));
            if let DbValue::List(list_lock) = entry.value() {
                let mut list = list_lock.write().await;
                for v in values {
                    list.push_back(v.clone());
                }
                let new_size =
                    key.len() as u64 + list.iter().map(|v| v.len() as u64).sum::<u64>();
                ctx.memory.decrease_memory(old_size);
                ctx.memory.increase_memory(new_size);
                if ctx.memory.is_enabled() {
                    ctx.memory.track_access(key).await;
                }
            }
        }
        LogEntry::LPop { key, count } => {
            if let Some(mut entry) = ctx.db.get_mut(key) {
                if let DbValue::List(list_lock) = entry.value_mut() {
                    let mut list = list_lock.write().await;
                    let mut old_size = 0;
                    if let Some(entry) = ctx.db.get(key) {
                        old_size =
                            key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
                    }

                    for _ in 0..*count {
                        if list.pop_front().is_none() {
                            break;
                        }
                    }

                    let new_size =
                        key.len() as u64 + list.iter().map(|v| v.len() as u64).sum::<u64>();
                    ctx.memory.decrease_memory(old_size);
                    ctx.memory.increase_memory(new_size);
                    if ctx.memory.is_enabled() {
                        ctx.memory.track_access(key).await;
                    }
                }
            }
        }
        LogEntry::RPop { key, count } => {
            if let Some(mut entry) = ctx.db.get_mut(key) {
                if let DbValue::List(list_lock) = entry.value_mut() {
                    let mut list = list_lock.write().await;
                    let mut old_size = 0;
                    if let Some(entry) = ctx.db.get(key) {
                        old_size =
                            key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
                    }

                    for _ in 0..*count {
                        if list.pop_back().is_none() {
                            break;
                        }
                    }

                    let new_size =
                        key.len() as u64 + list.iter().map(|v| v.len() as u64).sum::<u64>();
                    ctx.memory.decrease_memory(old_size);
                    ctx.memory.increase_memory(new_size);
                    if ctx.memory.is_enabled() {
                        ctx.memory.track_access(key).await;
                    }
                }
            }
        }
        LogEntry::SAdd { key, members } => {
            let mut old_size = 0;
            if let Some(entry) = ctx.db.get(key) {
                old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
            }

            if ctx.memory.is_enabled() {
                let added_size: u64 = members.iter().map(|v| v.len() as u64).sum();
                let needed = if old_size == 0 {
                    key.len() as u64 + added_size
                } else {
                    added_size
                };
                if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
                    bail!(e);
                }
            }

            let entry = ctx
                .db
                .entry(key.clone())
                .or_insert_with(|| DbValue::Set(RwLock::new(HashSet::new())));
            if let DbValue::Set(set_lock) = entry.value() {
                let mut set = set_lock.write().await;
                for m in members {
                    set.insert(m.clone());
                }
                let new_size =
                    key.len() as u64 + set.iter().map(|v| v.len() as u64).sum::<u64>();
                ctx.memory.decrease_memory(old_size);
                ctx.memory.increase_memory(new_size);
                if ctx.memory.is_enabled() {
                    ctx.memory.track_access(key).await;
                }
            }
        }
        LogEntry::SRem { key, members } => {
            if let Some(mut entry) = ctx.db.get_mut(key) {
                if let DbValue::Set(set_lock) = entry.value_mut() {
                    let mut set = set_lock.write().await;
                    let mut old_size = 0;
                    if let Some(entry) = ctx.db.get(key) {
                        old_size =
                            key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
                    }

                    for m in members {
                        set.remove(m);
                    }

                    let new_size =
                        key.len() as u64 + set.iter().map(|v| v.len() as u64).sum::<u64>();
                    ctx.memory.decrease_memory(old_size);
                    ctx.memory.increase_memory(new_size);
                    if ctx.memory.is_enabled() {
                        ctx.memory.track_access(key).await;
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

pub async fn process_command(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    match command.name.as_str() {
        "PING" => Response::SimpleString("PONG".to_string()),
        "AUTH" => Response::Error("AUTH can only be used as the first command.".to_string()),
        "GET" => handle_get(command, ctx, transaction_handle).await,
        "SET" => handle_set(command, ctx, transaction_handle).await,
        "DELETE" => handle_delete(command, ctx, transaction_handle).await,
        "JSON.SET" => handle_json_set(command, ctx, transaction_handle).await,
        "JSON.GET" => handle_json_get(command, ctx, transaction_handle).await,
        "JSON.DEL" => handle_json_del(command, ctx, transaction_handle).await,
        "LPUSH" => handle_lpush(command, ctx, transaction_handle).await,
        "RPUSH" => handle_rpush(command, ctx, transaction_handle).await,
        "LPOP" => handle_lpop(command, ctx, transaction_handle).await,
        "RPOP" => handle_rpop(command, ctx, transaction_handle).await,
        "LLEN" => handle_llen(command, ctx, transaction_handle).await,
        "LRANGE" => handle_lrange(command, ctx, transaction_handle).await,
        "SADD" => handle_sadd(command, ctx, transaction_handle).await,
        "SREM" => handle_srem(command, ctx, transaction_handle).await,
        "SMEMBERS" => handle_smembers(command, ctx, transaction_handle).await,
        "SCARD" => handle_scard(command, ctx, transaction_handle).await,
        "SISMEMBER" => handle_sismember(command, ctx, transaction_handle).await,
        "KEYS" => handle_keys(command, ctx, transaction_handle).await,
        "FLUSHDB" => handle_flushdb(command, ctx).await,
        "SAVE" => handle_save(command, ctx).await,
        "MEMUSAGE" => handle_memory_usage(command, ctx).await,
        "CREATEINDEX" => handle_createindex(command, ctx).await,
        "IDX.CREATE" => handle_idx_create(command, ctx).await,
        "IDX.DROP" => handle_idx_drop(command, ctx).await,
        "IDX.LIST" => handle_idx_list(command, ctx).await,
        "BEGIN" => handle_begin(command, ctx, transaction_handle).await,
        "COMMIT" => handle_commit(command, ctx, transaction_handle).await,
        "ROLLBACK" => handle_rollback(command, ctx, transaction_handle).await,
        _ => Response::Error(format!("Unknown command: {}", command.name)),
    }
}

async fn log_to_wal(log_entry: LogEntry, ctx: &AppContext) -> Response {
    let (ack_tx, ack_rx) = oneshot::channel();
    let log_req = LogRequest {
        entry: log_entry,
        ack: ack_tx,
    };
    if ctx
        .logger
        .send(PersistenceRequest::Log(log_req))
        .await
        .is_err()
    {
        return Response::Error("Persistence engine is down".to_string());
    }
    match ack_rx.await {
        Ok(Ok(())) => Response::Ok,
        Ok(Err(e)) => Response::Error(format!("WAL write error: {}", e)),
        Err(_) => Response::Error("Persistence engine dropped ACK channel".to_string()),
    }
}

async fn db_value_to_response(value: &DbValue, key: &str, ctx: &AppContext) -> Response {
    if ctx.memory.is_enabled() {
        ctx.memory.track_access(key).await;
    }
    match value {
        DbValue::Bytes(b) => Response::Bytes(b.clone()),
        DbValue::Json(v) => Response::Bytes(v.to_string().into_bytes()),
        DbValue::List(_) => Response::Error("WRONGTYPE Operation against a list".to_string()),
        DbValue::Set(_) => Response::Error("WRONGTYPE Operation against a set".to_string()),
        DbValue::JsonB(b) => Response::Bytes(b.clone()),
        DbValue::Array(_) => {
            Response::Error("WRONGTYPE Operation against an array value".to_string())
        }
    }
}

async fn handle_get(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 2 {
        return Response::Error("GET requires one argument".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };

    let tx_guard = transaction_handle.read().await;
    if let Some(tx) = tx_guard.as_ref() {
        // 1. Check writes cache (read-your-writes)
        if let Some(entry) = tx.writes.get(&key) {
            return match entry.value() {
                Some(val) => db_value_to_response(val, &key, ctx).await,
                None => Response::Nil, // Deleted in this transaction
            };
        }

        // 2. Check read cache (repeatable reads)
        if let Some(entry) = tx.read_cache.get(&key) {
            return match entry.value() {
                Some(val) => db_value_to_response(val, &key, ctx).await,
                None => Response::Nil, // Known to not exist at first read
            };
        }

        // 3. Read from main DB and populate read cache
        let db_entry = ctx.db.get(&key);
        let value_clone = db_entry.as_ref().map(|e| e.value().clone());
        let response = match &value_clone {
            Some(val) => db_value_to_response(val, &key, ctx).await,
            None => Response::Nil,
        };
        // Drop the lock on the main DB before inserting into the cache
        drop(db_entry);
        tx.read_cache.insert(key, value_clone);
        return response;
    }
    drop(tx_guard);

    // No transaction, proceed as before
    match ctx.db.get(&key) {
        Some(entry) => {
            if ctx.memory.is_enabled() {
                ctx.memory.track_access(&key).await;
            }
            match entry.value() {
                DbValue::Bytes(b) => Response::Bytes(b.clone()),
                DbValue::Json(v) => Response::Bytes(v.to_string().into_bytes()),
                DbValue::List(_) => Response::Error("WRONGTYPE Operation against a list".to_string()),
                DbValue::Set(_) => Response::Error("WRONGTYPE Operation against a set".to_string()),
                DbValue::JsonB(b) => Response::Bytes(b.clone()),
                DbValue::Array(_) => {
                    Response::Error("WRONGTYPE Operation against an array value".to_string())
                }
            }
        }
        None => Response::Nil,
    }
}

async fn handle_set(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 3 {
        return Response::Error("SET requires two arguments".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };
    let value = command.args[2].clone();

    let log_entry = LogEntry::SetBytes {
        key: key.clone(),
        value: value.clone(),
    };

    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.as_mut() {
        tx.log_entries.push(log_entry);
        tx.writes
            .insert(key.clone(), Some(DbValue::Bytes(value.clone())));
        return Response::Ok;
    }
    drop(tx_guard);

    let mut old_size = 0;
    if let Some(entry) = ctx.db.get(&key) {
        old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
    }

    if ctx.memory.is_enabled() {
        let new_size = key.len() as u64 + value.len() as u64;
        let needed = new_size.saturating_sub(old_size);
        if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
            return Response::Error(e.to_string());
        }
    }

    let ack_response = log_to_wal(log_entry, ctx).await;
    if let Response::Ok = ack_response {
        ctx.db.insert(key.clone(), DbValue::Bytes(value.clone()));
        let new_size = key.len() as u64 + value.len() as u64;
        ctx.memory.decrease_memory(old_size);
        ctx.memory.increase_memory(new_size);
        if ctx.memory.is_enabled() {
            ctx.memory.track_access(&key).await;
        }
    }
    ack_response
}

async fn handle_delete(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() < 2 {
        return Response::Error("DELETE requires at least one argument".to_string());
    }

    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.as_mut() {
        let mut deleted_count = 0;
        for key_bytes in &command.args[1..] {
            if let Ok(key) = String::from_utf8(key_bytes.clone()) {
                tx.log_entries.push(LogEntry::Delete { key: key.clone() });
                tx.writes.insert(key, None);
                deleted_count += 1;
            }
        }
        return Response::Integer(deleted_count);
    }
    drop(tx_guard);

    let mut deleted_count = 0;
    for key_bytes in &command.args[1..] {
        let key = match String::from_utf8(key_bytes.clone()) {
            Ok(k) => k,
            Err(_) => continue,
        };

        let mut old_size = 0;
        if let Some(entry) = ctx.db.get(&key) {
            old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
        }

        let old_value: Option<Value> = ctx.db.get(&key).and_then(|entry| match entry.value() {
            DbValue::Json(v) => Some(v.clone()),
            _ => None,
        });

        let log_entry = LogEntry::Delete { key: key.clone() };
        let ack_response = log_to_wal(log_entry, ctx).await;

        if let Response::Ok = ack_response {
            if ctx.db.remove(&key).is_some() {
                ctx.memory.decrease_memory(old_size);
                if ctx.memory.is_enabled() {
                    ctx.memory.forget_key(&key).await;
                }
                if let Some(ref old_val) = old_value {
                    ctx.index_manager
                        .remove_key_from_indexes(&key, old_val)
                        .await;
                }
                deleted_count += 1;
            }
        }
    }
    Response::Integer(deleted_count)
}

pub fn json_path_to_pointer(path: &str) -> String {
    if path == "." || path.is_empty() {
        return "".to_string();
    }
    let p = path.strip_prefix('.').unwrap_or(path);
    format!("/{}", p.replace('.', "/"))
}

async fn handle_json_set(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    let (full_path, value_str) = if command.args.len() == 3 {
        // This is for `JSON.SET <key or key.path> <value>`
        (
            String::from_utf8_lossy(&command.args[1]).to_string(),
            String::from_utf8_lossy(&command.args[2]).to_string(),
        )
    } else if command.args.len() == 4 {
        // This is for `JSON.SET <key> <path> <value>`
        let key = String::from_utf8_lossy(&command.args[1]);
        let path = String::from_utf8_lossy(&command.args[2]);
        let full_path = if path == "." {
            key.to_string()
        } else {
            format!("{}.{}", key, path)
        };
        (
            full_path,
            String::from_utf8_lossy(&command.args[3]).to_string(),
        )
    } else {
        return Response::Error(format!("JSON.SET requires 2 or 3 arguments, got {}. Args: {:?}", command.args.len() - 1, command.args.iter().skip(1).map(|a| String::from_utf8_lossy(a).to_string()).collect::<Vec<String>>()));
    };

    let mut parts = full_path.splitn(2, '.');
    let key = match parts.next() {
        Some(k) if !k.is_empty() => k.to_string(),
        _ => return Response::Error("Invalid path: missing key".to_string()),
    };
    let path = parts.next().unwrap_or("");

    // --- TRANSACTION LOGIC FIRST ---
    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.as_mut() {
        let log_entry = LogEntry::JsonSet {
            path: full_path.clone(),
            value: value_str.to_string(),
        };
        tx.log_entries.push(log_entry);

        // Apply change to the transaction's write set for read-your-writes
        let value: Value = match serde_json::from_str(&value_str) {
            Ok(v) => v,
            Err(_) => return Response::Error("Value is not valid JSON".to_string()),
        };

        let current_db_val = tx
            .writes
            .get(&key)
            .and_then(|v| v.value().clone())
            .or_else(|| tx.read_cache.get(&key).and_then(|v| v.value().clone()))
            .or_else(|| ctx.db.get(&key).map(|v| v.value().clone()));

        let mut current_val = match current_db_val {
            Some(DbValue::Json(v)) => v.clone(),
            Some(DbValue::JsonB(b)) => serde_json::from_slice(&b).unwrap_or(json!({})),
            _ => json!({}),
        };

        let pointer = if path == "." || path.is_empty() { "".to_string() } else { json_path_to_pointer(&path) };
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
                }
                else {
                    return Response::Error("Path creation failed: part is not an object".to_string());
                }
            }
            *current = value;
        }

        let new_value_bytes = match serde_json::to_vec(&current_val) {
            Ok(b) => b,
            Err(_) => return Response::Error("Failed to serialize new JSON value".to_string()),
        };

        tx.writes
            .insert(key, Some(DbValue::JsonB(new_value_bytes)));

        return Response::Ok;
    }
    drop(tx_guard);
    // --- END TRANSACTION LOGIC ---

    let value: Value = match serde_json::from_str(&value_str) {
        Ok(v) => v,
        Err(_) => return Response::Error("Value is not valid JSON".to_string()),
    };

    let (old_size, mut current_val, old_val_for_index) = {
        let old_db_value = ctx.db.get(&key);
        let old_size = if let Some(entry) = &old_db_value {
            key.len() as u64 + memory::estimate_db_value_size(entry.value()).await
        } else {
            0
        };

        let current_val = match old_db_value.as_deref() {
            Some(DbValue::Json(v)) => v.clone(),
            Some(DbValue::JsonB(b)) => serde_json::from_slice(&b).unwrap_or(json!({})),
            Some(_) => return Response::Error("WRONGTYPE Operation against a non-JSON value".to_string()),
            None => json!({}),
        };
        (old_size, current_val.clone(), current_val.clone())
    };

    let pointer = if path == "." || path.is_empty() { "".to_string() } else { json_path_to_pointer(&path) };
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
            }
            else {
                return Response::Error("Path creation failed: part is not an object".to_string());
            }
        }
        *current = value;
    }

    let new_value_bytes = match serde_json::to_vec(&current_val) {
        Ok(b) => b,
        Err(_) => return Response::Error("Failed to serialize new JSON value".to_string()),
    };

    let log_entry = LogEntry::SetJsonB {
        key: key.clone(),
        value: new_value_bytes.clone(),
    };

    if ctx.memory.is_enabled() {
        let new_size = key.len() as u64 + new_value_bytes.len() as u64;
        let needed = new_size.saturating_sub(old_size);
        if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
            return Response::Error(e.to_string());
        }
    }

    let ack_response = log_to_wal(log_entry, ctx).await;

    if let Response::Ok = ack_response {
        ctx.db.insert(key.clone(), DbValue::JsonB(new_value_bytes.clone()));

        let new_size = key.len() as u64 + new_value_bytes.len() as u64;
        ctx.memory.decrease_memory(old_size);
        ctx.memory.increase_memory(new_size);
        if ctx.memory.is_enabled() {
            ctx.memory.track_access(&key).await;
        }

        ctx.index_manager
            .remove_key_from_indexes(&key, &old_val_for_index)
            .await;
        ctx.index_manager
            .add_key_to_indexes(&key, &current_val)
            .await;
    }
    ack_response
}

async fn handle_json_get(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 2 {
        return Response::Error("JSON.GET requires a key/path".to_string());
    }
    let full_path = match String::from_utf8(command.args[1].clone()) {
        Ok(p) => p,
        Err(_) => return Response::Error("Invalid path".to_string()),
    };

    let mut parts = full_path.splitn(2, '.');
    let key = match parts.next() {
        Some(k) if !k.is_empty() => k,
        _ => return Response::Error("Invalid path: missing key".to_string()),
    };
    let inner_path = parts.next().unwrap_or("");

    let tx_guard = transaction_handle.read().await;
    if let Some(tx) = tx_guard.as_ref() {
        // Transactional read
        let entry_opt = tx.writes.get(key).map(|e| e.value().clone()).or_else(|| {
            tx.read_cache.get(key).map(|e| e.value().clone()).or_else(|| {
                let db_entry = ctx.db.get(key);
                let value_clone = db_entry.as_ref().map(|e| e.value().clone());
                drop(db_entry);
                tx.read_cache.insert(key.to_string(), value_clone.clone());
                Some(value_clone)
            })
        });

        return match entry_opt {
            Some(Some(db_value)) => {
                json_db_value_to_response(&db_value, inner_path, key, ctx).await
            }
            _ => Response::Nil,
        };
    }
    drop(tx_guard);

    // Non-transactional read
    match ctx.db.get(key) {
        Some(entry) => json_db_value_to_response(entry.value(), inner_path, key, ctx).await,
        None => Response::Nil,
    }
}

async fn json_db_value_to_response(
    db_value: &DbValue,
    inner_path: &str,
    key: &str,
    ctx: &AppContext,
) -> Response {
    if ctx.memory.is_enabled() {
        ctx.memory.track_access(key).await;
    }
    match db_value {
        DbValue::Json(v) => {
            let pointer = json_path_to_pointer(inner_path);
            match v.pointer(&pointer) {
                Some(val) => Response::Bytes(val.to_string().into_bytes()),
                None => Response::Nil,
            }
        }
        DbValue::JsonB(b) => {
            let v: Value = match serde_json::from_slice(&b) {
                Ok(val) => val,
                Err(_) => return Response::Error("Could not parse JSONB value".to_string()),
            };
            if inner_path.is_empty() || inner_path == "." {
                return Response::Bytes(v.to_string().into_bytes());
            }
            let pointer = json_path_to_pointer(inner_path);
            match v.pointer(&pointer) {
                Some(val) => Response::Bytes(val.to_string().into_bytes()),
                None => Response::Nil,
            }
        }
        _ => Response::Error("WRONGTYPE Operation against a non-JSON value".to_string()),
    }
}

async fn handle_json_del(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 2 {
        return Response::Error("JSON.DEL requires a key/path".to_string());
    }
    let path = match String::from_utf8(command.args[1].clone()) {
        Ok(p) => p,
        Err(_) => return Response::Error("Invalid path".to_string()),
    };

    let mut parts = path.splitn(2, '.');
    let key = match parts.next() {
        Some(k) if !k.is_empty() => k.to_string(),
        _ => return Response::Error("Invalid path: missing key".to_string()),
    };
    let inner_path = parts.next().unwrap_or("");

    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.as_mut() {
        let log_entry = if inner_path.is_empty() || inner_path == "." {
            LogEntry::Delete { key: key.clone() }
        } else {
            LogEntry::JsonDelete { path: path.clone() }
        };
        tx.log_entries.push(log_entry);

        // Apply change to the transaction's write set for read-your-writes
        if inner_path.is_empty() || inner_path == "." {
            tx.writes.insert(key.clone(), None);
        } else {
            let current_db_val = tx
                .writes
                .get(&key)
                .and_then(|v| v.value().clone())
                .or_else(|| tx.read_cache.get(&key).and_then(|v| v.value().clone()))
                .or_else(|| ctx.db.get(&key).map(|v| v.value().clone()));

            let mut current_val = match current_db_val {
                Some(DbValue::Json(v)) => v.clone(),
                Some(DbValue::JsonB(b)) => serde_json::from_slice(&b).unwrap_or(json!({})),
                _ => json!({}), // Key doesn't exist, so deletion is a no-op
            };

            let mut pointer_parts: Vec<&str> = inner_path.split('.').collect();
            let final_key = pointer_parts.pop().unwrap();
            let parent_pointer = json_path_to_pointer(&pointer_parts.join("."));
            let mut modified = false;

            if let Some(target) = current_val.pointer_mut(&parent_pointer) {
                if let Some(obj) = target.as_object_mut() {
                    if obj.remove(final_key).is_some() {
                        modified = true;
                    }
                }
            }

            if modified {
                let new_value_bytes = match serde_json::to_vec(&current_val) {
                    Ok(b) => b,
                    Err(_) => return Response::Error("Failed to serialize new JSON value".to_string()),
                };
                tx.writes
                    .insert(key.clone(), Some(DbValue::JsonB(new_value_bytes)));
            } else {
                // If not modified, we don't need to add it to the writeset
            }
        }

        return Response::Integer(1); // Assuming success, though can't confirm deletion
    }
    drop(tx_guard);

    let (old_size, mut current_val, old_val_for_index) = {
        let old_db_value = match ctx.db.get(&key) {
            Some(val) => val,
            None => return Response::Integer(0),
        };

        let old_size =
            key.len() as u64 + memory::estimate_db_value_size(old_db_value.value()).await;

        let current_val = match old_db_value.value() {
            DbValue::Json(v) => v.clone(),
            DbValue::JsonB(b) => serde_json::from_slice(&b).unwrap_or(json!({})),
            _ => return Response::Error("WRONGTYPE Operation against a non-JSON value".to_string()),
        };
        (old_size, current_val.clone(), current_val.clone())
    };

    if inner_path.is_empty() || inner_path == "." {
        let log_entry = LogEntry::Delete { key: key.clone() };
        let ack_response = log_to_wal(log_entry, ctx).await;
        if let Response::Ok = ack_response {
            ctx.db.remove(&key);
            ctx.memory.decrease_memory(old_size);
            if ctx.memory.is_enabled() {
                ctx.memory.forget_key(&key).await;
            }
            ctx.index_manager
                .remove_key_from_indexes(&key, &old_val_for_index)
                .await;
            Response::Integer(1)
        } else {
            ack_response
        }
    } else {
        let mut pointer_parts: Vec<&str> = inner_path.split('.').collect();
        let final_key = pointer_parts.pop().unwrap();
        let parent_pointer = json_path_to_pointer(&pointer_parts.join("."));
        let mut modified = false;

        if let Some(target) = current_val.pointer_mut(&parent_pointer) {
            if let Some(obj) = target.as_object_mut() {
                if obj.remove(final_key).is_some() {
                    modified = true;
                }
            }
        }

        if !modified {
            return Response::Integer(0);
        }

        let new_value_bytes = match serde_json::to_vec(&current_val) {
            Ok(b) => b,
            Err(_) => return Response::Error("Failed to serialize new JSON value".to_string()),
        };

        let log_entry = LogEntry::SetJsonB {
            key: key.clone(),
            value: new_value_bytes.clone(),
        };
        let ack_response = log_to_wal(log_entry, ctx).await;

        if let Response::Ok = ack_response {
            ctx.db
                .insert(key.clone(), DbValue::JsonB(new_value_bytes.clone()));
            let new_size = key.len() as u64 + new_value_bytes.len() as u64;
            ctx.memory.decrease_memory(old_size);
            ctx.memory.increase_memory(new_size);
            if ctx.memory.is_enabled() {
                ctx.memory.track_access(&key).await;
            }
            ctx.index_manager
                .remove_key_from_indexes(&key, &old_val_for_index)
                .await;
            ctx.index_manager
                .add_key_to_indexes(&key, &current_val)
                .await;
            Response::Integer(1)
        } else {
            ack_response
        }
    }
}

async fn handle_lpush(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() < 3 {
        return Response::Error("LPUSH requires a key and at least one value".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };
    let values: Vec<Vec<u8>> = command.args[2..].to_vec();

    let log_entry = LogEntry::LPush {
        key: key.clone(),
        values: values.clone(),
    };

    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.as_mut() {
        tx.log_entries.push(log_entry);

        let current_db_val = tx
            .writes
            .get(&key)
            .and_then(|v| v.value().clone())
            .or_else(|| tx.read_cache.get(&key).and_then(|v| v.value().clone()))
            .or_else(|| ctx.db.get(&key).map(|v| v.value().clone()));

        let mut current_list = match current_db_val {
            Some(DbValue::List(list_lock)) => list_lock.read().await.clone(),
            _ => VecDeque::new(),
        };

        for v in values.into_iter().rev() {
            current_list.push_front(v);
        }

        let new_len = current_list.len() as i64;
        tx.writes
            .insert(key, Some(DbValue::List(RwLock::new(current_list))));

        return Response::Integer(new_len);
    }
    drop(tx_guard);

    let mut old_size = 0;
    if let Some(entry) = ctx.db.get(&key) {
        old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
    }

    if ctx.memory.is_enabled() {
        let added_size: u64 = values.iter().map(|v| v.len() as u64).sum();
        let needed = if old_size == 0 {
            key.len() as u64 + added_size
        } else {
            added_size
        };
        if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
            return Response::Error(e.to_string());
        }
    }

    let ack_response = log_to_wal(log_entry, ctx).await;

    if let Response::Ok = ack_response {
        let entry = ctx
            .db
            .entry(key.clone())
            .or_insert_with(|| DbValue::List(RwLock::new(VecDeque::new())));
        if let DbValue::List(list_lock) = entry.value() {
            let mut list = list_lock.write().await;
            for v in values {
                list.push_front(v);
            }
            let new_size =
                key.len() as u64 + list.iter().map(|v| v.len() as u64).sum::<u64>();
            ctx.memory.decrease_memory(old_size);
            ctx.memory.increase_memory(new_size);
            if ctx.memory.is_enabled() {
                ctx.memory.track_access(&key).await;
            }
            Response::Integer(list.len() as i64)
        } else {
            Response::Error("WRONGTYPE Operation against a non-list value".to_string())
        }
    } else {
        ack_response
    }
}

async fn handle_rpush(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() < 3 {
        return Response::Error("RPUSH requires a key and at least one value".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };
    let values: Vec<Vec<u8>> = command.args[2..].to_vec();

    let log_entry = LogEntry::RPush {
        key: key.clone(),
        values: values.clone(),
    };

    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.as_mut() {
        tx.log_entries.push(log_entry);

        let current_db_val = tx
            .writes
            .get(&key)
            .and_then(|v| v.value().clone())
            .or_else(|| tx.read_cache.get(&key).and_then(|v| v.value().clone()))
            .or_else(|| ctx.db.get(&key).map(|v| v.value().clone()));

        let mut current_list = match current_db_val {
            Some(DbValue::List(list_lock)) => list_lock.read().await.clone(),
            _ => VecDeque::new(),
        };

        for v in values {
            current_list.push_back(v);
        }

        let new_len = current_list.len() as i64;
        tx.writes
            .insert(key, Some(DbValue::List(RwLock::new(current_list))));

        return Response::Integer(new_len);
    }
    drop(tx_guard);

    let mut old_size = 0;
    if let Some(entry) = ctx.db.get(&key) {
        old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
    }

    if ctx.memory.is_enabled() {
        let added_size: u64 = values.iter().map(|v| v.len() as u64).sum();
        let needed = if old_size == 0 {
            key.len() as u64 + added_size
        } else {
            added_size
        };
        if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
            return Response::Error(e.to_string());
        }
    }

    let ack_response = log_to_wal(log_entry, ctx).await;

    if let Response::Ok = ack_response {
        let entry = ctx
            .db
            .entry(key.clone())
            .or_insert_with(|| DbValue::List(RwLock::new(VecDeque::new())));
        if let DbValue::List(list_lock) = entry.value() {
            let mut list = list_lock.write().await;
            for v in values {
                list.push_back(v);
            }
            let new_size =
                key.len() as u64 + list.iter().map(|v| v.len() as u64).sum::<u64>();
            ctx.memory.decrease_memory(old_size);
            ctx.memory.increase_memory(new_size);
            if ctx.memory.is_enabled() {
                ctx.memory.track_access(&key).await;
            }
            Response::Integer(list.len() as i64)
        } else {
            Response::Error("WRONGTYPE Operation against a non-list value".to_string())
        }
    } else {
        ack_response
    }
}

async fn handle_lpop(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 2 && command.args.len() != 3 {
        return Response::Error("LPOP requires a key and an optional count".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };
    let was_count_provided = command.args.len() == 3;
    let count = if was_count_provided {
        match str::from_utf8(&command.args[2]).unwrap_or("1").parse::<usize>() {
            Ok(c) => c,
            Err(_) => return Response::Error("Invalid count".to_string()),
        }
    } else {
        1
    };

    let log_entry = LogEntry::LPop {
        key: key.clone(),
        count,
    };

    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.as_mut() {
        tx.log_entries.push(log_entry);

        let current_db_val = tx
            .writes
            .get(&key)
            .and_then(|v| v.value().clone())
            .or_else(|| tx.read_cache.get(&key).and_then(|v| v.value().clone()))
            .or_else(|| ctx.db.get(&key).map(|v| v.value().clone()));

        let mut current_list = match current_db_val {
            Some(DbValue::List(list_lock)) => list_lock.read().await.clone(),
            _ => VecDeque::new(),
        };

        let mut popped = Vec::new();
        for _ in 0..count {
            if let Some(val) = current_list.pop_front() {
                popped.push(val);
            } else {
                break;
            }
        }

        tx.writes
            .insert(key, Some(DbValue::List(RwLock::new(current_list))));

        if popped.is_empty() {
            return Response::Nil;
        } else if !was_count_provided {
            return Response::Bytes(popped.into_iter().next().unwrap());
        } else {
            return Response::MultiBytes(popped);
        }
    }
    drop(tx_guard);

    let mut old_size = 0;
    if let Some(entry) = ctx.db.get(&key) {
        old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
    }

    let ack_response = log_to_wal(log_entry, ctx).await;

    if let Response::Ok = ack_response {
        if let Some(mut entry) = ctx.db.get_mut(&key) {
            if let DbValue::List(list_lock) = entry.value_mut() {
                let mut list = list_lock.write().await;
                let mut popped = Vec::new();
                for _ in 0..count {
                    if let Some(val) = list.pop_front() {
                        popped.push(val);
                    } else {
                        break;
                    }
                }

                let new_size =
                    key.len() as u64 + list.iter().map(|v| v.len() as u64).sum::<u64>();
                ctx.memory.decrease_memory(old_size);
                ctx.memory.increase_memory(new_size);
                if ctx.memory.is_enabled() {
                    ctx.memory.track_access(&key).await;
                }

                if popped.is_empty() {
                    Response::Nil
                } else if !was_count_provided {
                    Response::Bytes(popped.into_iter().next().unwrap())
                } else {
                    Response::MultiBytes(popped)
                }
            } else {
                Response::Error("WRONGTYPE Operation against a non-list value".to_string())
            }
        } else {
            Response::Nil
        }
    } else {
        ack_response
    }
}

async fn handle_rpop(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 2 && command.args.len() != 3 {
        return Response::Error("RPOP requires a key and an optional count".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };
    let was_count_provided = command.args.len() == 3;
    let count = if was_count_provided {
        match str::from_utf8(&command.args[2]).unwrap_or("1").parse::<usize>() {
            Ok(c) => c,
            Err(_) => return Response::Error("Invalid count".to_string()),
        }
    } else {
        1
    };

    let log_entry = LogEntry::RPop {
        key: key.clone(),
        count,
    };

    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.as_mut() {
        tx.log_entries.push(log_entry);

        let current_db_val = tx
            .writes
            .get(&key)
            .and_then(|v| v.value().clone())
            .or_else(|| tx.read_cache.get(&key).and_then(|v| v.value().clone()))
            .or_else(|| ctx.db.get(&key).map(|v| v.value().clone()));

        let mut current_list = match current_db_val {
            Some(DbValue::List(list_lock)) => list_lock.read().await.clone(),
            _ => VecDeque::new(),
        };

        let mut popped = Vec::new();
        for _ in 0..count {
            if let Some(val) = current_list.pop_back() {
                popped.push(val);
            } else {
                break;
            }
        }

        tx.writes
            .insert(key, Some(DbValue::List(RwLock::new(current_list))));

        if popped.is_empty() {
            return Response::Nil;
        } else if !was_count_provided {
            return Response::Bytes(popped.into_iter().next().unwrap());
        } else {
            return Response::MultiBytes(popped);
        }
    }
    drop(tx_guard);

    let mut old_size = 0;
    if let Some(entry) = ctx.db.get(&key) {
        old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
    }

    let ack_response = log_to_wal(log_entry, ctx).await;

    if let Response::Ok = ack_response {
        if let Some(mut entry) = ctx.db.get_mut(&key) {
            if let DbValue::List(list_lock) = entry.value_mut() {
                let mut list = list_lock.write().await;
                let mut popped = Vec::new();
                for _ in 0..count {
                    if let Some(val) = list.pop_back() {
                        popped.push(val);
                    } else {
                        break;
                    }
                }

                let new_size =
                    key.len() as u64 + list.iter().map(|v| v.len() as u64).sum::<u64>();
                ctx.memory.decrease_memory(old_size);
                ctx.memory.increase_memory(new_size);
                if ctx.memory.is_enabled() {
                    ctx.memory.track_access(&key).await;
                }

                if popped.is_empty() {
                    Response::Nil
                } else if !was_count_provided {
                    Response::Bytes(popped.into_iter().next().unwrap())
                } else {
                    Response::MultiBytes(popped)
                }
            } else {
                Response::Error("WRONGTYPE Operation against a non-list value".to_string())
            }
        } else {
            Response::Nil
        }
    } else {
        ack_response
    }
}

async fn handle_llen(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 2 {
        return Response::Error("LLEN requires one argument".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };

    let tx_guard = transaction_handle.read().await;
    if let Some(tx) = tx_guard.as_ref() {
        let entry_opt = tx.writes.get(&key).map(|e| e.value().clone()).or_else(|| {
            tx.read_cache.get(&key).map(|e| e.value().clone()).or_else(|| {
                let db_entry = ctx.db.get(&key);
                let value_clone = db_entry.as_ref().map(|e| e.value().clone());
                drop(db_entry);
                tx.read_cache.insert(key.to_string(), value_clone.clone());
                Some(value_clone)
            })
        });

        return match entry_opt {
            Some(Some(DbValue::List(list_lock))) => {
                let list = list_lock.read().await;
                Response::Integer(list.len() as i64)
            }
            Some(Some(_)) => {
                Response::Error("WRONGTYPE Operation against a non-list value".to_string())
            }
            _ => Response::Integer(0),
        };
    }
    drop(tx_guard);

    match ctx.db.get(&key) {
        Some(entry) => {
            if ctx.memory.is_enabled() {
                ctx.memory.track_access(&key).await;
            }
            if let DbValue::List(list_lock) = entry.value() {
                let list = list_lock.read().await;
                Response::Integer(list.len() as i64)
            } else {
                Response::Error("WRONGTYPE Operation against a non-list value".to_string())
            }
        }
        None => Response::Integer(0),
    }
}

async fn handle_lrange(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 4 {
        return Response::Error("LRANGE requires three arguments".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };
    let start = match str::from_utf8(&command.args[2])
        .unwrap_or("")
        .parse::<i64>()
    {
        Ok(s) => s,
        Err(_) => return Response::Error("Invalid start index".to_string()),
    };
    let stop = match str::from_utf8(&command.args[3])
        .unwrap_or("")
        .parse::<i64>()
    {
        Ok(s) => s,
        Err(_) => return Response::Error("Invalid stop index".to_string()),
    };

    let tx_guard = transaction_handle.read().await;
    if let Some(tx) = tx_guard.as_ref() {
        let entry_opt = tx.writes.get(&key).map(|e| e.value().clone()).or_else(|| {
            tx.read_cache.get(&key).map(|e| e.value().clone()).or_else(|| {
                let db_entry = ctx.db.get(&key);
                let value_clone = db_entry.as_ref().map(|e| e.value().clone());
                drop(db_entry);
                tx.read_cache.insert(key.to_string(), value_clone.clone());
                Some(value_clone)
            })
        });

        return match entry_opt {
            Some(Some(DbValue::List(list_lock))) => {
                let list = list_lock.read().await;
                let len = list.len() as i64;
                let start = if start < 0 { len + start } else { start };
                let stop = if stop < 0 { len + stop } else { stop };
                let start = if start < 0 { 0 } else { start as usize };
                let stop = if stop < 0 { 0 } else { stop as usize };

                if start > stop || start >= list.len() {
                    return Response::MultiBytes(vec![]);
                }
                let stop = std::cmp::min(stop, list.len() - 1);

                let mut result = Vec::new();
                for i in start..=stop {
                    if let Some(val) = list.get(i) {
                        result.push(val.clone());
                    }
                }
                Response::MultiBytes(result)
            }
            Some(Some(_)) => {
                Response::Error("WRONGTYPE Operation against a non-list value".to_string())
            }
            _ => Response::MultiBytes(vec![]),
        };
    }
    drop(tx_guard);

    match ctx.db.get(&key) {
        Some(entry) => {
            if ctx.memory.is_enabled() {
                ctx.memory.track_access(&key).await;
            }
            if let DbValue::List(list_lock) = entry.value() {
                let list = list_lock.read().await;
                let len = list.len() as i64;
                let start = if start < 0 { len + start } else { start };
                let stop = if stop < 0 { len + stop } else { stop };
                let start = if start < 0 { 0 } else { start as usize };
                let stop = if stop < 0 { 0 } else { stop as usize };

                if start > stop || start >= list.len() {
                    return Response::MultiBytes(vec![]);
                }
                let stop = std::cmp::min(stop, list.len() - 1);

                let mut result = Vec::new();
                for i in start..=stop {
                    if let Some(val) = list.get(i) {
                        result.push(val.clone());
                    }
                }
                Response::MultiBytes(result)
            } else {
                Response::Error("WRONGTYPE Operation against a non-list value".to_string())
            }
        }
        None => Response::MultiBytes(vec![]),
    }
}

async fn handle_sadd(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() < 3 {
        return Response::Error("SADD requires a key and at least one member".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };
    let members: Vec<Vec<u8>> = command.args[2..].to_vec();

    let log_entry = LogEntry::SAdd {
        key: key.clone(),
        members: members.clone(),
    };

    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.as_mut() {
        tx.log_entries.push(log_entry);

        let current_db_val = tx
            .writes
            .get(&key)
            .and_then(|v| v.value().clone())
            .or_else(|| tx.read_cache.get(&key).and_then(|v| v.value().clone()))
            .or_else(|| ctx.db.get(&key).map(|v| v.value().clone()));

        let mut current_set = match current_db_val {
            Some(DbValue::Set(set_lock)) => set_lock.read().await.clone(),
            _ => HashSet::new(),
        };

        let mut added_count = 0;
        for m in members {
            if current_set.insert(m) {
                added_count += 1;
            }
        }

        tx.writes
            .insert(key, Some(DbValue::Set(RwLock::new(current_set))));

        return Response::Integer(added_count);
    }
    drop(tx_guard);

    let mut old_size = 0;
    if let Some(entry) = ctx.db.get(&key) {
        old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
    }

    if ctx.memory.is_enabled() {
        let added_size: u64 = members.iter().map(|v| v.len() as u64).sum();
        let needed = if old_size == 0 {
            key.len() as u64 + added_size
        } else {
            added_size
        };
        if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
            return Response::Error(e.to_string());
        }
    }

    let ack_response = log_to_wal(log_entry, ctx).await;

    if let Response::Ok = ack_response {
        let entry = ctx
            .db
            .entry(key.clone())
            .or_insert_with(|| DbValue::Set(RwLock::new(HashSet::new())));
        if let DbValue::Set(set_lock) = entry.value() {
            let mut set = set_lock.write().await;
            let mut added_count = 0;
            for m in members {
                if set.insert(m) {
                    added_count += 1;
                }
            }
            let new_size =
                key.len() as u64 + set.iter().map(|v| v.len() as u64).sum::<u64>();
            ctx.memory.decrease_memory(old_size);
            ctx.memory.increase_memory(new_size);
            if ctx.memory.is_enabled() {
                ctx.memory.track_access(&key).await;
            }
            Response::Integer(added_count)
        } else {
            Response::Error("WRONGTYPE Operation against a non-set value".to_string())
        }
    } else {
        ack_response
    }
}

async fn handle_srem(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() < 3 {
        return Response::Error("SREM requires a key and at least one member".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };
    let members: Vec<Vec<u8>> = command.args[2..].to_vec();

    let log_entry = LogEntry::SRem {
        key: key.clone(),
        members: members.clone(),
    };

    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.as_mut() {
        tx.log_entries.push(log_entry);

        let current_db_val = tx
            .writes
            .get(&key)
            .and_then(|v| v.value().clone())
            .or_else(|| tx.read_cache.get(&key).and_then(|v| v.value().clone()))
            .or_else(|| ctx.db.get(&key).map(|v| v.value().clone()));

        let mut current_set = match current_db_val {
            Some(DbValue::Set(set_lock)) => set_lock.read().await.clone(),
            _ => HashSet::new(),
        };

        let mut removed_count = 0;
        for m in members {
            if current_set.remove(&m) {
                removed_count += 1;
            }
        }

        tx.writes
            .insert(key, Some(DbValue::Set(RwLock::new(current_set))));

        return Response::Integer(removed_count);
    }
    drop(tx_guard);

    let mut old_size = 0;
    if let Some(entry) = ctx.db.get(&key) {
        old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
    }

    let ack_response = log_to_wal(log_entry, ctx).await;

    if let Response::Ok = ack_response {
        if let Some(mut entry) = ctx.db.get_mut(&key) {
            if let DbValue::Set(set_lock) = entry.value_mut() {
                let mut set = set_lock.write().await;
                let mut removed_count = 0;
                for m in members {
                    if set.remove(&m) {
                        removed_count += 1;
                    }
                }

                let new_size =
                    key.len() as u64 + set.iter().map(|v| v.len() as u64).sum::<u64>();
                ctx.memory.decrease_memory(old_size);
                ctx.memory.increase_memory(new_size);
                if ctx.memory.is_enabled() {
                    ctx.memory.track_access(&key).await;
                }

                Response::Integer(removed_count)
            } else {
                Response::Error("WRONGTYPE Operation against a non-set value".to_string())
            }
        } else {
            Response::Integer(0)
        }
    } else {
        ack_response
    }
}

async fn handle_smembers(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 2 {
        return Response::Error("SMEMBERS requires one argument".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };

    let tx_guard = transaction_handle.read().await;
    if let Some(tx) = tx_guard.as_ref() {
        let entry_opt = tx.writes.get(&key).map(|e| e.value().clone()).or_else(|| {
            tx.read_cache.get(&key).map(|e| e.value().clone()).or_else(|| {
                let db_entry = ctx.db.get(&key);
                let value_clone = db_entry.as_ref().map(|e| e.value().clone());
                drop(db_entry);
                tx.read_cache.insert(key.to_string(), value_clone.clone());
                Some(value_clone)
            })
        });

        return match entry_opt {
            Some(Some(DbValue::Set(set_lock))) => {
                let set = set_lock.read().await;
                let members: Vec<Vec<u8>> = set.iter().cloned().collect();
                Response::MultiBytes(members)
            }
            Some(Some(_)) => {
                Response::Error("WRONGTYPE Operation against a non-set value".to_string())
            }
            _ => Response::MultiBytes(vec![]),
        };
    }
    drop(tx_guard);

    match ctx.db.get(&key) {
        Some(entry) => {
            if ctx.memory.is_enabled() {
                ctx.memory.track_access(&key).await;
            }
            if let DbValue::Set(set_lock) = entry.value() {
                let set = set_lock.read().await;
                let members: Vec<Vec<u8>> = set.iter().cloned().collect();
                Response::MultiBytes(members)
            } else {
                Response::Error("WRONGTYPE Operation against a non-set value".to_string())
            }
        }
        None => Response::MultiBytes(vec![]),
    }
}

async fn handle_scard(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 2 {
        return Response::Error("SCARD requires one argument".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };

    let tx_guard = transaction_handle.read().await;
    if let Some(tx) = tx_guard.as_ref() {
        let entry_opt = tx.writes.get(&key).map(|e| e.value().clone()).or_else(|| {
            tx.read_cache.get(&key).map(|e| e.value().clone()).or_else(|| {
                let db_entry = ctx.db.get(&key);
                let value_clone = db_entry.as_ref().map(|e| e.value().clone());
                drop(db_entry);
                tx.read_cache.insert(key.to_string(), value_clone.clone());
                Some(value_clone)
            })
        });

        return match entry_opt {
            Some(Some(DbValue::Set(set_lock))) => {
                let set = set_lock.read().await;
                Response::Integer(set.len() as i64)
            }
            Some(Some(_)) => {
                Response::Error("WRONGTYPE Operation against a non-set value".to_string())
            }
            _ => Response::Integer(0),
        };
    }
    drop(tx_guard);

    match ctx.db.get(&key) {
        Some(entry) => {
            if ctx.memory.is_enabled() {
                ctx.memory.track_access(&key).await;
            }
            if let DbValue::Set(set_lock) = entry.value() {
                let set = set_lock.read().await;
                Response::Integer(set.len() as i64)
            } else {
                Response::Error("WRONGTYPE Operation against a non-set value".to_string())
            }
        }
        None => Response::Integer(0),
    }
}

async fn handle_sismember(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 3 {
        return Response::Error("SISMEMBER requires a key and a member".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };
    let member = &command.args[2];

    let tx_guard = transaction_handle.read().await;
    if let Some(tx) = tx_guard.as_ref() {
        let entry_opt = tx.writes.get(&key).map(|e| e.value().clone()).or_else(|| {
            tx.read_cache.get(&key).map(|e| e.value().clone()).or_else(|| {
                let db_entry = ctx.db.get(&key);
                let value_clone = db_entry.as_ref().map(|e| e.value().clone());
                drop(db_entry);
                tx.read_cache.insert(key.to_string(), value_clone.clone());
                Some(value_clone)
            })
        });

        return match entry_opt {
            Some(Some(DbValue::Set(set_lock))) => {
                let set = set_lock.read().await;
                if set.contains(member) {
                    Response::Integer(1)
                } else {
                    Response::Integer(0)
                }
            }
            Some(Some(_)) => {
                Response::Error("WRONGTYPE Operation against a non-set value".to_string())
            }
            _ => Response::Integer(0),
        };
    }
    drop(tx_guard);

    match ctx.db.get(&key) {
        Some(entry) => {
            if ctx.memory.is_enabled() {
                ctx.memory.track_access(&key).await;
            }
            if let DbValue::Set(set_lock) = entry.value() {
                let set = set_lock.read().await;
                if set.contains(member) {
                    Response::Integer(1)
                } else {
                    Response::Integer(0)
                }
            } else {
                Response::Error("WRONGTYPE Operation against a non-set value".to_string())
            }
        }
        None => Response::Integer(0),
    }
}

async fn handle_keys(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 2 {
        return Response::Error("KEYS requires one argument (pattern)".to_string());
    }
    let pattern = match String::from_utf8(command.args[1].clone()) {
        Ok(p) => p,
        Err(_) => return Response::Error("Invalid pattern".to_string()),
    };
    // This is a simple glob-style pattern, not regex
    let pattern = pattern.replace('*', ".*").replace('?', ".");
    let re = match regex::Regex::new(&format!("^{}$", pattern)) {
        Ok(r) => r,
        Err(_) => return Response::Error("Invalid pattern syntax".to_string()),
    };

    let tx_guard = transaction_handle.read().await;
    if let Some(tx) = tx_guard.as_ref() {
        // In a transaction, KEYS should operate on a merged view of the main DB and the transaction's writes.
        // This is complex to do efficiently. For now, we'll merge the keysets.
        let mut keys: HashSet<String> = ctx
            .db
            .iter()
            .filter(|entry| re.is_match(entry.key()))
            .map(|entry| entry.key().clone())
            .collect();

        for item in tx.writes.iter() {
            let key = item.key();
            if re.is_match(key) {
                if item.value().is_some() {
                    keys.insert(key.clone());
                } else {
                    keys.remove(key);
                }
            }
        }
        let key_bytes: Vec<Vec<u8>> = keys.into_iter().map(|k| k.into_bytes()).collect();
        return Response::MultiBytes(key_bytes);
    }
    drop(tx_guard);

    let keys: Vec<Vec<u8>> = ctx
        .db
        .iter()
        .filter(|entry| re.is_match(entry.key()))
        .map(|entry| entry.key().as_bytes().to_vec())
        .collect();
    Response::MultiBytes(keys)
}

async fn handle_flushdb(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() != 1 {
        return Response::Error("FLUSHDB takes no arguments".to_string());
    }
    // This is a dangerous command. In a real system, we might want a confirmation
    // or specific privileges. For now, we just clear the database.
    // We don't log this; it's a meta-operation that implies starting fresh.
    // A snapshot will be triggered on next write anyway.
    ctx.db.clear();
    ctx.index_manager.indexes.clear();
    ctx.index_manager.prefix_to_indexes.clear();
    ctx.json_cache.clear();
    ctx.schema_cache.clear();
    println!("Database flushed.");
    Response::Ok
}

async fn handle_save(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() != 1 {
        return Response::Error("SAVE takes no arguments".to_string());
    }
    if !ctx.config.persistence {
        return Response::Error("SAVE is not supported when persistence is disabled".to_string());
    }
    // This is a placeholder. A proper implementation would require access
    // to the persistence engine to force a snapshot.
    println!("SAVE command received. Snapshotting is handled automatically.");
    Response::Ok
}

async fn handle_memory_usage(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() != 1 {
        return Response::Error("MEMUSAGE takes no arguments".to_string());
    }
    let usage = ctx.memory.current_memory();
    Response::Integer(usage as i64)
}

async fn handle_createindex(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() != 4 || String::from_utf8_lossy(&command.args[2]).to_uppercase() != "ON" {
        return Response::Error("Usage: CREATEINDEX <key-prefix> ON <json-path>".to_string());
    }
    let key_prefix = match String::from_utf8(command.args[1].clone()) {
        Ok(p) => p,
        Err(_) => return Response::Error("Invalid key prefix".to_string()),
    };
    let json_path = match String::from_utf8(command.args[3].clone()) {
        Ok(p) => p,
        Err(_) => return Response::Error("Invalid JSON path".to_string()),
    };

    // Fabricate an index name from the prefix and path
    let index_name = format!(
        "{}_{}",
        key_prefix.trim_end_matches('*'),
        json_path.replace('.', "_")
    );

    // Adapt to the IDX.CREATE format
    let adapted_command = Command {
        name: "IDX.CREATE".to_string(),
        args: vec![
            b"IDX.CREATE".to_vec(),
            index_name.into_bytes(),
            key_prefix.into_bytes(),
            json_path.into_bytes(),
        ],
    };
    handle_idx_create(adapted_command, ctx).await
}

pub async fn handle_idx_create(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() != 4 {
        return Response::Error(
            "IDX.CREATE requires an index name, a key prefix, and a JSON path".to_string(),
        );
    }
    let index_name = match String::from_utf8(command.args[1].clone()) {
        Ok(n) => n,
        Err(_) => return Response::Error("Invalid index name".to_string()),
    };
    let key_prefix = match String::from_utf8(command.args[2].clone()) {
        Ok(p) => p,
        Err(_) => return Response::Error("Invalid key prefix".to_string()),
    };
    let json_path = match String::from_utf8(command.args[3].clone()) {
        Ok(p) => p,
        Err(_) => return Response::Error("Invalid JSON path".to_string()),
    };

    if !key_prefix.ends_with('*') {
        return Response::Error("Key prefix must end with '*'".to_string());
    }

    let full_index_name = format!("{}|{}", key_prefix, json_path);

    if ctx.index_manager.indexes.contains_key(&full_index_name) {
        return Response::Error(format!("Index on this prefix and path already exists"));
    }
    if ctx.index_manager.name_to_internal_name.contains_key(&index_name) {
        return Response::Error(format!("Index '{}' already exists", index_name));
    }

    let index = Arc::new(Index::default());
    ctx.index_manager
        .indexes
        .insert(full_index_name.clone(), index.clone());
    ctx.index_manager
        .name_to_internal_name
        .insert(index_name.clone(), full_index_name.clone());
    ctx.index_manager
        .prefix_to_indexes
        .entry(key_prefix.clone())
        .or_default()
        .push(full_index_name);

    // --- Backfill the index ---
    let pattern = key_prefix.strip_suffix('*').unwrap_or(&key_prefix);
    let pointer = json_path_to_pointer(&json_path);
    let mut backfilled_count = 0;

    for entry in ctx.db.iter() {
        if entry.key().starts_with(pattern) {
            let val = match entry.value() {
                DbValue::Json(v) => v.clone(),
                DbValue::JsonB(b) => serde_json::from_slice(&b).unwrap_or_default(),
                _ => continue,
            };

            if let Some(indexed_val) = val.pointer(&pointer) {
                let index_key = serde_json::to_string(indexed_val).unwrap_or_default();
                let mut index_data = index.write().await;
                index_data
                    .entry(index_key)
                    .or_default()
                    .insert(entry.key().clone());
                backfilled_count += 1;
            }
        }
    }

    println!(
        "Index '{}' created for prefix '{}' on path '{}'. Backfilled {} items.",
        index_name, key_prefix, json_path, backfilled_count
    );
    Response::Ok
}

pub async fn handle_idx_drop(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() != 2 {
        return Response::Error("IDX.DROP requires an index name".to_string());
    }
    let index_name_to_drop = match String::from_utf8(command.args[1].clone()) {
        Ok(n) => n,
        Err(_) => return Response::Error("Invalid index name".to_string()),
    };

    if let Some((_, internal_name)) = ctx.index_manager.name_to_internal_name.remove(&index_name_to_drop) {
        ctx.index_manager.indexes.remove(&internal_name);
        let prefix = internal_name.split('|').next().unwrap().to_string();
        if let Some(mut prefixes) = ctx.index_manager.prefix_to_indexes.get_mut(&prefix) {
            prefixes.retain(|name| name != &internal_name);
        }
        Response::Integer(1)
    } else {
        Response::Integer(0)
    }
}

async fn handle_idx_list(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() != 1 {
        return Response::Error("IDX.LIST takes no arguments".to_string());
    }
    let index_names: Vec<Vec<u8>> = ctx
        .index_manager
        .indexes
        .iter()
        .map(|item| item.key().as_bytes().to_vec())
        .collect();
    Response::MultiBytes(index_names)
}

async fn handle_begin(
    command: Command,
    _ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 1 {
        return Response::Error("BEGIN takes no arguments".to_string());
    }

    let mut tx_guard = transaction_handle.write().await;
    if tx_guard.is_some() {
        return Response::Error("Transaction already in progress".to_string());
    }

    let new_tx = Transaction::new();
    println!("Transaction {} started.", new_tx.id);
    *tx_guard = Some(new_tx);

    Response::Ok
}

pub fn apply_json_set_to_db(db: &crate::types::Db, path: &str, value: Value) -> Result<()> {
    let mut parts = path.splitn(2, '.');
    let key = match parts.next() {
        Some(k) if !k.is_empty() => k.to_string(),
        _ => bail!("Invalid path: missing key"),
    };
    let inner_path = parts.next().unwrap_or("");

    let mut current_val = db.get(&key).map(|entry| {
        match entry.value() {
            DbValue::Json(v) => v.clone(),
            DbValue::JsonB(b) => serde_json::from_slice(&b).unwrap_or(json!({})),
            _ => json!({})
        }
    }).unwrap_or(json!({}));

    let pointer = json_path_to_pointer(inner_path);
    if pointer.is_empty() {
        current_val = value;
    } else if let Some(target) = current_val.pointer_mut(&pointer) {
        *target = value;
    } else {
        let mut current = &mut current_val;
        for part in inner_path.split('.') {
            if part.is_empty() { continue; }
            if current.is_object() {
                current = current.as_object_mut().unwrap().entry(part).or_insert(json!({}));
            }
            else {
                bail!("Path creation failed: part is not an object");
            }
        }
        *current = value;
    }

    let new_value_bytes = serde_json::to_vec(&current_val)?;
    db.insert(key, DbValue::JsonB(new_value_bytes));
    Ok(())
}

pub fn apply_json_delete_to_db(db: &crate::types::Db, path: &str) -> Result<i64> {
    let mut parts = path.splitn(2, '.');
    let key = match parts.next() {
        Some(k) if !k.is_empty() => k.to_string(),
        _ => bail!("Invalid path: missing key"),
    };
    let inner_path = parts.next().unwrap_or("");

    if inner_path.is_empty() || inner_path == "." {
        if db.remove(&key).is_some() {
            Ok(1)
        } else {
            Ok(0)
        }
    } else {
        if let Some(entry) = db.get(&key) {
            let mut current_val = match entry.value() {
                DbValue::Json(v) => v.clone(),
                DbValue::JsonB(b) => serde_json::from_slice(&b).unwrap_or(json!({})),
                _ => bail!("WRONGTYPE Operation against a non-JSON value"),
            };

            let mut pointer_parts: Vec<&str> = inner_path.split('.').collect();
            let final_key = pointer_parts.pop().unwrap();
            let parent_pointer = json_path_to_pointer(&pointer_parts.join("."));
            let mut modified = false;

            if let Some(target) = current_val.pointer_mut(&parent_pointer) {
                if let Some(obj) = target.as_object_mut() {
                    if obj.remove(final_key).is_some() {
                        modified = true;
                    }
                }
            }

            if modified {
                let new_bytes = serde_json::to_vec(&current_val)?;
                db.insert(key, DbValue::JsonB(new_bytes));
                return Ok(1);
            }
        }
        Ok(0)
    }
}

async fn handle_commit(
    _command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    let tx = {
        let mut tx_guard = transaction_handle.write().await;
        match tx_guard.take() {
            Some(t) => t,
            None => return Response::Error("No transaction in progress".to_string()),
        }
    };

    // Acquire a global lock to ensure the commit is atomic.
    let _commit_lock_guard = ctx.commit_lock.lock().await;

    match ctx.config.durability {
        DurabilityLevel::None => {
            // Apply to memory first, then fire-and-forget to WAL
            for entry in &tx.log_entries {
                if let Err(e) = apply_log_entry(entry, ctx).await {
                    return Response::Error(format!(
                        "CRITICAL: In-memory apply failed for 'none' durability: {}. Transaction partially applied.",
                        e
                    ));
                }
            }
            if ctx.config.persistence {
                for entry in tx.log_entries {
                    let (ack_tx, _) = oneshot::channel(); // We don't wait for the ack
                    let _ = ctx.logger.try_send(PersistenceRequest::Log(LogRequest { entry, ack: ack_tx }));
                }
            }
        }
        DurabilityLevel::Fsync | DurabilityLevel::Full => {
            // For both Fsync and Full, we write to WAL and wait for the OS buffer ack.
            // A true 'Full' would require a blocking fsync, which the current persistence engine doesn't support per-request.
            if ctx.config.persistence {
                for entry in &tx.log_entries {
                    let (ack_tx, ack_rx) = oneshot::channel();
                    if ctx
                        .logger
                        .send(PersistenceRequest::Log(LogRequest { entry: entry.clone(), ack: ack_tx }))
                        .await
                        .is_err()
                    {
                        return Response::Error(
                            "Persistence engine is down, commit failed. Transaction rolled back."
                                .to_string(),
                        );
                    }
                    match ack_rx.await {
                        Ok(Ok(())) => {} // Success
                        Ok(Err(e)) => {
                            return Response::Error(format!(
                                "WAL write error during commit: {}. Transaction rolled back.",
                                e
                            ))
                        }
                        Err(_) => {
                            return Response::Error(
                                "Persistence engine dropped ACK channel during commit. Transaction rolled back."
                                    .to_string(),
                            )
                        }
                    }
                }
            }

            // Apply changes to the in-memory DB after WAL write.
            for entry in &tx.log_entries {
                if let Err(e) = apply_log_entry(entry, ctx).await {
                    return Response::Error(format!(
                        "CRITICAL: In-memory apply failed after WAL write: {}. Database may be inconsistent.",
                        e
                    ));
                }
            }
        }
    }

    Response::Ok
}

async fn handle_rollback(
    command: Command,
    _ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 1 {
        return Response::Error("ROLLBACK takes no arguments".to_string());
    }

    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.take() {
        println!("Transaction {} rolled back.", tx.id);
        Response::Ok
    } else {
        Response::Error("No transaction in progress".to_string())
    }
}