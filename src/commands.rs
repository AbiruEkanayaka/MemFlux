
use serde_json::{json, Value};
use std::collections::{HashSet, VecDeque};
use std::str;
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};
use crate::indexing::Index;
use crate::memory;
use crate::transaction::{Transaction, TransactionHandle};
use crate::types::*;



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

pub async fn get_visible_db_value<'a>(
    key: &str,
    ctx: &'a AppContext,
    tx: Option<&Transaction>,
) -> Option<DbValue> {
    if let Some(tx) = tx {
        // 1. Check writes cache (read-your-writes)
        if let Some(entry) = tx.writes.get(key) {
            return entry.value().clone();
        }

        // 2. Read from main DB using snapshot visibility
        if let Some(version_chain_lock) = ctx.db.get(key) {
            let version_chain = version_chain_lock.read().await;
            for version in version_chain.iter().rev() {
                if tx.snapshot.is_visible(version, &ctx.tx_status_manager) {
                    if ctx.memory.is_enabled() {
                        ctx.memory.track_access(key).await;
                    }
                    return Some(version.value.clone());
                }
            }
        }
        return None; // No visible version found
    }

    // No transaction, read the latest committed version.
    if let Some(version_chain_lock) = ctx.db.get(key) {
        let version_chain = version_chain_lock.read().await;
        for version in version_chain.iter().rev() {
            if ctx.tx_status_manager.get_status(version.creator_txid)
                == Some(TransactionStatus::Committed)
            {
                if version.expirer_txid == 0
                    || ctx
                        .tx_status_manager
                        .get_status(version.expirer_txid)
                        != Some(TransactionStatus::Committed)
                {
                    // It's visible
                    if ctx.memory.is_enabled() {
                        ctx.memory.track_access(key).await;
                    }
                    return Some(version.value.clone());
                } else {
                    // It was created by a committed transaction, but also expired by a committed transaction.
                    // So it's not visible, and no older version can be either.
                    return None;
                }
            }
        }
    }
    None
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
    match get_visible_db_value(&key, ctx, tx_guard.as_ref()).await {
        Some(db_value) => db_value_to_response(&db_value, &key, ctx).await,
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

    // Non-transactional SET: a single, atomic, auto-committed operation.
    let txid = ctx.tx_id_manager.new_txid();
    ctx.tx_status_manager.begin(txid);

    // --- Memory and WAL --- 
    let mut old_size = 0;
    if let Some(version_chain_lock) = ctx.db.get(&key) {
        let version_chain = version_chain_lock.read().await;
        if let Some(latest_version) = version_chain.last() {
            if ctx.tx_status_manager.get_status(latest_version.creator_txid) == Some(TransactionStatus::Committed) {
                 old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
            }
        }
    }

    if ctx.memory.is_enabled() {
        let new_size = key.len() as u64 + value.len() as u64;
        let needed = new_size.saturating_sub(old_size);
        if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
            ctx.tx_status_manager.abort(txid);
            return Response::Error(e.to_string());
        }
    }

    let ack_response = log_to_wal(log_entry, ctx).await;
    if !matches!(ack_response, Response::Ok) {
        ctx.tx_status_manager.abort(txid);
        return ack_response;
    }

    // --- Apply changes to in-memory DB ---
    let mut version_chain_lock = ctx.db.entry(key.clone()).or_default();
    let mut version_chain = version_chain_lock.value_mut().write().await;

    // Expire previous latest committed version
    if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| {
        v.expirer_txid == 0
            && ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)
    }) {
        latest_version.expirer_txid = txid;
    }

    let new_version = VersionedValue {
        value: DbValue::Bytes(value.clone()),
        creator_txid: txid,
        expirer_txid: 0,
    };
    version_chain.push(new_version);

    let new_size = key.len() as u64 + value.len() as u64;
    ctx.memory.decrease_memory(old_size);
    ctx.memory.increase_memory(new_size);
    if ctx.memory.is_enabled() {
        ctx.memory.track_access(&key).await;
    }

    ctx.tx_status_manager.commit(txid);

    Response::Ok
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

        let txid = ctx.tx_id_manager.new_txid();
        ctx.tx_status_manager.begin(txid);

        let log_entry = LogEntry::Delete { key: key.clone() };
        let ack_response = log_to_wal(log_entry, ctx).await;
        if !matches!(ack_response, Response::Ok) {
            ctx.tx_status_manager.abort(txid);
            // Should we stop or continue? For now, continue with next key.
            continue;
        }

        let mut old_size = 0;
        let mut old_value_for_index: Option<Value> = None;

        if let Some(version_chain_lock) = ctx.db.get(&key) {
            let mut version_chain = version_chain_lock.value().write().await;
            let mut expired_something = false;
            if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| {
                v.expirer_txid == 0
                    && ctx.tx_status_manager.get_status(v.creator_txid)
                        == Some(TransactionStatus::Committed)
            }) {
                latest_version.expirer_txid = txid;
                expired_something = true;

                old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
                old_value_for_index = match &latest_version.value {
                    DbValue::Json(v) => Some(v.clone()),
                    DbValue::JsonB(b) => serde_json::from_slice(&b).ok(),
                    _ => None,
                };
            }

            if expired_something {
                ctx.memory.decrease_memory(old_size);
                if ctx.memory.is_enabled() {
                    ctx.memory.forget_key(&key).await;
                }
                if let Some(ref old_val) = old_value_for_index {
                    ctx.index_manager
                        .remove_key_from_indexes(&key, old_val)
                        .await;
                }
                deleted_count += 1;
            }
        }

        ctx.tx_status_manager.commit(txid);
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

    let value: Value = match serde_json::from_str(&value_str) {
        Ok(v) => v,
        Err(_) => return Response::Error("Value is not valid JSON".to_string()),
    };

    // --- TRANSACTION LOGIC FIRST ---
    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.as_mut() {
        let log_entry = LogEntry::JsonSet {
            path: full_path.clone(),
            value: value_str.to_string(),
        };
        tx.log_entries.push(log_entry);

        let current_db_val = get_visible_db_value(&key, ctx, Some(tx)).await;

        let mut current_val = match current_db_val {
            Some(DbValue::Json(v)) => v.clone(),
            Some(DbValue::JsonB(b)) => serde_json::from_slice(&b).unwrap_or(json!({})),
            Some(_) => {
                return Response::Error("WRONGTYPE Operation against a non-JSON value".to_string())
            }
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
    
    // --- Non-transactional JSON.SET ---
    let txid = ctx.tx_id_manager.new_txid();
    ctx.tx_status_manager.begin(txid);

    let mut version_chain_lock = ctx.db.entry(key.clone()).or_default();
    let mut version_chain = version_chain_lock.value_mut().write().await;

    let mut old_size = 0;
    let mut old_val_for_index = json!({});

    let mut current_val = if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| {
        v.expirer_txid == 0
            && ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)
    }) {
        latest_version.expirer_txid = txid;
        old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
        match &latest_version.value {
            DbValue::Json(v) => {
                old_val_for_index = v.clone();
                v.clone()
            }
            DbValue::JsonB(b) => {
                let v: Value = serde_json::from_slice(b).unwrap_or_default();
                old_val_for_index = v.clone();
                v
            }
            _ => return Response::Error("WRONGTYPE Operation against a non-JSON value".to_string()),
        }
    } else {
        json!({})
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
            ctx.tx_status_manager.abort(txid);
            return Response::Error(e.to_string());
        }
    }

    let ack_response = log_to_wal(log_entry, ctx).await;
    if !matches!(ack_response, Response::Ok) {
        ctx.tx_status_manager.abort(txid);
        return ack_response;
    }

    let new_version = VersionedValue {
        value: DbValue::JsonB(new_value_bytes.clone()),
        creator_txid: txid,
        expirer_txid: 0,
    };
    version_chain.push(new_version);

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

    ctx.tx_status_manager.commit(txid);
    Response::Ok
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
        if let Some(entry) = tx.writes.get(key) {
            return match entry.value() {
                Some(val) => json_db_value_to_response(val, inner_path, key, ctx).await,
                None => Response::Nil, // Deleted in this transaction
            };
        }

        if let Some(version_chain_lock) = ctx.db.get(key) {
            let version_chain = version_chain_lock.read().await;
            for version in version_chain.iter().rev() {
                if tx
                    .snapshot
                    .is_visible(version, &ctx.tx_status_manager)
                {
                    return json_db_value_to_response(&version.value, inner_path, key, ctx).await;
                }
            }
        }
        return Response::Nil;
    }
    drop(tx_guard);

    // Non-transactional read
    match ctx.db.get(key) {
        Some(version_chain_lock) => {
            let version_chain = version_chain_lock.read().await;
            if let Some(latest_version) = version_chain.last() {
                let status = ctx
                    .tx_status_manager
                    .get_status(latest_version.creator_txid);
                if status == Some(TransactionStatus::Committed) {
                    return json_db_value_to_response(&latest_version.value, inner_path, key, ctx)
                        .await;
                }
            }
            Response::Nil
        }
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

        if inner_path.is_empty() || inner_path == "." {
            tx.writes.insert(key.clone(), None);
        } else {
            let current_db_val = get_visible_db_value(&key, ctx, Some(tx)).await;

            let mut current_val = match current_db_val {
                Some(DbValue::Json(v)) => v.clone(),
                Some(DbValue::JsonB(b)) => serde_json::from_slice(&b).unwrap_or(json!({})),
                Some(_) => {
                    return Response::Error(
                        "WRONGTYPE Operation against a non-JSON value".to_string(),
                    )
                }
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
                    Err(_) => {
                        return Response::Error("Failed to serialize new JSON value".to_string())
                    }
                };
                tx.writes
                    .insert(key.clone(), Some(DbValue::JsonB(new_value_bytes)));
            }
        }

        return Response::Integer(1); // Assuming success, though can't confirm deletion
    }
    drop(tx_guard);

    // --- Non-transactional JSON.DEL ---
    let txid = ctx.tx_id_manager.new_txid();
    ctx.tx_status_manager.begin(txid);

    let mut version_chain_lock = ctx.db.entry(key.clone()).or_default();
    let mut version_chain = version_chain_lock.value_mut().write().await;

    let old_size;
    let old_val_for_index;
    let mut modified = false;

    let new_val = if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| {
        v.expirer_txid == 0
            && ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)
    }) {
        old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
        let current_val = match &latest_version.value {
            DbValue::Json(v) => {
                old_val_for_index = v.clone();
                v.clone()
            }
            DbValue::JsonB(b) => {
                let v: Value = serde_json::from_slice(b).unwrap_or_default();
                old_val_for_index = v.clone();
                v
            }
            _ => return Response::Error("WRONGTYPE Operation against a non-JSON value".to_string()),
        };

        if inner_path.is_empty() || inner_path == "." {
            latest_version.expirer_txid = txid;
            modified = true;
            current_val // This value will not be used, but we need to satisfy the if let
        } else {
            let mut temp_val = current_val;
            let mut pointer_parts: Vec<&str> = inner_path.split('.').collect();
            let final_key = pointer_parts.pop().unwrap();
            let parent_pointer = json_path_to_pointer(&pointer_parts.join("."));

            if let Some(target) = temp_val.pointer_mut(&parent_pointer) {
                if let Some(obj) = target.as_object_mut() {
                    if obj.remove(final_key).is_some() {
                        latest_version.expirer_txid = txid;
                        modified = true;
                    }
                }
            }
            temp_val
        }
    } else {
        // Key does not exist
        ctx.tx_status_manager.abort(txid);
        return Response::Integer(0);
    };

    if !modified {
        ctx.tx_status_manager.abort(txid);
        return Response::Integer(0);
    }

    if inner_path.is_empty() || inner_path == "." {
        let log_entry = LogEntry::Delete { key: key.clone() };
        let ack_response = log_to_wal(log_entry, ctx).await;
        if !matches!(ack_response, Response::Ok) {
            // This is bad, we modified in-memory but WAL failed.
            // A robust system would need to revert the in-memory change.
            ctx.tx_status_manager.abort(txid);
            return ack_response;
        }
        ctx.memory.decrease_memory(old_size);
        if ctx.memory.is_enabled() {
            ctx.memory.forget_key(&key).await;
        }
        ctx.index_manager
            .remove_key_from_indexes(&key, &old_val_for_index)
            .await;
    } else {
        let new_value_bytes = match serde_json::to_vec(&new_val) {
            Ok(b) => b,
            Err(_) => return Response::Error("Failed to serialize new JSON value".to_string()),
        };

        let log_entry = LogEntry::SetJsonB {
            key: key.clone(),
            value: new_value_bytes.clone(),
        };
        let ack_response = log_to_wal(log_entry, ctx).await;
        if !matches!(ack_response, Response::Ok) {
            ctx.tx_status_manager.abort(txid);
            return ack_response;
        }

        let new_version = VersionedValue {
            value: DbValue::JsonB(new_value_bytes.clone()),
            creator_txid: txid,
            expirer_txid: 0,
        };
        version_chain.push(new_version);

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
            .add_key_to_indexes(&key, &new_val)
            .await;
    }

    ctx.tx_status_manager.commit(txid);
    Response::Integer(1)
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

        let current_db_val = get_visible_db_value(&key, ctx, Some(tx)).await;

        let mut current_list = match current_db_val {
            Some(DbValue::List(list_lock)) => list_lock.read().await.clone(),
            Some(_) => {
                return Response::Error("WRONGTYPE Operation against a non-list value".to_string())
            }
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

    // Non-transactional LPUSH
    let txid = ctx.tx_id_manager.new_txid();
    ctx.tx_status_manager.begin(txid);

    let ack_response = log_to_wal(log_entry, ctx).await;
    if !matches!(ack_response, Response::Ok) {
        ctx.tx_status_manager.abort(txid);
        return ack_response;
    }

    let mut version_chain_lock = ctx.db.entry(key.clone()).or_default();
    let mut version_chain = version_chain_lock.value_mut().write().await;

    let mut old_size = 0;
    let mut new_list = if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| {
        v.expirer_txid == 0
            && ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)
    }) {
        latest_version.expirer_txid = txid;
        old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
        match &latest_version.value {
            DbValue::List(list_lock) => list_lock.read().await.clone(),
            _ => return Response::Error("WRONGTYPE Operation against a non-list value".to_string()),
        }
    } else {
        VecDeque::new()
    };

    for v in values {
        new_list.push_front(v);
    }

    let new_len = new_list.len() as i64;
    let new_db_value = DbValue::List(RwLock::new(new_list));
    let new_size = key.len() as u64 + memory::estimate_db_value_size(&new_db_value).await;

    if ctx.memory.is_enabled() {
        let needed = new_size.saturating_sub(old_size);
        if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
            // This is tricky. The WAL entry is written. A real DB would need a compensating WAL entry.
            // For now, we abort the txid and return an error.
            ctx.tx_status_manager.abort(txid);
            return Response::Error(e.to_string());
        }
    }

    let new_version = VersionedValue {
        value: new_db_value,
        creator_txid: txid,
        expirer_txid: 0,
    };
    version_chain.push(new_version);

    ctx.memory.decrease_memory(old_size);
    ctx.memory.increase_memory(new_size);
    if ctx.memory.is_enabled() {
        ctx.memory.track_access(&key).await;
    }

    ctx.tx_status_manager.commit(txid);

    Response::Integer(new_len)
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

        let current_db_val = get_visible_db_value(&key, ctx, Some(tx)).await;

        let mut current_list = match current_db_val {
            Some(DbValue::List(list_lock)) => list_lock.read().await.clone(),
            Some(_) => {
                return Response::Error("WRONGTYPE Operation against a non-list value".to_string())
            }
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

    // Non-transactional RPUSH
    let txid = ctx.tx_id_manager.new_txid();
    ctx.tx_status_manager.begin(txid);

    let ack_response = log_to_wal(log_entry, ctx).await;
    if !matches!(ack_response, Response::Ok) {
        ctx.tx_status_manager.abort(txid);
        return ack_response;
    }

    let mut version_chain_lock = ctx.db.entry(key.clone()).or_default();
    let mut version_chain = version_chain_lock.value_mut().write().await;

    let mut old_size = 0;
    let mut new_list = if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| {
        v.expirer_txid == 0
            && ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)
    }) {
        latest_version.expirer_txid = txid;
        old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
        match &latest_version.value {
            DbValue::List(list_lock) => list_lock.read().await.clone(),
            _ => return Response::Error("WRONGTYPE Operation against a non-list value".to_string()),
        }
    } else {
        VecDeque::new()
    };

    for v in values {
        new_list.push_back(v);
    }

    let new_len = new_list.len() as i64;
    let new_db_value = DbValue::List(RwLock::new(new_list));
    let new_size = key.len() as u64 + memory::estimate_db_value_size(&new_db_value).await;

    if ctx.memory.is_enabled() {
        let needed = new_size.saturating_sub(old_size);
        if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
            ctx.tx_status_manager.abort(txid);
            return Response::Error(e.to_string());
        }
    }

    let new_version = VersionedValue {
        value: new_db_value,
        creator_txid: txid,
        expirer_txid: 0,
    };
    version_chain.push(new_version);

    ctx.memory.decrease_memory(old_size);
    ctx.memory.increase_memory(new_size);
    if ctx.memory.is_enabled() {
        ctx.memory.track_access(&key).await;
    }

    ctx.tx_status_manager.commit(txid);

    Response::Integer(new_len)
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

        let current_db_val = get_visible_db_value(&key, ctx, Some(tx)).await;

        let mut current_list = match current_db_val {
            Some(DbValue::List(list_lock)) => list_lock.read().await.clone(),
            Some(_) => {
                return Response::Error("WRONGTYPE Operation against a non-list value".to_string())
            }
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

    // Non-transactional LPOP
    let txid = ctx.tx_id_manager.new_txid();
    ctx.tx_status_manager.begin(txid);

    let ack_response = log_to_wal(log_entry, ctx).await;
    if !matches!(ack_response, Response::Ok) {
        ctx.tx_status_manager.abort(txid);
        return ack_response;
    }

    let mut version_chain_lock = ctx.db.entry(key.clone()).or_default();
    let mut version_chain = version_chain_lock.value_mut().write().await;

    let mut old_size = 0;
    let mut popped = Vec::new();
    let mut list_exists = false;

    let mut new_list = if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| {
        v.expirer_txid == 0
            && ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)
    }) {
        latest_version.expirer_txid = txid;
        list_exists = true;
        old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
        match &latest_version.value {
            DbValue::List(list_lock) => list_lock.read().await.clone(),
            _ => return Response::Error("WRONGTYPE Operation against a non-list value".to_string()),
        }
    } else {
        VecDeque::new()
    };

    if list_exists {
        for _ in 0..count {
            if let Some(val) = new_list.pop_front() {
                popped.push(val);
            } else {
                break;
            }
        }
    }

    let new_db_value = DbValue::List(RwLock::new(new_list));
    let new_size = key.len() as u64 + memory::estimate_db_value_size(&new_db_value).await;

    if ctx.memory.is_enabled() {
        let needed = new_size.saturating_sub(old_size);
        if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
            ctx.tx_status_manager.abort(txid);
            return Response::Error(e.to_string());
        }
    }

    let new_version = VersionedValue {
        value: new_db_value,
        creator_txid: txid,
        expirer_txid: 0,
    };
    version_chain.push(new_version);

    ctx.memory.decrease_memory(old_size);
    ctx.memory.increase_memory(new_size);
    if ctx.memory.is_enabled() {
        ctx.memory.track_access(&key).await;
    }

    ctx.tx_status_manager.commit(txid);

    if popped.is_empty() {
        Response::Nil
    } else if !was_count_provided {
        Response::Bytes(popped.into_iter().next().unwrap())
    } else {
        Response::MultiBytes(popped)
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
    
                let current_db_val = get_visible_db_value(&key, ctx, Some(tx)).await;
        let mut current_list = match current_db_val {
            Some(DbValue::List(list_lock)) => list_lock.read().await.clone(),
            Some(_) => {
                return Response::Error("WRONGTYPE Operation against a non-list value".to_string())
            }
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

    // Non-transactional RPOP
    let txid = ctx.tx_id_manager.new_txid();
    ctx.tx_status_manager.begin(txid);

    let ack_response = log_to_wal(log_entry, ctx).await;
    if !matches!(ack_response, Response::Ok) {
        ctx.tx_status_manager.abort(txid);
        return ack_response;
    }

    let mut version_chain_lock = ctx.db.entry(key.clone()).or_default();
    let mut version_chain = version_chain_lock.value_mut().write().await;

    let mut old_size = 0;
    let mut popped = Vec::new();
    let mut list_exists = false;

    let mut new_list = if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| {
        v.expirer_txid == 0
            && ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)
    }) {
        latest_version.expirer_txid = txid;
        list_exists = true;
        old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
        match &latest_version.value {
            DbValue::List(list_lock) => list_lock.read().await.clone(),
            _ => return Response::Error("WRONGTYPE Operation against a non-list value".to_string()),
        }
    } else {
        VecDeque::new()
    };

    if list_exists {
        for _ in 0..count {
            if let Some(val) = new_list.pop_back() {
                popped.push(val);
            } else {
                break;
            }
        }
    }

    let new_db_value = DbValue::List(RwLock::new(new_list));
    let new_size = key.len() as u64 + memory::estimate_db_value_size(&new_db_value).await;

    if ctx.memory.is_enabled() {
        let needed = new_size.saturating_sub(old_size);
        if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
            ctx.tx_status_manager.abort(txid);
            return Response::Error(e.to_string());
        }
    }

    let new_version = VersionedValue {
        value: new_db_value,
        creator_txid: txid,
        expirer_txid: 0,
    };
    version_chain.push(new_version);

    ctx.memory.decrease_memory(old_size);
    ctx.memory.increase_memory(new_size);
    if ctx.memory.is_enabled() {
        ctx.memory.track_access(&key).await;
    }

    ctx.tx_status_manager.commit(txid);

    if popped.is_empty() {
        Response::Nil
    } else if !was_count_provided {
        Response::Bytes(popped.into_iter().next().unwrap())
    } else {
        Response::MultiBytes(popped)
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
    match get_visible_db_value(&key, ctx, tx_guard.as_ref()).await {
        Some(DbValue::List(list_lock)) => {
            let list = list_lock.read().await;
            Response::Integer(list.len() as i64)
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a non-list value".to_string()),
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
    match get_visible_db_value(&key, ctx, tx_guard.as_ref()).await {
        Some(DbValue::List(list_lock)) => {
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
        Some(_) => Response::Error("WRONGTYPE Operation against a non-list value".to_string()),
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

        let current_db_val = get_visible_db_value(&key, ctx, Some(tx)).await;

        let mut current_set = match current_db_val {
            Some(DbValue::Set(set_lock)) => set_lock.read().await.clone(),
            Some(_) => {
                return Response::Error("WRONGTYPE Operation against a non-set value".to_string())
            }
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

    // Non-transactional SADD
    let txid = ctx.tx_id_manager.new_txid();
    ctx.tx_status_manager.begin(txid);

    let existing_value = get_visible_db_value(&key, ctx, None).await;

    let mut old_size = 0;
    let mut new_set = match existing_value {
        Some(DbValue::Set(set_lock)) => {
            let s = set_lock.read().await;
            old_size = key.len() as u64 + memory::estimate_db_value_size(&DbValue::Set(RwLock::new(s.clone()))).await;
            s.clone()
        },
        Some(_) => {
            ctx.tx_status_manager.abort(txid);
            return Response::Error("WRONGTYPE Operation against a non-set value".to_string());
        }
        None => HashSet::new(),
    };

    let ack_response = log_to_wal(log_entry, ctx).await;
    if !matches!(ack_response, Response::Ok) {
        ctx.tx_status_manager.abort(txid);
        return ack_response;
    }

    let mut version_chain_lock = ctx.db.entry(key.clone()).or_default();
    let mut version_chain = version_chain_lock.value_mut().write().await;

    // Expire the previous version if it existed
    if old_size > 0 {
        if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| {
            v.expirer_txid == 0
                && ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)
        }) {
            latest_version.expirer_txid = txid;
        }
    }

    let mut added_count = 0;
    for m in members {
        if new_set.insert(m) {
            added_count += 1;
        }
    }

    let new_db_value = DbValue::Set(RwLock::new(new_set));
    let new_size = key.len() as u64 + memory::estimate_db_value_size(&new_db_value).await;

    if ctx.memory.is_enabled() {
        let needed = new_size.saturating_sub(old_size);
        if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
            ctx.tx_status_manager.abort(txid);
            return Response::Error(e.to_string());
        }
    }

    let new_version = VersionedValue {
        value: new_db_value,
        creator_txid: txid,
        expirer_txid: 0,
    };
    version_chain.push(new_version);

    ctx.memory.decrease_memory(old_size);
    ctx.memory.increase_memory(new_size);
    if ctx.memory.is_enabled() {
        ctx.memory.track_access(&key).await;
    }

    ctx.tx_status_manager.commit(txid);

    Response::Integer(added_count)
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

        let current_db_val = get_visible_db_value(&key, ctx, Some(tx)).await;

        let mut current_set = match current_db_val {
            Some(DbValue::Set(set_lock)) => set_lock.read().await.clone(),
            Some(_) => {
                return Response::Error("WRONGTYPE Operation against a non-set value".to_string())
            }
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

    // Non-transactional SREM
    let txid = ctx.tx_id_manager.new_txid();
    ctx.tx_status_manager.begin(txid);

    let ack_response = log_to_wal(log_entry, ctx).await;
    if !matches!(ack_response, Response::Ok) {
        ctx.tx_status_manager.abort(txid);
        return ack_response;
    }

    let mut version_chain_lock = ctx.db.entry(key.clone()).or_default();
    let mut version_chain = version_chain_lock.value_mut().write().await;

    let mut old_size = 0;
    let mut removed_count = 0;

    let mut new_set = if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| {
        v.expirer_txid == 0
            && ctx.tx_status_manager.get_status(v.creator_txid) == Some(TransactionStatus::Committed)
    }) {
        latest_version.expirer_txid = txid;
        old_size = key.len() as u64 + memory::estimate_db_value_size(&latest_version.value).await;
        match &latest_version.value {
            DbValue::Set(set_lock) => set_lock.read().await.clone(),
            _ => return Response::Error("WRONGTYPE Operation against a non-set value".to_string()),
        }
    } else {
        HashSet::new()
    };

    for m in members {
        if new_set.remove(&m) {
            removed_count += 1;
        }
    }

    let new_db_value = DbValue::Set(RwLock::new(new_set));
    let new_size = key.len() as u64 + memory::estimate_db_value_size(&new_db_value).await;

    if ctx.memory.is_enabled() {
        let needed = new_size.saturating_sub(old_size);
        if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
            ctx.tx_status_manager.abort(txid);
            return Response::Error(e.to_string());
        }
    }

    let new_version = VersionedValue {
        value: new_db_value,
        creator_txid: txid,
        expirer_txid: 0,
    };
    version_chain.push(new_version);

    ctx.memory.decrease_memory(old_size);
    ctx.memory.increase_memory(new_size);
    if ctx.memory.is_enabled() {
        ctx.memory.track_access(&key).await;
    }

    ctx.tx_status_manager.commit(txid);

    Response::Integer(removed_count)
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
    match get_visible_db_value(&key, ctx, tx_guard.as_ref()).await {
        Some(DbValue::Set(set_lock)) => {
            let set = set_lock.read().await;
            let members: Vec<Vec<u8>> = set.iter().cloned().collect();
            Response::MultiBytes(members)
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a non-set value".to_string()),
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
    match get_visible_db_value(&key, ctx, tx_guard.as_ref()).await {
        Some(DbValue::Set(set_lock)) => {
            let set = set_lock.read().await;
            Response::Integer(set.len() as i64)
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a non-set value".to_string()),
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
    match get_visible_db_value(&key, ctx, tx_guard.as_ref()).await {
        Some(DbValue::Set(set_lock)) => {
            let set = set_lock.read().await;
            if set.contains(member) {
                Response::Integer(1)
            } else {
                Response::Integer(0)
            }
        }
        Some(_) => Response::Error("WRONGTYPE Operation against a non-set value".to_string()),
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
        let mut keys: HashSet<String> = HashSet::new();

        // 1. Get potentially visible keys from the main DB
        for db_entry in ctx.db.iter() {
            let key = db_entry.key();
            if get_visible_db_value(key, ctx, Some(tx)).await.is_some() {
                keys.insert(key.clone());
            }
        }

        // 2. Merge with transactional writes
        for write_entry in tx.writes.iter() {
            if write_entry.value().is_some() {
                keys.insert(write_entry.key().clone());
            } else {
                // It's a delete
                keys.remove(write_entry.key());
            }
        }

        // 3. Filter and return
        let key_bytes: Vec<Vec<u8>> = keys
            .into_iter()
            .filter(|k| re.is_match(k))
            .map(|k| k.into_bytes())
            .collect();
        return Response::MultiBytes(key_bytes);
    }
    drop(tx_guard);

    // Non-transactional KEYS
    let mut keys = Vec::new();
    let matching_keys: Vec<String> = ctx
        .db
        .iter()
        .filter(|entry| re.is_match(entry.key()))
        .map(|entry| entry.key().clone())
        .collect();

    for key in matching_keys {
        if let Some(entry) = ctx.db.get(&key) {
            let version_chain = entry.value().read().await;
            if let Some(latest_version) = version_chain.last() {
                if ctx.tx_status_manager.get_status(latest_version.creator_txid)
                    == Some(TransactionStatus::Committed)
                {
                    if latest_version.expirer_txid == 0
                        || ctx
                            .tx_status_manager
                            .get_status(latest_version.expirer_txid)
                            != Some(TransactionStatus::Committed)
                    {
                        keys.push(key.as_bytes().to_vec());
                    }
                }
            }
        }
    }
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
            let version_chain = entry.value().read().await;
            if let Some(latest_version) = version_chain.last() {
                if ctx.tx_status_manager.get_status(latest_version.creator_txid)
                    != Some(TransactionStatus::Committed)
                {
                    continue;
                }

                let val = match &latest_version.value {
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
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 1 {
        return Response::Error("BEGIN takes no arguments".to_string());
    }

    let mut tx_guard = transaction_handle.write().await;
    if tx_guard.is_some() {
        return Response::Error("Transaction already in progress".to_string());
    }

    let new_tx = Transaction::new(&ctx.tx_id_manager, &ctx.tx_status_manager);
    println!("Transaction {} started.", new_tx.txid);
    *tx_guard = Some(new_tx);

    Response::Ok
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

    // --- Write to WAL first ---
    if ctx.config.persistence {
        for entry in &tx.log_entries {
            let (ack_tx, ack_rx) = oneshot::channel();
            if ctx
                .logger
                .send(PersistenceRequest::Log(LogRequest {
                    entry: entry.clone(),
                    ack: ack_tx,
                }))
                .await
                .is_err()
            {
                ctx.tx_status_manager.abort(tx.txid);
                return Response::Error(
                    "Persistence engine is down, commit failed. Transaction rolled back.".to_string(),
                );
            }
            match ack_rx.await {
                Ok(Ok(())) => {} // Success
                Ok(Err(e)) => {
                    ctx.tx_status_manager.abort(tx.txid);
                    return Response::Error(format!(
                        "WAL write error during commit: {}. Transaction rolled back.",
                        e
                    ));
                }
                Err(_) => {
                    ctx.tx_status_manager.abort(tx.txid);
                    return Response::Error(
                        "Persistence engine dropped ACK channel during commit. Transaction rolled back."
                            .to_string(),
                    );
                }
            }
        }
    }

    // --- Apply changes to in-memory DB ---
    for write_item in tx.writes.iter() {
        let key = write_item.key();
        let new_value_opt = write_item.value();

        let mut version_chain_lock = ctx.db.entry(key.clone()).or_default();
        let mut version_chain = version_chain_lock.value_mut().write().await;

        // Find the version that was visible to this transaction and expire it.
        for version in version_chain.iter_mut().rev() {
            if tx.snapshot.is_visible(version, &ctx.tx_status_manager) {
                if version.expirer_txid == 0 {
                    version.expirer_txid = tx.txid;
                }
                break; // Only expire the most recent visible version
            }
        }

        // Add the new version if it's an insert or update
        if let Some(new_db_value) = new_value_opt {
            let new_version = VersionedValue {
                value: new_db_value.clone(),
                creator_txid: tx.txid,
                expirer_txid: 0,
            };
            version_chain.push(new_version);
        }
    }

    // --- Mark transaction as committed ---
    ctx.tx_status_manager.commit(tx.txid);
    println!("Transaction {} committed.", tx.txid);

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