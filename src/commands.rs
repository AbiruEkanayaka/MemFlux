
use crate::storage_executor::{get_visible_db_value, json_path_to_pointer, StorageExecutor};
use crate::config::DurabilityLevel;
use crate::transaction::{Transaction, TransactionHandle};
use crate::types::*;
use crate::vacuum;
use serde_json::Value;
use std::collections::HashSet;
use std::str;
use std::sync::Arc;
use tokio::sync::oneshot;
use crate::indexing::Index;



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
        "WIPEDB" => handle_wipedb(command, ctx, transaction_handle).await,
        "SAVE" => handle_save(command, ctx).await,
        "MEMUSAGE" => handle_memory_usage(command, ctx).await,
        "CREATEINDEX" => handle_createindex(command, ctx).await,
        "IDX.CREATE" => handle_idx_create(command, ctx).await,
        "IDX.DROP" => handle_idx_drop(command, ctx).await,
        "IDX.LIST" => handle_idx_list(command, ctx).await,
        "BEGIN" => handle_begin(command, ctx, transaction_handle).await,
        "COMMIT" => handle_commit(command, ctx, transaction_handle).await,
        "ROLLBACK" => handle_rollback(command, ctx, transaction_handle).await,
        "SAVEPOINT" => handle_savepoint(command, ctx, transaction_handle).await,
        "ROLLBACK_TO_SAVEPOINT" => handle_rollback_to_savepoint(command, ctx, transaction_handle).await,
        "RELEASE_SAVEPOINT" => handle_release_savepoint(command, ctx, transaction_handle).await,
        "VACUUM" => handle_vacuum(ctx).await,
        _ => Response::Error(format!("Unknown command: {}", command.name)),
    }
}

async fn handle_vacuum(ctx: &AppContext) -> Response {
    match vacuum::vacuum(ctx).await {
        Ok((versions_removed, keys_removed)) => Response::SimpleString(format!(
            "Removed {} versions and {} keys",
            versions_removed,
            keys_removed
        )),
        Err(e) => Response::Error(format!("VACUUM failed: {}", e)),
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

    let executor = StorageExecutor::new(ctx.clone().into(), transaction_handle);
    executor.set(key, value).await}

async fn handle_delete(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() < 2 {
        return Response::Error("DELETE requires at least one argument".to_string());
    }

    let keys: Vec<String> = command.args[1..]
        .iter()
        .filter_map(|arg| String::from_utf8(arg.clone()).ok())
        .collect();

    if keys.is_empty() && command.args.len() > 1 {
        return Response::Error("Invalid key(s) provided".to_string());
    }

let executor = StorageExecutor::new(ctx.clone().into(), transaction_handle);
    return executor.delete(keys).await;
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
        return Response::Error(format!(
            "JSON.SET requires 2 or 3 arguments, got {}. Args: {:?}",
            command.args.len() - 1,
            command.args.iter().skip(1).map(|a| String::from_utf8_lossy(a).to_string()).collect::<Vec<String>>()
        ));
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

    let executor = StorageExecutor::new(ctx.clone().into(), transaction_handle);
    executor.json_set(key, path, value).await
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

    let executor = StorageExecutor::new(ctx.clone().into(), transaction_handle);
    executor.json_del(key, inner_path).await
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

    let executor = StorageExecutor::new(ctx.clone().into(), transaction_handle);
    executor.lpush(key, values).await
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

    let executor = StorageExecutor::new(ctx.clone().into(), transaction_handle);
    executor.rpush(key, values).await
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

    let executor = StorageExecutor::new(ctx.clone().into(), transaction_handle);
    let mut response = executor.lpop(key, count).await;

    // The executor returns a MultiBytes response. We need to adjust it if only one item was requested without a count.
    if !was_count_provided {
        if let Response::MultiBytes(ref mut vals) = response {
            if vals.len() == 1 {
                return Response::Bytes(vals.remove(0));
            } else if vals.is_empty() {
                return Response::Nil;
            }
        }
    }
    response
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

    let executor = StorageExecutor::new(ctx.clone().into(), transaction_handle);
    let mut response = executor.rpop(key, count).await;

    // The executor returns a MultiBytes response. We need to adjust it if only one item was requested without a count.
    if !was_count_provided {
        if let Response::MultiBytes(ref mut vals) = response {
            if vals.len() == 1 {
                return Response::Bytes(vals.remove(0));
            } else if vals.is_empty() {
                return Response::Nil;
            }
        }
    }
    response
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

    let executor = StorageExecutor::new(ctx.clone().into(), transaction_handle);
    executor.sadd(key, members).await
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

    let executor = StorageExecutor::new(ctx.clone().into(), transaction_handle);
    executor.srem(key, members).await
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
    ctx.index_manager.clear();
    ctx.json_cache.clear();
    ctx.schema_cache.clear();
    ctx.view_cache.clear();
    println!("Database flushed.");
    Response::Ok
}

async fn handle_wipedb(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 1 {
        return Response::Error("WIPEDB takes no arguments".to_string());
    }

    // It's a very destructive command, so let's prevent it from being used inside a transaction.
    let tx_guard = transaction_handle.read().await;
    if tx_guard.is_some() {
        return Response::Error(
            "WIPEDB cannot be run inside a transaction. Please COMMIT or ROLLBACK first."
                .to_string(),
        );
    }
    drop(tx_guard);

    // This is a dangerous command. In a real system, we might want a confirmation
    // or specific privileges. For now, we just clear everything.
    // This is a meta-operation and is not logged to the WAL.

    // 1. Clear main data store
    ctx.db.clear();

    // 2. Clear all caches
    ctx.json_cache.clear();
    ctx.schema_cache.clear();
    ctx.view_cache.clear();

    // 3. Reset index manager
    ctx.index_manager.clear();

    // 4. Reset memory manager
    ctx.memory.reset().await;

    // 5. Reset transaction managers
    ctx.tx_id_manager.reset();
    ctx.tx_status_manager.reset();

    println!("Database wiped completely.");
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
    let new_tx = Arc::new(Transaction::new(&ctx.tx_id_manager, &ctx.tx_status_manager));
    println!("Transaction {} ({}) started.", new_tx.id, new_tx.txid);
    ctx.active_transactions.insert(new_tx.txid, new_tx.clone());
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
            Some(tx) => tx,
            None => return Response::Error("No transaction in progress".to_string()),
        }
    };

    // Remove from active transactions before commit checks
    ctx.active_transactions.remove(&tx.txid);

    if ctx.config.isolation_level == crate::config::IsolationLevel::Serializable {
        // SSI: Check for incoming conflicts. If a key we read was changed by a
        // concurrent transaction that has already committed, we must abort.
        if tx.ssi_in_conflict.load(std::sync::atomic::Ordering::Relaxed) {
            ctx.tx_status_manager.abort(tx.txid);
            println!("Transaction {} ({}) aborted due to serialization conflict.", tx.id, tx.txid);
            return Response::Error("ABORT: Serialization failure, please retry transaction".to_string());
        }
    }

    // --- Persistence ---
    if ctx.config.persistence {
        let log_entries = tx.log_entries.read().await;
        if !log_entries.is_empty() {
            // In a real high-throughput scenario, you'd batch these.
            // For now, we log them and only wait on the final commit marker.
            for (i, entry) in log_entries.iter().enumerate() {
                let is_last = i == log_entries.len() - 1;
                let durability = if is_last { ctx.config.durability.clone() } else { DurabilityLevel::None };
                let (ack_tx, ack_rx) = oneshot::channel();
                let log_req = LogRequest { entry: entry.clone(), ack: ack_tx, durability };

                if ctx.logger.send(PersistenceRequest::Log(log_req)).await.is_err() {
                    ctx.tx_status_manager.abort(tx.txid);
                    return Response::Error("Persistence engine is down, commit failed. Transaction rolled back.".to_string());
                }

                if is_last {
                     match ack_rx.await {
                        Ok(Ok(())) => { /* Continue */ }
                        Ok(Err(e)) => {
                            ctx.tx_status_manager.abort(tx.txid);
                            return Response::Error(format!("WAL write error during commit: {}. Transaction rolled back.", e));
                        }
                        Err(_) => {
                            ctx.tx_status_manager.abort(tx.txid);
                            return Response::Error("Persistence engine dropped ACK channel during commit. Transaction rolled back.".to_string());
                        }
                    }
                }
            }
        }
    }

    if ctx.config.isolation_level == crate::config::IsolationLevel::Serializable {
        // SSI: Check for outgoing conflicts. Flag any concurrent transactions that have
        // read data that we are now writing.
        for write_item in tx.writes.iter() {
            let written_key = write_item.key();
            for other_tx_entry in ctx.active_transactions.iter() {
                let other_tx = other_tx_entry.value();
                // Check if other_tx is concurrent and has read the key we are writing.
                if tx.snapshot.xip.contains(&other_tx.txid) && other_tx.reads.contains_key(written_key) {
                    other_tx.ssi_in_conflict.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    // --- Apply changes to in-memory DB ---
    for item in tx.writes.iter() {
        let key = item.key();
        let value = item.value();
        let mut version_chain_lock = ctx.db.entry(key.clone()).or_default();
        let mut version_chain = version_chain_lock.value_mut().write().await;

        // Find the version that was visible to this transaction and expire it.
        if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| {
            tx.snapshot
                .is_visible(v, &ctx.tx_status_manager)
        }) {
            latest_version.expirer_txid = tx.txid;
        }

        if let Some(db_value) = value {
            let new_version = VersionedValue {
                value: db_value.clone(),
                creator_txid: tx.txid,
                expirer_txid: 0,
            };
            version_chain.push(new_version);
        }
    }

    // --- Mark transaction as committed ---
    ctx.tx_status_manager.commit(tx.txid);
    println!("Transaction {} ({}) committed.", tx.id, tx.txid);

    Response::Ok
}

async fn handle_rollback(
    command: Command,
    ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 1 {
        return Response::Error("ROLLBACK takes no arguments".to_string());
    }
    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.take() {
        ctx.tx_status_manager.abort(tx.txid);
        ctx.active_transactions.remove(&tx.txid);
        println!("Transaction {} ({}) rolled back.", tx.id, tx.txid);
        Response::Ok
    } else {
        Response::Error("No transaction in progress".to_string())
    }
}

async fn handle_savepoint(
    command: Command,
    _ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 2 {
        return Response::Error("SAVEPOINT requires a name".to_string());
    }
    let savepoint_name = match String::from_utf8(command.args[1].clone()) {
        Ok(n) => n,
        Err(_) => return Response::Error("Invalid savepoint name".to_string()),
    };

    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.as_mut() {
        let mut savepoints = tx.savepoints.write().await;
        if savepoints.contains_key(&savepoint_name) {
            return Response::Error(format!("Savepoint '{}' already exists", savepoint_name));
        }
        let log_entries = tx.log_entries.read().await.clone();
        savepoints.insert(
            savepoint_name.clone(),
            (log_entries, tx.writes.clone()),
        );
        tx.log_entries.write().await.push(LogEntry::Savepoint { name: savepoint_name.clone() });
        println!("Savepoint '{}' created for transaction {}.", savepoint_name, tx.id);
        Response::Ok
    } else {
        Response::Error("No transaction in progress".to_string())
    }
}

async fn handle_rollback_to_savepoint(
    command: Command,
    _ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 2 {
        return Response::Error("ROLLBACK TO SAVEPOINT requires a name".to_string());
    }
    let savepoint_name = match String::from_utf8(command.args[1].clone()) {
        Ok(n) => n,
        Err(_) => return Response::Error("Invalid savepoint name".to_string()),
    };

    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.as_mut() {
        let savepoints = tx.savepoints.read().await;
        if let Some((saved_log_entries, saved_writes)) = savepoints.get(&savepoint_name) {
            // Revert log_entries and writes to the savepoint state
            let mut log_entries = tx.log_entries.write().await;
            *log_entries = saved_log_entries.clone();
            // This is a bit tricky with DashMap. A simple clear and extend is easiest.
            tx.writes.clear();
            for item in saved_writes.iter() {
                tx.writes.insert(item.key().clone(), item.value().clone());
            }
            log_entries.push(LogEntry::RollbackToSavepoint { name: savepoint_name.clone() });
            println!("Transaction {} rolled back to savepoint '{}'.", tx.id, savepoint_name);
            Response::Ok
        } else {
            Response::Error(format!("Savepoint '{}' not found", savepoint_name))
        }
    } else {
        Response::Error("No transaction in progress".to_string())
    }
}

async fn handle_release_savepoint(
    command: Command,
    _ctx: &AppContext,
    transaction_handle: TransactionHandle,
) -> Response {
    if command.args.len() != 2 {
        return Response::Error("RELEASE SAVEPOINT requires a name".to_string());
    }
    let savepoint_name = match String::from_utf8(command.args[1].clone()) {
        Ok(n) => n,
        Err(_) => return Response::Error("Invalid savepoint name".to_string()),
    };

    let mut tx_guard = transaction_handle.write().await;
    if let Some(tx) = tx_guard.as_mut() {
        if tx.savepoints.write().await.remove(&savepoint_name).is_some() {
            tx.log_entries.write().await.push(LogEntry::ReleaseSavepoint { name: savepoint_name.clone() });
            println!("Savepoint '{}' released for transaction {}.", savepoint_name, tx.id);
            Response::Ok
        } else {
            Response::Error(format!("Savepoint '{}' not found", savepoint_name))
        }
    } else {
        Response::Error("No transaction in progress".to_string())
    }
}