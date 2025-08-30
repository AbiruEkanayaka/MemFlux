use anyhow::{bail, Result};
use serde_json::{json, Value};
use std::collections::{HashSet, VecDeque};
use std::str;
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};

use crate::indexing::Index;
use crate::memory;
use crate::types::*;

macro_rules! log_and_wait {
    ($logger:expr, $entry:expr) => {
        async {
            let (ack_tx, ack_rx) = oneshot::channel();
            if $logger
                .send(LogRequest {
                    entry: $entry,
                    ack: ack_tx,
                })
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
    };
}

pub async fn process_command(command: Command, ctx: &AppContext) -> Response {
    match command.name.as_str() {
        "PING" => Response::SimpleString("PONG".to_string()),
        "AUTH" => Response::Error("AUTH can only be used as the first command.".to_string()),
        "GET" => handle_get(command, ctx).await,
        "SET" => handle_set(command, ctx).await,
        "DELETE" => handle_delete(command, ctx).await,
        "JSON.SET" => handle_json_set(command, ctx).await,
        "JSON.GET" => handle_json_get(command, ctx).await,
        "JSON.DEL" => handle_json_del(command, ctx).await,
        "LPUSH" => handle_lpush(command, ctx).await,
        "RPUSH" => handle_rpush(command, ctx).await,
        "LPOP" => handle_lpop(command, ctx).await,
        "RPOP" => handle_rpop(command, ctx).await,
        "LLEN" => handle_llen(command, ctx).await,
        "LRANGE" => handle_lrange(command, ctx).await,
        "SADD" => handle_sadd(command, ctx).await,
        "SREM" => handle_srem(command, ctx).await,
        "SMEMBERS" => handle_smembers(command, ctx).await,
        "SCARD" => handle_scard(command, ctx).await,
        "SISMEMBER" => handle_sismember(command, ctx).await,
        "KEYS" => handle_keys(command, ctx).await,
        "FLUSHDB" => handle_flushdb(command, ctx).await,
        "SAVE" => handle_save(command, ctx).await,
        "MEMUSAGE" => handle_memory_usage(command, ctx).await,
        "CREATEINDEX" => handle_createindex(command, ctx).await,
        "IDX.CREATE" => handle_idx_create(command, ctx).await,
        "IDX.DROP" => handle_idx_drop(command, ctx).await,
        "IDX.LIST" => handle_idx_list(command, ctx).await,
        _ => Response::Error(format!("Unknown command: {}", command.name)),
    }
}

async fn handle_get(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() != 2 {
        return Response::Error("GET requires one argument".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };

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
                DbValue::Array(_) => Response::Error("WRONGTYPE Operation against an array value".to_string()),
            }
        }
        None => Response::Nil,
    }
}

async fn handle_set(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() != 3 {
        return Response::Error("SET requires two arguments".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };
    let value = command.args[2].clone();

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

    let log_entry = LogEntry::SetBytes {
        key: key.clone(),
        value: value.clone(),
    };
    let ack_response = log_and_wait!(ctx.logger, log_entry).await;

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

async fn handle_delete(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() < 2 {
        return Response::Error("DELETE requires at least one argument".to_string());
    }
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
        let (ack_tx, ack_rx) = oneshot::channel();
        if ctx.logger
            .send(LogRequest {
                entry: log_entry,
                ack: ack_tx,
            })
            .await
            .is_err()
        {
            return Response::Error("Persistence engine is down".to_string());
        }
        match ack_rx.await {
            Ok(Ok(())) => {
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
            Ok(Err(e)) => return Response::Error(format!("WAL write error: {}", e)),
            Err(_) => return Response::Error("Persistence engine dropped ACK channel".to_string()),
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

async fn handle_json_set(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() != 3 {
        return Response::Error("JSON.SET requires a key/path and a value".to_string());
    }
    let path = match String::from_utf8(command.args[1].clone()) {
        Ok(p) => p,
        Err(_) => return Response::Error("Invalid path".to_string()),
    };
    let value_str = match String::from_utf8(command.args[2].clone()) {
        Ok(v) => v,
        Err(_) => return Response::Error("Invalid value".to_string()),
    };
    let value: Value = match serde_json::from_str(&value_str) {
        Ok(v) => v,
        Err(_) => return Response::Error("Value is not valid JSON".to_string()),
    };

    let mut parts = path.splitn(2, '.');
    let key = match parts.next() {
        Some(k) if !k.is_empty() => k.to_string(),
        _ => return Response::Error("Invalid path: missing key".to_string()),
    };
    let inner_path = parts.next().unwrap_or("");

    let (old_size, mut current_val, old_val_for_index) = {
        let old_db_value = ctx.db.get(&key);
        let old_size = if let Some(entry) = &old_db_value {
            key.len() as u64 + memory::estimate_db_value_size(entry.value()).await
        } else {
            0
        };

        let current_val = match old_db_value.as_deref() {
            Some(DbValue::Json(v)) => v.clone(),
            Some(DbValue::JsonB(b)) => serde_json::from_slice(b).unwrap_or(json!({})),
            Some(_) => return Response::Error("WRONGTYPE Operation against a non-JSON value".to_string()),
            None => json!({}),
        };
        (old_size, current_val.clone(), current_val.clone())
    };

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

    if ctx.memory.is_enabled() {
        let new_size = key.len() as u64 + new_value_bytes.len() as u64;
        let needed = new_size.saturating_sub(old_size);
        if let Err(e) = ctx.memory.ensure_memory_for(needed, ctx).await {
            return Response::Error(e.to_string());
        }
    }

    let log_entry = LogEntry::SetJsonB {
        key: key.clone(),
        value: new_value_bytes.clone(),
    };
    let ack_response = log_and_wait!(ctx.logger, log_entry).await;

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

async fn handle_json_get(command: Command, ctx: &AppContext) -> Response {
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

    match ctx.db.get(key) {
        Some(entry) => {
            if ctx.memory.is_enabled() {
                ctx.memory.track_access(key).await;
            }
            match entry.value() {
                DbValue::Json(v) => {
                    let pointer = json_path_to_pointer(inner_path);
                    match v.pointer(&pointer) {
                        Some(val) => Response::Bytes(val.to_string().into_bytes()),
                        None => Response::Nil,
                    }
                }
                DbValue::JsonB(b) => {
                    let v: Value = match serde_json::from_slice(b) {
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
        None => Response::Nil,
    }
}

async fn handle_json_del(command: Command, ctx: &AppContext) -> Response {
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

    let (old_size, mut current_val, old_val_for_index) = {
        let old_db_value = match ctx.db.get(&key) {
            Some(val) => val,
            None => return Response::Integer(0),
        };

        let old_size = key.len() as u64 + memory::estimate_db_value_size(old_db_value.value()).await;

        let current_val = match old_db_value.value() {
            DbValue::Json(v) => v.clone(),
            DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or(json!({})),
            _ => return Response::Error("WRONGTYPE Operation against a non-JSON value".to_string()),
        };
        (old_size, current_val.clone(), current_val.clone())
    };

    if inner_path.is_empty() || inner_path == "." {
        let log_entry = LogEntry::Delete { key: key.clone() };
        let ack_response = log_and_wait!(ctx.logger, log_entry).await;
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
        let ack_response = log_and_wait!(ctx.logger, log_entry).await;

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
            Response::Integer(1)
        } else {
            ack_response
        }
    }
}

async fn handle_lpush(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() < 3 {
        return Response::Error("LPUSH requires a key and at least one value".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };
    let values: Vec<Vec<u8>> = command.args[2..].to_vec();

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

    let log_entry = LogEntry::LPush {
        key: key.clone(),
        values: values.clone(),
    };
    let ack_response = log_and_wait!(ctx.logger, log_entry).await;

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

async fn handle_rpush(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() < 3 {
        return Response::Error("RPUSH requires a key and at least one value".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };
    let values: Vec<Vec<u8>> = command.args[2..].to_vec();

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

    let log_entry = LogEntry::RPush {
        key: key.clone(),
        values: values.clone(),
    };
    let ack_response = log_and_wait!(ctx.logger, log_entry).await;

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

async fn handle_lpop(command: Command, ctx: &AppContext) -> Response {
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

    let mut old_size = 0;
    if let Some(entry) = ctx.db.get(&key) {
        old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
    }

    let log_entry = LogEntry::LPop {
        key: key.clone(),
        count,
    };
    let ack_response = log_and_wait!(ctx.logger, log_entry).await;

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

async fn handle_rpop(command: Command, ctx: &AppContext) -> Response {
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

    let mut old_size = 0;
    if let Some(entry) = ctx.db.get(&key) {
        old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
    }

    let log_entry = LogEntry::RPop {
        key: key.clone(),
        count,
    };
    let ack_response = log_and_wait!(ctx.logger, log_entry).await;

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

async fn handle_llen(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() != 2 {
        return Response::Error("LLEN requires one argument".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };

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

async fn handle_lrange(command: Command, ctx: &AppContext) -> Response {
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

async fn handle_sadd(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() < 3 {
        return Response::Error("SADD requires a key and at least one member".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };
    let members: Vec<Vec<u8>> = command.args[2..].to_vec();

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

    let log_entry = LogEntry::SAdd {
        key: key.clone(),
        members: members.clone(),
    };
    let ack_response = log_and_wait!(ctx.logger, log_entry).await;

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

async fn handle_srem(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() < 3 {
        return Response::Error("SREM requires a key and at least one member".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };
    let members: Vec<Vec<u8>> = command.args[2..].to_vec();

    let mut old_size = 0;
    if let Some(entry) = ctx.db.get(&key) {
        old_size = key.len() as u64 + memory::estimate_db_value_size(entry.value()).await;
    }

    let log_entry = LogEntry::SRem {
        key: key.clone(),
        members: members.clone(),
    };
    let ack_response = log_and_wait!(ctx.logger, log_entry).await;

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

async fn handle_smembers(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() != 2 {
        return Response::Error("SMEMBERS requires one argument".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };

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

async fn handle_scard(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() != 2 {
        return Response::Error("SCARD requires one argument".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };

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

async fn handle_sismember(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() != 3 {
        return Response::Error("SISMEMBER requires a key and a member".to_string());
    }
    let key = match String::from_utf8(command.args[1].clone()) {
        Ok(k) => k,
        Err(_) => return Response::Error("Invalid key".to_string()),
    };
    let member = &command.args[2];

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

async fn handle_keys(command: Command, ctx: &AppContext) -> Response {
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

async fn handle_save(command: Command, _ctx: &AppContext) -> Response {
    if command.args.len() != 1 {
        return Response::Error("SAVE takes no arguments".to_string());
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
        return Response::Error(format!("Index '{}' already exists", index_name));
    }

    let index = Arc::new(Index::default());
    ctx.index_manager
        .indexes
        .insert(full_index_name.clone(), index.clone());
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
                DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_default(),
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

async fn handle_idx_drop(command: Command, ctx: &AppContext) -> Response {
    if command.args.len() != 2 {
        return Response::Error("IDX.DROP requires an index name".to_string());
    }
    let index_name_to_drop = match String::from_utf8(command.args[1].clone()) {
        Ok(n) => n,
        Err(_) => return Response::Error("Invalid index name".to_string()),
    };

    let mut found_key: Option<String> = None;
    for item in ctx.index_manager.indexes.iter() {
        let internal_name = item.key();
        let parts: Vec<&str> = internal_name.splitn(2, '|').collect();
        if parts.len() == 2 {
            let key_prefix = parts[0];
            let json_path = parts[1];

            let fabricated_name = format!(
                "{}_{}",
                key_prefix.trim_end_matches('*'),
                json_path.replace('.', "_")
            );

            if fabricated_name == index_name_to_drop {
                found_key = Some(internal_name.clone());
                break;
            }
        }
    }

    if let Some(key) = found_key {
        ctx.index_manager.indexes.remove(&key);
        let prefix = key.split('|').next().unwrap().to_string();
        if let Some(mut prefixes) = ctx.index_manager.prefix_to_indexes.get_mut(&prefix) {
            prefixes.retain(|name| name != &key);
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
            DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or(json!({})),
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
            } else {
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
                DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or(json!({})),
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
