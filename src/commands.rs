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

/// Retrieves the value for a key and returns it as a Response.
///
/// Validates that the command contains exactly one argument (the key) and that the key is valid UTF-8.
/// If the key exists:
/// - For DbValue::Bytes and DbValue::JsonB returns Response::Bytes containing the stored bytes.
/// - For DbValue::Json returns Response::Bytes containing the JSON string bytes.
/// - For non-scalar types (List, Set, Array) returns a WRONGTYPE Response error.
/// If the key does not exist returns Response::Nil.
/// If memory tracking is enabled in the context, the access is recorded.
///
/// # Examples
///
/// ```rust,ignore
/// // Example usage (context setup omitted):
/// let cmd = Command::new(vec![b"GET".to_vec(), b"mykey".to_vec()]);
/// let resp = handle_get(cmd, &ctx).await;
/// match resp {
///     Response::Bytes(b) => println!("got bytes: {:?}", b),
///     Response::Nil => println!("key not found"),
///     Response::Error(e) => println!("error: {}", e),
///     _ => {}
/// }
/// ```
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

/// Convert a dot-delimited JSON path into a JSON Pointer string.
///
/// The input uses '.' as a separator (e.g., "a.b.c") and may optionally begin with a leading '.'.
/// A single dot "." or an empty string returns an empty pointer (used to indicate the root).
///
/// # Examples
///
/// ```
/// assert_eq!(json_path_to_pointer("a.b.c"), "/a/b/c");
/// assert_eq!(json_path_to_pointer(".a.b"), "/a/b");
/// assert_eq!(json_path_to_pointer("."), "");
/// assert_eq!(json_path_to_pointer(""), "");
/// ```
pub fn json_path_to_pointer(path: &str) -> String {
    if path == "." || path.is_empty() {
        return "".to_string();
    }
    let p = path.strip_prefix('.').unwrap_or(path);
    format!("/{}", p.replace('.', "/"))
}

/// Set or update a JSON value at a dotted key.path in the database.
///
/// Expects `command.args` to contain exactly two arguments after the command name:
/// 1) a key path of the form `key` or `key.inner.path` and 2) a JSON-encoded value string.
/// The function parses the provided JSON value, loads the current value (supports both in-memory `Json` and stored `JsonB`),
/// then replaces the root value when the path is empty or updates/creates the nested value at the specified inner path.
/// The new value is serialized to JSON bytes (stored as JSONB), persisted via a WAL `SetJsonB` entry (awaiting acknowledgement),
/// and on success the in-memory DB, memory accounting, access tracking, and indexes are updated to reflect the change.
///
/// Error responses are returned for invalid arguments, malformed JSON, type mismatches (non-JSON existing value),
/// serialization failures, or if memory allocation for the new value cannot be ensured. The final returned `Response`
/// reflects the WAL acknowledgement (success or WAL-related error).
///
/// # Examples
///
/// ```
/// // Pseudocode example; adapt to test harness and AppContext construction in this crate:
/// // let cmd = Command::from_args(&["JSON.SET", "user:1.profile", "{\"name\":\"alice\"}"]);
/// // let resp = tokio::runtime::Handle::current().block_on(handle_json_set(cmd, &app_ctx));
/// // assert!(matches!(resp, Response::Ok));
/// ```
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

/// Retrieve a JSON value (or a nested path) stored at a key.
///
/// The command must have exactly one argument: the "key.path" string where `key` is the database key
/// and `path` is a dot-delimited JSON path. If `path` is empty or `"."` the entire JSON value is
/// returned.
///
/// Behavior:
/// - If the key is not present: returns `Response::Nil`.
/// - If the stored value is `DbValue::Json` or `DbValue::JsonB`:
///   - If the requested inner path points to a JSON value, returns `Response::Bytes` with the JSON
///     serialization of that value.
///   - If the pointer does not exist, returns `Response::Nil`.
///   - If the inner path is empty or `"."` for `JsonB`, returns the full JSON serialization.
/// - If the stored value is not JSON: returns `Response::Error("WRONGTYPE Operation against a non-JSON value")`.
/// - If the provided path bytes are not valid UTF-8 or a stored JSONB blob cannot be parsed, returns
///   a `Response::Error` describing the problem.
///
/// Memory access is tracked via the application context when memory tracking is enabled.
///
/// # Examples
///
/// ```no_run
/// use tokio::runtime::Runtime;
/// // Construct `command` where args[1] is the UTF-8 bytes of "user:1.address.city"
/// // and `ctx` is your AppContext. Call from an async runtime:
/// # let rt = Runtime::new().unwrap();
/// # let command = /* build Command with args */ unimplemented!();
/// # let ctx = /* obtain AppContext */ unimplemented!();
/// # let _ = rt.block_on(async { crate::commands::handle_json_get(command, &ctx).await });
/// ```
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

/// Delete a JSON value or a nested member at a JSON path for a given key.
///
/// If the provided path refers to the root (empty string or "."), the entire key is deleted.
/// If the path targets a nested member (e.g., "mykey.a.b"), the function removes the final
/// member from the JSON object at that path and writes the updated JSON back as JSONB.
/// Behavior summary:
/// - Returns Integer(1) when a deletion (key removal or nested-member removal) occurs.
/// - Returns Integer(0) when the key does not exist or the targeted nested member was absent.
/// - Returns Error("WRONGTYPE ...") if the key exists but is not a JSON value.
/// - Returns Error on invalid input (bad UTF-8 path, invalid path) or on WAL/serialization failures.
///
/// Side effects:
/// - Writes a Delete or SetJsonB entry to the WAL and waits for acknowledgement; failures from the
///   WAL acknowledgement are returned directly.
/// - On successful mutation, updates memory accounting, optional memory-tracking structures,
///   and index entries (removes old index mappings; for nested-member removals, adds updated mappings).
///
/// # Examples
///
/// no_run:
/// ```no_run
/// // Construct a JSON.DEL command targeting key "user" and nested path "profile.age":
/// let cmd = Command { args: vec![b"JSON.DEL".to_vec(), b"user.profile.age".to_vec()] };
/// // `ctx` is an application context available in the server; call the handler:
/// let resp = tokio::runtime::Handle::current().block_on(handle_json_del(cmd, &ctx));
/// match resp {
///     Response::Integer(1) => println!("Deleted successfully"),
///     Response::Integer(0) => println!("Nothing to delete"),
///     Response::Error(e) => eprintln!("Error: {}", e),
///     _ => {}
/// }
/// ```
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

/// Create an index for JSON values under keys matching a prefix.
///
/// Expects a `CREATEINDEX <key-prefix> ON <json-path>` command:
/// - `<key-prefix>` must be a UTF-8 string (may end with `*` to indicate a prefix).
/// - The third token must be the literal `ON` (case-insensitive).
/// - `<json-path>` is a UTF-8 dot-delimited path used to extract values from stored JSON.
///
/// The function synthesizes an internal index name from the prefix and JSON path, converts
/// the command into an `IDX.CREATE` command, and delegates to `handle_idx_create`.
///
/// Returns the `Response` produced by `handle_idx_create`. If the input arguments are invalid
/// (wrong count or non-UTF-8 prefix/path), returns a `Response::Error` with usage or parse info.
///
/// # Examples
///
/// ```
/// # // Illustrative example; types and context values depend on the surrounding crate.
/// # async fn example(ctx: &AppContext) {
/// let cmd = Command {
///     name: "CREATEINDEX".to_string(),
///     args: vec![b"CREATEINDEX".to_vec(), b"user:*".to_vec(), b"ON".to_vec(), b"address.city".to_vec()],
/// };
/// let resp = handle_createindex(cmd, ctx).await;
/// // inspect `resp` for success or error
/// # }
/// ```
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

/// Create a new index for keys matching a prefix and a JSON path, backfilling existing entries.
///
/// The command must have exactly three arguments after the command name:
/// 1. index name (user-facing name)
/// 2. key prefix (must end with `*`)
/// 3. JSON path (dot-delimited)
///
/// If an index with the same (prefix, path) pair already exists, or the provided index name
/// is already in use, the function returns an error Response. On success it registers the
/// index, scans the database to backfill entries that match the prefix and contain a value at
/// the JSON path, and returns `Response::Ok`.
///
/// # Examples
///
/// ```no_run
/// // Minimal usage sketch (context and command must be constructed from the application).
/// // let command = Command::from_parts("IDX.CREATE", vec![b"myidx".to_vec(), b"user:*".to_vec(), b".name".to_vec()]);
/// // let resp = tokio::runtime::Runtime::new().unwrap().block_on(async {
/// //     handle_idx_create(command, &app_context).await
/// // });
/// // assert!(matches!(resp, Response::Ok));
/// ```
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

/// Drop an index by its external name.
///
/// Expects a single argument: the external index name. If the argument count is not exactly
/// one (i.e., `command.args.len() != 2` because the first arg is the command name), returns
/// `Response::Error("IDX.DROP requires an index name")`. If the provided name is not valid
/// UTF-8, returns `Response::Error("Invalid index name")`.
///
/// On success, removes the index mapping and any prefix->index association:
/// - Returns `Response::Integer(1)` when an index with the given name was found and removed.
/// - Returns `Response::Integer(0)` when no index with that external name existed.
///
/// # Examples
///
/// ```
/// // Pseudocode / illustrative example:
/// // let cmd = Command::new(vec![b"IDX.DROP".to_vec(), b"my_index".to_vec()]);
/// // let resp = tokio::runtime::Runtime::new().unwrap().block_on(handle_idx_drop(cmd, &ctx));
/// // assert_eq!(resp, Response::Integer(1) | Response::Integer(0));
/// ```
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

/// Returns the list of existing index names.
///
/// Expects no command arguments; if any are provided, returns an error Response.
/// On success returns `Response::MultiBytes` containing the index names as UTF-8 bytes.
///
/// # Examples
///
/// ```
/// // Returns a MultiBytes response with index names (as Vec<Vec<u8>>).
/// let resp = handle_idx_list(command, &ctx).await;
/// match resp {
///     Response::MultiBytes(names) => { /* use names */ }
///     _ => panic!("unexpected response"),
/// }
/// ```
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

/// Applies a JSON set operation to the given database without logging.
///
/// Splits `path` at the first dot into a storage key and an inner JSON path:
/// - `key` is required (the portion before the first dot).
/// - `inner_path` is the optional dot-delimited path inside the JSON value (empty means the root).
///
/// Behavior:
/// - Loads the current value for `key` from `db`. Accepts both `DbValue::Json` and `DbValue::JsonB`; non-JSON or missing entries are treated as `{}`.
/// - If `inner_path` is empty or `"."`, replaces the entire stored value with `value`.
/// - If the pointer for `inner_path` exists, replaces that target with `value`.
/// - If the pointer does not exist, creates nested objects along `inner_path` as needed; fails if a non-object is encountered while creating the path.
/// - Serializes the resulting JSON to bytes and writes it back as `DbValue::JsonB` under `key`.
///
/// Returns an error when:
/// - `path` does not contain a valid key (empty key),
/// - a non-object is encountered while creating nested path elements,
/// - or JSON serialization fails.
///
/// # Examples
///
/// ```
/// use serde_json::json;
/// use serde_json::Value;
///
/// // Assuming `db` is an initialized `crate::types::Db` instance available in your context:
/// // let mut db = crate::types::Db::default();
///
/// // Replace the whole value for key "user:1"
/// let _ = apply_json_set_to_db(&db, "user:1", json!({"name":"alice"}));
///
/// // Set a nested field creating intermediate objects as needed
/// let _ = apply_json_set_to_db(&db, "user:1.profile.age", json!(30));
/// ```
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

/// Delete a member from a JSON value stored under a key, or remove the whole key.
///
/// If `path` is `"<key>"` or `"<key>."` or `"<key>."` (i.e., no inner path or `.`), the entire key is removed from the DB:
/// returns `Ok(1)` if the key existed and was removed, `Ok(0)` if it did not exist.
///
/// If `path` is `"<key>.<a>.<b>...<field>"`, the function attempts to delete the final member (`field`) from the JSON object
/// located at the nested path. On success the JSON is serialized to JSONB bytes and written back as `DbValue::JsonB`.
/// Returns `Ok(1)` if a deletion occurred, or `Ok(0)` if the key or target member was not present.
///
/// Errors:
/// - Returns an error if `path` does not begin with a non-empty key.
/// - Returns an error with message `WRONGTYPE Operation against a non-JSON value` if the value at `key` is not JSON.
///
/// # Examples
///
/// ```no_run
/// use serde_json::json;
///
/// // Assume `db` is an existing `crate::types::Db` populated with:
/// //   key "user:1" => JSON { "profile": { "name": "alice", "age": 30 } }
/// // Deleting a nested field:
/// let _ = crate::commands::apply_json_delete_to_db(&db, "user:1.profile.age")?;
/// // returns Ok(1) and updates stored JSON to { "profile": { "name": "alice" } }
///
/// // Deleting the whole key:
/// let _ = crate::commands::apply_json_delete_to_db(&db, "user:1")?;
/// // returns Ok(1) if the key existed
/// ```
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
