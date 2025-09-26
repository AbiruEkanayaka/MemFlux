use anyhow::{bail, Result};
use lz4_flex;
use rayon::prelude::*;
use std::io::{Read, Write};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::task::{self, JoinHandle};


use crate::config::Config;
use crate::types::{
    Db, DbValue, LogEntry, PersistenceRequest, SerializableDbValue, SnapshotEntry, VersionedValue,
};
use tokio::sync::RwLock;

pub struct PersistenceEngine {
    receiver: mpsc::Receiver<PersistenceRequest>,
    primary_wal_path: String,
    overflow_wal_path: String,
    snapshot_path: String,
    snapshot_temp_path: String,
    wal_size_threshold_bytes: u64,
    db: Db,
}

impl PersistenceEngine {
    pub fn new(config: &Config, db: Db) -> (Self, mpsc::Sender<PersistenceRequest>) {
        let (tx, rx) = mpsc::channel(1024);
        let engine = PersistenceEngine {
            receiver: rx,
            primary_wal_path: config.wal_file.clone(),
            overflow_wal_path: config.wal_overflow_file.clone(),
            snapshot_path: config.snapshot_file.clone(),
            snapshot_temp_path: config.snapshot_temp_file.clone(),
            wal_size_threshold_bytes: config.wal_size_threshold_mb * 1024 * 1024,
            db,
        };
        (engine, tx)
    }

    async fn fsync_loop(file: File, mut receiver: mpsc::Receiver<()>) {
        use tokio::time::{sleep, Duration};
        loop {
            if receiver.recv().await.is_none() {
                break;
            }
            sleep(Duration::from_millis(5)).await;
            if let Err(e) = file.sync_data().await {
                eprintln!("WAL fsync error: {}", e);
            }
        }
    }

    fn spawn_snapshot_task(
        db_clone: Db,
        snapshot_path: String,
        snapshot_temp_path: String,
        wal_to_compact: String,
    ) -> JoinHandle<Result<String>> {
        tokio::spawn(async move {
            let temp_path = snapshot_temp_path.clone();
            let (tx, mut rx) = mpsc::channel::<SnapshotEntry>(128);

            let writer_task = task::spawn_blocking(move || -> anyhow::Result<()> {
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&temp_path)?;
                let writer = std::io::BufWriter::new(file);
                let mut lz4_writer = lz4_flex::frame::FrameEncoder::new(writer);

                while let Some(entry) = rx.blocking_recv() {
                    let data = bincode::serialize(&entry)?;
                    let len = data.len() as u32;
                    lz4_writer.write_all(&len.to_le_bytes())?;
                    lz4_writer.write_all(&data)?;
                }

                let mut writer = lz4_writer.finish()?;
                writer.flush()?;
                writer.get_ref().sync_all()?;
                Ok(())
            });

            let reader_task = async move {
                let keys: Vec<String> = db_clone.iter().map(|item| item.key().clone()).collect();
                for key in keys {
                    if let Some(item) = db_clone.get(&key) {
                        let version_chain = item.value().read().await;
                        if let Some(latest_version) = version_chain.last() {
                            let serializable_value =
                                SerializableDbValue::from_db_value(&latest_version.value).await;
                            let entry = SnapshotEntry {
                                key: item.key().clone(),
                                value: serializable_value,
                            };
                            if tx.send(entry).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            };

            let (writer_result, _) = tokio::join!(writer_task, reader_task);
            writer_result??;

            tokio::fs::rename(&snapshot_temp_path, &snapshot_path).await?;
            println!("Snapshot created successfully at {}", snapshot_path);

            let wal_file_to_truncate = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&wal_to_compact)
                .await?;
            wal_file_to_truncate.set_len(0).await?;

            println!(
                "Compacted WAL file {} has been truncated.",
                &wal_to_compact
            );

            Ok(wal_to_compact)
        })
    }

    pub async fn run(mut self) -> Result<()> {
        // --- Startup: Ensure both WAL files exist to allow for swift switching ---
        if !tokio::fs::try_exists(&self.primary_wal_path).await? {
            File::create(&self.primary_wal_path).await?;
        }
        if !tokio::fs::try_exists(&self.overflow_wal_path).await? {
            File::create(&self.overflow_wal_path).await?;
        }

        let mut active_wal_path = self.primary_wal_path.clone();
        let mut inactive_wal_path = self.overflow_wal_path.clone();

        let mut file = OpenOptions::new()
            .append(true)
            .open(&active_wal_path)
            .await?;
        println!(
            "PersistenceEngine started, writing to {} with async fsync batching.",
            active_wal_path
        );

        let (mut fsync_notify_tx, fsync_notify_rx) = mpsc::channel::<()>(1);
        let mut fsync_task: JoinHandle<()> =
            tokio::spawn(Self::fsync_loop(file.try_clone().await?, fsync_notify_rx));

        let mut write_buffer = Vec::with_capacity(8192);
        let mut compaction_task: Option<JoinHandle<Result<String>>> = None;
        let mut pending_compaction_path: Option<String> = None;

        loop {
            tokio::select! {
                // Branch 1: A compaction task finishes. This can happen even if the DB is idle.
                res = async { compaction_task.as_mut().unwrap().await }, if compaction_task.is_some() => {
                    compaction_task = None; // Consume the task before handling the result
                    match res? { // res is Result<Result<String, Error>, JoinError>
                        Ok(truncated_wal_path) => {
                            if truncated_wal_path == self.primary_wal_path {
                                // Phase 1 complete: Primary WAL was just compacted. We were writing to overflow.
                                // Now, switch back to primary immediately.
                                println!("Primary WAL compacted. Switching back to primary WAL.");
                                file.sync_all().await?;
                                fsync_task.abort();

                                std::mem::swap(&mut active_wal_path, &mut inactive_wal_path);
                                file = OpenOptions::new().append(true).open(&active_wal_path).await?;
                                println!("Switched writes to new WAL: {}", active_wal_path);

                                let (new_tx, new_rx) = mpsc::channel::<()>(1);
                                fsync_notify_tx = new_tx;
                                fsync_task = tokio::spawn(Self::fsync_loop(file.try_clone().await?, new_rx));

                                // The overflow WAL (now inactive) contains new data and needs to be compacted next.
                                pending_compaction_path = Some(inactive_wal_path.clone());
                            } else {
                                // Phase 2 complete: Overflow WAL was just compacted. Cycle is complete.
                                println!("Overflow WAL compacted. Compaction cycle complete.");
                            }
                        }
                        Err(e) => {
                            eprintln!("FATAL: Background snapshot task failed: {}. Shutting down persistence engine.", e);
                            bail!("Background snapshot failed: {}", e);
                        }
                    }
                },

                // Branch 2: A new write request comes in.
                maybe_req = self.receiver.recv() => {
                    let first_req = match maybe_req {
                        Some(req) => req,
                        None => break, // Channel closed, shutdown.
                    };

                    let mut batch = vec![first_req];
                    // Stop batching if we see a sync request or a full durability request
                    while batch.len() < 256 {
                        if let Ok(req) = self.receiver.try_recv() {
                            let stop_batching = match &req {
                                PersistenceRequest::Sync(_) => true,
                                PersistenceRequest::Log(log_req) => log_req.durability == crate::config::DurabilityLevel::Full,
                            };
                            batch.push(req);
                            if stop_batching {
                                break;
                            }
                        } else {
                            break;
                        }
                    }

                    let mut log_requests = Vec::new();
                    let mut sync_requests = Vec::new();
                    let mut highest_durability = crate::config::DurabilityLevel::None;

                    write_buffer.clear();

                    for req in batch {
                        match req {
                            PersistenceRequest::Log(log_req) => {
                                if log_req.durability > highest_durability {
                                    highest_durability = log_req.durability.clone();
                                }
                                match bincode::serialize(&log_req.entry) {
                                    Ok(data) => {
                                        let len = data.len() as u32;
                                        write_buffer.extend_from_slice(&len.to_le_bytes());
                                        write_buffer.extend_from_slice(&data);
                                        log_requests.push(log_req);
                                    }
                                    Err(e) => {
                                        let _ = log_req.ack.send(Err(format!("Serialization failed: {}", e)));
                                    }
                                }
                            }
                            PersistenceRequest::Sync(sync_ack) => {
                                highest_durability = crate::config::DurabilityLevel::Full;
                                sync_requests.push(sync_ack);
                            }
                        }
                    }

                    let write_result = if !write_buffer.is_empty() {
                        file.write_all(&write_buffer).await
                    } else {
                        Ok(())
                    };

                    let final_result = if write_result.is_ok() {
                        match highest_durability {
                            crate::config::DurabilityLevel::Full => {
                                file.sync_all().await
                            }
                            crate::config::DurabilityLevel::Fsync | crate::config::DurabilityLevel::None => {
                                // For both Fsync and None, we acknowledge after the OS write.
                                // The background fsync loop will handle flushing to disk.
                                let _ = fsync_notify_tx.try_send(());
                                Ok(())
                            }
                        }
                    } else {
                        write_result
                    };

                    let result_for_ack = final_result.map_err(|e| e.to_string());

                    for log_req in log_requests {
                        let _ = log_req.ack.send(result_for_ack.clone());
                    }
                    for sync_req in sync_requests {
                        let _ = sync_req.send(result_for_ack.clone());
                    }

                    if let Err(e) = &result_for_ack {
                        bail!("Failed to write to WAL: {}", e);
                    }

                    // Check if we need to trigger the start of a new compaction cycle
                    if active_wal_path == self.primary_wal_path
                        && file.metadata().await?.len() > self.wal_size_threshold_bytes
                        && compaction_task.is_none()
                        && pending_compaction_path.is_none()
                    {
                        println!(
                            "WAL size exceeds threshold. Switching to overflow WAL and triggering compaction."
                        );

                        file.sync_all().await?;
                        fsync_task.abort();

                        std::mem::swap(&mut active_wal_path, &mut inactive_wal_path);
                        println!("Switching writes to new WAL: {}", active_wal_path);

                        file = OpenOptions::new()
                            .write(true)
                            .truncate(true)
                            .open(&active_wal_path)
                            .await?;

                        let (new_tx, new_rx) = mpsc::channel::<()>(1);
                        fsync_notify_tx = new_tx;
                        fsync_task = tokio::spawn(Self::fsync_loop(file.try_clone().await?, new_rx));

                        let wal_to_compact = inactive_wal_path.clone();
                        compaction_task = Some(Self::spawn_snapshot_task(
                            self.db.clone(),
                            self.snapshot_path.clone(),
                            self.snapshot_temp_path.clone(),
                            wal_to_compact,
                        ));
                    }
                }
            }

            // After the select, check if a pending compaction needs to be started.
            // This is crucial for kicking off the second phase of the cycle.
            if let Some(wal_to_compact) = pending_compaction_path.take() {
                if compaction_task.is_none() {
                    println!("Starting compaction for overflow WAL: {}", wal_to_compact);
                    compaction_task = Some(Self::spawn_snapshot_task(
                        self.db.clone(),
                        self.snapshot_path.clone(),
                        self.snapshot_temp_path.clone(),
                        wal_to_compact,
                    ));
                } else {
                    // This shouldn't happen, but as a safeguard, put it back.
                    pending_compaction_path = Some(wal_to_compact);
                }
            }
        }
        Ok(())
    }
}


pub async fn load_db_from_disk(
    snapshot_path: &str,
    primary_wal_path: &str,
    overflow_wal_path: &str,
) -> Result<Db> {
    let db: Db = std::sync::Arc::new(dashmap::DashMap::new());
    if let Err(e) = load_from_snapshot(snapshot_path, &db).await {
        if e.downcast_ref::<std::io::Error>()
            .map_or(true, |io_err| io_err.kind() != std::io::ErrorKind::NotFound)
        {
            eprintln!(
                "Warning: Could not load snapshot file '{}': {}. Proceeding with WAL only.",
                snapshot_path, e
            );
        }
    }
    // It's important to replay the primary WAL first, then the overflow,
    // to ensure the correct order of operations is restored.
    if let Err(e) = replay_wal(primary_wal_path, &db).await {
        eprintln!(
            "Error replaying WAL file '{}': {}. State may be incomplete.",
            primary_wal_path, e
        );
    }
    if let Err(e) = replay_wal(overflow_wal_path, &db).await {
        eprintln!(
            "Error replaying WAL file '{}': {}. State may be incomplete.",
            overflow_wal_path, e
        );
    }
    Ok(db)
}

async fn load_from_snapshot(snapshot_path: &str, db: &Db) -> Result<()> {
    let snapshot_path_owned = snapshot_path.to_string();
    let db_clone = db.clone();

    let count = task::spawn_blocking(move || -> anyhow::Result<i32> {
        let file = match std::fs::File::open(&snapshot_path_owned) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e.into()),
        };
        
        let reader = std::io::BufReader::new(file);
        let mut lz4_reader = lz4_flex::frame::FrameDecoder::new(reader);
        let mut total_count = 0;
        const BATCH_SIZE: usize = 4096;

        loop {
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {
                let mut len_buf = [0u8; 4];
                match lz4_reader.read_exact(&mut len_buf) {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e.into()),
                }
                let entry_len = u32::from_le_bytes(len_buf) as usize;
                let mut entry_buf = vec![0u8; entry_len];
                lz4_reader.read_exact(&mut entry_buf)?;
                let entry: SnapshotEntry = bincode::deserialize(&entry_buf)?;
                batch.push(entry);
            }

            if batch.is_empty() {
                break;
            }

            let batch_count = batch.len();
            batch.into_par_iter().for_each(|entry| {
                let db_value = entry.value.into_db_value();
                let versioned_value = VersionedValue {
                    value: db_value,
                    creator_txid: 0, // Pre-existing data
                    expirer_txid: 0,
                };
                db_clone.insert(entry.key, RwLock::new(vec![versioned_value]));
            });
            total_count += batch_count as i32;
        }
        
        Ok(total_count)
    }).await??;

    if count > 0 {
        println!(
            "Successfully loaded {} entries from snapshot file: {}",
            count, snapshot_path
        );
    }

    Ok(())
}

async fn replay_wal(wal_path: &str, db: &Db) -> Result<()> {
    use std::collections::{HashSet, VecDeque};
    let file = match File::open(wal_path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    println!("Replaying entries from WAL file: {}", wal_path);
    let mut reader = BufReader::new(file);
    let mut count = 0;
    let mut len_buf = [0u8; 4];
    loop {
        match reader.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let entry_len = u32::from_le_bytes(len_buf) as usize;
        let mut entry_buf = vec![0u8; entry_len];
        reader.read_exact(&mut entry_buf).await?;
        let entry: LogEntry = match bincode::deserialize(&entry_buf) {
            Ok(e) => e,
            Err(e) => {
                eprintln!("WAL is corrupt. Stopping recovery. Error: {}", e);
                break;
            }
        };
        count += 1;
        match entry {
            LogEntry::SetBytes { key, value } => {
                let new_version = VersionedValue {
                    value: DbValue::Bytes(value),
                    creator_txid: 0,
                    expirer_txid: 0,
                };
                db.insert(key, RwLock::new(vec![new_version]));
            }
            LogEntry::SetJsonB { key, value } => {
                let new_version = VersionedValue {
                    value: DbValue::JsonB(value),
                    creator_txid: 0,
                    expirer_txid: 0,
                };
                db.insert(key, RwLock::new(vec![new_version]));
            }
            LogEntry::Delete { key } => {
                db.remove(&key);
            }
            LogEntry::JsonSet { path, value } => {
                // This is a simplified version for WAL replay.
                // It overwrites the latest version.
                let mut parts = path.splitn(2, '.');
                if let Some(key) = parts.next() {
                    let value: serde_json::Value = serde_json::from_str(&value).unwrap_or(serde_json::Value::Null);
                    let entry = db.entry(key.to_string()).or_insert_with(|| {
                        RwLock::new(vec![VersionedValue {
                            value: DbValue::JsonB(b"{}".to_vec()),
                            creator_txid: 0,
                            expirer_txid: 0,
                        }])
                    });
                    let mut version_chain = entry.value().write().await;
                    if let Some(latest_version) = version_chain.last_mut() {
                        let mut current_val: serde_json::Value = match &latest_version.value {
                            DbValue::Json(v) => v.clone(),
                            DbValue::JsonB(b) => serde_json::from_slice(&b).unwrap_or_default(),
                            _ => serde_json::json!({}),
                        };

                        let inner_path = parts.next().unwrap_or("");
                        let pointer = crate::commands::json_path_to_pointer(inner_path);
                        if pointer.is_empty() {
                            current_val = value;
                        } else if let Some(target) = current_val.pointer_mut(&pointer) {
                            *target = value;
                        } // Simplified: does not create nested paths

                        if let Ok(new_bytes) = serde_json::to_vec(&current_val) {
                            latest_version.value = DbValue::JsonB(new_bytes);
                        }
                    }
                }
            }
            LogEntry::JsonDelete { path } => {
                let mut parts = path.splitn(2, '.');
                if let Some(key) = parts.next() {
                    if let Some(entry) = db.get(key) {
                        let mut version_chain = entry.value().write().await;
                        if let Some(latest_version) = version_chain.last_mut() {
                             let mut current_val: serde_json::Value = match &latest_version.value {
                                DbValue::Json(v) => v.clone(),
                                DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_default(),
                                _ => continue,
                            };
                            let inner_path = parts.next().unwrap_or("");
                            if inner_path.is_empty() || inner_path == "." {
                                // This would mean deleting the whole key, but we handle that with LogEntry::Delete
                            } else {
                                let mut pointer_parts: Vec<&str> = inner_path.split('.').collect();
                                let final_key = pointer_parts.pop().unwrap();
                                let parent_pointer = crate::commands::json_path_to_pointer(&pointer_parts.join("."));

                                if let Some(target) = current_val.pointer_mut(&parent_pointer) {
                                    if let Some(obj) = target.as_object_mut() {
                                        obj.remove(final_key);
                                    }
                                }
                            }
                            if let Ok(new_bytes) = serde_json::to_vec(&current_val) {
                                latest_version.value = DbValue::JsonB(new_bytes);
                            }
                        }
                    }
                }
            }
            LogEntry::LPush { key, values } => {
                let entry = db.entry(key).or_insert_with(|| {
                    RwLock::new(vec![VersionedValue {
                        value: DbValue::List(RwLock::new(VecDeque::new())),
                        creator_txid: 0,
                        expirer_txid: 0,
                    }])
                });
                if let Some(latest_version) = entry.value().write().await.last_mut() {
                    if let DbValue::List(list_lock) = &mut latest_version.value {
                        let mut list = list_lock.write().await;
                        for v in values {
                            list.push_front(v);
                        }
                    }
                }
            }
            LogEntry::RPush { key, values } => {
                let entry = db.entry(key).or_insert_with(|| {
                    RwLock::new(vec![VersionedValue {
                        value: DbValue::List(RwLock::new(VecDeque::new())),
                        creator_txid: 0,
                        expirer_txid: 0,
                    }])
                });
                if let Some(latest_version) = entry.value().write().await.last_mut() {
                    if let DbValue::List(list_lock) = &mut latest_version.value {
                        let mut list = list_lock.write().await;
                        for v in values {
                            list.push_back(v);
                        }
                    }
                }
            }
            LogEntry::LPop { key, count } => {
                if let Some(entry) = db.get(&key) {
                    if let Some(latest_version) = entry.value().write().await.last_mut() {
                        if let DbValue::List(list_lock) = &mut latest_version.value {
                            let mut list = list_lock.write().await;
                            for _ in 0..count {
                                if list.pop_front().is_none() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            LogEntry::RPop { key, count } => {
                if let Some(entry) = db.get(&key) {
                    if let Some(latest_version) = entry.value().write().await.last_mut() {
                        if let DbValue::List(list_lock) = &mut latest_version.value {
                            let mut list = list_lock.write().await;
                            for _ in 0..count {
                                if list.pop_back().is_none() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            LogEntry::SAdd { key, members } => {
                let entry = db.entry(key).or_insert_with(|| {
                    RwLock::new(vec![VersionedValue {
                        value: DbValue::Set(RwLock::new(HashSet::new())),
                        creator_txid: 0,
                        expirer_txid: 0,
                    }])
                });
                if let Some(latest_version) = entry.value().write().await.last_mut() {
                    if let DbValue::Set(set_lock) = &mut latest_version.value {
                        let mut set = set_lock.write().await;
                        for m in members {
                            set.insert(m);
                        }
                    }
                }
            }
            LogEntry::SRem { key, members } => {
                if let Some(entry) = db.get(&key) {
                    if let Some(latest_version) = entry.value().write().await.last_mut() {
                        if let DbValue::Set(set_lock) = &mut latest_version.value {
                            let mut set = set_lock.write().await;
                            for m in members {
                                set.remove(&m);
                            }
                        }
                    }
                }
            }
            LogEntry::RenameTable { old_name, new_name } => {
                // 1. Rename data keys
                let old_prefix = format!("{}:", old_name);
                let new_prefix = format!("{}:", new_name);
                let keys_to_rename: Vec<String> = db
                    .iter()
                    .filter(|entry| entry.key().starts_with(&old_prefix))
                    .map(|entry| entry.key().clone())
                    .collect();

                for old_key in keys_to_rename {
                    if let Some((k, v)) = db.remove(&old_key) {
                        let new_key = k.replacen(&old_prefix, &new_prefix, 1);
                        db.insert(new_key, v);
                    }
                }

                // 2. Rename schema key and update its content
                let old_schema_key = format!("{}{}", crate::schema::SCHEMA_PREFIX, old_name);
                if let Some((_, schema_val_rwlock)) = db.remove(&old_schema_key) {
                    let mut version_chain = schema_val_rwlock.write().await;
                    if let Some(latest_version) = version_chain.last_mut() {
                        if let DbValue::Bytes(bytes) = &latest_version.value {
                            if let Ok(mut schema) =
                                serde_json::from_slice::<crate::schema::VirtualSchema>(bytes)
                            {
                                schema.table_name = new_name.clone();
                                if let Ok(new_bytes) = serde_json::to_vec(&schema) {
                                    let new_schema_key =
                                        format!("{}{}", crate::schema::SCHEMA_PREFIX, new_name);
                                    let new_version = VersionedValue {
                                        value: DbValue::Bytes(new_bytes),
                                        creator_txid: 0,
                                        expirer_txid: 0,
                                    };
                                    db.insert(new_schema_key, RwLock::new(vec![new_version]));
                                }
                            }
                        }
                    }
                }
            }
            _ => { /* Do nothing for transaction-related log entries during replay */ }
        }
    }
    if count > 0 {
        println!("Successfully replayed {} entries from WAL.", count);
    }
    Ok(())
}