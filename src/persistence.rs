use anyhow::{anyhow, bail, Result};
use std::io::SeekFrom;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::commands::{apply_json_delete_to_db, apply_json_set_to_db};
use crate::config::Config;
use crate::types::*;

pub struct PersistenceEngine {
    receiver: mpsc::Receiver<LogRequest>,
    wal_path: String,
    snapshot_path: String,
    snapshot_temp_path: String,
    wal_size_threshold_bytes: u64,
    db: Db,
}

impl PersistenceEngine {
    pub fn new(config: &Config, db: Db) -> (Self, Logger) {
        let (tx, rx) = mpsc::channel(1024);
        let engine = PersistenceEngine {
            receiver: rx,
            wal_path: config.wal_file.clone(),
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

    async fn create_snapshot(&self) -> Result<()> {
        let mut temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.snapshot_temp_path)
            .await?;
        let mut writer = tokio::io::BufWriter::new(&mut temp_file);

        for item in self.db.iter() {
            let serializable_value = SerializableDbValue::from_db_value(item.value()).await;
            let entry = SnapshotEntry {
                key: item.key().clone(),
                value: serializable_value,
            };
            let mut data = serde_json::to_vec(&entry)?;
            data.push(b'\n');
            writer.write_all(&data).await?;
        }
        writer.flush().await?;
        temp_file.sync_all().await?;
        drop(temp_file);
        tokio::fs::rename(&self.snapshot_temp_path, &self.snapshot_path).await?;
        Ok(())
    }

    pub async fn run(mut self) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_path)
            .await?;
        println!(
            "PersistenceEngine started, writing to {} with async fsync batching.",
            self.wal_path
        );
        let (mut fsync_notify_tx, fsync_notify_rx) = mpsc::channel::<()>(1);
        let mut fsync_task: JoinHandle<()> =
            tokio::spawn(Self::fsync_loop(file.try_clone().await?, fsync_notify_rx));
        let mut write_buffer = Vec::with_capacity(8192);

        while let Some(first_req) = self.receiver.recv().await {
            write_buffer.clear();
            let mut batch = vec![first_req];
            while batch.len() < 256 {
                if let Ok(req) = self.receiver.try_recv() {
                    batch.push(req);
                } else {
                    break;
                }
            }
            for req in &batch {
                let data = bincode::serialize(&req.entry)?;
                let len = data.len() as u32;
                write_buffer.extend_from_slice(&len.to_le_bytes());
                write_buffer.extend_from_slice(&data);
            }
            let write_result = file.write_all(&write_buffer).await.map_err(|e| anyhow!(e));
            let _ = fsync_notify_tx.try_send(());
            let write_result_for_ack = write_result.map_err(|e| e.to_string());
            for req in batch {
                let _ = req.ack.send(write_result_for_ack.clone());
            }
            if write_result_for_ack.is_err() {
                bail!(
                    "Failed to write to WAL: {}",
                    write_result_for_ack.unwrap_err()
                );
            }
            if file.metadata().await?.len() > self.wal_size_threshold_bytes {
                println!("WAL size exceeds threshold. Triggering snapshot and compaction...");
                file.sync_all().await?;
                fsync_task.abort();
                if let Err(e) = self.create_snapshot().await {
                    eprintln!("FATAL: Snapshot creation failed: {}. Shutting down persistence engine to prevent data loss.", e);
                    bail!("Snapshot creation failed: {}", e);
                }
                println!("Snapshot created successfully at {}", self.snapshot_path);
                file.set_len(0).await?;
                file.seek(SeekFrom::Start(0)).await?;
                println!("WAL file {} has been truncated.", self.wal_path);
                let (new_tx, new_rx) = mpsc::channel::<()>(1);
                fsync_notify_tx = new_tx;
                fsync_task = tokio::spawn(Self::fsync_loop(file.try_clone().await?, new_rx));
                println!("Fsync task restarted for new WAL.");
            }
        }
        Ok(())
    }
}


pub async fn load_db_from_disk(snapshot_path: &str, wal_path: &str) -> Result<Db> {
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
    if let Err(e) = replay_wal(wal_path, &db).await {
        eprintln!(
            "Error replaying WAL file '{}': {}. State may be incomplete.",
            wal_path, e
        );
    }
    Ok(db)
}

async fn load_from_snapshot(snapshot_path: &str, db: &Db) -> Result<()> {
    let file = match File::open(snapshot_path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    println!(
        "Loading database state from snapshot file: {}",
        snapshot_path
    );
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut count = 0;
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            break;
        }
        let entry: SnapshotEntry = serde_json::from_str(&line)?;
        db.insert(entry.key, entry.value.into_db_value());
        count += 1;
    }
    println!("Successfully loaded {} entries from snapshot.", count);
    Ok(())
}

async fn replay_wal(wal_path: &str, db: &Db) -> Result<()> {
    use std::collections::{HashSet, VecDeque};
    use tokio::sync::RwLock;
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
                db.insert(key, DbValue::Bytes(value));
            }
            LogEntry::Delete { key } => {
                db.remove(&key);
            }
            LogEntry::JsonSet { path, value } => {
                let value: serde_json::Value = serde_json::from_str(&value).unwrap_or(serde_json::Value::Null);
                let _ = apply_json_set_to_db(db, &path, value);
            }
            LogEntry::JsonDelete { path } => {
                let _ = apply_json_delete_to_db(db, &path);
            }
            LogEntry::LPush { key, values } => {
                let entry = db
                    .entry(key)
                    .or_insert_with(|| DbValue::List(RwLock::new(VecDeque::new())));
                if let DbValue::List(list_lock) = entry.value() {
                    let mut list = list_lock.write().await;
                    for v in values {
                        list.push_front(v);
                    }
                }
            }
            LogEntry::RPush { key, values } => {
                let entry = db
                    .entry(key)
                    .or_insert_with(|| DbValue::List(RwLock::new(VecDeque::new())));
                if let DbValue::List(list_lock) = entry.value() {
                    let mut list = list_lock.write().await;
                    for v in values {
                        list.push_back(v);
                    }
                }
            }
            LogEntry::LPop { key, count } => {
                if let Some(mut entry) = db.get_mut(&key) {
                    if let DbValue::List(list_lock) = entry.value_mut() {
                        let mut list = list_lock.write().await;
                        for _ in 0..count {
                            if list.pop_front().is_none() {
                                break;
                            }
                        }
                    }
                }
            }
            LogEntry::RPop { key, count } => {
                if let Some(mut entry) = db.get_mut(&key) {
                    if let DbValue::List(list_lock) = entry.value_mut() {
                        let mut list = list_lock.write().await;
                        for _ in 0..count {
                            if list.pop_back().is_none() {
                                break;
                            }
                        }
                    }
                }
            }
            LogEntry::SAdd { key, members } => {
                let entry = db
                    .entry(key)
                    .or_insert_with(|| DbValue::Set(RwLock::new(HashSet::new())));
                if let DbValue::Set(set_lock) = entry.value() {
                    let mut set = set_lock.write().await;
                    for m in members {
                        set.insert(m);
                    }
                }
            }
            LogEntry::SRem { key, members } => {
                if let Some(mut entry) = db.get_mut(&key) {
                    if let DbValue::Set(set_lock) = entry.value_mut() {
                        let mut set = set_lock.write().await;
                        for m in members {
                            set.remove(&m);
                        }
                    }
                }
            }
        }
    }
    if count > 0 {
        println!("Successfully replayed {} entries from WAL.", count);
    }
    Ok(())
}









