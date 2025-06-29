use anyhow::{anyhow, bail, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt;
use std::io::SeekFrom;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::JoinHandle;
use bincode;


pub enum DbValue {
    Json(Value),
    List(RwLock<VecDeque<Vec<u8>>>),
    Set(RwLock<HashSet<Vec<u8>>>),
    Bytes(Vec<u8>),
}

/// Custom Debug implementation to avoid printing the lock internals.
impl fmt::Debug for DbValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbValue::Json(v) => write!(f, "Json({:?})", v),
            DbValue::List(_) => write!(f, "List(<RwLock>)"),
            DbValue::Set(_) => write!(f, "Set(<RwLock>)"),
            DbValue::Bytes(b) => write!(f, "Bytes({:?})", String::from_utf8_lossy(b)),
        }
    }
}

// --- Persistence Engine & WAL (Updated for Multi-Model) ---

const WAL_FILE: &str = "memflux.wal";
const SNAPSHOT_FILE: &str = "memflux.snapshot";
const SNAPSHOT_TEMP_FILE: &str = "memflux.snapshot.tmp";
const WAL_SIZE_THRESHOLD: u64 = 16 * 1024 * 1024; // 16 MB

/// A serializable representation of DbValue, used for snapshotting.
#[derive(Serialize, Deserialize, Debug)]
pub enum SerializableDbValue {
    Json(Value),
    List(VecDeque<Vec<u8>>),
    Set(Vec<Vec<u8>>), // HashSet is serialized as a Vec
    Bytes(Vec<u8>),
}

impl SerializableDbValue {
    /// Conversion from the in-memory type to the on-disk type.
    pub async fn from_db_value(db_value: &DbValue) -> Self {
        match db_value {
            DbValue::Json(v) => SerializableDbValue::Json(v.clone()),
            DbValue::Bytes(b) => SerializableDbValue::Bytes(b.clone()),
            DbValue::List(lock) => {
                let list = lock.read().await;
                SerializableDbValue::List(list.clone())
            }
            DbValue::Set(lock) => {
                let set = lock.read().await;
                SerializableDbValue::Set(set.iter().cloned().collect())
            }
        }
    }

    /// Conversion from the on-disk type to the in-memory type.
    pub fn into_db_value(self) -> DbValue {
        match self {
            SerializableDbValue::Json(v) => DbValue::Json(v),
            SerializableDbValue::Bytes(b) => DbValue::Bytes(b),
            SerializableDbValue::List(v) => DbValue::List(RwLock::new(v)),
            SerializableDbValue::Set(v) => DbValue::Set(RwLock::new(v.into_iter().collect())),
        }
    }
}

/// Defines the operations that can be written to the Write-Ahead Log.
#[derive(Serialize, Deserialize, Debug)]
enum LogEntry {
    // General Commands
    SetBytes { key: String, value: Vec<u8> },
    Delete { key: String },
    // JSON Commands
    JsonSet { path: String, value: Value },
    JsonDelete { path: String },
    // List Commands
    LPush { key: String, values: Vec<Vec<u8>> },
    RPush { key: String, values: Vec<Vec<u8>> },
    LPop { key: String, count: usize },
    RPop { key: String, count: usize },
    // Set Commands
    SAdd { key: String, members: Vec<Vec<u8>> },
    SRem { key: String, members: Vec<Vec<u8>> },
}

/// Defines the structure of an entry in the snapshot file.
#[derive(Serialize, Deserialize, Debug)]
struct SnapshotEntry {
    key: String,
    value: SerializableDbValue,
}

struct LogRequest {
    entry: LogEntry,
    ack: oneshot::Sender<Result<(), String>>,
}

type Logger = mpsc::Sender<LogRequest>;

struct PersistenceEngine {
    receiver: mpsc::Receiver<LogRequest>,
    wal_path: String,
    snapshot_path: String,
    db: Db, // Has access to the main DB for snapshotting
}

impl PersistenceEngine {
    fn new(wal_path: &str, snapshot_path: &str, db: Db) -> (Self, Logger) {
        let (tx, rx) = mpsc::channel(1024);
        let engine = PersistenceEngine {
            receiver: rx,
            wal_path: wal_path.to_string(),
            snapshot_path: snapshot_path.to_string(),
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

    /// Creates a snapshot of the current DB state.
    async fn create_snapshot(&self) -> Result<()> {
        let mut temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(SNAPSHOT_TEMP_FILE)
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
        tokio::fs::rename(SNAPSHOT_TEMP_FILE, self.snapshot_path.as_str()).await?;
        Ok(())
    }

    /// Main loop: batch writes, fsync in background, and trigger snapshots.
    async fn run(mut self) -> Result<()> {
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
                bail!("Failed to write to WAL: {}", write_result_for_ack.unwrap_err());
            }
            if file.metadata().await?.len() > WAL_SIZE_THRESHOLD {
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

/// Loads database state from snapshot and WAL files on startup.
async fn load_db_from_disk(snapshot_path: &str, wal_path: &str) -> Result<Db> {
    let db: Db = Arc::new(DashMap::new());
    if let Err(e) = load_from_snapshot(snapshot_path, &db).await {
        if e.downcast_ref::<std::io::Error>().map_or(true, |io_err| io_err.kind() != std::io::ErrorKind::NotFound) {
            eprintln!("Warning: Could not load snapshot file '{}': {}. Proceeding with WAL only.", snapshot_path, e);
        }
    }
    if let Err(e) = replay_wal(wal_path, &db).await {
        eprintln!("Error replaying WAL file '{}': {}. State may be incomplete.", wal_path, e);
    }
    Ok(db)
}

/// Populates the DB from a snapshot file.
async fn load_from_snapshot(snapshot_path: &str, db: &Db) -> Result<()> {
    let file = match File::open(snapshot_path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    println!("Loading database state from snapshot file: {}", snapshot_path);
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut count = 0;
    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 { break; }
        let entry: SnapshotEntry = serde_json::from_str(&line)?;
        db.insert(entry.key, entry.value.into_db_value());
        count += 1;
    }
    println!("Successfully loaded {} entries from snapshot.", count);
    Ok(())
}

/// Applies log entries from the WAL file to an existing database.
async fn replay_wal(wal_path: &str, db: &Db) -> Result<()> {
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
        // Read 4-byte length prefix
        match reader.read_exact(&mut len_buf).await {
            Ok(_) => {},
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
                let _ = apply_json_set_to_db(db, &path, value);
            }
            LogEntry::JsonDelete { path } => {
                let _ = apply_json_delete_to_db(db, &path);
            }
            LogEntry::LPush { key, values } => {
                let entry = db.entry(key).or_insert_with(|| DbValue::List(RwLock::new(VecDeque::new())));
                if let DbValue::List(list_lock) = entry.value() {
                    let mut list = list_lock.write().await;
                    for v in values.into_iter().rev() {
                        list.push_front(v);
                    }
                }
            }
            LogEntry::RPush { key, values } => {
                let entry = db.entry(key).or_insert_with(|| DbValue::List(RwLock::new(VecDeque::new())));
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
                            if list.pop_front().is_none() { break; }
                        }
                    }
                }
            }
            LogEntry::RPop { key, count } => {
                if let Some(mut entry) = db.get_mut(&key) {
                    if let DbValue::List(list_lock) = entry.value_mut() {
                        let mut list = list_lock.write().await;
                        for _ in 0..count {
                            if list.pop_back().is_none() { break; }
                        }
                    }
                }
            }
            LogEntry::SAdd { key, members } => {
                let entry = db.entry(key).or_insert_with(|| DbValue::Set(RwLock::new(HashSet::new())));
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

// --- Secondary Indexing Foundation ---

/// Maps an indexed value (e.g., age 30) to a set of DB keys ("user:1", "user:42")
type Index = RwLock<BTreeMap<String, HashSet<String>>>;

/// Manages all indexes.
#[derive(Default, Debug)]
pub struct IndexManager {
    /// Maps an index name (e.g., "user:*|profile.age") to the index data.
    indexes: DashMap<String, Arc<Index>>,
    /// Maps a key prefix (e.g., "user:*") to a list of index names that apply to it.
    prefix_to_indexes: DashMap<String, Vec<String>>,
}

impl IndexManager {
    /// Finds all index names that could apply to a given key.
    fn get_indexes_for_key(&self, key: &str) -> Vec<(String, String)> {
        let mut applicable = Vec::new();
        for item in self.prefix_to_indexes.iter() {
            let prefix = item.key();
            let pattern = prefix.strip_suffix('*').unwrap_or(prefix);
            if key.starts_with(pattern) {
                for index_name in item.value() {
                    // index name is "prefix|json.path"
                    if let Some(json_path) = index_name.split('|').nth(1) {
                         applicable.push((index_name.clone(), json_path.to_string()));
                    }
                }
            }
        }
        applicable
    }

    /// Removes a key from all relevant indexes based on its old value.
    async fn remove_key_from_indexes(&self, key: &str, old_value: &Value) {
        for (index_name, json_path) in self.get_indexes_for_key(key) {
            if let Some(index) = self.indexes.get(&index_name) {
                if let Some(old_indexed_val) = old_value.pointer(&json_path_to_pointer(&json_path)) {
                    let index_key = serde_json::to_string(old_indexed_val).unwrap_or_default();
                    let mut index_data = index.write().await;
                    if let Some(keys_set) = index_data.get_mut(&index_key) {
                        keys_set.remove(key);
                        if keys_set.is_empty() {
                            index_data.remove(&index_key);
                        }
                    }
                }
            }
        }
    }

    /// Adds a key to all relevant indexes based on its new value.
    async fn add_key_to_indexes(&self, key: &str, new_value: &Value) {
        for (index_name, json_path) in self.get_indexes_for_key(key) {
            if let Some(index) = self.indexes.get(&index_name) {
                if let Some(new_indexed_val) = new_value.pointer(&json_path_to_pointer(&json_path)) {
                    let index_key = serde_json::to_string(new_indexed_val).unwrap_or_default();
                    let mut index_data = index.write().await;
                    index_data
                        .entry(index_key)
                        .or_default()
                        .insert(key.to_string());
                }
            }
        }
    }
}

// --- Type Aliases & Core Structs ---

type Db = Arc<DashMap<String, DbValue>>;
/// A context struct to hold shared state, making it easier to pass around.
#[derive(Clone)]
struct AppContext {
    db: Db,
    logger: Logger,
    index_manager: Arc<IndexManager>,
}

/// A more generic command structure.
struct Command {
    name: String,
    args: Vec<Vec<u8>>,
}

enum Response {
    Ok,
    Value(Value),
    Bytes(Vec<u8>),
    MultiBytes(Vec<Vec<u8>>),
    Integer(i64),
    Nil,
    Error(String),
}

impl Response {
    fn into_protocol_format(self) -> Vec<u8> {
        match self {
            Response::Ok => b"+OK\r\n".to_vec(),
            Response::Value(v) => {
                let s = v.to_string();
                format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
            }
            Response::Bytes(b) => {
                let mut response = format!("${}\r\n", b.len()).into_bytes();
                response.extend_from_slice(&b);
                response.extend_from_slice(b"\r\n");
                response
            }
            Response::MultiBytes(vals) => {
                let mut response = format!("*{}\r\n", vals.len()).into_bytes();
                for v in vals {
                    response.extend(Response::Bytes(v).into_protocol_format());
                }
                response
            }
            Response::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            Response::Nil => b"$-1\r\n".to_vec(),
            Response::Error(e) => format!("-ERR {}\r\n", e).into_bytes(),
        }
    }
}

// --- Main Server Logic ---

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Load DB state from disk (snapshot then WAL)
    let db = load_db_from_disk(SNAPSHOT_FILE, WAL_FILE).await?;
    println!("Database loaded with {} top-level keys.", db.len());

    // 2. Set up the persistence engine
    let (persistence_engine, logger) = PersistenceEngine::new(WAL_FILE, SNAPSHOT_FILE, db.clone());
    tokio::spawn(async move {
        if let Err(e) = persistence_engine.run().await {
            eprintln!("Fatal error in persistence engine: {}", e);
            std::process::exit(1);
        }
    });

    // 3. Set up the Index Manager and Application Context
    let index_manager = Arc::new(IndexManager::default());
    let app_context = Arc::new(AppContext {
        db,
        logger,
        index_manager,
    });
    
    // 4. Start the server
    let listener = TcpListener::bind("127.0.0.1:6380").await?;
    println!("MemFlux (RESP Protocol) listening on 127.0.0.1:6380");
    loop {
        let (stream, _) = listener.accept().await?;
        let context_clone = app_context.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, context_clone).await {
                if e.downcast_ref::<std::io::Error>().map_or(true, |io_err| io_err.kind() != std::io::ErrorKind::BrokenPipe) {
                     eprintln!("Connection error: {:?}", e);
                }
            }
        });
    }
}

/// Handles a single client connection.
async fn handle_connection(mut stream: TcpStream, ctx: Arc<AppContext>) -> Result<()> {
    let (reader, mut writer) = stream.split();
    let mut buf_reader = BufReader::new(reader);
    loop {
        match parse_command_from_stream(&mut buf_reader).await {
            Ok(Some(command)) => {
                // Special-case SQL for streaming
                if command.name == "SQL" {
                    let sql = match command.args[1..]
                        .iter()
                        .map(|arg| String::from_utf8(arg.clone()))
                        .collect::<Result<Vec<_>, _>>()
                    {
                        Ok(parts) => parts.join(" "),
                        Err(_) => {
                            let response = Response::Error("Invalid UTF-8 in SQL query".to_string());
                            writer.write_all(&response.into_protocol_format()).await?;
                            continue;
                        }
                    };

                    // ---- NEW: Use custom parser ----
                    let mut parser = custom_sql_parser::Parser::new(&sql);
                    let ast = match parser.parse_statement() {
                        Ok(ast) => ast,
                        Err(e) => {
                            let response = Response::Error(format!("SQL Parse Error: {}", e));
                            writer.write_all(&response.into_protocol_format()).await?;
                            continue;
                        }
                    };

                    let logical_plan = match query_engine::custom_ast_to_logical_plan(ast) {
                        Ok(plan) => plan,
                        Err(e) => {
                            let response = Response::Error(format!("Planning Error: {}", e));
                            writer.write_all(&response.into_protocol_format()).await?;
                            continue;
                        }
                    };
                    // ---- END NEW ----

                    let physical_plan = match query_engine::logical_to_physical_plan(logical_plan, &ctx.index_manager) {
                        Ok(plan) => plan,
                        Err(e) => {
                            let response = Response::Error(format!("Optimization Error: {}", e));
                            writer.write_all(&response.into_protocol_format()).await?;
                            continue;
                        }
                    };

                    // The execution model is internally streaming between operators, but because
                    // the RESP protocol requires knowing the array length upfront, we must
                    // collect all results before sending the first byte of the response.
                    use futures::StreamExt;
                    let mut stream_rows = query_engine::execute(physical_plan, ctx.clone());
                    let mut rows = Vec::new();
                    while let Some(row_result) = stream_rows.next().await {
                        match row_result {
                            Ok(val) => rows.push(val),
                            Err(e) => {
                                let response = Response::Error(format!("Execution Error: {}", e));
                                writer.write_all(&response.into_protocol_format()).await?;
                                // Instead of continue, break to avoid infinite error loop
                                break;
                            }
                        }
                    }
                    // Write RESP array header
                    let header = format!("*{}\r\n", rows.len());
                    writer.write_all(header.as_bytes()).await?;
                    // Write each row as a bulk string (JSON)
                    for row in rows {
                        let s = row.to_string();
                        let bulk = format!("${}\r\n{}\r\n", s.len(), s);
                        writer.write_all(bulk.as_bytes()).await?;
                    }
                    continue;
                }

                // Non-SQL: respond as before
                let response = process_command(command, &ctx).await;
                writer.write_all(&response.into_protocol_format()).await?;
            }
            Ok(None) => {
                println!("Client disconnected.");
                break;
            }
            Err(e) => {
                let response = Response::Error(e.to_string());
                // Write error but do NOT break; allow client to continue sending commands
                let _ = writer.write_all(&response.into_protocol_format()).await;
                // Instead of break, continue to allow further commands
                continue;
            }
        }
    }
    Ok(())
}

/// Parses a RESP command from the stream into our generic Command struct.
async fn parse_command_from_stream<R>(reader: &mut R) -> Result<Option<Command>>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None);
    }
    if !line.starts_with('*') {
        bail!("Invalid command format: expected array specifier ('*')");
    }
    let num_args: usize = line[1..].trim().parse()?;
    let mut args = Vec::with_capacity(num_args);
    for _ in 0..num_args {
        line.clear();
        reader.read_line(&mut line).await?;
        if !line.starts_with('$') {
            bail!("Invalid command format: expected bulk string specifier ('$')");
        }
        let len: usize = line[1..].trim().parse()?;
        let mut arg_buf = vec![0; len];
        reader.read_exact(&mut arg_buf).await?;
        args.push(arg_buf);
        line.clear();
        reader.read_line(&mut line).await?; // Read trailing CRLF
    }
    let name = String::from_utf8(args.get(0).ok_or_else(|| anyhow!("Command name not provided"))?.clone())?.to_uppercase();
    Ok(Some(Command { name, args }))
}


// --- Custom SQL Parser (from scratch) ---
mod custom_sql_parser {
    use serde_json::Value;

    // --- Tokenizer / Lexer ---
    #[derive(Debug, Clone, PartialEq)]
    pub enum Token {
        Select, From, Where, Order, By, Limit, As, Asc, Desc, Group,
        Identifier(String), StringLiteral(String), Number(String),
        Asterisk, Comma, LParen, RParen,
        Operator(String), // =, >, <, >=, <=, !=
        Eof, Illegal(String),
    }

    pub struct Lexer<'a> {
        input: &'a [u8],
        position: usize,
    }

    impl<'a> Lexer<'a> {
        pub fn new(input: &'a str) -> Self {
            Self { input: input.as_bytes(), position: 0 }
        }

        fn current_char(&self) -> u8 {
            if self.position >= self.input.len() { 0 } else { self.input[self.position] }
        }

        fn advance(&mut self) {
            self.position += 1;
        }

        fn skip_whitespace(&mut self) {
            while self.current_char().is_ascii_whitespace() { self.advance(); }
        }

        fn read_identifier(&mut self) -> String {
            let start = self.position;
            while self.current_char().is_ascii_alphanumeric() || self.current_char() == b'_' || self.current_char() == b'.' {
                self.advance();
            }
            String::from_utf8_lossy(&self.input[start..self.position]).to_string()
        }

        fn read_number(&mut self) -> String {
            let start = self.position;
            while self.current_char().is_ascii_digit() || self.current_char() == b'.' { self.advance(); }
            String::from_utf8_lossy(&self.input[start..self.position]).to_string()
        }

        fn read_string(&mut self) -> String {
            let start = self.position + 1;
            self.advance();
            while self.current_char() != b'\'' && self.current_char() != 0 { self.advance(); }
            let s = String::from_utf8_lossy(&self.input[start..self.position]).to_string();
            self.advance(); // Skip closing quote
            s
        }

        pub fn next_token(&mut self) -> Token {
            self.skip_whitespace();
            let tok = match self.current_char() {
                b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                    let ident = self.read_identifier();
                    return match ident.to_uppercase().as_str() {
                        "SELECT" => Token::Select, "FROM" => Token::From, "WHERE" => Token::Where,
                        "ORDER" => Token::Order, "BY" => Token::By, "LIMIT" => Token::Limit,
                        "AS" => Token::As, "ASC" => Token::Asc, "DESC" => Token::Desc,
                        "GROUP" => Token::Group,
                        _ => Token::Identifier(ident),
                    };
                }
                b'0'..=b'9' => return Token::Number(self.read_number()),
                b'\'' => return Token::StringLiteral(self.read_string()),
                b'=' => Token::Operator("=".to_string()),
                b'!' if self.input.get(self.position + 1) == Some(&b'=') => { self.advance(); Token::Operator("!=".to_string()) },
                b'>' if self.input.get(self.position + 1) == Some(&b'=') => { self.advance(); Token::Operator(">=".to_string()) },
                b'<' if self.input.get(self.position + 1) == Some(&b'=') => { self.advance(); Token::Operator("<=".to_string()) },
                b'>' => Token::Operator(">".to_string()),
                b'<' => Token::Operator("<".to_string()),
                b'*' => Token::Asterisk, b',' => Token::Comma,
                b'(' => Token::LParen, b')' => Token::RParen,
                0 => Token::Eof,
                c => Token::Illegal(c.to_string()),
            };
            self.advance();
            tok
        }
    }

    // --- AST (Abstract Syntax Tree) ---
    #[derive(Debug, Clone)]
    pub enum Expression {
        Column(String),
        Literal(Value),
        FunctionCall { name: String, args: Vec<Expression> },
        BinaryOp { left: Box<Expression>, op: String, right: Box<Expression> },
    }

    #[derive(Debug, Clone)]
    pub struct OrderByExpr { pub expr: Expression, pub asc: bool }

    #[derive(Debug, Clone)]
    pub struct SelectItem { pub expr: Expression, pub alias: Option<String> }

    #[derive(Debug, Default)]
    pub struct Ast {
        pub select_items: Vec<SelectItem>,
        pub from: Option<String>,
        pub selection: Option<Expression>,
        pub group_by: Vec<Expression>,
        pub order_by: Vec<OrderByExpr>,
        pub limit: Option<usize>,
    }

    // --- Parser ---
    pub struct Parser<'a> {
        lexer: Lexer<'a>,
        current: Token,
        peek: Token,
    }

    type ParseResult<T> = Result<T, String>;

    impl<'a> Parser<'a> {
        pub fn new(input: &'a str) -> Self {
            let mut lexer = Lexer::new(input);
            let current = lexer.next_token();
            let peek = lexer.next_token();
            Self { lexer, current, peek }
        }

        fn advance(&mut self) {
            self.current = self.peek.clone();
            self.peek = self.lexer.next_token();
        }

        fn expect_and_advance(&mut self, expected: Token) -> ParseResult<()> {
            if self.current == expected {
                self.advance();
                Ok(())
            } else {
                Err(format!("Expected token {:?}, got {:?}", expected, self.current))
            }
        }

        pub fn parse_statement(&mut self) -> ParseResult<Ast> {
            let mut ast = Ast::default();
            self.expect_and_advance(Token::Select)?;
            ast.select_items = self.parse_select_list()?;
            self.expect_and_advance(Token::From)?;
            ast.from = Some(self.parse_identifier()?);
            if self.current == Token::Where {
                self.advance();
                ast.selection = Some(self.parse_expression()?);
            }
            if self.current == Token::Group {
                self.advance();
                self.expect_and_advance(Token::By)?;
                ast.group_by = self.parse_expression_list()?;
            }
            if self.current == Token::Order {
                self.advance();
                self.expect_and_advance(Token::By)?;
                ast.order_by = self.parse_order_by_list()?;
            }
            if self.current == Token::Limit {
                self.advance();
                ast.limit = Some(self.parse_limit()?);
            }
            Ok(ast)
        }

        fn parse_select_list(&mut self) -> ParseResult<Vec<SelectItem>> {
            let mut items = vec![];
            items.push(self.parse_select_item()?);
            while self.current == Token::Comma {
                self.advance();
                items.push(self.parse_select_item()?);
            }
            Ok(items)
        }
        
        fn parse_select_item(&mut self) -> ParseResult<SelectItem> {
            let expr = self.parse_expression()?;
            let alias = if self.current == Token::As {
                self.advance();
                Some(self.parse_identifier()?)
            } else if let Token::Identifier(_) = self.current { // Support implicit alias: `SELECT name n FROM users`
                 let ident = self.parse_identifier()?;
                 Some(ident)
            } else {
                None
            };
            Ok(SelectItem { expr, alias })
        }

        fn parse_expression_list(&mut self) -> ParseResult<Vec<Expression>> {
            let mut exprs = vec![self.parse_expression()?];
            while self.current == Token::Comma {
                self.advance();
                exprs.push(self.parse_expression()?);
            }
            Ok(exprs)
        }

        fn parse_expression(&mut self) -> ParseResult<Expression> {
            let left = self.parse_primary_expression()?;
            if let Token::Operator(op) = self.current.clone() {
                self.advance();
                let right = self.parse_primary_expression()?;
                return Ok(Expression::BinaryOp { left: Box::new(left), op, right: Box::new(right) });
            }
            Ok(left)
        }

        fn parse_primary_expression(&mut self) -> ParseResult<Expression> {
            match self.current.clone() {
                Token::Identifier(name) => {
                    self.advance();
                    if self.current == Token::LParen { // Function call
                        self.advance();
                        let args = if self.current != Token::RParen { self.parse_expression_list()? } else { vec![] };
                        self.expect_and_advance(Token::RParen)?;
                        Ok(Expression::FunctionCall { name, args })
                    } else { // Column
                        Ok(Expression::Column(name))
                    }
                }
                Token::Asterisk => { self.advance(); Ok(Expression::Column("*".to_string())) },
                Token::Number(s) => { self.advance(); Ok(Expression::Literal(serde_json::from_str(&s).unwrap_or(Value::Null))) },
                Token::StringLiteral(s) => { self.advance(); Ok(Expression::Literal(Value::String(s))) },
                _ => Err(format!("Unexpected token in expression: {:?}", self.current)),
            }
        }
        
        fn parse_identifier(&mut self) -> ParseResult<String> {
            match self.current.clone() {
                Token::Identifier(s) => { self.advance(); Ok(s) },
                _ => Err(format!("Expected identifier, got {:?}", self.current)),
            }
        }
        
        fn parse_order_by_list(&mut self) -> ParseResult<Vec<OrderByExpr>> {
            let mut list = vec![];
            loop {
                let expr = self.parse_expression()?;
                let asc = match self.current {
                    Token::Asc => { self.advance(); true },
                    Token::Desc => { self.advance(); false },
                    _ => true,
                };
                list.push(OrderByExpr { expr, asc });
                if self.current != Token::Comma { break; }
                self.advance();
            }
            Ok(list)
        }

        fn parse_limit(&mut self) -> ParseResult<usize> {
            match self.current.clone() {
                Token::Number(s) => {
                    self.advance();
                    s.parse::<usize>().map_err(|e| e.to_string())
                },
                _ => Err(format!("Expected number for LIMIT, got {:?}", self.current)),
            }
        }
    }
}


// --- SQL Query Engine Module (Updated to use custom parser's AST) ---
mod query_engine {
    use super::{AppContext, DbValue, IndexManager, Response, custom_sql_parser};
    use anyhow::{anyhow, bail, Result};
    use async_stream::try_stream;
    use futures::stream::{Stream, TryStreamExt};
    use futures::StreamExt;
    use serde_json::{json, Map, Value};
    use std::collections::HashSet;
    use std::cmp::Ordering;
    use std::collections::HashMap;
    use std::ops::Bound;
    use std::sync::Arc;

    type Row = Value;

    // --- Core Data Structures (Unchanged) ---
    #[derive(Debug, Clone)]
    pub enum Expression {
        Column(String),
        Literal(Value),
        BinaryOp { left: Box<Expression>, op: Operator, right: Box<Expression> },
        AggregateFunction(AggregateFunction),
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Operator { Eq, NotEq, Lt, LtEq, Gt, GtEq }

    #[derive(Debug, Clone)]
    pub struct SortKey { pub expr: Expression, pub asc: bool }

    #[derive(Debug, Clone)]
    pub struct AggregateFunction { pub func: AggregateType, pub args: Vec<Expression> }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum AggregateType { Count, Sum }

    // --- Logical and Physical Plans (Unchanged) ---
    #[derive(Debug)]
    pub enum LogicalPlan {
        TableScan { table_name: String },
        Filter { input: Box<LogicalPlan>, predicate: Expression },
        Projection { input: Box<LogicalPlan>, expressions: Vec<(Expression, String)> },
        Sort { input: Box<LogicalPlan>, sort_keys: Vec<SortKey> },
        Limit { input: Box<LogicalPlan>, n: usize },
        Aggregate { input: Box<LogicalPlan>, group_by_exprs: Vec<Expression>, agg_funcs: Vec<AggregateFunction> },
    }

    #[derive(Debug)]
    pub enum PhysicalPlan {
        TableScan { prefix: String },
        IndexRangeScan { index_name: String, start_bound: Bound<Value>, end_bound: Bound<Value>, scan_forward: bool },
        Filter { input: Box<PhysicalPlan>, predicate: Expression },
        Projection { input: Box<PhysicalPlan>, expressions: Vec<(Expression, String)> },
        Sort { input: Box<PhysicalPlan>, sort_keys: Vec<SortKey> },
        Limit { input: Box<PhysicalPlan>, n: usize },
        Aggregate { input: Box<PhysicalPlan>, group_by_exprs: Vec<Expression>, agg_funcs: Vec<AggregateFunction> },
    }

    // --- Expression Evaluation (Unchanged) ---
    fn compare_values(left: &Value, right: &Value) -> Result<Option<Ordering>> {
        match (left, right) {
            (Value::Number(l), Value::Number(r)) => {
                if let (Some(l_f64), Some(r_f64)) = (l.as_f64(), r.as_f64()) {
                    Ok(l_f64.partial_cmp(&r_f64))
                } else { bail!("Cannot compare non-f64 numbers") }
            }
            (Value::String(l), Value::String(r)) => Ok(Some(l.cmp(r))),
            (Value::Null, Value::Null) => Ok(Some(Ordering::Equal)),
            (Value::Null, _) => Ok(Some(Ordering::Less)),
            (_, Value::Null) => Ok(Some(Ordering::Greater)),
            _ => Ok(None),
        }
    }

    impl Expression {
        pub fn evaluate(&self, row: &Row) -> Result<Value> {
            match self {
                Expression::Literal(v) => Ok(v.clone()),
                Expression::Column(col_name) => {
                    let pointer = format!("/{}", col_name.replace('.', "/"));
                    Ok(row.pointer(&pointer).cloned().unwrap_or(Value::Null))
                }
                Expression::BinaryOp { left, op, right } => {
                    let left_val = left.evaluate(row)?;
                    let right_val = right.evaluate(row)?;
                    let result = match op {
                        Operator::Eq => left_val == right_val,
                        Operator::NotEq => left_val != right_val,
                        Operator::Gt => compare_values(&left_val, &right_val)?.map_or(false, |o| o.is_gt()),
                        Operator::Lt => compare_values(&left_val, &right_val)?.map_or(false, |o| o.is_lt()),
                        Operator::GtEq => compare_values(&left_val, &right_val)?.map_or(false, |o| o.is_ge()),
                        Operator::LtEq => compare_values(&left_val, &right_val)?.map_or(false, |o| o.is_le()),
                    };
                    Ok(Value::Bool(result))
                }
                Expression::AggregateFunction(agg) => {
                    let col_name = format!("__{:?}({:?})", agg.func, agg.args);
                    Ok(row.get(&col_name).cloned().unwrap_or(Value::Null))
                }
            }
        }
    }

    // --- NEW: Custom AST to Logical Plan Conversion ---
    fn custom_expr_to_engine_expr(expr: custom_sql_parser::Expression) -> Result<Expression> {
        match expr {
            custom_sql_parser::Expression::Column(name) => {
                // Support nested fields: profile.name -> profile.name
                Ok(Expression::Column(name))
            },
            custom_sql_parser::Expression::Literal(val) => Ok(Expression::Literal(val)),
            custom_sql_parser::Expression::BinaryOp { left, op, right } => {
                let op = match op.as_str() {
                    "=" => Operator::Eq, "!=" => Operator::NotEq,
                    ">" => Operator::Gt, ">=" => Operator::GtEq,
                    "<" => Operator::Lt, "<=" => Operator::LtEq,
                    _ => bail!("Unsupported operator: {}", op),
                };
                Ok(Expression::BinaryOp {
                    left: Box::new(custom_expr_to_engine_expr(*left)?),
                    op,
                    right: Box::new(custom_expr_to_engine_expr(*right)?),
                })
            }
            custom_sql_parser::Expression::FunctionCall { name, args } => {
                let agg_type = match name.to_uppercase().as_str() {
                    "COUNT" => AggregateType::Count,
                    "SUM" => AggregateType::Sum,
                    _ => bail!("Unsupported function: {}", name),
                };
                let engine_args = args.into_iter()
                    .map(custom_expr_to_engine_expr)
                    .collect::<Result<Vec<_>>>()?;
                
                // Special handling for COUNT(*) -> COUNT(1)
                if agg_type == AggregateType::Count && engine_args.iter().any(|arg| matches!(arg, Expression::Column(c) if c == "*")) {
                    Ok(Expression::AggregateFunction(AggregateFunction {
                        func: agg_type,
                        args: vec![Expression::Literal(Value::Number(1.into()))],
                    }))
                } else {
                     Ok(Expression::AggregateFunction(AggregateFunction { func: agg_type, args: engine_args }))
                }
            }
        }
    }

    pub fn custom_ast_to_logical_plan(ast: custom_sql_parser::Ast) -> Result<LogicalPlan> {
        // 1. FROM -> TableScan
        let table_name = ast.from.ok_or_else(|| anyhow!("FROM clause is required"))?;
        let mut plan = LogicalPlan::TableScan { table_name };

        // 2. WHERE -> Filter
        if let Some(selection) = ast.selection {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: custom_expr_to_engine_expr(selection)?,
            };
        }
        
        // 3. GROUP BY / Aggregates -> Aggregate
        let mut projection_exprs = Vec::new();
        let mut agg_funcs = Vec::new();

        for item in ast.select_items {
            let name = item.alias.unwrap_or_else(|| {
                // Recreate the string representation for unaliased columns
                match &item.expr {
                    custom_sql_parser::Expression::Column(c) => c.clone(),
                    custom_sql_parser::Expression::FunctionCall {name, args} => format!("{}({:?})", name, args),
                    _ => "expr".to_string(),
                }
            });
            let expr = custom_expr_to_engine_expr(item.expr)?;
            if let Expression::AggregateFunction(agg) = &expr {
                agg_funcs.push(agg.clone());
            }
            projection_exprs.push((expr, name));
        }

        let group_by_exprs = ast.group_by
            .into_iter()
            .map(custom_expr_to_engine_expr)
            .collect::<Result<Vec<_>>>()?;
        
        if !agg_funcs.is_empty() || !group_by_exprs.is_empty() {
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by_exprs,
                agg_funcs,
            };
        }

        // 4. SELECT -> Projection
        plan = LogicalPlan::Projection {
            input: Box::new(plan),
            expressions: projection_exprs,
        };

        // 5. ORDER BY -> Sort
        if !ast.order_by.is_empty() {
            let sort_keys = ast.order_by.into_iter().map(|o| {
                Ok(SortKey {
                    expr: custom_expr_to_engine_expr(o.expr)?,
                    asc: o.asc,
                })
            }).collect::<Result<Vec<_>>>()?;
            plan = LogicalPlan::Sort { input: Box::new(plan), sort_keys };
        }
        
        // 6. LIMIT -> Limit
        if let Some(n) = ast.limit {
            plan = LogicalPlan::Limit { input: Box::new(plan), n };
        }

        Ok(plan)
    }

    // --- Logical to Physical Plan Conversion (Optimizer) (Unchanged) ---
    fn find_optimizable_predicate(predicate: &Expression) -> Option<(String, Operator, Value)> {
        if let Expression::BinaryOp { left, op, right } = predicate {
            if let (Expression::Column(c), Expression::Literal(l)) = (&**left, &**right) {
                return Some((c.clone(), op.clone(), l.clone()));
            }
            if let (Expression::Literal(l), Expression::Column(c)) = (&**left, &**right) {
                let flipped_op = match op {
                    Operator::Gt => Operator::Lt, Operator::Lt => Operator::Gt,
                    Operator::GtEq => Operator::LtEq, Operator::LtEq => Operator::GtEq,
                    _ => op.clone(),
                };
                return Some((c.clone(), flipped_op, l.clone()));
            }
        }
        None
    }

    pub fn logical_to_physical_plan(plan: LogicalPlan, index_manager: &Arc<IndexManager>) -> Result<PhysicalPlan> {
        match plan {
            LogicalPlan::Projection { input, expressions } => Ok(PhysicalPlan::Projection {
                input: Box::new(logical_to_physical_plan(*input, index_manager)?),
                expressions,
            }),
            LogicalPlan::TableScan { table_name } => Ok(PhysicalPlan::TableScan {
                prefix: format!("{}:", table_name),
            }),
            LogicalPlan::Filter { input, predicate } => {
                if let LogicalPlan::TableScan { ref table_name } = *input {
                    if let Some((col, op, val)) = find_optimizable_predicate(&predicate) {
                        let index_name = format!("{}:*|{}", table_name, col);
                        if index_manager.indexes.contains_key(&index_name) {
                            println!("Optimizer: Using IndexRangeScan for predicate: {} {} {:?}", col, format!("{:?}", op), val);
                            let (start_bound, end_bound) = match op {
                                Operator::Eq => (Bound::Included(val.clone()), Bound::Included(val)),
                                Operator::Gt => (Bound::Excluded(val), Bound::Unbounded),
                                Operator::GtEq => (Bound::Included(val), Bound::Unbounded),
                                Operator::Lt => (Bound::Unbounded, Bound::Excluded(val)),
                                Operator::LtEq => (Bound::Unbounded, Bound::Included(val)),
                                _ => return Err(anyhow!("Cannot use NotEq for index scan")),
                            };
                            return Ok(PhysicalPlan::IndexRangeScan {
                                index_name, start_bound, end_bound, scan_forward: true,
                            });
                        }
                    }
                }
                println!("Optimizer: Using TableScan with post-filtering.");
                Ok(PhysicalPlan::Filter {
                    input: Box::new(logical_to_physical_plan(*input, index_manager)?),
                    predicate,
                })
            }
            LogicalPlan::Sort { input, sort_keys } => {
                if let LogicalPlan::Filter { input: filter_input, predicate } = input.as_ref() {
                     if let LogicalPlan::TableScan { table_name } = filter_input.as_ref() {
                        if let Some((col, op, val)) = find_optimizable_predicate(predicate) {
                             if let Some(sort_key) = sort_keys.get(0) {
                                if let Expression::Column(sort_col) = &sort_key.expr {
                                    if *sort_col == col {
                                        let index_name = format!("{}:*|{}", table_name, col);
                                        if index_manager.indexes.contains_key(&index_name) {
                                            println!("Optimizer: Fusing Sort and Filter into a single ordered IndexRangeScan.");
                                            let (start_bound, end_bound) = match op {
                                                Operator::Eq => (Bound::Included(val.clone()), Bound::Included(val)),
                                                Operator::Gt => (Bound::Excluded(val), Bound::Unbounded),
                                                Operator::GtEq => (Bound::Included(val), Bound::Unbounded),
                                                Operator::Lt => (Bound::Unbounded, Bound::Excluded(val)),
                                                Operator::LtEq => (Bound::Unbounded, Bound::Included(val)),
                                                _ => return Err(anyhow!("Cannot use NotEq for index scan")),
                                            };
                                            return Ok(PhysicalPlan::IndexRangeScan {
                                                index_name, start_bound, end_bound, scan_forward: sort_key.asc,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(PhysicalPlan::Sort {
                    input: Box::new(logical_to_physical_plan(*input, index_manager)?),
                    sort_keys,
                })
            }
            LogicalPlan::Limit { input, n } => Ok(PhysicalPlan::Limit {
                input: Box::new(logical_to_physical_plan(*input, index_manager)?),
                n,
            }),
            LogicalPlan::Aggregate { input, group_by_exprs, agg_funcs } => Ok(PhysicalPlan::Aggregate {
                input: Box::new(logical_to_physical_plan(*input, index_manager)?),
                group_by_exprs,
                agg_funcs,
            }),
        }
    }

    // --- Execution Engine (Unchanged) ---
    pub fn execute(plan: PhysicalPlan, ctx: Arc<AppContext>) -> impl Stream<Item = Result<Row>> {
        Box::pin(_execute(plan, ctx))
    }

    fn bound_to_string_bound(bound: Bound<Value>) -> Result<Bound<String>> {
        match bound {
            Bound::Included(v) => Ok(Bound::Included(serde_json::to_string(&v)?)),
            Bound::Excluded(v) => Ok(Bound::Excluded(serde_json::to_string(&v)?)),
            Bound::Unbounded => Ok(Bound::Unbounded),
        }
    }

    fn insert_nested(map: &mut serde_json::Map<String, Value>, path: &str, value: Value) {
        let mut parts = path.split('.').peekable();
        let mut current = map;
        while let Some(part) = parts.next() {
            if parts.peek().is_none() {
                current.insert(part.to_string(), value);
                return;
            } else {
                current = current
                    .entry(part)
                    .or_insert_with(|| Value::Object(serde_json::Map::new()))
                    .as_object_mut()
                    .unwrap();
            }
        }
    }

    fn get_first_column_name(expressions: &[(Expression, String)]) -> Option<&str> {
        expressions.get(0).map(|(_, name)| name.as_str())
    }

    fn is_count_star(expressions: &[(Expression, String)]) -> bool {
        if let Some((Expression::AggregateFunction(AggregateFunction { func: AggregateType::Count, .. }), _)) = expressions.get(0) {
            true
        } else {
            false
        }
    }

    fn _execute(plan: PhysicalPlan, ctx: Arc<AppContext>) -> impl Stream<Item = Result<Row>> {
        async_stream::try_stream! {
            match plan {
                PhysicalPlan::TableScan { prefix } => {
                    println!("Executor: Performing TableScan on prefix '{}'", prefix);
                    let keys_to_scan: Vec<String> = ctx.db.iter().filter(|i| i.key().starts_with(&prefix)).map(|i| i.key().clone()).collect();
                    for key in keys_to_scan {
                        if let Some(entry) = ctx.db.get(&key) {
                            if let DbValue::Json(val) = entry.value() {
                                let mut augmented_val = val.clone();
                                if let (Value::Object(map), Some(id_part)) = (&mut augmented_val, key.split(':').last()) {
                                    map.insert("id".to_string(), Value::String(id_part.to_string()));
                                }
                                yield augmented_val;
                            }
                        }
                    }
                },
                PhysicalPlan::IndexRangeScan { index_name, start_bound, end_bound, scan_forward } => {
                     println!("Executor: Performing IndexRangeScan on index '{}'", index_name);
                    if let Some(index) = ctx.index_manager.indexes.get(&index_name) {
                        let index_data = index.read().await;
                        let start = bound_to_string_bound(start_bound)?;
                        let end = bound_to_string_bound(end_bound)?;
                        let range = index_data.range((start, end));
                        let pks_iters: Box<dyn Iterator<Item = &HashSet<String>> + Send> = if scan_forward {
                            Box::new(range.map(|(_k, v)| v))
                        } else {
                            Box::new(range.rev().map(|(_k, v)| v))
                        };
                        for pks in pks_iters {
                            for pk in pks.iter() {
                                if let Some(entry) = ctx.db.get(pk.as_str()) {
                                    if let DbValue::Json(val) = entry.value() {
                                        let mut augmented_val = val.clone();
                                        if let (Value::Object(map), Some(id_part)) = (&mut augmented_val, pk.split(':').last()) {
                                            map.insert("id".to_string(), Value::String(id_part.to_string()));
                                        }
                                        yield augmented_val;
                                    }
                                }
                            }
                        }
                    }
                },
                PhysicalPlan::Filter { input, predicate } => {
                    println!("Executor: Applying filter");
                    let input_stream = Box::pin(_execute(*input, ctx));
                    let filtered_stream = input_stream.try_filter(move |row| {
                        let pred_clone = predicate.clone();
                        let row_owned = row.clone();
                        async move { matches!(pred_clone.evaluate(&row_owned), Ok(Value::Bool(true))) }
                    });
                    for await row in filtered_stream { yield row?; }
                },
                PhysicalPlan::Projection { input, expressions } => {
                    let input_stream = Box::pin(_execute(*input, ctx));
                    for await row_res in input_stream {
                        let row = row_res?;
                        // Handle SELECT * as a special case
                        if expressions.len() == 1 && matches!(&expressions[0].0, Expression::Column(c) if c == "*") {
                            yield row;
                            continue;
                        }

                        // Always yield an object for column projections (even single column)
                        let all_are_columns = expressions.iter().all(|(expr, _)| matches!(expr, Expression::Column(_)));
                        if all_are_columns {
                            let mut new_map = serde_json::Map::new();
                            for (expr, _alias) in &expressions {
                                if let Expression::Column(col_name) = expr {
                                    let val = expr.evaluate(&row)?;
                                    insert_nested(&mut new_map, col_name, val);
                                }
                            }
                            yield Value::Object(new_map);
                            continue;
                        }

                        // For single aggregate, yield as object with correct key
                        if expressions.len() == 1 {
                            if let (Expression::AggregateFunction(_), alias) = &expressions[0] {
                                let mut new_map = serde_json::Map::new();
                                let val = expressions[0].0.evaluate(&row)?;
                                new_map.insert(alias.clone(), val);
                                yield Value::Object(new_map);
                                continue;
                            }
                        }

                        // Mixed expressions (columns + aggregates): use alias as key, value from expr.evaluate
                        let mut new_map = serde_json::Map::new();
                        for (expr, alias) in &expressions {
                            let mut val = expr.evaluate(&row)?;
                            // If aggregate value is Null, but row has only one value, use that value
                            if val.is_null() {
                                if let Expression::AggregateFunction(_) = expr {
                                    if let Some(map) = row.as_object() {
                                        if map.len() == 1 {
                                            if let Some(the_one_value) = map.values().next() {
                                                val = the_one_value.clone();
                                            }
                                        }
                                    }
                                }
                            }
                            new_map.insert(alias.clone(), val);
                        }
                        yield Value::Object(new_map);
                    }
                },
                PhysicalPlan::Limit { input, n } => {
                    println!("Executor: Applying limit of {}", n);
                    let input_stream = Box::pin(_execute(*input, ctx));
                    let limited_stream = input_stream.take(n);
                    for await row in limited_stream { yield row?; }
                },
                PhysicalPlan::Sort { input, sort_keys } => {
                    println!("Executor: Performing in-memory sort (pipeline-breaker)");
                    let input_stream = Box::pin(_execute(*input, ctx));
                    let mut rows: Vec<Row> = input_stream.try_collect().await?;

                    rows.sort_by(|a, b| {
                        for key in &sort_keys {
                            let val_a = key.expr.evaluate(a).unwrap_or(Value::Null);
                            let val_b = key.expr.evaluate(b).unwrap_or(Value::Null);
                            let ord = compare_values(&val_a, &val_b).unwrap_or(Some(Ordering::Equal)).unwrap_or(Ordering::Equal);
                            let final_ord = if key.asc { ord } else { ord.reverse() };
                            if final_ord != Ordering::Equal { return final_ord; }
                        }
                        Ordering::Equal
                    });
                    for row in rows { yield row; }
                },
                PhysicalPlan::Aggregate { input, group_by_exprs, agg_funcs } => {
                    println!("Executor: Performing aggregation (pipeline-breaker)");
                    let input_stream = Box::pin(_execute(*input, ctx));
                    type AggState = Vec<Value>;
                    let mut groups: std::collections::HashMap<Value, AggState> = std::collections::HashMap::new();

                    for await row_res in input_stream {
                        let row = row_res?;
                        let group_key = if group_by_exprs.is_empty() { Value::Null } else {
                            let mut keys = Vec::with_capacity(group_by_exprs.len());
                            for expr in &group_by_exprs { keys.push(expr.evaluate(&row)?); }
                            Value::Array(keys)
                        };
                        let state = groups.entry(group_key).or_insert_with(|| {
                            agg_funcs.iter().map(|af| match af.func {
                                AggregateType::Count => serde_json::json!(0), AggregateType::Sum => serde_json::json!(0),
                            }).collect()
                        });
                        for (i, agg_func) in agg_funcs.iter().enumerate() {
                            let current_val = &mut state[i];
                            match agg_func.func {
                                AggregateType::Count => {
                                    *current_val = serde_json::json!(current_val.as_u64().unwrap_or(0) + 1);
                                }
                                AggregateType::Sum => {
                                    let row_val = agg_func.args[0].evaluate(&row)?;
                                    let sum = current_val.as_f64().unwrap_or(0.0) + row_val.as_f64().unwrap_or(0.0);
                                    *current_val = serde_json::json!(sum);
                                }
                            }
                        }
                    }
                    for (group_key, final_vals) in groups {
                        let mut result_row = serde_json::Map::new();
                        if let Value::Array(keys) = group_key {
                            for (i, key_val) in keys.into_iter().enumerate() {
                                if let Some(Expression::Column(name)) = group_by_exprs.get(i) {
                                    insert_nested(&mut result_row, name, key_val);
                                }
                            }
                        } else if !group_by_exprs.is_empty() {
                            if let Some(Expression::Column(name)) = group_by_exprs.get(0) {
                                insert_nested(&mut result_row, name, group_key.clone());
                            }
                        }
                        // ===== START OF FIX =====
                        for (i, agg_func) in agg_funcs.iter().enumerate() {
                            let col_name = format!("__{:?}({:?})", agg_func.func, agg_func.args);
                            result_row.insert(col_name, final_vals[i].clone());
                        }
                        // ===== END OF FIX =====
                        yield Value::Object(result_row);
                    }
                }
            }
        }
    }

    pub async fn handle_sql(cmd: super::Command, ctx: Arc<AppContext>) -> Response {
        let sql = match cmd.args[1..]
            .iter()
            .map(|arg| String::from_utf8(arg.clone()))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(parts) => parts.join(" "),
            Err(_) => return Response::Error("Invalid UTF-8 in SQL query".to_string()),
        };

        let mut parser = custom_sql_parser::Parser::new(&sql);
        let ast = match parser.parse_statement() {
            Ok(ast) => ast,
            Err(e) => return Response::Error(format!("SQL Parse Error: {}", e)),
        };

        let logical_plan = match custom_ast_to_logical_plan(ast) {
            Ok(plan) => plan,
            Err(e) => return Response::Error(format!("Planning Error: {}", e)),
        };
        println!("Logical Plan: {:?}", logical_plan);

        let physical_plan = match logical_to_physical_plan(logical_plan, &ctx.index_manager) {
            Ok(plan) => plan,
            Err(e) => return Response::Error(format!("Optimization Error: {}", e)),
        };
        println!("Physical Plan: {:?}", physical_plan);

        let result_stream = execute(physical_plan, ctx);
        let results: Vec<Row> = match result_stream.try_collect().await {
            Ok(rows) => rows,
            Err(e) => return Response::Error(format!("Execution Error: {}", e)),
        };
        Response::Value(json!(results))
    }
}


// --- Command Processing & Handlers ---

/// Macro to simplify writing to the WAL and waiting for acknowledgement.
macro_rules! log_and_wait {
    ($logger:expr, $entry:expr) => {
        {
            let (ack_tx, ack_rx) = oneshot::channel();
            if $logger.send(LogRequest { entry: $entry, ack: ack_tx }).await.is_err() {
                return Response::Error("Persistence engine is down".to_string());
            }
            match ack_rx.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(Response::Error(format!("WAL write error: {}", e))),
                Err(_) => Err(Response::Error("Persistence engine dropped ACK channel".to_string())),
            }
        }
    };
}

/// Macro to check for the correct number of arguments.
macro_rules! check_args {
    ($cmd:expr, $n:expr) => {
        if $cmd.args.len() != $n {
            return Response::Error(format!("wrong number of arguments for '{}' command", $cmd.name));
        }
    };
    ($cmd:expr, $n:expr, $m:expr) => {
        if $cmd.args.len() < $n || $cmd.args.len() > $m {
             return Response::Error(format!("wrong number of arguments for '{}' command", $cmd.name));
        }
    };
}


/// The main command router.
async fn process_command(cmd: Command, ctx: &AppContext) -> Response {
    match cmd.name.as_str() {
        // General
        "GET" => handle_get(cmd, &ctx.db),
        "SET" => handle_set(cmd, ctx).await,
        "DELETE" => handle_delete(cmd, ctx).await,
        // JSON
        "JSON.GET" => handle_json_get(cmd, &ctx.db),
        "JSON.SET" => handle_json_set(cmd, ctx).await,
        // Lists
        "LPUSH" => handle_lpush(cmd, ctx).await,
        "RPUSH" => handle_rpush(cmd, ctx).await,
        "LPOP" => handle_lpop(cmd, ctx).await,
        "RPOP" => handle_rpop(cmd, ctx).await,
        "LRANGE" => handle_lrange(cmd, &ctx.db).await,
        // Sets
        "SADD" => handle_sadd(cmd, ctx).await,
        "SREM" => handle_srem(cmd, ctx).await,
        "SISMEMBER" => handle_sismember(cmd, &ctx.db).await,
        "SMEMBERS" => handle_smembers(cmd, &ctx.db).await,
        // Indexing & Querying
        "CREATEINDEX" => handle_create_index(cmd, ctx).await,
        "SQL" => query_engine::handle_sql(cmd, Arc::new(ctx.clone())).await,
        _ => Response::Error(format!("unknown command '{}'", cmd.name)),
    }
}

// --- Handler Implementations ---

fn handle_get(cmd: Command, db: &Db) -> Response {
    check_args!(cmd, 2);
    let key = String::from_utf8_lossy(&cmd.args[1]);
    match db.get(&*key) {
        Some(entry) => match entry.value() {
            DbValue::Bytes(b) => Response::Bytes(b.clone()),
            _ => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        },
        None => Response::Nil,
    }
}

async fn handle_set(cmd: Command, ctx: &AppContext) -> Response {
    check_args!(cmd, 3);
    let key = String::from_utf8_lossy(&cmd.args[1]).to_string();
    let value = cmd.args[2].clone();
    let log_entry = LogEntry::SetBytes { key: key.clone(), value: value.clone() };

    if let Err(e) = log_and_wait!(&ctx.logger, log_entry) { return e; }

    // SET overwrites any existing key, regardless of type.
    ctx.db.insert(key, DbValue::Bytes(value));
    Response::Ok
}

async fn handle_delete(cmd: Command, ctx: &AppContext) -> Response {
    check_args!(cmd, 2);
    let key = String::from_utf8_lossy(&cmd.args[1]).to_string();

    // Must check for indexed values BEFORE deleting.
    if let Some(entry) = ctx.db.get(&key) {
        if let DbValue::Json(val) = entry.value() {
            ctx.index_manager.remove_key_from_indexes(&key, val).await;
        }
    }
    
    let log_entry = LogEntry::Delete { key: key.clone() };
    if let Err(e) = log_and_wait!(&ctx.logger, log_entry) { return e; }
    
    if ctx.db.remove(&key).is_some() {
        Response::Integer(1)
    } else {
        Response::Integer(0)
    }
}

fn handle_json_get(cmd: Command, db: &Db) -> Response {
    check_args!(cmd, 2, 3);
    let path = String::from_utf8_lossy(&cmd.args[1]);
    let (top_key, json_path) = match path.split_once('.') {
        Some((k, p)) => (k, Some(p)),
        None => (path.as_ref(), None),
    };
    match db.get(top_key) {
        Some(entry) => match entry.value() {
            DbValue::Json(val) => {
                let target = if let Some(p) = json_path {
                    val.pointer(&json_path_to_pointer(p)).unwrap_or(&Value::Null)
                } else {
                    val
                };
                if target.is_null() {
                    Response::Nil
                } else {
                    Response::Value(target.clone())
                }
            }
            _ => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        },
        None => Response::Nil,
    }
}

async fn handle_json_set(cmd: Command, ctx: &AppContext) -> Response {
    check_args!(cmd, 3);
    let path = String::from_utf8_lossy(&cmd.args[1]).to_string();
    let value_res: Result<Value, _> = serde_json::from_slice(&cmd.args[2]);
    let value = match value_res {
        Ok(v) => v,
        Err(_) => return Response::Error("value is not a valid JSON".to_string()),
    };
    
    // Log first
    let log_entry = LogEntry::JsonSet { path: path.clone(), value: value.clone() };
    if let Err(e) = log_and_wait!(&ctx.logger, log_entry) { return e; }

    // Then apply, updating indexes as we go
    match apply_json_set_with_indexing(ctx, &path, value).await {
        Ok(_) => Response::Ok,
        Err(e) => Response::Error(e.to_string()),
    }
}

async fn apply_json_set_with_indexing(ctx: &AppContext, path: &str, value: Value) -> Result<()> {
    let path_parts: Vec<&str> = path.split('.').collect();
    let top_key = path_parts.first().ok_or_else(|| anyhow!("Path cannot be empty"))?.to_string();
    
    // We use the entry API to handle the key being present or not, but need to do it carefully.
    // If the key exists, we need the old value to update the index.
    if let Some(mut entry) = ctx.db.get_mut(&top_key) {
        // Key exists, update it.
        let old_value = match entry.value() {
            DbValue::Json(j) => j.clone(),
            _ => bail!("WRONGTYPE: Key holds a non-JSON value."),
        };
        ctx.index_manager.remove_key_from_indexes(&top_key, &old_value).await;

        let new_value = entry.value_mut();
        if let DbValue::Json(json_val) = new_value {
             recursive_set(json_val, &path_parts[1..], value)?;
             ctx.index_manager.add_key_to_indexes(&top_key, json_val).await;
        }
    } else {
        // Key does not exist, create it.
        let mut new_json = json!({});
        recursive_set(&mut new_json, &path_parts[1..], value)?;
        ctx.index_manager.add_key_to_indexes(&top_key, &new_json).await;
        ctx.db.insert(top_key, DbValue::Json(new_json));
    }
    Ok(())
}

async fn handle_lpush(cmd: Command, ctx: &AppContext) -> Response {
    if cmd.args.len() < 3 {
        return Response::Error("wrong number of arguments for 'lpush' command".to_string());
    }
    let key = String::from_utf8_lossy(&cmd.args[1]).to_string();
    let values: Vec<Vec<u8>> = cmd.args.into_iter().skip(2).collect();

    let log_entry = LogEntry::LPush { key: key.clone(), values: values.clone() };
    if let Err(e) = log_and_wait!(&ctx.logger, log_entry) { return e; }

    let entry = ctx.db.entry(key.clone()).or_insert_with(|| DbValue::List(RwLock::new(VecDeque::new())));
    match entry.value() {
        DbValue::List(list_lock) => {
            let mut list = list_lock.write().await;
            for v in values.into_iter().rev() {
                list.push_front(v);
            }
            Response::Integer(list.len() as i64)
        }
        _ => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

async fn handle_rpush(cmd: Command, ctx: &AppContext) -> Response {
    if cmd.args.len() < 3 {
        return Response::Error("wrong number of arguments for 'rpush' command".to_string());
    }
    let key = String::from_utf8_lossy(&cmd.args[1]).to_string();
    let values: Vec<Vec<u8>> = cmd.args.into_iter().skip(2).collect();

    let log_entry = LogEntry::RPush { key: key.clone(), values: values.clone() };
    if let Err(e) = log_and_wait!(&ctx.logger, log_entry) { return e; }
    
    let entry = ctx.db.entry(key.clone()).or_insert_with(|| DbValue::List(RwLock::new(VecDeque::new())));
    match entry.value() {
        DbValue::List(list_lock) => {
            let mut list = list_lock.write().await;
            for v in values {
                list.push_back(v);
            }
            Response::Integer(list.len() as i64)
        }
        _ => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

async fn handle_lpop(cmd: Command, ctx: &AppContext) -> Response {
    check_args!(cmd, 2);
    let key = String::from_utf8_lossy(&cmd.args[1]).to_string();

    let log_entry = LogEntry::LPop { key: key.clone(), count: 1 };
    if let Err(e) = log_and_wait!(&ctx.logger, log_entry) { return e; }

    match ctx.db.get_mut(&key) {
        Some(mut entry) => match entry.value_mut() {
            DbValue::List(list_lock) => {
                let mut list = list_lock.write().await;
                match list.pop_front() {
                    Some(val) => Response::Bytes(val),
                    None => Response::Nil,
                }
            }
            _ => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        },
        None => Response::Nil,
    }
}
async fn handle_rpop(cmd: Command, ctx: &AppContext) -> Response {
    check_args!(cmd, 2);
    let key = String::from_utf8_lossy(&cmd.args[1]).to_string();
    
    let log_entry = LogEntry::RPop { key: key.clone(), count: 1 };
    if let Err(e) = log_and_wait!(&ctx.logger, log_entry) { return e; }

    match ctx.db.get_mut(&key) {
        Some(mut entry) => match entry.value_mut() {
            DbValue::List(list_lock) => {
                let mut list = list_lock.write().await;
                match list.pop_back() {
                    Some(val) => Response::Bytes(val),
                    None => Response::Nil,
                }
            }
            _ => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        },
        None => Response::Nil,
    }
}

async fn handle_lrange(cmd: Command, db: &Db) -> Response {
    check_args!(cmd, 4);
    let key = String::from_utf8_lossy(&cmd.args[1]);
    let start_res = std::str::from_utf8(&cmd.args[2]).map_err(|_| "start is not valid utf8").and_then(| s|s.parse::<i64>().map_err(|_| "start is not a valid integer"));
    let stop_res = std::str::from_utf8(&cmd.args[3]).map_err(|_| "stop is not valid utf8").and_then(|s| s.parse::<i64>().map_err(|_| "stop is not a valid integer"));

    let (start, stop) = match (start_res, stop_res) {
        (Ok(s), Ok(st)) => (s, st),
        (Err(e), _) | (_, Err(e)) => return Response::Error(e.to_string()),
    };

    match db.get(&*key) {
        Some(entry) => match entry.value() {
            DbValue::List(list_lock) => {
                let list = list_lock.read().await;
                let len = list.len() as i64;
                // Redis semantics: negative indices count from end
                let mut start = if start < 0 { len + start } else { start };
                let mut stop = if stop < 0 { len + stop } else { stop };
                // Clamp to bounds
                if start < 0 { start = 0; }
                if stop < 0 { stop = 0; }
                if start >= len { return Response::MultiBytes(vec![]); }
                if stop >= len { stop = len - 1; }
                if start > stop { return Response::MultiBytes(vec![]); }
                let start = start as usize;
                let stop = stop as usize;
                let items = list.iter().skip(start).take(stop - start + 1).cloned().collect();
                Response::MultiBytes(items)
            }
            _ => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        },
        None => Response::MultiBytes(vec![]), // Return empty list if key does not exist
    }
}


async fn handle_sadd(cmd: Command, ctx: &AppContext) -> Response {
    if cmd.args.len() < 3 {
        return Response::Error("wrong number of arguments for 'sadd' command".to_string());
    }
    let key = String::from_utf8_lossy(&cmd.args[1]).to_string();
    let members: Vec<Vec<u8>> = cmd.args.into_iter().skip(2).collect();

    let log_entry = LogEntry::SAdd { key: key.clone(), members: members.clone() };
    if let Err(e) = log_and_wait!(&ctx.logger, log_entry) { return e; }
    
    let entry = ctx.db.entry(key.clone()).or_insert_with(|| DbValue::Set(RwLock::new(HashSet::new())));
    match entry.value() {
        DbValue::Set(set_lock) => {
            let mut set = set_lock.write().await;
            let mut added_count = 0;
            for m in members {
                if set.insert(m) {
                    added_count += 1;
                }
            }
            Response::Integer(added_count)
        }
        _ => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
    }
}

async fn handle_srem(cmd: Command, ctx: &AppContext) -> Response {
    if cmd.args.len() < 3 {
        return Response::Error("wrong number of arguments for 'srem' command".to_string());
    }
    let key = String::from_utf8_lossy(&cmd.args[1]).to_string();
    let members: Vec<Vec<u8>> = cmd.args.into_iter().skip(2).collect();

    let log_entry = LogEntry::SRem { key: key.clone(), members: members.clone() };
    if let Err(e) = log_and_wait!(&ctx.logger, log_entry) { return e; }

    match ctx.db.get_mut(&key) {
        Some(mut entry) => match entry.value_mut() {
            DbValue::Set(set_lock) => {
                let mut set = set_lock.write().await;
                let mut removed_count = 0;
                for m in members {
                    if set.remove(&m) {
                        removed_count += 1;
                    }
                }
                Response::Integer(removed_count)
            }
             _ => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        },
        None => Response::Integer(0),
    }
}

async fn handle_sismember(cmd: Command, db: &Db) -> Response {
    check_args!(cmd, 3);
    let key = String::from_utf8_lossy(&cmd.args[1]);
    let member = &cmd.args[2];
    match db.get(&*key) {
        Some(entry) => match entry.value() {
            DbValue::Set(set_lock) => {
                let set = set_lock.read().await;
                if set.contains(member) {
                    Response::Integer(1)
                } else {
                    Response::Integer(0)
                }
            }
            _ => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        },
        None => Response::Integer(0),
    }
}

async fn handle_smembers(cmd: Command, db: &Db) -> Response {
    check_args!(cmd, 2);
    let key = String::from_utf8_lossy(&cmd.args[1]);
    match db.get(&*key) {
        Some(entry) => match entry.value() {
            DbValue::Set(set_lock) => {
                let set = set_lock.read().await;
                Response::MultiBytes(set.iter().cloned().collect())
            }
             _ => Response::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        },
        None => Response::MultiBytes(vec![]),
    }
}

async fn handle_create_index(cmd: Command, ctx: &AppContext) -> Response {
    check_args!(cmd, 4);
    if String::from_utf8_lossy(&cmd.args[2]).to_uppercase() != "ON" {
        return Response::Error("Syntax error: expected CREATEINDEX <name> ON <prefix>".to_string());
    }
    let prefix = String::from_utf8_lossy(&cmd.args[1]).to_string();
    let json_path = String::from_utf8_lossy(&cmd.args[3]).to_string();

    let index_name = format!("{}|{}", prefix, json_path);
    if ctx.index_manager.indexes.contains_key(&index_name) {
        return Response::Error(format!("index '{}' already exists", index_name));
    }
    
    println!("Creating index '{}' on prefix '{}' with path '{}'", index_name, prefix, json_path);
    let new_index = Arc::new(Index::default());
    ctx.index_manager.indexes.insert(index_name.clone(), new_index.clone());
    ctx.index_manager.prefix_to_indexes.entry(prefix.clone()).or_default().push(index_name.clone());

    // --- Back-filling the index ---
    let mut index_writer = new_index.write().await;
    let pattern = prefix.strip_suffix('*').unwrap_or(&prefix);

    for item in ctx.db.iter() {
        if item.key().starts_with(pattern) {
            if let DbValue::Json(json_val) = item.value() {
                 if let Some(indexed_val) = json_val.pointer(&json_path_to_pointer(&json_path)) {
                    if !indexed_val.is_null() {
                        let index_key = serde_json::to_string(indexed_val).unwrap_or_default();
                        index_writer
                            .entry(index_key)
                            .or_default()
                            .insert(item.key().clone());
                    }
                }
            }
        }
    }
    println!("Index '{}' created and populated.", index_name);
    Response::Ok
}

// --- Helper Functions ---

fn json_path_to_pointer(path: &str) -> String {
    format!("/{}", path.replace('.', "/"))
}

fn apply_json_set_to_db(db: &Db, path: &str, value_to_set: Value) -> Result<()> {
    let path_parts: Vec<&str> = path.split('.').collect();
    let top_key = path_parts.first().ok_or_else(|| anyhow!("Path cannot be empty"))?.to_string();
    let mut entry = db.entry(top_key).or_insert_with(|| DbValue::Json(json!({})));

    match entry.value_mut() {
        DbValue::Json(json_val) => recursive_set(json_val, &path_parts[1..], value_to_set),
        _ => bail!("WRONGTYPE: Cannot apply JSON.SET to a non-JSON value"),
    }
}

fn apply_json_delete_to_db(db: &Db, path: &str) -> Result<bool> {
    let path_parts: Vec<&str> = path.split('.').collect();
    let top_key = path_parts.first().ok_or_else(|| anyhow!("Path cannot be empty"))?.to_string();

    if path_parts.len() == 1 {
        return Ok(db.remove(&top_key).is_some());
    }

    match db.get_mut(&top_key) {
        Some(mut entry) => match entry.value_mut() {
            DbValue::Json(json_val) => recursive_delete(json_val, &path_parts[1..]),
            _ => bail!("WRONGTYPE: Cannot apply JSON.DELETE to a non-JSON value"),
        }
        None => Ok(false),
    }
}

fn recursive_set<'a>(current: &'a mut Value, path: &'a [&str], value_to_set: Value) -> Result<()> {
    if path.is_empty() {
        *current = value_to_set;
        return Ok(());
    }
    let key = path[0];
    let next_val = current
        .as_object_mut()
        .ok_or_else(|| anyhow!("Cannot set value: part of the path is not an object."))?
        .entry(key)
        .or_insert(json!({}));
    recursive_set(next_val, &path[1..], value_to_set)
}

fn recursive_delete<'a>(current: &'a mut Value, path: &'a [&str]) -> Result<bool> {
    if path.is_empty() {
        bail!("Invalid delete path");
    }
    let key = path[0];
    let obj = current.as_object_mut()
        .ok_or_else(|| anyhow!("Cannot delete value: part of the path is not an object."))?;
    if path.len() == 1 {
        return Ok(obj.remove(key).is_some());
    }
    if let Some(next_val) = obj.get_mut(key) {
        recursive_delete(next_val, &path[1..])
    } else {
        Ok(false)
    }
}