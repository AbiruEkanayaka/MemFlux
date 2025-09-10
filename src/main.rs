use anyhow::{anyhow, Result};
use dashmap::DashMap;
use std::fs::File;
use std::io::BufReader as StdBufReader;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

mod arc;
mod commands;
mod config;
mod indexing;
mod memory;
mod persistence;
mod protocol;
mod query_engine;
mod schema;
mod types;

use config::Config;
use indexing::IndexManager;
use memory::MemoryManager;
use persistence::{load_db_from_disk, PersistenceEngine};
use protocol::parse_command_from_stream;
use query_engine::functions;
use schema::{load_schemas_from_db, VIEW_PREFIX};
use types::{AppContext, FunctionRegistry, Response, ViewCache, ViewDefinition};

fn generate_self_signed_cert(cert_path: &str, key_path: &str) -> Result<()> {
    println!("Generating self-signed certificate...");
    let subject_alt_names = vec!["localhost".to_string()];
    let cert = rcgen::generate_simple_self_signed(subject_alt_names)?;
    std::fs::write(cert_path, cert.serialize_pem()?)?;
    std::fs::write(key_path, cert.serialize_private_key_pem())?;
    println!("Certificate created at: {}", cert_path);
    println!("Key created at: {}", key_path);
    Ok(())
}

fn load_tls_config(config: &Config) -> Result<Arc<rustls::ServerConfig>> {
    let cert_path = &config.cert_file;
    let key_path = &config.key_file;

    if !Path::new(cert_path).exists() || !Path::new(key_path).exists() {
        generate_self_signed_cert(cert_path, key_path)?;
    }

    let certs = {
        let cert_file = File::open(cert_path)?;
        let mut cert_reader = StdBufReader::new(cert_file);
        rustls_pemfile::certs(&mut cert_reader)?
            .into_iter()
            .map(rustls::Certificate)
            .collect()
    };

    let key = {
        let key_file = File::open(key_path)?;
        let mut key_reader = StdBufReader::new(key_file);
        rustls_pemfile::pkcs8_private_keys(&mut key_reader)?
            .into_iter()
            .map(rustls::PrivateKey)
            .next()
            .ok_or_else(|| anyhow!("No private key found in {}", key_path))?
    };

    let tls_config = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(Arc::new(tls_config))
}

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Load configuration
    let config = Arc::new(Config::load("config.json")?);

    // 2. Load DB state from disk (snapshot then WAL)
    let db = load_db_from_disk(
        &config.snapshot_file,
        &config.wal_file,
        &config.wal_overflow_file,
    )
    .await?;
    println!("Database loaded with {} top-level keys.", db.len());

    // 3. Set up the persistence engine
    let (persistence_engine, logger) = PersistenceEngine::new(&config, db.clone());
    tokio::spawn(async move {
        if let Err(e) = persistence_engine.run().await {
            eprintln!("Fatal error in persistence engine: {}", e);
            std::process::exit(1);
        }
    });

    // 4. Load virtual schemas
    let schema_cache = Arc::new(DashMap::new());
    if let Err(e) = load_schemas_from_db(&db, &schema_cache).await {
        eprintln!(
            "Warning: Could not load virtual schemas: {}. Continuing without them.",
            e
        );
    } else {
        if !schema_cache.is_empty() {
            println!("Loaded {} virtual schemas.", schema_cache.len());
        }
    }

    // Load views
    let view_cache = Arc::new(DashMap::new());
    if let Err(e) = load_views_from_db(&db, &view_cache).await {
        eprintln!(
            "Warning: Could not load views: {}. Continuing without them.",
            e
        );
    } else {
        if !view_cache.is_empty() {
            println!("Loaded {} views.", view_cache.len());
        }
    }

    // 5. Set up Memory Manager and calculate initial usage
    let memory_manager = Arc::new(MemoryManager::new(
        config.maxmemory_mb,
        config.eviction_policy.clone(),
    ));
    if memory_manager.is_enabled() {
        println!(
            "Maxmemory policy is enabled ({}MB) with '{:?}' eviction policy.",
            config.maxmemory_mb, config.eviction_policy
        );
    }
    println!("Calculating initial memory usage...");
    let mut initial_mem: u64 = 0;
    let mut keys = Vec::new();
    for item in db.iter() {
        let key_size = item.key().len() as u64;
        let value_size = memory::estimate_db_value_size(item.value()).await;
        initial_mem += key_size + value_size;
        keys.push(item.key().clone());
    }
    memory_manager.increase_memory(initial_mem);
    if memory_manager.is_enabled() {
        memory_manager.prime(keys).await;
    }
    println!(
        "Initial memory usage: {} MB",
        memory_manager.current_memory() / 1024 / 1024
    );

    // 6. Set up Index Manager
    let index_manager = Arc::new(IndexManager::default());

    // 7. Enforce memory limit at startup if needed
    if memory_manager.is_enabled() && memory_manager.current_memory() > memory_manager.max_memory() {
        println!(
            "Initial memory usage ({}MB) exceeds maxmemory ({}MB). Evicting keys...",
            memory_manager.current_memory() / 1024 / 1024,
            memory_manager.max_memory() / 1024 / 1024
        );
        
        // Create a temporary context for eviction
        let temp_ctx = AppContext {
            db: db.clone(),
            logger: logger.clone(),
            index_manager: index_manager.clone(),
            json_cache: Arc::new(DashMap::new()), // Not needed for eviction
            schema_cache: schema_cache.clone(),   // Not needed for eviction
            view_cache: view_cache.clone(),       // Not needed for eviction
            function_registry: Arc::new(FunctionRegistry::new()), // Not needed for eviction
            config: config.clone(),
            memory: memory_manager.clone(),
        };

        if let Err(e) = memory_manager.ensure_memory_for(0, &temp_ctx).await {
            eprintln!("Error during initial eviction: {}. Server may be over memory limit.", e);
        } else {
            println!(
                "Memory usage after initial eviction: {} MB",
                memory_manager.current_memory() / 1024 / 1024
            );
        }
    }

    // 8. Set up the Application Context
    let json_cache = Arc::new(DashMap::new());
    let mut function_registry = FunctionRegistry::new();
    functions::register_string_functions(&mut function_registry);
    functions::register_numeric_functions(&mut function_registry);
    functions::register_datetime_functions(&mut function_registry);
    let function_registry = Arc::new(function_registry);
    let app_context = Arc::new(AppContext {
        db,
        logger,
        index_manager,
        json_cache,
        schema_cache,
        view_cache,
        function_registry,
        config: config.clone(),
        memory: memory_manager,
    });

    // 9. Start the server
    let addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&addr).await?;

    let acceptor: Option<TlsAcceptor> = if config.encrypt {
        println!("TLS encryption is enabled.");
        let tls_config = load_tls_config(&config)?;
        Some(TlsAcceptor::from(tls_config))
    } else {
        None
    };

    println!("MemFlux (RESP Protocol) listening on {}", addr);
    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let context_clone = app_context.clone();

        if let Some(acceptor) = acceptor.clone() {
            tokio::spawn(async move {
                match acceptor.accept(stream).await {
                    Ok(tls_stream) => {
                        if let Err(e) = handle_connection(tls_stream, context_clone).await {
                            if e.downcast_ref::<std::io::Error>().map_or(true, |io_err| {
                                io_err.kind() != std::io::ErrorKind::BrokenPipe
                            }) {
                                eprintln!("Connection error from {}: {:?}", peer_addr, e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("TLS handshake error from {}: {}", peer_addr, e);
                    }
                }
            });
        } else {
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, context_clone).await {
                    if e.downcast_ref::<std::io::Error>().map_or(true, |io_err| {
                        io_err.kind() != std::io::ErrorKind::BrokenPipe
                    }) {
                        eprintln!("Connection error from {}: {:?}", peer_addr, e);
                    }
                }
            });
        }
    }
}

async fn load_views_from_db(db: &types::Db, view_cache: &ViewCache) -> Result<()> {
    for item in db.iter() {
        let key = item.key();
        if key.starts_with(VIEW_PREFIX) {
            let view_def_result: Result<ViewDefinition, _> = match item.value() {
                types::DbValue::Bytes(bytes) => serde_json::from_slice(bytes),
                _ => {
                    eprintln!(
                        "Warning: View key '{}' has non-Bytes value type. Skipping.",
                        key
                    );
                    continue;
                }
            };

            match view_def_result {
                Ok(view_def) => {
                    // Validate view definition has required fields
                    if view_def.name.is_empty() {
                        eprintln!("Warning: View with empty name in key '{}'. Skipping.", key);
                        continue;
                    }
                    let view_name = view_def.name.clone();
                    view_cache.insert(view_name, Arc::new(view_def));
                }
                Err(e) => {
                    eprintln!("Failed to parse view for key '{}': {}", key, e);
                }
            }
        }
    }
    Ok(())
}

/// Handles a single client connection.
async fn handle_connection<S>(stream: S, ctx: Arc<AppContext>) -> Result<()> 
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut buf_reader = BufReader::new(reader);
    let mut authenticated = ctx.config.requirepass.is_empty();

    loop {
        match parse_command_from_stream(&mut buf_reader).await {
            Ok(Some(command)) => {
                if !authenticated {
                    if command.name == "AUTH" {
                        if command.args.len() == 2
                            && String::from_utf8_lossy(&command.args[1]) == ctx.config.requirepass
                        {
                            authenticated = true;
                            writer.write_all(&Response::Ok.into_protocol_format()).await?;
                        } else {
                            let response = Response::Error("Invalid password".to_string());
                            writer.write_all(&response.into_protocol_format()).await?;
                        }
                    } else {
                        let response =
                            Response::Error("NOAUTH Authentication required.".to_string());
                        writer.write_all(&response.into_protocol_format()).await?;
                    }
                    continue;
                }

                // Special-case SQL for streaming
                if command.name == "SQL" {
                    // Stream SQL results row-by-row
                    let sql = match command.args[1..]
                        .iter()
                        .map(|arg| String::from_utf8(arg.clone()))
                        .collect::<Result<Vec<_>, _>>()
                    {
                        Ok(parts) => parts.join(" "),
                        Err(_) => {
                            let response =
                                Response::Error("Invalid UTF-8 in SQL query".to_string());
                            writer.write_all(&response.into_protocol_format()).await?;
                            continue;
                        }
                    };

                    // Parse and plan as before
                    let ast = match query_engine::simple_parser::parse_sql(&sql) {
                        Ok(stmt) => stmt,
                        Err(e) => {
                            let response = Response::Error(format!("SQL Parse Error: {}", e));
                            writer.write_all(&response.into_protocol_format()).await?;
                            continue;
                        }
                    };

                    let is_select = matches!(ast, query_engine::AstStatement::Select(_));
                    let has_returning = match &ast {
                        query_engine::AstStatement::Insert(s) => !s.returning.is_empty(),
                        query_engine::AstStatement::Update(s) => !s.returning.is_empty(),
                        query_engine::AstStatement::Delete(s) => !s.returning.is_empty(),
                        _ => false,
                    };
                    let is_select_like = is_select || has_returning;

                    let _is_ddl = matches!(
                        ast,
                        query_engine::AstStatement::CreateTable(_)
                            | query_engine::AstStatement::DropTable(_)
                            | query_engine::AstStatement::AlterTable(_)
                            | query_engine::AstStatement::TruncateTable(_)
                    );

                    let logical_plan = match query_engine::ast_to_logical_plan(
                        ast,
                        &ctx.schema_cache,
                        &ctx.view_cache,
                        &ctx.function_registry,
                    ) {
                        Ok(plan) => plan,
                        Err(e) => {
                            let response = Response::Error(format!("Planning Error: {}", e));
                            writer.write_all(&response.into_protocol_format()).await?;
                            continue;
                        }
                    };
                    let physical_plan =
                        match query_engine::logical_to_physical_plan(logical_plan, &ctx.index_manager)
                        {
                            Ok(plan) => plan,
                            Err(e) => {
                                let response =
                                    Response::Error(format!("Optimization Error: {}", e));
                                writer.write_all(&response.into_protocol_format()).await?;
                                continue;
                            }
                        };

                    // Start streaming RESP array
                    use futures::StreamExt;
                    let mut stream_rows =
                        Box::pin(query_engine::execute(physical_plan, ctx.clone(), None));

                    if is_select_like {
                        // For SELECT, we always stream back rows.
                        // We collect them first to set the RESP array header.
                        let mut rows = Vec::new();
                        let mut error_occured = false;
                        while let Some(row_result) = stream_rows.next().await {
                            match row_result {
                                Ok(val) => rows.push(val),
                                Err(e) => {
                                    let response =
                                        Response::Error(format!("Execution Error: {}", e));
                                    writer.write_all(&response.into_protocol_format()).await?;
                                    error_occured = true;
                                    break;
                                }
                            }
                        }

                        if !error_occured {
                            // Write RESP array header
                            let header = format!("*{}\r\n", rows.len()).into_bytes();
                            writer.write_all(&header).await?;
                            // Write each row as a bulk string (JSON)
                            for row in rows {
                                let s = row.to_string();
                                let s_bytes = s.as_bytes();
                                let bulk_header = format!("${}\r\n", s_bytes.len()).into_bytes();
                                writer.write_all(&bulk_header).await?;
                                writer.write_all(s_bytes).await?;
                                writer.write_all(b"\r\n").await?;
                            }
                        }
                    } else {
                        // This branch handles DML (INSERT, UPDATE, DELETE) and DDL (CREATE, etc.)
                        if let Some(result) = stream_rows.next().await {
                            match result {
                                Ok(value) => {
                                    let response = if let Some(count) =
                                        value.get("rows_affected").and_then(|v| v.as_i64())
                                    {
                                        Response::Integer(count)
                                    } else {
                                        // DDL commands like CREATE TABLE fall here
                                        Response::Ok
                                    };
                                    writer.write_all(&response.into_protocol_format()).await?;
                                }
                                Err(e) => {
                                    let response =
                                        Response::Error(format!("Execution Error: {}", e));
                                    writer.write_all(&response.into_protocol_format()).await?;
                                }
                            }
                        } else {
                            let response =
                                Response::Error("Command executed with no result".to_string());
                            writer.write_all(&response.into_protocol_format()).await?;
                        }
                    }
                    continue;
                }

                // Non-SQL: respond as before
                let response = commands::process_command(command, &ctx).await;
                writer.write_all(&response.into_protocol_format()).await?;
            }
            Ok(None) => {
                println!("Client disconnected.");
                break;
            }
            Err(e) => {
                let response = Response::Error(e.to_string());
                let _ = writer.write_all(&response.into_protocol_format()).await;
                if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                    if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}


