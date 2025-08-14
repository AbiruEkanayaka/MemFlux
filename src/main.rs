use anyhow::Result;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::AsyncWriteExt;

mod commands;
mod indexing;
mod persistence;
mod protocol;
mod query_engine;
mod schema;
mod types;

use indexing::IndexManager;
use persistence::{load_db_from_disk, PersistenceEngine};
use protocol::parse_command_from_stream;
use query_engine::functions;
use schema::load_schemas_from_db;
use types::{AppContext, FunctionRegistry, Response};

const WAL_FILE: &str = "memflux.wal";
const SNAPSHOT_FILE: &str = "memflux.snapshot";

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

    // 3. Load virtual schemas
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

    // 4. Set up the Application Context
    let index_manager = Arc::new(IndexManager::default());
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
        function_registry,
    });

    // 5. Start the server
    let listener = TcpListener::bind("127.0.0.1:6380").await?;
    println!("MemFlux (RESP Protocol) listening on 127.0.0.1:6380");
    loop {
        let (stream, _) = listener.accept().await?;
        let context_clone = app_context.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, context_clone).await {
                if e.downcast_ref::<std::io::Error>()
                    .map_or(true, |io_err| io_err.kind() != std::io::ErrorKind::BrokenPipe)
                {
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
                                let response = Response::Error(format!("Optimization Error: {}", e));
                                writer.write_all(&response.into_protocol_format()).await?;
                                continue;
                            }
                        };

                    // Start streaming RESP array
                    use futures::StreamExt;
                    let mut stream_rows = Box::pin(query_engine::execute(physical_plan, ctx.clone(), None));

                    if is_select {
                        // For SELECT, we always stream back rows.
                        // We collect them first to set the RESP array header.
                        let mut rows = Vec::new();
                        let mut error_occured = false;
                        while let Some(row_result) = stream_rows.next().await {
                            match row_result {
                                Ok(val) => rows.push(val),
                                Err(e) => {
                                    let response = Response::Error(format!("Execution Error: {}", e));
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
                                    let response = Response::Error(format!("Execution Error: {}", e));
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


