pub mod ffi;
use anyhow::Result;
use dashmap::DashMap;
use futures::{Stream, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use tokio::task::JoinHandle;

// Core modules of the database engine
pub mod arc;
pub mod commands;
pub mod config;
pub mod cypher_engine;
pub mod indexing;
pub mod memory;
pub mod persistence;
pub mod protocol;
pub mod query_engine;
pub mod schema;
pub mod storage;
pub mod storage_executor;
pub mod transaction;
pub mod types;
pub mod vacuum;

// Public exports for the library API
use crate::config::Config;
use crate::indexing::IndexManager;
use crate::memory::MemoryManager;
use crate::persistence::{PersistenceEngine, load_db_from_disk};
use crate::query_engine::functions;
use crate::schema::{
    ColumnDefinition, DataType, SchemaSource, VIEW_PREFIX, VirtualSchema, load_schemas_from_db,
};
use crate::transaction::TransactionHandle;
use crate::types::{
    AppContext, Db, DbValue, FunctionRegistry, PersistenceRequest, SchemaCache,
    TransactionIdManager, TransactionStatusManager, ViewCache, ViewDefinition,
};
use std::collections::{BTreeMap, HashSet};

/// The main database instance, providing the primary API for interaction.
pub struct MemFluxDB {
    pub app_context: Arc<AppContext>,
    // The handle to the persistence engine's background task.
    // Kept to ensure the task is not dropped prematurely.
    _persistence_handle: Option<JoinHandle<()>>,
    // The handle to the vacuum background task.
    _vacuum_handle: Option<JoinHandle<()>>,
}

impl MemFluxDB {
    /// Opens or creates a database instance based on the provided configuration object.
    /// This is the core constructor used by both the server and the FFI layer.
    pub async fn open_with_config(config: Config) -> Result<Self> {
        let config = Arc::new(config);

        let db = if config.persistence {
            load_db_from_disk(
                &config.snapshot_file,
                &config.wal_file,
                &config.wal_overflow_file,
            )
            .await?
        } else {
            println!("Persistence is disabled. Starting with an in-memory database.");
            Arc::new(DashMap::new())
        };
        if config.persistence {
            println!("Database loaded with {} top-level keys.", db.len());
        }

        let tx_id_manager = Arc::new(TransactionIdManager::new());
        let tx_status_manager = Arc::new(TransactionStatusManager::new());
        // let active_transactions = Arc::new(DashMap::new()); // No longer used with FluxMap

        // Create a dummy logger channel to satisfy AppContext.
        // FluxMap handles its own logging internally if configured.
        let (logger, persistence_handle) = {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<PersistenceRequest>(1024);
            let handle = tokio::spawn(async move {
                while let Some(req) = rx.recv().await {
                    match req {
                        PersistenceRequest::Log(log_req) => {
                            // Auto-ack legacy log requests so they don't hang if called
                            let _ = log_req.ack.send(Ok(()));
                        }
                        PersistenceRequest::Sync(ack) => {
                            let _ = ack.send(Ok(()));
                        }
                    }
                }
            });
            (tx, Some(handle))
        };

        let memory_manager = Arc::new(MemoryManager::new(
            config.maxmemory_mb,
            config.eviction_policy.clone(),
        ));
        if memory_manager.is_enabled() {
            println!(
                "Maxmemory policy is enabled ({}MB) with \'{:?}\' eviction policy.",
                config.maxmemory_mb, config.eviction_policy
            );
            println!(
                "Default transaction isolation level: {:?}.",
                config.isolation_level
            );
        }

        // Initialize FluxMap database (in-memory for Phase 1)
        // Note: Persistence and full memory management integration will be completed in later phases.
        // For now, we initialize an in-memory FluxMap database.
        let flux_db = match fluxmap::db::Database::new_in_memory().await {
            Ok(db) => Arc::new(db),
            Err(e) => return Err(anyhow::anyhow!("Failed to initialize FluxMap: {}", e)),
        };

        let backend = crate::storage::flux::FluxMapBackend::new(flux_db.clone());
        let storage: Arc<dyn crate::storage::StorageEngine> = Arc::new(backend);

        let schema_cache = Arc::new(DashMap::new());
        if let Err(e) =
            load_schemas_from_db(&storage, &schema_cache).await
        {
            eprintln!("Warning: Could not load virtual schemas: {}.", e);
        } else if !schema_cache.is_empty() {
            println!("Loaded {} virtual schemas.", schema_cache.len());
        }

        if let Err(e) =
            load_graph_schemas_from_db(&storage, &schema_cache).await
        {
            eprintln!("Warning: Could not load graph virtual schemas: {}.", e);
        }

        let view_cache = Arc::new(DashMap::new());
        if let Err(e) =
            load_views_from_db(&storage, &view_cache).await
        {
            eprintln!("Warning: Could not load views: {}.", e);
        } else if !view_cache.is_empty() {
            println!("Loaded {} views.", view_cache.len());
        }
        
        // Memory priming - uses storage scan now
        println!("Calculating initial memory usage...");
        let mut initial_mem: u64 = 0;
        let all_data = storage.prefix_scan("").await;
        let mut keys = Vec::new();
        for (key, value) in all_data {
            let key_size = key.len() as u64;
            let value_size = memory::estimate_db_value_size(&value).await;
            initial_mem += key_size + value_size;
            keys.push(key);
        }
        memory_manager.increase_memory(initial_mem);
        if memory_manager.is_enabled() {
            memory_manager.prime(keys).await;
        }
        println!(
            "Initial memory usage: {} MB",
            memory_manager.current_memory() / 1024 / 1024
        );

        let index_manager = Arc::new(IndexManager::default());
        let json_cache = Arc::new(DashMap::new());
        let mut function_registry = FunctionRegistry::new();
        functions::register_string_functions(&mut function_registry);
        functions::register_numeric_functions(&mut function_registry);
        functions::register_datetime_functions(&mut function_registry);
        let function_registry = Arc::new(function_registry);

        let app_context = Arc::new(AppContext {
            storage: storage.clone(),
            logger,
            index_manager,
            json_cache,
            schema_cache,
            view_cache,
            function_registry,
            config: config.clone(),
            memory: memory_manager,
            table_locks: Arc::new(DashMap::new()),
        });

        if app_context.memory.is_enabled()
            && app_context.memory.current_memory() > app_context.memory.max_memory()
        {
            println!(
                "Initial memory usage ({}MB) exceeds maxmemory ({}MB). Evicting keys...",
                app_context.memory.current_memory() / 1024 / 1024,
                app_context.memory.max_memory() / 1024 / 1024
            );
            if let Err(e) = app_context.memory.ensure_memory_for(0, &app_context).await {
                eprintln!("Error during initial eviction: {}.", e);
            } else {
                println!(
                    "Memory usage after initial eviction: {} MB",
                    app_context.memory.current_memory() / 1024 / 1024
                );
            }
        }

        let vacuum_storage = storage.clone();
        let vacuum_handle = tokio::spawn(async move {
            // Run vacuum every 60 seconds.
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                println!("Running background vacuum...");
                match vacuum_storage.vacuum().await {
                    Ok((versions, keys)) => {
                        if versions > 0 || keys > 0 {
                            println!(
                                "Vacuum complete. Removed {} versions and {} keys.",
                                versions, keys
                            );
                        }
                    }
                    Err(e) => eprintln!("Error during background vacuum: {}", e),
                }
            }
        });

        Ok(MemFluxDB {
            app_context,
            _persistence_handle: persistence_handle,
            _vacuum_handle: Some(vacuum_handle),
        })
    }

    /// Opens or creates a database instance based on the provided configuration file.
    /// This is a convenience wrapper around `open_with_config` for server use.
    pub async fn open(config_path: &str) -> Result<Self> {
        let config = Config::load(config_path)?;
        Self::open_with_config(config).await
    }

    /// Executes a SQL query and returns a stream of result rows.
    pub fn execute_sql_stream<'a>(
        &'a self,
        sql: &'a str,
        transaction_handle: TransactionHandle,
    ) -> impl Stream<Item = Result<Value>> + Send + 'a {
        use query_engine::{ast_to_logical_plan, execute, logical_to_physical_plan};

        async_stream::try_stream! {
            let physical_plan_result = (|| {
                let ast = query_engine::simple_parser::parse_sql(sql)?;
                let logical_plan = ast_to_logical_plan(
                    ast,
                    &self.app_context.schema_cache,
                    &self.app_context.view_cache,
                    &self.app_context.function_registry,
                )?;
                logical_to_physical_plan(logical_plan, &self.app_context)
            })();

            match physical_plan_result {
                Ok(physical_plan) => {
                    // TODO: Pass transaction handle down to the query engine
                    let mut stream = execute(physical_plan, self.app_context.clone(), None, None, Some(transaction_handle));
                    while let Some(row_result) = stream.next().await {
                        yield row_result?;
                    }
                }
                Err(e) => {
                    yield Err(e)?;
                }
            }
        }
    }

    pub fn execute_cypher_stream<'a>(
        &'a self,
        cypher: &'a str,
        transaction_handle: TransactionHandle,
    ) -> impl Stream<Item = Result<Value>> + Send + 'a {
        use cypher_engine::{execution, logical_plan, parser, physical_plan};

        async_stream::try_stream! {
            let plan_result = (|| {
                let ast = parser::parse_cypher(cypher)?;
                let logical = logical_plan::ast_to_logical_plan(ast, &self.app_context.index_manager)?;
                physical_plan::logical_to_physical_plan(logical, &self.app_context.index_manager)
            })();

            match plan_result {
                Ok(physical_plan) => {
                    let mut stream = Box::pin(execution::execute(physical_plan, self.app_context.clone(), transaction_handle));
                    while let Some(row_result) = stream.next().await {
                        yield row_result?;
                    }
                }
                Err(e) => {
                    yield Err(e)?;
                }
            }
        }
    }

    /// Executes a command, either SQL or a direct database command.
    pub async fn execute_command(
        &self,
        command: types::Command,
        transaction_handle: TransactionHandle,
    ) -> types::Response {
        if command.name == "SQL" {
            let sql = command.args[1..]
                .iter()
                .map(|arg| String::from_utf8_lossy(arg))
                .collect::<Vec<_>>()
                .join(" ");

            let ast = query_engine::simple_parser::parse_sql(&sql);
            let is_select_like = match ast {
                Ok(query_engine::AstStatement::Select(_)) => true,
                Ok(query_engine::AstStatement::Insert(s)) => !s.returning.is_empty(),
                Ok(query_engine::AstStatement::Update(s)) => !s.returning.is_empty(),
                Ok(query_engine::AstStatement::Delete(s)) => !s.returning.is_empty(),
                _ => false,
            };

            let mut stream = Box::pin(self.execute_sql_stream(&sql, transaction_handle));

            if is_select_like {
                let mut rows = Vec::new();
                while let Some(row_result) = stream.next().await {
                    match row_result {
                        Ok(val) => rows.push(val),
                        Err(e) => {
                            return types::Response::Error(format!("Execution Error: {}", e));
                        }
                    }
                }
                // Convert Vec<Value> to MultiBytes response
                let mut multi_bytes = Vec::new();
                for row in rows {
                    multi_bytes.push(row.to_string().into_bytes());
                }
                types::Response::MultiBytes(multi_bytes)
            } else {
                let mut final_response = types::Response::Ok; // Default to OK
                let mut encountered_error = None;

                while let Some(result) = stream.next().await {
                    match result {
                        Ok(value) => {
                            if let Some(count) = value.get("rows_affected").and_then(|v| v.as_i64())
                            {
                                final_response = types::Response::Integer(count);
                            } else {
                                final_response = types::Response::Ok; // Or some other success indicator
                            }
                        }
                        Err(e) => {
                            encountered_error =
                                Some(types::Response::Error(format!("Execution Error: {}", e)));
                            break; // Stop processing on first error
                        }
                    }
                }

                if let Some(err_resp) = encountered_error {
                    err_resp
                } else {
                    final_response
                }
            }
        } else if command.name == "CYPHER" {
            let cypher = command.args[1..]
                .iter()
                .map(|arg| String::from_utf8_lossy(arg))
                .collect::<Vec<_>>()
                .join(" ");
            let mut stream = Box::pin(self.execute_cypher_stream(&cypher, transaction_handle));
            let mut rows = Vec::new();
            while let Some(row_result) = stream.next().await {
                match row_result {
                    Ok(val) => rows.push(val.to_string().into_bytes()),
                    Err(e) => return types::Response::Error(format!("Execution Error: {}", e)),
                }
            }
            types::Response::MultiBytes(rows)
        } else {
            commands::process_command(command, self.app_context.clone(), transaction_handle).await
        }
    }
}

/// Loads view definitions from the database.
pub async fn load_views_from_db(
    storage: &Arc<dyn crate::storage::StorageEngine>,
    view_cache: &ViewCache,
) -> Result<()> {
    let items_to_process = storage.prefix_scan(VIEW_PREFIX).await;

    for (key, db_value) in items_to_process {
        let view_def_result: std::result::Result<ViewDefinition, _> = match &db_value {
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
    Ok(())
}

pub async fn load_graph_schemas_from_db(
    storage: &Arc<dyn crate::storage::StorageEngine>,
    schema_cache: &SchemaCache,
) -> Result<()> {
    let mut node_labels = HashSet::new();
    let mut rel_types = HashSet::new();

    // Scan for nodes
    let node_keys = storage.prefix_scan("_pk_node:").await;
    for (_, value) in node_keys {
        if let DbValue::Bytes(label_bytes) = value {
            if let Ok(label) = String::from_utf8(label_bytes) {
                node_labels.insert(label);
            }
        }
    }

    // Scan for relationships
    let edge_keys = storage.prefix_scan("_edge:out:").await;
    for (key, _) in edge_keys {
        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() >= 4 {
            rel_types.insert(parts[3].to_string());
        }
    }

    for label in node_labels {
        if schema_cache.contains_key(&label) {
            continue; // Don't overwrite native schemas
        }
        let mut columns = BTreeMap::new();
        columns.insert(
            "_id".to_string(),
            ColumnDefinition {
                data_type: DataType::Text,
                nullable: false,
                default: None,
            },
        );
        // All node properties are treated as JSONB for now. We can infer this later.
        // A generic 'properties' column is a good start.
        columns.insert(
            "properties".to_string(),
            ColumnDefinition {
                data_type: DataType::JsonB,
                nullable: true,
                default: None,
            },
        );

        let schema = VirtualSchema {
            table_name: label.clone(),
            columns,
            column_order: vec!["_id".to_string(), "properties".to_string()],
            constraints: vec![],
            source: SchemaSource::GraphNode,
        };
        println!("Registering virtual table for graph node label: {}", label);
        schema_cache.insert(label, Arc::new(schema));
    }

    for rel_type in rel_types {
        if schema_cache.contains_key(&rel_type) {
            continue; // Don't overwrite
        }
        let mut columns = BTreeMap::new();
        columns.insert(
            "_id".to_string(),
            ColumnDefinition {
                data_type: DataType::Text,
                nullable: false,
                default: None,
            },
        );
        columns.insert(
            "_from_id".to_string(),
            ColumnDefinition {
                data_type: DataType::Text,
                nullable: false,
                default: None,
            },
        );
        columns.insert(
            "_to_id".to_string(),
            ColumnDefinition {
                data_type: DataType::Text,
                nullable: false,
                default: None,
            },
        );
        columns.insert(
            "properties".to_string(),
            ColumnDefinition {
                data_type: DataType::JsonB,
                nullable: true,
                default: None,
            },
        );

        let schema = VirtualSchema {
            table_name: rel_type.clone(),
            columns,
            column_order: vec![
                "_id".to_string(),
                "_from_id".to_string(),
                "_to_id".to_string(),
                "properties".to_string(),
            ],
            constraints: vec![],
            source: SchemaSource::GraphRelationship,
        };
        println!(
            "Registering virtual table for graph relationship type: {}",
            rel_type
        );
        schema_cache.insert(rel_type, Arc::new(schema));
    }

    Ok(())
}
