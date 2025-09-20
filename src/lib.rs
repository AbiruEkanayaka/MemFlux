pub mod ffi;
use anyhow::{Result};
use dashmap::DashMap;
use futures::{Stream, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::sync::{RwLock};

// Core modules of the database engine
pub mod arc;
pub mod commands;
pub mod config;
pub mod indexing;
pub mod memory;
pub mod persistence;
pub mod protocol;
pub mod query_engine;
pub mod schema;
pub mod types;

// Public exports for the library API
use crate::config::Config;
use crate::indexing::IndexManager;
use crate::memory::MemoryManager;
use crate::persistence::{load_db_from_disk, PersistenceEngine};
use crate::query_engine::functions;
use crate::schema::{load_schemas_from_db, VIEW_PREFIX};
use crate::types::{AppContext, Db, FunctionRegistry, ViewCache, ViewDefinition};

/// The main database instance, providing the primary API for interaction.
pub struct MemFluxDB {
    pub app_context: Arc<AppContext>,
    // The handle to the persistence engine's background task.
    // Kept to ensure the task is not dropped prematurely.
    _persistence_handle: Option<JoinHandle<()>>, 
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

        let (logger, persistence_handle) = if config.persistence {
            let (persistence_engine, logger) = PersistenceEngine::new(&config, db.clone());
            let handle = tokio::spawn(async move {
                if let Err(e) = persistence_engine.run().await {
                    eprintln!("Fatal error in persistence engine: {}", e);
                }
            });
            (logger, Some(handle))
        } else {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<crate::types::LogRequest>(1024);
            tokio::spawn(async move {
                while let Some(req) = rx.recv().await {
                    let _ = req.ack.send(Ok(()));
                }
            });
            (tx, None)
        };

        let schema_cache = Arc::new(DashMap::new());
        if let Err(e) = load_schemas_from_db(&db, &schema_cache).await {
            eprintln!("Warning: Could not load virtual schemas: {}.", e);
        } else if !schema_cache.is_empty() {
            println!("Loaded {} virtual schemas.", schema_cache.len());
        }

        let view_cache = Arc::new(DashMap::new());
        if let Err(e) = load_views_from_db(&db, &view_cache).await {
            eprintln!("Warning: Could not load views: {}.", e);
        } else if !view_cache.is_empty() {
            println!("Loaded {} views.", view_cache.len());
        }

        let memory_manager = Arc::new(MemoryManager::new(
            config.maxmemory_mb,
            config.eviction_policy.clone(),
        ));
        if memory_manager.is_enabled() {
            println!(
                "Maxmemory policy is enabled ({}MB) with \'{:?}\' eviction policy.",
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
            view_cache,
            function_registry,
            config: config.clone(),
            memory: memory_manager,
            current_transaction: Arc::new(RwLock::new(None)),
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

        Ok(MemFluxDB {
            app_context,
            _persistence_handle: persistence_handle,
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
                logical_to_physical_plan(logical_plan, &self.app_context.index_manager)
            })();

            match physical_plan_result {
                Ok(physical_plan) => {
                    let mut stream = execute(physical_plan, self.app_context.clone(), None, None);
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
    pub async fn execute_command(&self, command: types::Command) -> types::Response {
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

            let mut stream = Box::pin(self.execute_sql_stream(&sql));

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
                if let Some(result) = stream.next().await {
                    match result {
                        Ok(value) => {
                            if let Some(count) = value.get("rows_affected").and_then(|v| v.as_i64()) {
                                types::Response::Integer(count)
                            } else {
                                types::Response::Ok
                            }
                        }
                        Err(e) => {
                            types::Response::Error(format!("Execution Error: {}", e))
                        }
                    }
                } else {
                    types::Response::Error("Command executed with no result".to_string())
                }
            }
        } else {
            commands::process_command(command, &self.app_context).await
        }
    }
}

/// Loads view definitions from the database.
pub async fn load_views_from_db(db: &Db, view_cache: &ViewCache) -> Result<()> {
    for item in db.iter() {
        let key = item.key();
        if key.starts_with(VIEW_PREFIX) {
            let view_def_result: std::result::Result<ViewDefinition, _> = match item.value() {
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
    }
    Ok(())
}
