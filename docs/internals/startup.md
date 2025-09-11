# Internals: Server & Library Initialization

This document outlines the step-by-step process MemFlux follows when it starts up. Due to the refactoring to support both a server and a library, the initialization logic is now centralized in `src/lib.rs` and used by both the server (`src/main.rs`) and the FFI layer (`src/ffi.rs`).

**Source Files:**
*   `src/lib.rs` (Core initialization logic)
*   `src/main.rs` (Server entry point)
*   `src/ffi.rs` (Library entry point)

## Central Initialization: `MemFluxDB::open_with_config`

The core of the startup sequence resides in the `MemFluxDB::open_with_config` function in `src/lib.rs`. This asynchronous function is responsible for creating a fully-hydrated database instance.

1.  **Load Database from Disk:**
    *   The `load_db_from_disk` function from `src/persistence.rs` is called.
    *   It first loads the most recent snapshot file (`memflux.snapshot`).
    *   It then replays the records from the WAL files (`memflux.wal` and `memflux.wal.overflow`) to restore any data written after the last snapshot.
    *   The result is a `Db` (`Arc<DashMap<String, DbValue>>`), the core in-memory data store.

2.  **Start Persistence Engine:**
    *   A `PersistenceEngine` is initialized and spawned on a separate Tokio task.
    *   This engine receives write commands via a channel (`Logger`), writes them to the active WAL file, and triggers snapshots when the WAL grows too large.

3.  **Load Virtual Schemas and Views:**
    *   The database is scanned for special keys (`_internal:schemas:` and `_internal:views:`) to populate the `SchemaCache` and `ViewCache`. This makes all persisted table schemas and views available immediately.

4.  **Initialize Memory Manager:**
    *   A `MemoryManager` is created based on the `maxmemory_mb` and `eviction_policy` settings.
    *   It calculates the initial memory usage of the data loaded from disk.
    *   The eviction policy data structures (e.g., LRU list) are "primed" with all the keys from the database.

5.  **Enforce `maxmemory` Limit:**
    *   If the initial memory usage exceeds the `maxmemory_mb` limit, the server immediately begins evicting keys until the memory usage is below the limit. This ensures the server always starts within its configured memory bounds.

6.  **Construct Application Context:**
    *   An `AppContext` struct is created. This struct bundles all the shared state of the server (the `Db`, `Logger`, `SchemaCache`, `IndexManager`, `MemoryManager`, `Config`, etc.) into a single `Arc`.

7.  **Return `MemFluxDB` Instance:**
    *   The function returns a `MemFluxDB` struct, which holds the `AppContext` and the handle to the persistence task, ready for use.

## Entry Points

### Server (`src/main.rs`)

The `memflux-server` binary has a very simple `main` function:
1.  It calls `MemFluxDB::open("config.json")`, which loads the config file and then calls the central `open_with_config` function.
2.  It takes the returned `MemFluxDB` instance.
3.  It binds a TCP listener to the configured `host` and `port`.
4.  It enters a loop, accepting new connections and spawning a Tokio task for each one to be handled by `handle_connection`.

### FFI Library (`src/ffi.rs`)

When used as a library, the entry point is `memflux_open`:
1.  It receives a JSON configuration string from the C caller.
2.  It deserializes this into an `FFIConfig` and then converts it into a standard `Config` object.
3.  It calls `MemFluxDB::open_with_config` with this config, blocking on the result since it's a C function.
4.  If successful, it wraps the `MemFluxDB` instance in a `Box` and returns it as an opaque pointer (`MemFluxDBHandle`) to the C caller.
