# Internals: Server Startup Sequence

This document outlines the step-by-step process the MemFlux server follows when it starts up.

**Source File:** `src/main.rs`

The `main` function orchestrates the entire initialization sequence:

1.  **Load Configuration:**
    *   The server looks for a `config.json` file in its working directory.
    *   If found, it's parsed into the `Config` struct.
    *   If not found, a default `config.json` is created and then loaded.
    *   This `Config` is wrapped in an `Arc` to be shared safely across threads.

2.  **Load Database from Disk:**
    *   The `load_db_from_disk` function from `src/persistence.rs` is called.
    *   It first attempts to load the most recent snapshot file (`memflux.snapshot`). This is a compressed file that quickly restores the bulk of the data.
    *   It then replays the records from the Write-Ahead Log (WAL) files (`memflux.wal` and `memflux.wal.overflow`) to restore any data that was written after the last snapshot was taken. This brings the database to a fully up-to-date state.
    *   The result is a `Db` (`Arc<DashMap<String, DbValue>>`), the core in-memory data store.

3.  **Start Persistence Engine:**
    *   A `PersistenceEngine` is initialized.
    *   It is spawned on a separate Tokio task to run in the background.
    *   This engine is responsible for receiving write commands via a channel (`Logger`), writing them to the active WAL file, and triggering snapshots when the WAL grows too large.

4.  **Load Virtual Schemas:**
    *   The `load_schemas_from_db` function is called.
    *   It scans the newly loaded database for special keys prefixed with `_internal:schemas:`.
    *   It parses the values of these keys into `VirtualSchema` structs and populates the `SchemaCache`. This makes table schemas available to the query engine immediately.

5.  **Initialize Memory Manager:**
    *   A `MemoryManager` is created based on the `maxmemory_mb` and `eviction_policy` settings from the config.
    *   If `maxmemory_mb` is greater than 0, the manager calculates the initial memory usage by iterating through all the data loaded from disk.
    *   The eviction policy data structures are then "primed" with all the keys from the database. For example, for the LRU policy, all keys are added to the LRU list.

6.  **Enforce `maxmemory` Limit:**
    *   If the initial memory usage exceeds the `maxmemory_mb` limit, the server immediately begins evicting keys *before* accepting any connections.
    *   It continues evicting keys according to the configured policy until the memory usage is below the limit. This ensures the server always starts within its configured memory bounds.

7.  **Set Up Application Context:**
    *   An `AppContext` struct is created. This struct bundles all the shared state of the server (the `Db`, `Logger`, `SchemaCache`, `IndexManager`, `MemoryManager`, `Config`, etc.) into a single `Arc`.
    *   This context is cloned for each new client connection, giving them safe, shared access to the server's state.

8.  **Start TCP Listener:**
    *   The server binds to the `host` and `port` specified in the config.
    *   **TLS Setup:** If `encrypt` is `true` in the config, it loads the specified TLS certificate and key. If they don't exist, it generates a self-signed certificate and key for immediate use in development.
    *   The server enters its main loop, calling `listener.accept().await` to wait for new connections.

9.  **Handle Connections:**
    *   For each new connection, a new Tokio task is spawned.
    *   If TLS is enabled, the TLS handshake is performed.
    *   The connection is passed to the `handle_connection` function, which reads RESP commands, processes them, and writes responses back to the client.
