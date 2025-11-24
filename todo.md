This is a comprehensive, architectural migration plan designed to surgically replace the internals of **MemFlux** with the **FluxMap** engine.

This plan prioritizes stability. Instead of a "big bang" rewrite, we define an **Abstraction Layer (Phase 0)** that allows us to run the old engine (`DashMap`) and the new engine (`FluxMap`) interchangeably, ensuring functional parity at every step.

---

# Phase 0: The Universal Storage Abstraction
**Goal:** Decouple the Query Engines (SQL/Cypher) and Command Handlers from the specific storage implementation (`DashMap`).

Currently, MemFlux interacts directly with `ctx.db` (the DashMap). We must introduce a trait that standardizes these interactions.

### 1.1. Define the `StorageEngine` Trait
Create `src/storage/mod.rs`. This trait abstracts data access, transaction management, and iteration.

```rust
// src/storage/mod.rs
use async_trait::async_trait;
use crate::types::{DbValue, TxId};
use std::sync::Arc;

// Abstract the concept of a Transaction
pub trait StorageTransaction: Send + Sync {
    fn id(&self) -> TxId;
    fn commit(self: Box<Self>) -> Result<(), anyhow::Error>;
    fn rollback(self: Box<Self>) -> Result<(), anyhow::Error>;
    // Staging writes
    fn set(&mut self, key: String, value: DbValue);
    fn delete(&mut self, key: String);
    fn get(&self, key: &str) -> Option<DbValue>;
}

// Abstract the Database Engine
#[async_trait]
pub trait StorageEngine: Send + Sync {
    // Basic K/V
    async fn get(&self, key: &str) -> Option<DbValue>;
    async fn set(&self, key: String, value: DbValue) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<()>;

    // Scans (Crucial for SQL/Graph)
    async fn prefix_scan(&self, prefix: &str) -> Vec<(String, DbValue)>;
    async fn range_scan(&self, start: &str, end: &str) -> Vec<(String, DbValue)>;

    // Transaction Management
    async fn begin_transaction(&self) -> Box<dyn StorageTransaction>;
}
```

### 1.2. Implement `LegacyDashMapBackend`
Wrap the existing `DashMap` logic (from `src/storage_executor.rs` and `src/types.rs`) into this struct.
*   **Action:** Move logic from `src/storage_executor.rs` into `src/storage/legacy.rs`.
*   **Refactor:** Update `AppContext` in `src/types.rs` to hold `Arc<dyn StorageEngine>` instead of the raw `Db` alias.

### 1.3. Verify Phase 0
*   **Compile:** Ensure the abstraction leaks no implementation details.
*   **Test:** Run `python3 test.py unit all`. Behavior must be identical.

---

# Phase 1: Primary Index Replacement (The Kernel Swap)
**Goal:** Replace `DashMap` with `FluxMap`'s `SkipList` for data storage, running in "Autocommit" mode initially.

### 1.1. Dependency & Traits
*   **Action:** Add `fluxmap` to `Cargo.toml`.
*   **Action:** Implement `fluxmap::mem::MemSize` for `DbValue` in `src/types.rs`. This is required for FluxMap's internal storage.

### 1.2. Implement `FluxMapBackend`
Create `src/storage/flux.rs`.
*   **Structure:** Holds `Arc<fluxmap::Database<String, DbValue>>`.
*   **Initialization:** Use `fluxmap::Database::new_in_memory()` initially.
*   **Method Mapping:**
    *   `get(key)` -> `fluxmap_db.handle().get(key)`
    *   `set(key, val)` -> `fluxmap_db.handle().insert(key, val)`
    *   `delete(key)` -> `fluxmap_db.handle().remove(key)`

### 1.3. The "Value" Adaptation
*   **Change:** MemFlux stores `VersionedValue` (manual MVCC). FluxMap handles MVCC internally.
*   **Adaptation:** The `FluxMapBackend` should store **raw** `DbValue`. We strip the `VersionedValue` wrapper.
*   **Swapping:** Update `src/lib.rs` (initialization) to instantiate `FluxMapBackend` instead of `LegacyDashMapBackend`.

### 1.4. Verify Phase 1
*   Run basic K/V tests (`test_commands.py`).
*   *Note:* Transactions will be broken or simulated (autocommit only) in this step. This is acceptable for Phase 1 verification of the data structure.

---

# Phase 2: Native MVCC & SSI Implementation
**Goal:** Retire MemFlux's manual `TransactionStatusManager` and use FluxMap's native transaction capabilities.

### 2.1. Mapping Transactions
Update `FluxMapBackend`'s implementation of `begin_transaction`.
*   **FluxMap Logic:** `fluxmap_db.handle().begin()` returns a transaction context.
*   **Wrapper Logic:** The `StorageTransaction` implementation for FluxMap will hold the active `fluxmap::handle` and the `fluxmap::Transaction`.

### 2.2. Implementing Serializable Snapshot Isolation (SSI)
*   **MemFlux Logic:** Currently iterates over `active_transactions` in `src/storage_executor.rs` to check conflicts manually.
*   **FluxMap Logic:** Logic is built-in.
*   **Action:** Delete the manual conflict detection loops in `src/storage_executor.rs` / `src/transaction.rs`.
*   **Error Handling:** Map `fluxmap::error::FluxError::SerializationConflict` to MemFlux's internal error types to trigger retries.

### 2.3. Verify Phase 2
*   Run `test_transactions.py`.
*   Run `test_isolation.rs` (Rust integration test) to prove write-skew protection still works, now powered by FluxMap.

---

# Phase 3: WAL Integration & Persistence
**Goal:** Replace MemFlux's custom `PersistenceEngine` with FluxMap's WAL.

### 3.1. Configuration Mapping
*   **Action:** Map `memflux::config::Config` fields (`wal_path`, `durability`, `fsync`) to `fluxmap::PersistenceOptions`.

### 3.2. Engine Swap
*   **In `src/lib.rs`:** Instead of spawning `memflux::persistence::PersistenceEngine`, use `fluxmap::Database::builder().durability_full(...)`.
*   **Recovery:** FluxMap handles recovery on `.build()`. Remove `memflux::persistence::load_db_from_disk`.

### 3.3. Snapshotting
*   FluxMap handles snapshots. Remove `memflux::persistence::spawn_snapshot_task`.
*   **Adaptation:** Ensure `DbValue` implements `Serialize/Deserialize` (it already does) for FluxMap's checkpoints.

### 3.4. Verify Phase 3
*   Run `test_persistence.py` and `test_recovery.py`.
*   Verify that killing the server and restarting restores data via FluxMap's WAL.

---

# Phase 4: Indexing Consolidation
**Goal:** Remove redundant secondary indexes where FluxMap's sorted nature provides them for free.

### 4.1. Analyze Indexes
MemFlux maintains `DashMap` based indexes. FluxMap is a SkipList (sorted).
*   **Primary Key Index (`_pk_node:id`):** This is redundant. FluxMap lookups are O(log N).
*   **Prefix Scans:** MemFlux uses `TABLE.SCAN` which iterates the whole DashMap.

### 4.2. Replace Scans
*   **Action:** Update `StorageEngine::prefix_scan`.
*   **Implementation:** Use `fluxmap_db.handle().prefix_scan_stream(prefix)`.
*   **Benefit:** This changes table scans from O(N) (full DB scan) to O(log N + K) (range seek + iteration).

### 4.3. Verify Phase 4
*   Run `test_table_commands.py` and `test_graph.py`.
*   Benchmark `KEYS *` and `TABLE.SCAN`. Performance should increase dramatically for large datasets.

---

# Phase 5: Memory Management Overhaul
**Goal:** Replace manual memory accounting with FluxMap's ARC/Eviction.

### 5.1. Implement `MemSize`
*   **Action:** Ensure `DbValue` and all its sub-enums (`Value`, `Vec<u8>`, etc.) implement `fluxmap::mem::MemSize` accurately.

### 5.2. Retire `MemoryManager`
*   **Action:** Delete `src/memory.rs`.
*   **Configuration:** Pass `max_memory` from MemFlux config to `FluxMap::builder().max_memory(...)`.
*   **Eviction Policy:** Map MemFlux policies (`LRU`, `LFU`, `ARC`) to `FluxMap::EvictionPolicy`.

### 5.3. Verify Phase 5
*   Run `test_memory.py`.
*   Verify that inserting data beyond `maxmemory` triggers FluxMap's eviction logic automatically.

---

# Phase 6: Query Engine Optimization
**Goal:** Rewrite Physical Planners to exploit FluxMap's range capabilities.

### 6.1. SQL Optimizer (`src/query_engine/physical_plan.rs`)
*   **Current:** `TableScan` reads all keys with prefix.
*   **New:** Ensure `PhysicalPlan::TableScan` maps directly to `StorageEngine::prefix_scan`.
*   **Range Queries:** If `WHERE id > 5` is present, push this down to `StorageEngine::range_scan` instead of filtering in memory.

### 6.2. Cypher Optimizer (`src/cypher_engine/physical_plan.rs`)
*   **Node Scan:** `MATCH (n:Person)` translates to `prefix_scan("_node:Person:")`.
*   **Edge Traversal:** `MATCH (a)-[:KNOWS]->(b)` translates to `prefix_scan("_edge:out:{a_id}:KNOWS:")`.
*   **Optimization:** This removes the need to iterate the entire DB for graph traversals, fixing MemFlux's scalability issue.

### 6.3. Verify Phase 6
*   Run `test_sql.py` and `test_cypher.py`.
*   Run `test.py bench` on graph traversals.

---

# Phase 7: Cleanup and Final Verification
**Goal:** Remove dead code and stabilize.

### 7.1. Code Removal
*   Delete `src/persistence.rs` (Old engine).
*   Delete `src/memory.rs` (Old manager).
*   Delete `src/transaction.rs` (Old logic).
*   Delete `src/storage/legacy.rs` (The DashMap wrapper).
*   Clean up `src/types.rs` (Remove `VersionedValue`, `TransactionStatusManager`).

### 7.2. Final Interface Polish
*   Rename `FluxMapBackend` to `MemFluxStore`.
*   Ensure FFI (`src/ffi.rs`) is pointing correctly to the new transaction handles.

### 7.3. Full Regression Suite
*   Run `python3 test.py unit all`.
*   Run benchmarks to confirm performance gains.

---

## Integration Diagram (Post-Migration)

```mermaid
graph TD
    Client[Client / FFI] --> Server[Server / Lib Entry]
    Server --> QE[Query Engine (SQL/Cypher)]
    QE --> Wrapper[Storage Abstraction]
    Wrapper --> FluxMap[FluxMap Kernel]
    
    FluxMap --> SkipList[Concurrent SkipList]
    FluxMap --> Tx[Transaction Manager (SSI)]
    FluxMap --> WAL[Persistence Engine (WAL)]
    FluxMap --> ARC[ARC Memory Manager]
```