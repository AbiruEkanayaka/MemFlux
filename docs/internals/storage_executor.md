# Internals: The Storage Executor

The `StorageExecutor` is a crucial component introduced during the MVCC refactoring. It centralizes all data modification logic, acting as the sole gateway for applying changes to the database's versioned data store.

**Source File:** `src/storage_executor.rs`

## Purpose and Design

Before the MVCC implementation, command handlers in `src/commands.rs` directly manipulated the `Db`. This became untenable with the introduction of transactions, as logic would need to be duplicated to handle both transactional writes (staging them in a private write set) and non-transactional "auto-commit" writes.

The `StorageExecutor` solves this by abstracting away the details of transaction handling.

-   **Unified Interface:** It provides a clean API for all write operations (e.g., `set`, `delete`, `json_set`, `lpush`).
-   **Transactional Awareness:** It is initialized with a `TransactionHandle` for the current connection. Its methods check if an explicit transaction is active.
-   **Two-Path Logic:**
    1.  **If a transaction is active:** The executor stages the operation in the transaction's private `writes` map and adds a corresponding `LogEntry` to the transaction's log. The in-memory `Db` is **not** touched.
    2.  **If no transaction is active (Auto-Commit):** The executor simulates a transaction for the single command. It creates a new transaction ID, logs the change to the WAL, applies the change directly to the `Db` by creating a new version, and immediately commits the transaction ID.

This design keeps the command handlers in `src/commands.rs` clean and simple. They are only responsible for parsing arguments and calling the appropriate method on the `StorageExecutor`.

## Key Functions

-   **`get_visible_db_value`**: This is the primary read function used throughout the system. It encapsulates the MVCC visibility logic. Given a key and an optional transaction, it traverses the key's version chain to find the specific version that is visible to that transaction's snapshot.

-   **`set`, `delete`, `json_set`, etc.**: These methods contain the two-path logic described above. They handle memory accounting with the `MemoryManager`, update indexes via the `IndexManager`, and correctly stage or apply writes based on the transactional context.

-   **`insert_rows`, `update_rows`, `delete_rows`**: These higher-level functions are used by the SQL engine's execution layer (`src/query_engine/execution.rs`). They are designed to handle DML operations that affect multiple rows, efficiently applying changes within the current transaction.