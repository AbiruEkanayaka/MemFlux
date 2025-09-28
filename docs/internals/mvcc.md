# Internals: Transaction Management & MVCC

MemFlux implements Multi-Version Concurrency Control (MVCC) to handle concurrent requests safely and provide transaction isolation. This model is fundamental to how data is stored, read, and modified.

**Source Files:**
*   `src/types.rs` (for MVCC-related structs)
*   `src/transaction.rs`
*   `src/storage_executor.rs`

## Core Concepts

### 1. Version Chains

Instead of a key mapping to a single value, every key in the main database (`Db`) maps to a `RwLock<Vec<VersionedValue>>`. This `Vec` is a **version chain**.

-   **`VersionedValue`**: This struct contains the actual `DbValue` along with two crucial pieces of metadata:
    -   `creator_txid: TxId`: The ID of the transaction that created this version.
    -   `expirer_txid: TxId`: The ID of the transaction that deleted or modified this version. A value of `0` means it is not expired.

-   **Writes as New Versions:** When a value is updated, the old version is not overwritten. Instead, the `expirer_txid` of the current version is set to the new transaction's ID, and a new `VersionedValue` is appended to the chain with the new data.

### 2. Transaction State

-   **`TransactionIdManager`**: A global atomic counter that issues a unique, monotonically increasing `TxId` for every new transaction.
-   **`TransactionStatusManager`**: A global `DashMap` that tracks the status (`Active`, `Committed`, `Aborted`) of every transaction ID.

### 3. Snapshots and Visibility

When a transaction begins, it creates a `Snapshot` of the current transaction state. The snapshot contains:
-   `xmin`: The lowest `TxId` among all currently active transactions.
-   `xmax`: The current highest `TxId`. Any transaction with an ID greater than or equal to `xmax` was not yet started when the snapshot was taken.
-   `xip`: A `HashSet` of all transaction IDs that were in progress when the snapshot was taken.

The `Snapshot::is_visible()` method is the core of the read path. It determines if a specific `VersionedValue` should be visible to the transaction holding the snapshot, based on these rules:

1.  The version's `creator_txid` must have **committed**.
2.  The `creator_txid` must be **less than `xmax`** (it must have committed before our transaction started).
3.  The `creator_txid` must **not be in `xip`** (it can't be a concurrent, uncommitted transaction).
4.  If the version has an `expirer_txid`, that transaction must **not** be visible to us. (i.e., if the deletion has been committed and is part of our snapshot's view, then the version is invisible).

### 4. Serializable Snapshot Isolation (SSI)

To achieve the `Serializable` isolation level, MemFlux implements a basic form of Serializable Snapshot Isolation (SSI). In addition to the snapshot rules, it tracks read/write conflicts:

-   **`Transaction.reads`**: When a transaction reads a key, it records the key and the `creator_txid` of the version it read in its private `reads` map.
-   **`Transaction.ssi_in_conflict`**: An atomic boolean flag.
-   **Conflict Detection:** When a transaction (T2) is about to commit, it checks if it has written to any keys that a concurrent, active transaction (T1) has read. If so, it flags T1 by setting its `ssi_in_conflict` flag to `true`.
-   **Abort:** When T1 later tries to commit, it first checks its own `ssi_in_conflict` flag. If it's `true`, it means a serializable conflict occurred, and T1 must abort to maintain correctness.

This mechanism prevents write skew and other anomalies that Snapshot Isolation alone is vulnerable to.