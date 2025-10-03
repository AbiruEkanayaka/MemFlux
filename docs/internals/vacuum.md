# Internals: The Vacuum Process

In a Multi-Version Concurrency Control (MVCC) system, old versions of data are not immediately removed when they are updated or deleted. Instead, they are marked as expired by a transaction ID. The **vacuum** process is the garbage collector responsible for permanently deleting these "dead" versions and reclaiming memory.

**Source File:** `src/vacuum.rs`

## How It Works

The vacuum process is run automatically in the background by a dedicated Tokio task, which triggers periodically (e.g., every 60 seconds).

The core logic is implemented in the `vacuum::vacuum` function.

### 1. The Vacuum Horizon

The most critical concept for the vacuum process is the **vacuum horizon**. This is the oldest transaction ID (`TxId`) that is still active in the system.

-   Any data version that was expired by a transaction that **committed before this horizon** is considered "dead" and is safe to be removed.
-   If a version was expired by a transaction that is still active, or that committed *after* the horizon, it must be kept. This is because the active transaction (or a new one that starts) might still need to see that old version to maintain its consistent snapshot of the database.

### 2. The Cleanup Process

1.  **Find Horizon:** The vacuum process first gets a list of all active transaction IDs from the `TransactionStatusManager` and finds the minimum `TxId`. This `TxId` is the vacuum horizon.

2.  **Iterate Keys:** It iterates over all keys in the database.

3.  **Scan Version Chains:** For each key, it acquires a write lock on its version chain (`Vec<VersionedValue>`) and scans the versions.

4.  **Identify Dead Versions:** It uses `Vec::retain` to keep only the versions that are *not* dead. A version is considered dead if:
    -   Its `expirer_txid` is not `0` (i.e., it has been expired).
    -   The transaction corresponding to `expirer_txid` has a status of `Committed`.
    -   The `expirer_txid` is older (less than) the calculated vacuum horizon.

5.  **Remove Empty Chains:** If, after removing dead versions, a key's version chain becomes empty, it means the key itself is fully dead (it was deleted and that deletion is now visible to all possible future transactions). The key is added to a list of keys to be removed from the main database `Db`.

6.  **Final Deletion:** Finally, the vacuum process iterates through the list of dead keys and removes them from the database, fully reclaiming the memory for both the key and its empty version chain.