# Persistence in MemFlux

MemFlux ensures data durability and allows for fast recovery by employing a persistence strategy that combines a **Write-Ahead Log (WAL)** with periodic **snapshotting**. This approach provides a balance between write performance and recovery speed.

## Overview

1.  **Write-Ahead Log (WAL):** Every data modification command (e.g., `SET`, `JSON.SET`, `LPUSH`) is first serialized and appended to the `memflux.wal` file on disk. The command is only then applied to the in-memory database. This ensures that no acknowledged write is ever lost in case of a crash.
2.  **Snapshotting:** When the WAL file reaches a certain size, a snapshot of the entire in-memory database is created. This snapshot is a more compact representation of the data, allowing for much faster startup times compared to replaying a very long WAL.
3.  **Compaction:** After a snapshot is successfully created, the WAL file is truncated, reclaiming disk space.

## Write-Ahead Log (WAL)

The WAL (`memflux.wal`) is the primary mechanism for durability.

*   **Format:** Each log entry is a binary record consisting of a 32-bit length prefix followed by the bincode-serialized `LogEntry` enum. This format is efficient to write and parse.
*   **Buffering:** To optimize disk I/O, writes are batched. The persistence engine collects multiple write requests from concurrent clients and writes them to the WAL file in a single operation.
*   **Synchronization:** Writes are asynchronously flushed to the disk (`fsync`) to ensure they are durable. This is done in a separate task to avoid blocking client requests.

## Snapshots

Snapshots (`memflux.snapshot`) provide a baseline for recovery.

*   **Format:** A snapshot is a compressed binary file. Each key-value pair is first serialized into a compact binary representation using `bincode`. The entire stream of serialized entries is then compressed using **LZ4**. This format is optimized for fast loading and a small disk footprint, not for human readability.
*   **Creation:** The snapshotting process is as follows:
    1.  A temporary file (`memflux.snapshot.tmp`) is created.
    2.  The entire in-memory database is iterated. Each key-value pair is serialized and written to a compressed stream in the temporary file.
    3.  The temporary file is synced to disk to ensure its contents are fully written.
    4.  The temporary file is atomically renamed to `memflux.snapshot`, atomically replacing the old snapshot.

### Snapshot Triggering

A new snapshot is automatically triggered when the WAL file size exceeds a predefined threshold.

*   **`WAL_SIZE_THRESHOLD`:** This is currently set to **128 MB**.

As demonstrated in `tests/test_persistence.py`, writing a large amount of data will cause the WAL file to grow, triggering the snapshot and subsequent compaction of the WAL.

## Recovery Process

When MemFlux starts, it restores its state from the disk in the following order:

1.  **Load Snapshot:** It first looks for `memflux.snapshot`. If the file exists, it is loaded into memory. This quickly restores the database to the state of the last snapshot.
2.  **Replay WAL:** It then opens `memflux.wal` and replays any log entries that were written *after* the last snapshot was created. This brings the database fully up-to-date, including any writes that occurred between the last snapshot and the server shutdown.

This two-phase process ensures that recovery is both fast (thanks to the snapshot) and complete (thanks to the WAL).
