# Persistence in MemFlux

MemFlux ensures data durability and allows for fast recovery by employing a persistence strategy that combines a **Write-Ahead Log (WAL)** with periodic **snapshotting**. This approach provides a balance between write performance and recovery speed.

## Overview

Persistence can be enabled or disabled via the `persistence` flag in `config.json`. When enabled, MemFlux ensures data durability and allows for fast recovery by employing a persistence strategy that combines a **Write-Ahead Log (WAL)** with periodic **snapshotting**. This approach provides a balance between write performance and recovery speed.

1.  **Write-Ahead Log (WAL):** Every data modification command (e.g., `SET`, `JSON.SET`, `LPUSH`) is first serialized and appended to the `memflux.wal` file on disk. The command is only then applied to the in-memory database. This ensures that no acknowledged write is ever lost in case of a crash.
2.  **Snapshotting:** When the WAL file reaches a certain size, a snapshot of the entire in-memory database is created. This snapshot is a more compact representation of the data, allowing for much faster startup times compared to replaying a very long WAL.
3.  **Compaction:** After a snapshot is successfully created, the old WAL file is truncated, reclaiming disk space. This process is designed to be non-blocking.

## Write-Ahead Log (WAL)

The WAL (`memflux.wal`) is the primary mechanism for durability.

*   **Format:** Each log entry is a binary record consisting of a 32-bit length prefix followed by the bincode-serialized `LogEntry` enum. This format is efficient to write and parse.
*   **Buffering:** To optimize disk I/O, writes are batched. The persistence engine collects multiple write requests from concurrent clients and writes them to the WAL file in a single operation.
*   **Durability Levels:** The `durability` setting in `config.json` controls when a write is acknowledged to the client:
    *   `"none"`: The write is acknowledged as soon as it is sent to the persistence engine's channel. The actual disk write happens in the background. This is the fastest but least safe option.
    *   `"fsync"` (Default): The write is acknowledged after the data has been written to the operating system's file buffer. A background task periodically calls `fsync` to ensure data is flushed to disk. This offers a good balance of performance and safety.
    *   `"full"`: The write is acknowledged only after the data has been fully flushed to the disk via `fsync`. This is the safest but slowest option.

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

*   **`wal_size_threshold_mb`:** This is configured in `config.json` and defaults to **128 MB**.

### Non-Blocking Compaction

A key challenge with WAL and snapshotting is that creating a snapshot can be a time-consuming, I/O-intensive operation. To prevent this from blocking new write commands, MemFlux uses a two-phase compaction strategy with a secondary WAL file (`memflux.wal.overflow` by default).

The process is as follows:

1.  Clients write to the primary WAL file (`memflux.wal`).
2.  When `memflux.wal` reaches the `wal_size_threshold_mb`, the server immediately switches all new writes to the overflow WAL (`memflux.wal.overflow`).
3.  A background task begins creating a new snapshot from the in-memory state.
4.  Once the snapshot is complete, the now-inactive primary WAL file (`memflux.wal`) is safely truncated.
5.  The server then switches writes *back* to the (now empty) primary WAL, and the process repeats for the overflow file.

This ensures that write operations are never blocked by the snapshotting process.

## Recovery Process

When MemFlux starts, it restores its state from the disk in the following order:

1.  **Load Snapshot:** It first looks for `memflux.snapshot`. If the file exists, it is loaded into memory. This quickly restores the database to the state of the last snapshot.
2.  **Replay WALs:** It then replays any log entries that were written *after* the last snapshot was created. To ensure correctness, it replays the primary WAL (`memflux.wal`) first, followed by the overflow WAL (`memflux.wal.overflow`). This brings the database fully up-to-date.

This two-phase process ensures that recovery is both fast (thanks to the snapshot) and complete (thanks to the WALs).
