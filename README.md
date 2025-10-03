# MemFlux - In-Memory Multi-Model Database with SQL

![Rust](https://img.shields.io/badge/rust-%23000000.svg?style=for-the-badge&logo=rust&logoColor=white)![Status](https://img.shields.io/badge/status-pre--alpha-red.svg?style=for-the-badge)![Contributions](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=for-the-badge)

---

> ### **:warning: Alpha Phase Notice :warning:**
>
> This project is in an **incredibly early, pre-alpha stage of development**. It is currently a solo project. As such, you should expect:
> *   **Bugs and Instability:** The server may crash, commands might not work as expected, and data could be corrupted.
> *   **Inconsistencies:** The API and feature set are subject to frequent and breaking changes without notice.
> *   **Incomplete Features:** Many SQL features and Redis commands are either missing or only partially implemented.
>
> This is not ready for any form of production use. It is a learning and experimentation project. **Contributions are very much welcomed!**

## What is MemFlux?

MemFlux is an experimental, high-performance, in-memory, multi-model database engine built in Rust. It aims to blend the speed and simplicity of key-value stores like Redis with the power and flexibility of SQL databases.

Database is designed with a dual-purpose architecture: it can be run as a **standalone server** (compatible with the Redis protocol) or be **embedded directly** into your applications as a library via a C-compatible FFI. This approach allows MemFlux to function as both a fast, networked database  and a powerful, in-process database for languages like Python, C++, and more.

**Core Features:**

*   **Multi-Model Data:** Natively supports:
    *   **Bytes/Strings:** Classic key-value operations.
    *   **JSON Documents:** Rich, schemaless JSON manipulation at the key or sub-path level.
    *   **Lists & Sets:** Redis-compatible list and set operations.
*   **Integrated SQL Query Engine:** A powerful, built-from-scratch query engine that operates directly on your in-memory data. Supports:
    *   Complex `SELECT` queries with `JOIN`s, `GROUP BY`, aggregates (`COUNT`, `SUM`, `AVG`), `ORDER BY`, `LIMIT`, subqueries, and `CASE` statements.
    *   Common Table Expressions (CTEs) using the `WITH` clause, including `WITH RECURSIVE` for hierarchical or graph-based queries.
    *   Data Manipulation Language (DML): `INSERT` (with `ON CONFLICT`), `UPDATE`, `DELETE`. All support a `RETURNING` clause.
    *   Data Definition Language (DDL): `CREATE/DROP/ALTER TABLE`, `CREATE/DROP VIEW`, and `CREATE SCHEMA` for managing virtual schemas and namespaces.
    *   Rich Data Types: `INTEGER`, `TEXT`, `TIMESTAMPTZ`, `NUMERIC`, `UUID`, `BYTEA`, arrays (`INTEGER[]`), and more.
    *   Advanced Constraints: `PRIMARY KEY`, `UNIQUE`, `CHECK`, and `FOREIGN KEY` with referential actions (`ON DELETE CASCADE`, `ON UPDATE SET NULL`, etc.).
    *   A rich function library (`LOWER`, `NOW()`, `DATE_PART`, `ABS()`, etc.).
*   **Transactional Integrity with MVCC:** Provides ACID-like properties with Snapshot Isolation using a Multi-Version Concurrency Control (MVCC) architecture. This allows for non-blocking reads and safe, concurrent writes.
*   **Dual-Mode Operation:**
    *   **Standalone Server:** Run as a TCP server with a Redis-compatible (RESP) protocol.
    *   **Embedded Library:** Integrate directly into your application via a C-compatible Foreign Function Interface (FFI) for zero-latency, in-process database operations.
*   **Configurable Durability & Persistence:** Persistence can be enabled or disabled. When enabled, durability is achieved through a Write-Ahead Log (WAL) and periodic snapshotting, with configurable durability levels (`fsync`, `full`).
*   **Secondary Indexing:** Create indexes on JSON fields to dramatically accelerate SQL query performance.
*   **Configurable Memory Management:** Set a `maxmemory` limit and choose from multiple eviction policies (`LRU`, `LFU`, `ARC`, `LFRU`, `Random`) to control memory usage.
*   **TLS Encryption:** Secure client connections with TLS when running in server mode.

> **Note:** As this is an alpha project, many of the features listed above are still under heavy development and may be incomplete or unstable.

## Full Documentation

While this README provides a quick start, the complete documentation contains a detailed reference for every command, SQL feature, and internal system.

**[---> Start with the Documentation Index](./docs/index.md) <---**

Key sections include:

*   **[Configuration](./docs/configuration.md):** How to configure the server, including memory limits, persistence, and TLS.
*   **[Python Library Guide](./docs/python_library.md):** A guide for using MemFlux as an embedded library in Python.
*   **[Commands](./docs/commands.md):** Detailed reference for all non-SQL, Redis-style commands.
*   **[SQL Reference](./docs/sql.md):** A comprehensive guide to the SQL engine, from DDL to complex `SELECT` queries.

## How to Use It

MemFlux can be used in two primary ways: as a standalone server or as an embedded library.

### 1. As a Standalone Server

In this mode, MemFlux runs as a background process and accepts client connections over the network using the Redis (RESP) protocol.

**Running the Server:**
1.  **Build the server:**
    ```sh
    cargo build --release
    ```
2.  **Run the server binary:**
    ```sh
    ./target/release/memflux-server
    ```
The server will start and listen on `127.0.0.1:8360`.

**Connecting to the Server:**
You can connect using any Redis-compatible client. For interactive use, the included Python script is recommended:
```sh
# Run the interactive client
python3 test.py
```

### 2. As an Embedded Library (via FFI)

In this mode, the database engine is loaded directly into your application's process, eliminating network overhead and providing direct, high-performance access.

The primary interface for this is the Python library included in `libs/python/`.

**Using the Python Library:**
1.  **Build the dynamic library:**
    ```sh
    cargo build --release
    ```
2.  **Use the `memflux` Python module in your script:**

    ```python
    import libs.python.memflux # Provided python lib relative from project source.\
    import sys

    # Path to the compiled shared library
    if sys.platform == "win32":
        LIB_PATH = "./target/release/memflux.dll"
    elif sys.platform == "darwin":
        LIB_PATH = "./target/release/libmemflux.dylib"
    else:
        LIB_PATH = "./target/release/libmemflux.so"

    # Configuration for the database instance
    DB_CONFIG = {
      "persistence": true,
      "durability": "fsync",
      "wal_file": "memflux.wal",
      "wal_overflow_file": "memflux.wal.overflow",
      "snapshot_file": "memflux.snapshot",
      "snapshot_temp_file": "memflux.snapshot.tmp",
      "wal_size_threshold_mb": 128,
      "maxmemory_mb": 0,
      "eviction_policy": "lru",
      "isolation_level": "serializable",
    }

    # Connect to the database (loads it in-process)
    conn = memflux.connect(config=DB_CONFIG, lib=LIB_PATH)

    with conn.cursor() as cur:
        cur.execute("SQL CREATE TABLE products (id INT, name TEXT, price REAL)")
        cur.execute("SQL INSERT INTO products VALUES (?, ?, ?)", (1, "Laptop", 1200.50))
        
        cur.execute("SQL SELECT name, price FROM products WHERE price > ?", (1000,))
        product = cur.fetchone()
        print(product) # Output: {'name': 'Laptop', 'price': 1200.5}

    conn.close()
    ```
For a more detailed guide, see the **[Python Library Guide](./docs/python_library.md)**.

## How It Works (A Basic Overview)

> **Note:** This is a simplified explanation of an alpha-stage project. The implementation details are subject to change.

1.  **Core Library (`src/lib.rs`):** The core database logic is encapsulated in a Rust library. This library manages the in-memory storage (`DashMap`), persistence, indexing, and the SQL query engine. It exposes a high-level `MemFluxDB` struct.

2.  **Server Binary (`src/main.rs`):** The `memflux-server` binary is a lightweight wrapper around the core library. It handles TCP connections, TLS, and uses the RESP protocol to parse client requests, which it passes to the `MemFluxDB` instance.

3.  **FFI Layer (`src/ffi.rs`):** A C-compatible Foreign Function Interface exposes the core library's functionality, allowing it to be loaded and used directly by other languages (like Python's `ctypes` module) for in-process execution.

4.  **Persistence Engine (MVCC):** To prevent data loss and enable transactions, MemFlux uses a Multi-Version Concurrency Control (MVCC) model combined with a Write-Ahead Log (WAL).
    *   **Versioning:** Instead of overwriting data, every write operation creates a new version of a value, tagged with a transaction ID. A key now points to a chain of these versions.
    *   **WAL:** Every data-modifying command is first serialized and written to the WAL file (`memflux.wal`) on disk. This ensures that no acknowledged write is ever lost.
    *   **Snapshots & Compaction:** When the WAL file grows too large, a non-blocking snapshot of the current state of the database is written to disk. A background **vacuum** process cleans up old, no-longer-visible data versions to reclaim memory.
    *   **Recovery:** On startup, the server restores the last snapshot and replays any subsequent entries from the WAL to bring the database to a consistent, up-to-date state.

5.  **SQL Query Engine Pipeline:**
    *   **Parser:** The raw SQL string is tokenized and parsed into an **Abstract Syntax Tree (AST)**, which is a tree-like representation of the query structure.
    *   **Logical Planner:** The AST is converted into a **Logical Plan**. This plan represents the "what" of the query (e.g., "filter these rows, then join with that table"). During this phase, the query is validated against any existing virtual schemas.
    *   **Physical Planner:** The Logical Plan is transformed into a **Physical Plan**. This plan represents the "how" of the query execution. It can perform simple optimizations, such as deciding to use an index scan instead of a full table scan if a suitable index exists.
    *   **Executor:** The Physical Plan is executed using a Volcano-style iterator model. Each node in the plan (e.g., `TableScan`, `Filter`, `Join`) pulls rows from the node(s) below it, processes them, and passes the results up to the next node, until the final results are streamed back to the client.

## Development Guide

> **Disclaimer:** This is a solo, pre-alpha project. The development process is informal. Contributions are highly encouraged!

For a deeper understanding of the project's architecture, see the **[Internals Documentation](./docs/internals/index.md)**.

### Prerequisites
*   The Rust programming language toolchain (`rustc`, `cargo`). You can install it from [rust-lang.org](https://www.rust-lang.org/).
*   Python 3 for running the test suite.

### Building
You can build the project for debugging or release:
```sh
# For development (faster compile times, no optimizations)
cargo build

# For benchmarking or running "for real"
cargo build --release
```

### Running Tests

The project includes a comprehensive test suite written in Python. It's the best way to validate changes and understand the expected behavior of different commands.

The test runner is the `test.py` script.

```sh
# First, start the server in a separate terminal
cargo run

# --- In another terminal ---

# Run all unit tests
python3 test.py unit all

# Run a specific test suite (e.g., just the SQL tests)
python3 test.py unit sql

# Available suites: byte, json, lists, sets, sql, snapshot, types,
# schema, aliases, case, like, subqueries, union, functions
```

## Conclusion & Future

MemFlux is an ambitious experiment to build a modern, multi-model database from the ground up in Rust. It explores the intersection of NoSQL flexibility and SQL's expressive power.

While the project is still in its infancy, it has a solid foundation with a working persistence layer and a surprisingly capable SQL engine.

**Potential Future Directions:**
*   More robust query optimization.
*   Expanded SQL syntax and function support.
*   More complex data types.
*   Improved concurrency control.
*   Thorough documentation and stabilization of the API.

Again, this is an alpha project. Use it, break it, and please consider contributing! All feedback, bug reports, and pull requests are welcome.
