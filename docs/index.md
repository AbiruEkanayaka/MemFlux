# MemFlux Documentation

Welcome to the official documentation for MemFlux, a high-performance, in-memory database inspired by Redis, but with the added power of a feature-rich SQL query engine for JSON data.

## Overview

MemFlux is designed to provide the speed and versatility of a key-value store with the structured query capabilities of a traditional database. It uses the RESP protocol for standard commands, making it compatible with many Redis clients, while also offering a powerful SQL interface for complex queries on schemaless JSON objects.

Persistence is built-in, with a Write-Ahead Log (WAL) for durability and automatic snapshotting for fast recovery.

## Core Features

*   **Multi-Data Model:** Supports raw bytes, lists (deque), sets, and complex JSON documents as values.
*   **Redis-like API:** Implements a subset of common Redis commands (e.g., `SET`, `GET`, `LPUSH`, `SADD`) over the RESP protocol.
*   **Powerful SQL Engine:** A custom-built SQL query engine allows for complex queries on JSON data, including:
    *   DDL: `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE` to define virtual schemas.
    *   DML: `INSERT`, `UPDATE`, `DELETE` statements.
    *   Advanced `SELECT`: `JOIN`s, aggregates (`COUNT`, `SUM`, `AVG`), `GROUP BY`, `ORDER BY`, `LIMIT`, subqueries, `CASE` expressions, and Common Table Expressions (CTEs) with `WITH` and `WITH RECURSIVE`.
*   **JSON Indexing:** Create indexes on specific JSON fields to accelerate query performance.
*   **Durable Persistence:** Uses a Write-Ahead Log (WAL) for command logging and automatic background snapshotting to ensure data safety and fast restarts.
*   **Built-in Functions:** A library of SQL functions for string manipulation, numeric operations, and date/time processing.

## For Developers

For developers interested in the internal architecture of MemFlux, the following documents provide a deeper dive into the source code and design decisions.

*   **[Internals Index](./internals/index.md)**

## Getting Started

### Running the Server

The server is a single binary built from the Rust source code.

1.  **Build the project:**
    ```sh
    cargo build --release
    ```
2.  **Run the server:**
    ```sh
    ./target/release/memflux
    ```

By default, the server listens on `127.0.0.1:8360`.

### Interacting with MemFlux

You can interact with MemFlux using any RESP-compliant client or the provided Python test client. The test client can connect to the server or load the database as an in-process library via FFI.

**Interactive CLI:**

The `test.py` script can be used as an interactive client for sending commands.

1.  **Run the client in interactive mode:**
    ```sh
    python3 test.py
    ```
2.  **Enter commands at the prompt:**
    ```
    > SET mykey "hello world"
    +OK
    Send: 0.04ms, Latency: 0.13ms, Total: 0.17ms
    > GET mykey
    $11
    hello world
    Send: 0.02ms, Latency: 0.07ms, Total: 0.09ms
    > SQL SELECT * FROM user WHERE age > 30
    *1
    $48
    {"age":35,"city":"SF","id":"5","name":"Carol"}
    Send: 0.04ms, Latency: 0.41ms, Total: 0.45ms
    ```

**Running Tests & Benchmarks:**

The `test.py` script is also the entry point for running unit tests and benchmarks.

*   **Run all unit tests (against live server):**
    ```sh
    # Make sure server is running in another terminal
    python3 test.py unit all
    ```
*   **Run all unit tests (using FFI):**
    ```sh
    # No server needed. Build the library first.
    cargo build
    python3 test.py --ffi ./target/debug/libmemflux.so unit all
    ```
*   **Run a benchmark:**
    ```sh
    # Run a command 10,000 times
    python3 test.py bench 10000 SET foo bar

    # Benchmark operations/second for 10 seconds
    python3 test.py bench-ops 100 10 LPUSH mylist item
    ```

## Documentation Structure

This documentation is organized into the following sections:

*   **[Configuration](./configuration.md):** How to configure the server, including TLS.
*   **[Commands](./commands.md):** Detailed reference for all non-SQL, Redis-style commands.
*   **[Data Types](./types.md):** An overview of the core data types and the SQL type system.
*   **[SQL Reference](./sql.md):** An introduction to the SQL engine and links to detailed sections.
*   **[Python Library Guide](./python_library.md):** A guide for using MemFlux as an embedded library in Python.
*   **[Persistence](./persistence.md):** An explanation of how data durability is achieved through WAL and snapshots.
*   **[Indexing](./indexing.md):** Guide to creating and using indexes for JSON data.
*   **[Memory Management](./memory.md):** How to configure memory limits and the eviction policy.
*   **[Advanced SQL](./advanced_sql.md):** A guide to advanced features like Views, additional data types, and constraints.