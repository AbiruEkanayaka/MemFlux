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

## Table of Contents
*   [What is MemFlux?](#what-is-memflux)
*   [Full Documentation](#full-documentation)
*   [How to Use It](#how-to-use-it)
    *   [Running the Server](#running-the-server)
    *   [Connecting to the Server](#connecting-to-the-server)
    *   [Command Examples](#command-examples)
*   [How It Works (A Basic Overview)](#how-it-works-a-basic-overview)
*   [Development Guide](#development-guide)
    *   [Prerequisites](#prerequisites)
    *   [Building](#building)
    *   [Running Tests](#running-tests)
*   [Conclusion & Future](#conclusion--future)

## What is MemFlux?

MemFlux is an experimental, high-performance, in-memory, multi-model database server built in Rust. It aims to blend the speed and simplicity of key-value stores like Redis with the powerful querying capabilities of a SQL database.

**Core Features:**

*   **Multi-Model Data:** Natively supports:
    *   **Bytes/Strings:** Classic key-value operations.
    *   **JSON Documents:** Rich, schemaless JSON manipulation at the key or sub-path level.
    *   **Lists & Sets:** Redis-compatible list and set operations.
*   **Integrated SQL Query Engine:** A powerful, built-from-scratch query engine that operates directly on your in-memory data. Supports:
    *   Complex `SELECT` queries with `JOIN`s, `GROUP BY`, aggregates (`COUNT`, `SUM`, `AVG`), `ORDER BY`, `LIMIT`, and more.
    *   Data Manipulation Language (DML): `INSERT`, `UPDATE`, `DELETE`.
    *   Data Definition Language (DDL): `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE` for managing "virtual schemas".
    *   Advanced features like subqueries, `CASE` statements, and a rich function library (`LOWER`, `NOW()`, `ABS()`, etc.).
*   **Redis (RESP) Compatible Protocol:** Use your favorite Redis client or the provided interactive script to connect and issue commands.
*   **Durable Persistence:** Durability is achieved through a Write-Ahead Log (WAL) and periodic snapshotting, ensuring your data is safe even if the server restarts.
*   **Secondary Indexing:** Create indexes on JSON fields to dramatically accelerate SQL query performance.
*   **Configurable Memory Management:** Set a `maxmemory` limit and choose from multiple eviction policies (`LRU`, `LFU`, `ARC`, `LFRU`, `Random`) to control memory usage.
*   **TLS Encryption:** Secure client connections with TLS, with automatic self-signed certificate generation for easy setup.

> **Note:** As this is an alpha project, many of the features listed above are still under heavy development and may be incomplete or unstable.

## Full Documentation

While this README provides a quick start, the complete documentation contains a detailed reference for every command, SQL feature, and internal system.

**[---> Start with the Documentation Index](./docs/index.md) <---**

Key sections include:

*   **[Configuration](./docs/configuration.md):** How to configure the server, including memory limits, persistence, and TLS.
*   **[Data Types](./docs/types.md):** An overview of the core data types (JSON, Lists, Sets) and the SQL type system.
*   **[Commands](./docs/commands.md):** Detailed reference for all non-SQL, Redis-style commands.
*   **[SQL Reference](./docs/sql.md):** A comprehensive guide to the SQL engine, from DDL to complex `SELECT` queries.
*   **[Persistence](./docs/persistence.md):** An explanation of how data durability is achieved.
*   **[Indexing](./docs/indexing.md):** Guide to creating and using indexes for performance.

## How to Use It

### Running the Server

1.  **Clone the repository:**
    ```sh
    git clone https://github.com/AbiruEkanayaka/MemFlux.git
    cd MemFlux
    ```
2.  **Build and run the server:**
    ```sh
    cargo run --release
    ```
The server will start and listen on `127.0.0.1:8360`.

### Connecting to the Server

You can connect using any Redis-compatible client. Alternatively, the project includes a Python-based interactive client and test runner.

```sh
# Ensure you have the prompt-toolkit for the best experience
pip install prompt-toolkit

# Run the interactive client
python3 test.py
```

You can now enter commands at the `>` prompt.

### Command Examples

#### 1. Key-Value Commands (Redis Style)

```redis
# Set a simple key-value pair
> SET mykey "Hello World"
+OK

# Get the value
> GET mykey
$11
Hello World

# Delete the key
> DELETE mykey
:1
```

#### 2. JSON Document Commands

MemFlux allows you to treat keys as JSON documents and manipulate them with a simple path syntax.

```redis
# Set a full JSON object for a user
> JSON.SET user:1 '{"name": "Alice", "age": 30, "city": "SF", "active": true}'
+OK

# Get the full document
> JSON.GET user:1
$59
{"active":true,"age":30,"city":"SF","name":"Alice"}

# Get a specific field from the document
> JSON.GET user:1.name
$7
"Alice"

# Set/update a specific field
> JSON.SET user:1.age '31'
+OK

# Get the updated document
> JSON.GET user:1
$60
{"active":true,"age":31,"city":"SF","name":"Alice"}
```

#### 3. SQL Query Engine

This is the most powerful feature of MemFlux. You can run SQL queries directly against your key-value data. The engine treats key prefixes (like `user:`) as tables.

**DDL: Creating a Virtual Schema**

While MemFlux is schemaless by default, you can enforce types and structure with `CREATE TABLE`. This enables type validation on `INSERT`/`UPDATE` and type-aware sorting.

```sql
> SQL CREATE TABLE user (id INTEGER, name TEXT, age INTEGER, city TEXT, active BOOLEAN)
+OK
```

**DML: Inserting and Modifying Data**

```sql
-- Insert a new user. Values are automatically cast to the schema types.
> SQL INSERT INTO user (id, name, age, city, active) VALUES ('2', 'Bob', '25', 'NY', 'true')
:1

-- Update data using a WHERE clause
> SQL UPDATE user SET city = 'New York' WHERE name = 'Bob'
:1
```

**DQL: Selecting and Joining Data**

```sql
-- Select specific columns from the 'user' table
> SQL SELECT name, age FROM user WHERE city = 'SF' ORDER BY age DESC
*1
$23
{"name":"Alice","age":31}

-- Let's add another table for orders
> JSON.SET orders:1 '{"order_id": 101, "user_id": "1", "item": "Laptop"}'
+OK
> JSON.SET orders:2 '{"order_id": 102, "user_id": "2", "item": "Mouse"}'
+OK

-- Perform a JOIN between users and orders
> SQL SELECT user.name, orders.item FROM user JOIN orders ON user.id = orders.user_id
*2
$32
{"user.name":"Alice","orders.item":"Laptop"}
$29
{"user.name":"Bob","orders.item":"Mouse"}
```

## How It Works (A Basic Overview)

> **Note:** This is a simplified explanation of an alpha-stage project. The implementation details are subject to change.

1.  **Core Storage:** At its heart, MemFlux uses a `DashMap`, a highly concurrent hash map, to store all data in memory. This allows for fast, thread-safe access to keys.

2.  **Protocol Layer:** A Tokio-based TCP server listens for connections. It parses the incoming byte stream according to the Redis Serialization Protocol (RESP), converting client requests into an internal `Command` struct.

3.  **Command Dispatch:** Based on the command name (`SET`, `JSON.SET`, `SQL`, etc.), the request is dispatched to the appropriate handler.
    *   **Simple Commands** (`GET`, `LPUSH`, etc.) directly manipulate the in-memory `DashMap`.
    *   **SQL Commands** are sent to the Query Engine.

4.  **Persistence Engine:** To prevent data loss, every write operation is first serialized and written to a **Write-Ahead Log (WAL)** file (`memflux.wal`) on disk.
    *   The client receives an "OK" confirmation only after the write has been committed to the WAL.
    *   When the WAL file grows beyond a certain threshold, the server performs a **non-blocking snapshot**. It switches writes to a secondary overflow WAL (`memflux.wal.overflow`) while a background task writes the entire current database state to a new snapshot file (`memflux.snapshot`).
    *   After the snapshot is successfully created, the original, now-inactive WAL is truncated. This two-file approach ensures that I/O-intensive snapshotting never blocks incoming write commands.
    *   On startup, the server restores its state by loading the latest snapshot and then replaying any subsequent entries from the WAL files.

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
