# Using MemFlux with Python

MemFlux can be embedded directly into a Python application as a library, offering high-performance, in-process database capabilities without needing a separate server. This is achieved through a Python wrapper around the core Rust FFI layer.

**Required Files:**
*   The compiled dynamic library (e.g., `libmemflux.so` on Linux, `libmemflux.dylib` on macOS, `memflux.dll` on Windows).
*   The Python wrapper module: `libs/python/memflux.py`.

## DB-API 2.0 Interface

The Python wrapper (`memflux.py`) provides a familiar interface that loosely follows the [Python DB-API 2.0 specification (PEP 249)](https://peps.python.org/pep-0249/). This makes it easy to integrate into existing Python applications.

The main components are:
*   `memflux.connect()`: A top-level function to connect to and initialize the database.
*   `Connection` object: Represents the database connection. It is thread-safe.
*   `Cursor` object: Represents a single transactional context. It is used to execute commands and fetch results. **Cursors should not be shared between threads.**

## Transactions in Python

Each `Cursor` object you create represents a separate transactional context.

- **Auto-Commit (Default):** By default, each `.execute()` call on a cursor is treated as a single, atomic transaction. The operation is sent to the database and immediately committed. This is the simplest way to use the library.

- **Explicit Transactions:** To execute multiple commands within a single transaction, you must use `BEGIN`, `COMMIT`, and `ROLLBACK` commands. This gives you control over the transaction boundaries.

```python
# Auto-commit mode
cur.execute("SET key1 'value1'") # This is committed immediately

# Explicit transaction
cur.execute("BEGIN")
cur.execute("SET key2 'value2'")
cur.execute("SET key3 'value3'")
cur.execute("ROLLBACK") # The changes to key2 and key3 are discarded
```

## Quick Start Example

Here is a complete example of how to use the library.

```python
import memflux
import os

# --- 1. Configuration ---
# Path to the compiled dynamic library
# This path may vary based on your OS and build profile (debug/release)
LIB_PATH = "./target/debug/libmemflux.so"

# Configuration for the database instance.
# These are the same settings as the persistence/memory sections of config.json
DB_CONFIG = {
  "wal_file": "memflux.wal",
  "wal_overflow_file": "memflux.wal.overflow",
  "snapshot_file": "memflux.snapshot",
  "snapshot_temp_file": "memflux.snapshot.tmp",
  "wal_size_threshold_mb": 128,
  "maxmemory_mb": 0,
  "eviction_policy": "lru",
}

# --- 2. Connecting to the Database ---
try:
    # The connect function loads the library and initializes the database
    conn = memflux.connect(config=DB_CONFIG, lib=LIB_PATH)
    print("Successfully connected to MemFlux in-process.")

    # --- 3. Using a Cursor ---
    # All operations are performed through a cursor.
    # The connection object is thread-safe, but cursors should not be shared between threads.
    with conn.cursor() as cur:

        # --- 4. Executing Commands ---
        # Execute simple Redis-style commands
        cur.execute('SET mykey "Hello from Python!"')
        
        # Use the cursor to fetch the result (if any)
        # For SET, the result is just "OK", which is handled internally
        
        cur.execute('GET mykey')
        result = cur.fetchone() # Fetch the single result
        print(f"GET mykey -> {result}") # Output: b'Hello from Python!'

        # Execute SQL commands
        cur.execute("SQL CREATE TABLE users (id INTEGER, name TEXT, email TEXT)")

        # Use parameter binding to safely insert data
        cur.execute(
            "SQL INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
            (1, "Alice", "alice@example.com")
        )
        cur.execute(
            "SQL INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
            (2, "Bob", "bob@example.com")
        )
        
        # The rowcount attribute shows the number of rows affected by the last DML statement
        print(f"Rows inserted: {cur.rowcount}")

        # --- 5. Fetching Results ---
        cur.execute("SQL SELECT name, email FROM users WHERE id > ?", (0,))
        
        print(f"Found {cur.rowcount} users:")
        
        # Cursors are iterable
        for row in cur:
            print(f"- Name: {row['name']}, Email: {row['email']}")

        # Alternatively, use fetchall()
        cur.execute("SQL SELECT * FROM users ORDER BY id DESC")
        all_users = cur.fetchall()
        print("\nAll users (reversed):", all_users)


except (memflux.InterfaceError, memflux.DatabaseError) as e:
    print(f"An error occurred: {e}")

finally:
    # --- 6. Closing the Connection ---
    if 'conn' in locals() and conn:
        conn.close()
        print("\nConnection closed.")

```

## Error Handling

The library defines a clear exception hierarchy:
- `memflux.MemfluxError`: The base class for all library errors.
- `memflux.InterfaceError`: An error related to the Python wrapper itself (e.g., wrong number of parameters, using a closed connection).
- `memflux.DatabaseError`: An error reported by the core MemFlux engine (e.g., a SQL syntax error, a key not found, etc.).

It's recommended to wrap database operations in a `try...except` block to handle these potential errors.
