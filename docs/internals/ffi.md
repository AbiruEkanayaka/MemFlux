# Internals: The FFI Layer

The Foreign Function Interface (FFI) layer exposes the core MemFlux database engine as a C-compatible dynamic library. This allows MemFlux to be embedded directly into other applications and languages, such as Python, C++, Node.js, etc., for high-performance, in-process database operations.

**Source File:** `src/ffi.rs`

## Core Design

The FFI layer acts as a bridge between the external C-style world and the internal, asynchronous Rust world of MemFlux.

1.  **Opaque Handles:** The database instance (`Arc<MemFluxDB>`) is wrapped and returned as an opaque pointer (`*mut MemFluxDBSharedHandle`). To manage transactions, a separate `TransactionHandle` is created and returned as a `*mut MemFluxCursorHandle`. The caller never interacts with the database or transaction state directly, only through these handles.

2.  **Synchronous API:** While MemFlux is internally asynchronous (using Tokio), the FFI presents a synchronous, blocking interface. This is a deliberate design choice to simplify integration with most programming languages, which typically expect blocking I/O calls.

3.  **Global Tokio Runtime:** To bridge the sync/async gap, the FFI layer maintains a single, global, multi-threaded Tokio runtime (`static TOKIO_RUNTIME`). All calls from the C side that need to interact with the database (e.g., `memflux_exec`) are executed within this runtime using `TOKIO_RUNTIME.block_on()`.

4.  **String-Based Commands:** All commands are passed as simple C strings. The `memflux_exec` function uses `shlex::split` to parse this string into command parts, similar to a command-line shell. This provides a simple yet flexible interface.

## Key FFI Functions

*   `memflux_open(config_json: *const c_char) -> *mut MemFluxDBSharedHandle`:
    *   This is the entry point for creating a database instance.
    *   It accepts a JSON string for configuration (`FFIConfig`).
    *   It calls `MemFluxDB::open_with_config` and blocks until the database is fully initialized.
    *   Returns an opaque handle to the shared database instance, or a null pointer on failure.

*   `memflux_close(handle: *mut MemFluxDBSharedHandle)`:
    *   Safely closes the database instance and shuts down all background tasks.

*   `memflux_cursor_open(handle: *mut MemFluxDBSharedHandle) -> *mut MemFluxCursorHandle`:
    *   Creates a new transactional context (a cursor).
    *   Returns an opaque handle to the cursor, which can be used for subsequent `memflux_exec` calls.

*   `memflux_cursor_close(handle: *mut MemFluxCursorHandle)`:
    *   Closes a cursor and cleans up its associated transaction state.

*   `memflux_exec(db_handle: *mut MemFluxDBSharedHandle, cursor_handle: *mut MemFluxCursorHandle, command_str: *const c_char) -> *mut FFIResponse`:
    *   Executes a command against the database within the context of the provided cursor.
    *   It parses the command string and calls the central `db.execute_command` method.
    *   It blocks until the command is complete and then converts the internal `Response` enum into a C-compatible `FFIResponse` struct.

*   `memflux_response_free(response: *mut FFIResponse)`:
    *   This is a crucial memory management function.
    *   The `FFIResponse` struct contains pointers to memory allocated by Rust (for strings, byte arrays, etc.). The C caller is responsible for calling this function to free that memory and prevent leaks.
    *   It reconstructs the Rust objects (e.g., `CString`, `Vec<u8>`) from the raw pointers and drops them.

## Data Structures

*   **`FFIResponse`:** A C-compatible struct that represents all possible return types from a command. It uses a `response_type` enum and a `union`-like structure of fields to hold different kinds of data (a single string, an integer, an array of byte arrays, etc.).
*   **Memory Transfer:** When returning variable-sized data like strings or byte arrays, the FFI layer allocates memory on the Rust heap, transfers ownership of that memory to the C caller via a raw pointer, and relies on the caller to pass that pointer back to `memflux_response_free` for deallocation. This is achieved using `CString::into_raw()` and `Vec::into_raw_parts()`, paired with `std::mem::forget`.
