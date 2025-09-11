# Internals: The FFI Layer

The Foreign Function Interface (FFI) layer exposes the core MemFlux database engine as a C-compatible dynamic library. This allows MemFlux to be embedded directly into other applications and languages, such as Python, C++, Node.js, etc., for high-performance, in-process database operations.

**Source File:** `src/ffi.rs`

## Core Design

The FFI layer acts as a bridge between the external C-style world and the internal, asynchronous Rust world of MemFlux.

1.  **Opaque Handle:** The database instance (`Arc<MemFluxDB>`) is wrapped in a struct and returned to the C caller as an opaque pointer (`*mut MemFluxDBHandle`). The caller never interacts with the database instance directly, only through this handle.

2.  **Synchronous API:** While MemFlux is internally asynchronous (using Tokio), the FFI presents a synchronous, blocking interface. This is a deliberate design choice to simplify integration with most programming languages, which typically expect blocking I/O calls.

3.  **Global Tokio Runtime:** To bridge the sync/async gap, the FFI layer maintains a single, global, multi-threaded Tokio runtime (`static TOKIO_RUNTIME`). All calls from the C side that need to interact with the database (e.g., `memflux_exec`) are executed within this runtime using `TOKIO_RUNTIME.block_on()`.

4.  **String-Based Commands:** All commands are passed as simple C strings. The `memflux_exec` function uses `shlex::split` to parse this string into command parts, similar to a command-line shell. This provides a simple yet flexible interface.

## Key FFI Functions

*   `memflux_open(config_json: *const c_char) -> *mut MemFluxDBHandle`:
    *   This is the entry point for creating a database instance.
    *   It accepts a JSON string for configuration (`FFIConfig`), which is a subset of the main `config.json`.
    *   It calls `MemFluxDB::open_with_config` and blocks until the database is fully initialized.
    *   Returns an opaque handle to the new database instance, or a null pointer on failure.

*   `memflux_close(handle: *mut MemFluxDBHandle)`:
    *   Safely closes the database instance.
    *   It takes ownership of the handle, reconstructs the `Box<MemFluxDBHandle>`, and allows Rust's ownership system to drop it, which in turn gracefully shuts down the persistence engine.

*   `memflux_exec(handle: *mut MemFluxDBHandle, command_str: *const c_char) -> *mut FFIResponse`:
    *   Executes a command against the database.
    *   It parses the command string and calls the central `db.execute_command` method.
    *   It blocks until the command is complete and then converts the internal `Response` enum into a C-compatible `FFIResponse` struct.

*   `memflux_response_free(response: *mut FFIResponse)`:
    *   This is a crucial memory management function.
    *   The `FFIResponse` struct contains pointers to memory allocated by Rust (for strings, byte arrays, etc.). The C caller is responsible for calling this function to free that memory and prevent leaks.
    *   It reconstructs the Rust objects (e.g., `CString`, `Vec<u8>`) from the raw pointers and drops them.

## Data Structures

*   **`FFIResponse`:** A C-compatible struct that represents all possible return types from a command. It uses a `response_type` enum and a `union`-like structure of fields to hold different kinds of data (a single string, an integer, an array of byte arrays, etc.).
*   **Memory Transfer:** When returning variable-sized data like strings or byte arrays, the FFI layer allocates memory on the Rust heap, transfers ownership of that memory to the C caller via a raw pointer, and relies on the caller to pass that pointer back to `memflux_response_free` for deallocation. This is achieved using `CString::into_raw()` and `Vec::into_raw_parts()`, paired with `std::mem::forget`.
