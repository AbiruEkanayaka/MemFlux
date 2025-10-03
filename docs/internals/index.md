# MemFlux Internals

This section of the documentation provides a deeper look into the internal architecture and implementation details of MemFlux. It is intended for developers who want to contribute to the project or understand how it works under the hood.

While the main documentation provides a user-focused overview, these documents dive into the source code (`src/`) to explain the core components.

## Core Systems

*   **[Server & Library Initialization](./startup.md):** A step-by-step guide to the initialization process, both for the server and when used as a library.

*   **[Transaction Management & MVCC](./mvcc.md):** An explanation of the Multi-Version Concurrency Control system, transaction lifecycle, and snapshot isolation.

*   **[The Storage Executor](./storage_executor.md):** Details on the component responsible for handling writes and applying them to the versioned data store.

*   **[The SQL Query Engine](./query_engine.md):** A detailed breakdown of the query pipeline, from parsing a raw SQL string to executing a physical plan and streaming results.

*   **[The FFI Layer](./ffi.md):** An overview of how the core Rust library is exposed via a C-compatible Foreign Function Interface.

*   **[Memory Management & Eviction](./memory_management.md):** An in-depth look at how memory is tracked and how the different eviction policies (LRU, LFU, ARC) are implemented.

*   **[The Vacuum Process](./vacuum.md):** An explanation of how old data versions are garbage-collected to reclaim memory.

*   **[Persistence Engine](../persistence.md):** (See main documentation) An explanation of how data durability is achieved through the Write-Ahead Log (WAL) and snapshotting.
