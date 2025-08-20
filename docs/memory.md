# Memory Management

MemFlux provides features to manage its memory usage, preventing the server from consuming more RAM than allocated. This is controlled primarily through the `maxmemory_mb` configuration setting.

## `maxmemory` Configuration

The `maxmemory_mb` option in `config.json` sets a hard limit on the total memory MemFlux will use for keys, values, and overhead.

-   **`"maxmemory_mb": 0` (Default):** Memory management is disabled. The server will consume as much memory as the operating system allows.
-   **`"maxmemory_mb": 1024`:** The server will limit its memory usage to 1024 MB (1 GB).

When this limit is enabled, MemFlux will actively monitor its estimated memory usage. If a new write command would cause the server to exceed the `maxmemory` limit, it will first attempt to free up space by evicting existing keys.

## Eviction Policy: LRU

MemFlux uses a **Least Recently Used (LRU)** eviction policy. This means that when memory needs to be freed, the server will delete the key that has not been accessed for the longest time.

-   **How it works:** The server maintains a list of keys in the order they were last accessed.
-   **Access:** Any read (`GET`, `JSON.GET`, etc.) or write (`SET`, `LPUSH`, etc.) operation on a key marks it as "recently used" by moving it to the front of the LRU list.
-   **Eviction:** When memory is full, keys are evicted from the tail of the list—the ones that have been idle the longest.

This policy ensures that frequently accessed, "hot" data is likely to remain in memory, while "cold" data is evicted first.

## Monitoring Memory

You can check the current memory usage of the server at any time using the `MEMUSAGE` command.

```
> MEMUSAGE
:12345678
```

This returns the total memory used in bytes. This value is an estimate but is the same one used to check against the `maxmemory` limit.
