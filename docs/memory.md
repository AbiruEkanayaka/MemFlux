# Memory Management

MemFlux provides features to manage its memory usage, preventing the server from consuming more RAM than allocated. This is controlled primarily through the `maxmemory_mb` configuration setting.

## `maxmemory` Configuration

The `maxmemory_mb` option in `config.json` sets a hard limit on the total memory MemFlux will use for keys, values, and overhead.

-   **`"maxmemory_mb": 0` (Default):** Memory management is disabled. The server will consume as much memory as the operating system allows.
-   **`"maxmemory_mb": 1024`:** The server will limit its memory usage to 1024 MB (1 GB).

When this limit is enabled, MemFlux will actively monitor its estimated memory usage. If a new write command would cause the server to exceed the `maxmemory` limit, it will first attempt to free up space by evicting existing keys.

## Eviction Policies

When the `maxmemory` limit is reached, MemFlux evicts keys to free up space. The eviction behavior is determined by the `eviction_policy` setting in `config.json`.

### LRU (Least Recently Used)

This is the default policy (`"eviction_policy": "lru"`). It evicts the key that has not been accessed for the longest time.

-   **How it works:** The server maintains a list of keys in the order they were last accessed.
-   **Access:** Any read (`GET`, `JSON.GET`, etc.) or write (`SET`, `LPUSH`, etc.) operation on a key marks it as "recently used" by moving it to the front of the LRU list.
-   **Eviction:** When memory is full, keys are evicted from the tail of the list—the ones that have been idle the longest.

This policy is a good general-purpose choice, ensuring that frequently accessed, "hot" data is likely to remain in memory.

### LFU (Least Frequently Used)

This policy (`"eviction_policy": "lfu"`) evicts the key that has been accessed the fewest number of times.

-   **How it works:** The server tracks an access frequency counter for each key.
-   **Access:** Each time a key is accessed, its frequency counter is incremented.
-   **Eviction:** When memory is full, the key with the lowest frequency count is evicted.

This policy can be more effective than LRU in use cases where some data is accessed very frequently for a short period (bursty traffic) but isn't important long-term. LFU will correctly identify and retain the truly popular items, even if they haven't been accessed recently.

## Monitoring Memory

You can check the current memory usage of the server at any time using the `MEMUSAGE` command.

```
> MEMUSAGE
:12345678
```

This returns the total memory used in bytes. This value is an estimate but is the same one used to check against the `maxmemory` limit.
