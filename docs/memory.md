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

### ARC (Adaptive Replacement Cache)

This policy (`"eviction_policy": "arc"`) is an advanced strategy that aims to offer the best of both LRU and LFU.

-   **How it works:** ARC maintains two lists of recently accessed keys and two "ghost" lists of recently evicted keys. It dynamically adjusts its behavior based on the workload, effectively choosing between an LRU-like or LFU-like strategy to maximize the cache hit rate.
-   **Eviction:** It intelligently decides whether to evict a recently used item or a frequently used item based on historical access patterns.

ARC generally has a higher performance than LRU and LFU but comes with slightly more memory overhead for its tracking structures.

### LFRU (Least Frequently/Recently Used)

This policy (`"eviction_policy": "lfru"`) is a hybrid that combines aspects of LFU and LRU.

-   **How it works:** The cache is divided into a "probationary" segment and a "protected" segment. New keys enter the probationary segment. Accessing a key in the probationary segment promotes it to the protected segment.
-   **Eviction:** Keys are always evicted from the probationary segment. If the protected segment becomes full, the least recently used item from it is moved back to the probationary segment, giving it another chance before eviction.

This policy attempts to prevent popular "one-hit wonder" keys from pushing out genuinely valuable, frequently-used keys.

### Random

This policy (`"eviction_policy": "random"`) is the simplest.

-   **How it works:** When memory is full, the server evicts a completely random key.
-   **Eviction:** A key is chosen at random from all existing keys.

This policy has very low overhead but is less efficient at preserving "hot" or important data compared to other strategies. It can be useful in workloads where access patterns are also random.

## Monitoring Memory

You can check the current memory usage of the server at any time using the `MEMUSAGE` command.

```
> MEMUSAGE
:12345678
```

This returns the total memory used in bytes. This value is an estimate but is the same one used to check against the `maxmemory` limit.

## Behavior at Startup

When MemFlux starts with a `maxmemory` limit, it performs two important actions related to memory management:

1.  **Initial Eviction Check:** The server immediately calculates the memory usage of the data loaded from the snapshot and WAL files. If this initial usage is already higher than the `maxmemory_mb` limit, MemFlux will begin evicting keys *before* accepting any client connections. It will continue to evict keys according to the configured `eviction_policy` until the memory usage is below the limit. This ensures the server always starts within its configured memory bounds.

2.  **Policy Priming:** The eviction data structures are "primed" with all the keys loaded from disk. This means:
    -   **LRU:** Keys are loaded in an order that reflects their position in the snapshot, with older data being more likely to be evicted first if no new accesses occur.
    -   **LFU:** All keys start with a frequency count of 1.
    -   **LFRU:** All keys start in the "probationary" segment.
    -   **ARC:** The cache is filled with all keys, with recently added keys placed in the "recent" list (T1).
    -   **Random:** The internal list of keys is populated with all keys from the database.