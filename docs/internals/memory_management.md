# Internals: Memory Management & Eviction

MemFlux provides several memory eviction policies to stay under a configured `maxmemory` limit. This document details the internal implementation of these policies. The user-facing guide can be found [here](../memory.md).

**Source Files:**
*   `src/memory.rs`
*   `src/arc.rs`

## Memory Tracking

The `MemoryManager` tracks the server's estimated memory usage via an `AtomicU64` counter.

*   **Estimation:** The size of each key and value is estimated. For simple values like Bytes, this is just the length. For complex types like Lists, Sets, and JSON, the size is calculated by summing the lengths of their components. This is a reasonable approximation that avoids expensive serialization for every check.
*   **Enforcement:** Before a write command is executed, the `MemoryManager` checks if the new data would exceed the `maxmemory` limit. If so, it calls `ensure_memory_for`, which begins evicting keys according to the configured policy until enough space is freed.

## Eviction Policy Implementations

### LRU (Least Recently Used)

*   **Data Structure:** A single `RwLock<VecDeque<String>>` (`lru_keys`).
*   **Mechanism:**
    *   The `VecDeque` acts as a queue. The front of the queue is the *most* recently used, and the back is the *least* recently used.
    *   **`track_access`:** When a key is accessed, it is found in the deque, removed, and pushed to the front. If it's a new key, it's simply pushed to the front.
    *   **`evict`:** When eviction is needed, the manager simply calls `pop_back()` on the deque to get the least recently used key.

### LFU (Least Frequently Used)

*   **Data Structures:**
    *   `lfu_freqs: RwLock<HashMap<String, u64>>`: Maps a key to its access frequency count.
    *   `lfu_freq_keys: RwLock<HashMap<u64, VecDeque<String>>>`: Maps a frequency count to a list of keys with that frequency. This allows for O(1) access to the list of least frequently used items.
    *   `lfu_min_freq: RwLock<u64>`: Tracks the lowest frequency currently in the cache to avoid searching the `freq_keys` map.
*   **Mechanism:**
    *   **`track_access`:** When a key is accessed, its old frequency is found. The key is removed from the list for `old_freq` and added to the list for `new_freq` (`old_freq + 1`). Its count is updated in `lfu_freqs`. If the list for `old_freq` becomes empty and `old_freq` was the `min_freq`, `min_freq` is incremented.
    *   **`evict`:** The manager gets the list of keys for the current `min_freq`. It pops a key from the front of that list (which is the least recently used *among those with the minimum frequency*).

### ARC (Adaptive Replacement Cache)

ARC is a more advanced policy that balances between LRU and LFU. It is implemented in `src/arc.rs`.

*   **Data Structures:** ARC maintains four lists (implemented as `VecDeque`):
    *   `t1`: "Top 1" - A cache for recently seen items (LRU-like).
    *   `t2`: "Top 2" - A cache for frequently seen items (LFU-like).
    *   `b1`: "Bottom 1" - A "ghost" list of recently evicted items from `t1`.
    *   `b2`: "Bottom 2" - A "ghost" list of recently evicted items from `t2`.
*   **Mechanism:**
    *   The core idea is to dynamically adjust a target size `p` for the `t1` cache.
    *   **Hit in `t1` or `t2`:** The item is promoted to the front of `t2`, marking it as frequently and recently used.
    *   **Miss (Hit in `b1`):** This means an item that was only seen recently (and evicted) is being requested again. This is a sign the workload is more "frequent" than "recent". The target size `p` for the `t1` cache is increased, and the item is moved from the ghost list `b1` to the main cache `t2`.
    *   **Miss (Hit in `b2`):** This means an item that was used frequently (and evicted) is being requested again. This is a sign the workload is more "recent" than "frequent". The target size `p` is decreased, and the item is moved from `b2` to `t2`.
    *   **True Miss:** A new item is added to `t1`.
    *   **`evict`:** Eviction happens from either `t1` or `t2` based on their relative sizes compared to `p`. The evicted item is moved to the corresponding ghost list (`b1` or `b2`).

### LFRU (Least Frequently/Recently Used)

*   **Data Structures:**
    *   `lfru_probationary: RwLock<VecDeque<String>>`
    *   `lfru_protected: RwLock<VecDeque<String>>`
*   **Mechanism:** This is a simplified hybrid policy.
    *   The cache is split into two segments. New keys always enter the "probationary" segment.
    *   Accessing a key in the probationary segment promotes it to the "protected" segment.
    *   Accessing a key in the protected segment just moves it to the front of that list (LRU behavior).
    *   Eviction always happens from the tail of the probationary segment. If the protected segment gets too large, its least recently used item is demoted back to the probationary segment, giving it a "second chance".

### Random

*   **Data Structure:** A `RwLock<Vec<String>>` (`random_keys`).
*   **Mechanism:**
    *   **`track_access`:** If a key is not in the vector, it's added.
    *   **`evict`:** A random index is chosen, and the key at that index is removed and returned.
