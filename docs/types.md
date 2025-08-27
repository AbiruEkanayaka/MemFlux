# MemFlux Data Types

MemFlux supports multiple data models, allowing you to store simple key-value pairs as well as complex, structured data. The type of a value is determined by the command used to create it.

## Core Data Types

These are the fundamental data structures you can store in a MemFlux key.

### Bytes (Strings)

-   **Description:** The simplest data type. A key is mapped to an opaque sequence of bytes. This is often used for storing strings, serialized objects, or binary data.
-   **Commands:** `SET`, `GET`
-   **Use Case:** Caching HTML fragments, storing session data, etc.

### JSON

-   **Description:** MemFlux can store and manipulate JSON documents natively. This is one of its most powerful features, as it allows for structured data that can be queried with the SQL engine.
-   **Commands:** `JSON.SET`, `JSON.GET`, `JSON.DEL`
-   **Use Case:** Storing user profiles, product catalogs, or any semi-structured data.

### Lists

-   **Description:** A list is a sequence of strings, sorted by insertion order. It's implemented as a deque (double-ended queue), making it efficient to add or remove elements from the head or tail.
-   **Commands:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`
-   **Use Case:** Implementing message queues, storing timelines, etc.

### Sets

-   **Description:** A set is an unordered collection of unique strings.
-   **Commands:** `SADD`, `SREM`, `SMEMBERS`, `SCARD`, `SISMEMBER`
-   **Use Case:** Storing tags, tracking unique visitors, etc.

## SQL Data Types

When using the SQL engine with virtual schemas (`CREATE TABLE`), you can assign specific data types to the fields within your JSON documents. This enables type validation, casting, and more efficient query execution.

These types define how the data inside the JSON is interpreted by the SQL engine.

-   **`TEXT`**: Represents a string of characters.
-   **`INTEGER`**: Represents a 64-bit signed integer. Values will be cast to numbers (e.g., the string `"123"` becomes the number `123`).
-   **`BOOLEAN`**: Represents a `true` or `false` value.
-   **`TIMESTAMP`**: Represents a date and time. The SQL engine expects this to be a string formatted according to ISO 8601 / RFC 3339 (e.g., `2025-08-14T10:00:00Z`).
