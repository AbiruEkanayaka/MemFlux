# Indexing in MemFlux

MemFlux provides a powerful indexing feature to accelerate query performance, especially for `SELECT` statements with `WHERE` clauses that filter on specific JSON fields.

## Overview

By default, a `WHERE` clause requires a full "table scan," where the database iterates over every key matching a prefix (e.g., every key starting with `user:`), loads its JSON value, and checks if it matches the predicate. This can be slow for large datasets.

An index is a separate data structure that maps a specific value for a JSON field to the set of keys that contain that value. When a query filters on an indexed field, the database can use the index to instantly find the relevant keys without scanning the entire dataset.

## Managing Indexes

Indexes are managed via a set of `IDX.*` commands.

### Creating an Index

You can create an index using the `IDX.CREATE` command or its alias `CREATEINDEX`.

**Syntax:**
```
IDX.CREATE <index-name> <key-prefix> <json-path>
CREATEINDEX <key-prefix> ON <json-path>
```

*   `<index-name>`: A unique name for your index. **Note:** This is only provided for `IDX.CREATE`.
*   `<key-prefix>`: A key pattern with a trailing `*` that specifies which keys this index applies to. For example, `user:*` would apply to `user:1`, `user:100`, etc.
*   `<json-path>`: The dot-notation path to the field within the JSON object you want to index.

**Example:**

Imagine you have user data stored in keys like `user:1`, `user:2`, etc., with the following structure:
```json
{
  "username": "alice",
  "email": "alice@example.com",
  "profile": {
    "age": 30,
    "city": "SF"
  }
}
```

To speed up queries that filter by city, you can create an index:

```
> IDX.CREATE user_city_idx user:* profile.city
+OK
```
Or using the alias:
```
> CREATEINDEX user:* ON profile.city
+OK
```
The `CREATEINDEX` alias automatically generates an index name for you based on the key prefix and path. The generated name is created by joining the prefix (without the trailing `*`) and the path with underscores (e.g., `user_profile_city`). You will need this name if you want to drop the index later.

When an index is created, MemFlux automatically **backfills** it by scanning all existing keys matching the prefix and adding them to the index.

### Listing Indexes

To see all created indexes, use `IDX.LIST`.

```
> IDX.LIST
*1
$19
user:*|profile.city
```
The response is an array of the full **internal** index names, which are composed of the key prefix and the JSON path, separated by a `|`. Note that this internal name is for informational purposes and is **not** the name used to drop the index.

### Dropping an Index

To remove an index, use `IDX.DROP` with the name you used to create it.

```
> IDX.DROP user_city_idx
:1
```
This will remove the index and free up the memory it was using. It returns the number of indexes dropped (1 or 0).

**Important:** To drop an index created with the `CREATEINDEX` alias, you must use its auto-generated name. For the example `CREATEINDEX user:* ON profile.city`, the name would be `user_profile_city`, so you would run:
```
> IDX.DROP user_profile_city
:1
```

## Automatic Maintenance

Once an index is created, it is **automatically kept up-to-date** by the database.

*   **`JSON.SET`:** When a JSON value is added or updated, MemFlux checks if the key matches any indexed prefixes. If it does, it removes the old value from the relevant indexes and adds the new value.
*   **`DELETE`:** When a key is deleted, it is removed from all indexes it was a part of.

This ensures that indexes are always consistent with the data, with no need for manual intervention.

## Query Engine Integration

The query optimizer automatically detects when a `WHERE` clause can be satisfied by an existing index.

Consider the following query:
```sql
SELECT username FROM user WHERE profile.city = 'SF'
```

*   **Without an index:** The engine performs a `TableScan` on all `user:*` keys.
*   **With the `user_city_idx`:** The optimizer sees that the `WHERE` clause is on `profile.city` and that a corresponding index exists. It replaces the `TableScan` with a much faster `IndexScan`, directly retrieving only the keys for users in 'SF'.

## Limitations

*   **Equality-Based:** The current indexing implementation is designed to accelerate equality checks (e.g., `field = 'value'`). It does not speed up range queries (`>`, `<`) or other operators.
*   **JSON Values Only:** Indexes can only be created on fields within `JSON` values, not on other data types like raw bytes, lists, or sets.
*   **Memory Usage:** Each index consumes additional memory to store the mapping from values to keys. Be mindful of the number and cardinality of indexed fields.
