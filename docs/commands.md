# MemFlux Commands

MemFlux supports a variety of Redis-compatible commands for interacting with different data structures. All commands are sent using the RESP protocol.

## Server Commands

#### `PING`
Checks if the server is responsive.
- **Syntax:** `PING`
- **Returns:** A simple string `PONG` if the server is alive.

#### `AUTH <password>`
Authenticates the client to the server. This is only required if `requirepass` is set in the server configuration. It must be the first command sent by a client.
- **Syntax:** `AUTH <password>`
- **Returns:** `+OK` on success, or an error if the password is incorrect.

#### `FLUSHDB`
Deletes all keys from the current database. This operation is instantaneous and also clears all indexes and caches.
- **Syntax:** `FLUSHDB`
- **Returns:** `+OK`

#### `SAVE`
This command is provided for compatibility but is a no-op. MemFlux handles saving data to disk automatically via its persistence engine.
- **Syntax:** `SAVE`
- **Returns:** `+OK`

## Key/Value (Bytes) Commands

These commands operate on simple key-value pairs where the value is a sequence of bytes (a string).

#### `GET <key>`
Retrieves the value stored at the specified key.
- **Syntax:** `GET <key>`
- **Returns:** A bulk string reply with the value, or a Nil reply if the key does not exist. Returns an error if the key holds a non-string value (like a List or Set).

#### `SET <key> <value>`
Sets the value for a given key, overwriting any existing value.
- **Syntax:** `SET <key> <value>`
- **Returns:** `+OK`

#### `DELETE <key> [key ...]`
Removes one or more specified keys.
- **Syntax:** `DELETE <key> [key ...]`
- **Returns:** An integer reply indicating the number of keys that were removed.

#### `KEYS <pattern>`
Finds all keys matching the given pattern.
- **Syntax:** `KEYS <pattern>`
- **Pattern:** Supports `*` to match any number of characters and `?` to match a single character.
- **Returns:** An array of keys matching the pattern.
- **Warning:** This can be a slow operation on large databases. Use with caution.

## JSON Commands

These commands allow for powerful operations on JSON documents stored as values.

#### `JSON.SET <key-path> <value>`
Sets or updates a value within a JSON document.
- **Syntax:** `JSON.SET <key-path> <value>`
- **Key Path:** A dot-notation path. The first part is the key, and subsequent parts are paths within the JSON object (e.g., `mykey.user.name`). If the key or path does not exist, it will be created.
- **Value:** A valid JSON string.
- **Returns:** `+OK`
- **Example:** `JSON.SET user:1.profile.age 31`

#### `JSON.GET <key-path>`
Retrieves a value from a JSON document.
- **Syntax:** `JSON.GET <key-path>`
- **Returns:** A bulk string reply with the JSON-encoded value at the specified path, or Nil if the path does not exist.
- **Example:** `JSON.GET user:1.profile`

#### `JSON.DEL <key-path>`
Deletes a value from a JSON document.
- **Syntax:** `JSON.DEL <key-path>`
- **Returns:** An integer reply: `1` if the path existed and was deleted, `0` otherwise.
- **Example:** `JSON.DEL user:1.profile.age`

## List Commands

List commands operate on a deque (double-ended queue) of byte strings.

#### `LPUSH <key> <value> [value ...]`
Prepends one or more values to the head of a list. Creates the list if it does not exist.
- **Syntax:** `LPUSH <key> <value> [value ...]`
- **Returns:** An integer reply with the new length of the list.

#### `RPUSH <key> <value> [value ...]`
Appends one or more values to the tail of a list. Creates the list if it does not exist.
- **Syntax:** `RPUSH <key> <value> [value ...]`
- **Returns:** An integer reply with the new length of the list.

#### `LPOP <key> [count]`
Removes and returns one or more elements from the head of a list.
- **Syntax:** `LPOP <key> [count]`
- **Count (Optional):** The number of elements to pop. Defaults to 1.
- **Returns:**
    - If `count` is not provided: A bulk string reply with the popped element.
    - If `count` is provided: An array of bulk string replies.
    - Nil if the key does not exist or the list is empty.

#### `RPOP <key> [count]`
Removes and returns one or more elements from the tail of a list.
- **Syntax:** `RPOP <key> [count]`
- **Returns:** Same as `LPOP`.

#### `LLEN <key>`
Returns the length of the list.
- **Syntax:** `LLEN <key>`
- **Returns:** An integer reply with the length of the list, or `0` if the key does not exist.

#### `LRANGE <key> <start> <stop>`
Returns a range of elements from the list.
- **Syntax:** `LRANGE <key> <start> <stop>`
- **Indexes:** `start` and `stop` are zero-based indexes. Negative indexes can be used to specify offsets from the end of the list (`-1` is the last element).
- **Returns:** An array of bulk string replies.

## Set Commands

Set commands operate on an unordered collection of unique byte strings.

#### `SADD <key> <member> [member ...]`
Adds one or more members to a set. Creates the set if it does not exist.
- **Syntax:** `SADD <key> <member> [member ...]`
- **Returns:** An integer reply with the number of members that were newly added to the set (not including members that were already present).

#### `SREM <key> <member> [member ...]`
Removes one or more members from a set.
- **Syntax:** `SREM <key> <member> [member ...]`
- **Returns:** An integer reply with the number of members that were successfully removed.

#### `SMEMBERS <key>`
Returns all members of the set.
- **Syntax:** `SMEMBERS <key>`
- **Returns:** An array of all members in the set. The order is not guaranteed.

#### `SCARD <key>`
Returns the number of members in the set (its cardinality).
- **Syntax:** `SCARD <key>`
- **Returns:** An integer reply with the size of the set, or `0` if the key does not exist.

#### `SISMEMBER <key> <member>`
Checks if a member exists in the set.
- **Syntax:** `SISMEMBER <key> <member>`
- **Returns:** An integer reply: `1` if the member exists, `0` otherwise.

## Indexing Commands

See the [Indexing Documentation](./indexing.md) for more details.

- `IDX.CREATE <index-name> <key-prefix> <json-path>`
- `IDX.DROP <index-name>`
- `IDX.LIST`
- `CREATEINDEX <key-prefix> ON <json-path>` (Alias for `IDX.CREATE`)
