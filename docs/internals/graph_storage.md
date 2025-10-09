# Graph Storage Internals

This document outlines the low-level storage schema for the property graph model within MemFlux. The design prioritizes efficient node and relationship lookups, leveraging the existing key-value store.

## Key Schema

The graph is stored using specific key prefixes to distinguish nodes and relationships from other data types.

### Node Representation

Nodes are stored with a key that includes their label and a unique ID. This allows for efficient scanning of all nodes with a specific label.

-   **Key Schema**: `_node:<Label>:<id>`
-   **Value**: `DbValue::JsonB`

The value is a JSONB-encoded object containing all the properties of the node.

**Example:**
A node representing a person named "Alice" with the label "User" and ID `u1` would be stored as:
-   Key: `_node:User:u1`
-   Value: `{"name": "Alice", "age": 30}` (as JSONB)

To allow for efficient lookups by ID alone, a primary key (PK) index is also maintained:
- **PK Index Key**: `_pk_node:<id>`
- **PK Index Value**: `DbValue::Bytes(<Label>)`

### Relationship Representation

Relationships (or edges) are stored using two keys to represent both outgoing and incoming connections. This creates adjacency lists for each node, allowing for efficient traversal in either direction.

-   **Outgoing Edge Key**: `_edge:out:<start_node_id>:<type>:<end_node_id>`
-   **Incoming Edge Key**: `_edge:in:<end_node_id>:<type>:<start_node_id>`
-   **Value**: `DbValue::JsonB`

The value for both keys is a JSONB-encoded object containing the properties of the relationship. Storing the properties with both keys duplicates the property data but ensures that all relationship information is available regardless of traversal direction, avoiding extra lookups.

**Example:**
A `KNOWS` relationship from node `u1` to `u2` with ID `r1` and a `since` property would have two entries:
1.  **Outgoing**:
    -   Key: `_edge:out:u1:KNOWS:u2`
    -   Value: `{"since": "2023-01-15", "_id": "r1"}` (as JSONB)
2.  **Incoming**:
    -   Key: `_edge:in:u2:KNOWS:u1`
    -   Value: `{"since": "2023-01-15", "_id": "r1"}` (as JSONB)

A primary key index is also maintained for relationships to allow deletion by ID:
- **PK Index Key**: `_pk_rel:<id>`
- **PK Index Value**: `DbValue::Bytes(<start_node_id>:<type>:<end_node_id>)`

This structure allows finding all of `u1`'s outgoing relationships with a prefix scan on `_edge:out:u1:` and all of `u2`'s incoming relationships with a prefix scan on `_edge:in:u2:`.

## Internal ID Strategy

-   **Node IDs**: Node IDs are generated using **UUIDs (v4)**. This avoids the need for a centralized, coordinating ID sequence, which is beneficial for concurrent writes.
-   **Relationship IDs**: Relationship IDs are also generated using **UUIDs (v4)**. They are stored as a property (`_id`) within the relationship's property JSON. This is primarily for external reference, as the composite key (`<start_id>:<type>:<end_id>`) is the primary identifier for a relationship instance internally.
