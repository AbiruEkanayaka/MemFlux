# MemFlux SQL Reference

MemFlux includes a powerful, custom-built SQL query engine that operates on JSON data. This allows you to perform complex, structured queries against your schemaless or semi-structured data.

## Overview

The SQL engine is accessed via a single command, `SQL`, followed by the query string.

```
> SQL SELECT user.name, orders.amount FROM user JOIN orders ON user.id = orders.user_id
```

The engine treats key prefixes as "tables". For example, keys like `user:1`, `user:2` are considered rows in the `user` table. The part of the key after the prefix is automatically available as the `id` field in `SELECT` queries.

### Key Concepts

*   **Virtual Schemas:** You can use DDL commands like `CREATE TABLE` to define a "virtual schema" for a key prefix. This provides data validation, type casting, and enables more robust query planning and error checking.
*   **Schemaless by Default:** If no virtual schema is defined, the query engine operates in a schemaless mode. It will attempt to infer types and relationships at query time.
*   **JSON-First:** The engine is designed to work seamlessly with JSON documents. You can use dot notation to access nested fields (e.g., `profile.age`).
*   **Streaming Results:** `SELECT` queries stream results back to the client row by row, making it efficient for large result sets.

## SQL Reference Guides

For detailed information on specific SQL commands and features, please refer to the following guides:

*   **[Data Definition Language (DDL)](./sql/ddl.md):** Learn how to define and manage your data structures with `CREATE TABLE`, `DROP TABLE`, and `ALTER TABLE`.
*   **[Data Manipulation Language (DML)](./sql/dml.md):** Learn how to create, modify, and delete data with `INSERT`, `UPDATE`, and `DELETE`.
*   **[The `SELECT` Statement](./sql/select.md):** A comprehensive guide to querying data, including `JOIN`s, `WHERE` clauses, `GROUP BY`, `ORDER BY`, Common Table Expressions (CTEs), and more.
*   **[Built-in Functions](./sql/functions.md):** A reference for all available scalar functions for data manipulation and transformation.
