# SQL: Data Definition Language (DDL)

DDL commands are used to define and manage the structure of your data through "virtual schemas." A virtual schema associates a key prefix (the "table") with a set of column definitions, enabling type casting, validation, and more predictable queries.

## `CREATE TABLE`

Defines a new virtual schema for a key prefix.

### Syntax
```sql
CREATE TABLE table_name (
    column_name data_type,
    column_name data_type,
    ...
);
```

### Parameters
- `table_name`: The name of the table, which corresponds to the key prefix (e.g., `user` for keys like `user:1`).
- `column_name`: The name of a field in the JSON objects.
- `data_type`: The virtual type to associate with the column. For a complete list of supported data types, see the [Data Types documentation](../types.md#sql-data-types).

### Behavior
- When a `CREATE TABLE` statement is executed, a schema definition is stored internally.
- From that point on, DML operations (`INSERT`, `UPDATE`) on keys matching the `table_name` prefix will have their values cast to the specified `data_type`. For example, if a column `age` is `INTEGER`, providing the string `"30"` in an `INSERT` statement will result in the number `30` being stored.
- The query planner will use this schema information to validate queries. For example, it will return an error if you try to `SELECT` a column that doesn't exist in the schema.

### Example
```sql
CREATE TABLE users (
    id TEXT,
    name TEXT,
    age INTEGER,
    is_active BOOLEAN
);
```

## `DROP TABLE`

Removes a virtual schema definition.

### Syntax
```sql
DROP TABLE table_name;
```

### Behavior
- This command only removes the schema definition. **It does not delete the underlying data associated with the key prefix.**
- After dropping a table's schema, the query engine will revert to schemaless behavior for that key prefix.

### Example
```sql
DROP TABLE users;
```

## `ALTER TABLE`

Modifies an existing virtual schema.

### Syntax
```sql
-- Add a new column
ALTER TABLE table_name ADD COLUMN column_name data_type;

-- Drop an existing column
ALTER TABLE table_name DROP COLUMN column_name;
```

### Behavior
- `ADD COLUMN`: Adds a new column definition to the schema. Existing data will not be modified, and the new field will be treated as `NULL` until it is set.
- `DROP COLUMN`: Removes a column definition from the schema. This does not remove the field from any existing JSON documents, but the field will no longer be visible to the query engine or subject to schema validation.

### Example
```sql
-- Add a 'city' column to the users table
ALTER TABLE users ADD COLUMN city TEXT;

-- Remove the 'is_active' column
ALTER TABLE users DROP COLUMN is_active;
```

## `TRUNCATE TABLE`

Deletes all data associated with a table's key prefix.

### Syntax
```sql
TRUNCATE TABLE table_name;
```

### Behavior
- This command is a DML operation in disguise. It effectively performs a `DELETE FROM table_name` without a `WHERE` clause.
- It deletes all keys matching the table's prefix (e.g., `TRUNCATE TABLE users` deletes all `users:*` keys).
- **It does not remove the virtual schema definition.**

### Example
```sql
TRUNCATE TABLE users;
```

## `CREATE INDEX`

Creates an index on a table to speed up queries. This is the SQL-standard way to create an index and is functionally equivalent to the `IDX.CREATE` command.

### Syntax
```sql
CREATE [UNIQUE] INDEX index_name ON table_name (column_name);
```

### Parameters
- `UNIQUE`: An optional keyword that enforces a uniqueness constraint on the column(s) being indexed. An `INSERT` or `UPDATE` that creates a duplicate value in a unique index will fail.
- `index_name`: A name for the new index.
- `table_name`: The table containing the column to index.
- `column_name`: The column (JSON field) to create the index on. Expression-based indexes are also supported.

### Behavior
- When an index is created, it is automatically **backfilled** with all existing data from the table.
- Once created, the index is automatically maintained by MemFlux on all `INSERT`, `UPDATE`, and `DELETE` operations.
- The query planner will automatically use the index to accelerate `WHERE` clauses that filter on the indexed column.

### Example
```sql
-- Create a standard index on the email column
CREATE INDEX idx_users_email ON users (email);

-- Create a unique index on the username column
CREATE UNIQUE INDEX uq_users_username ON users (username);
```
