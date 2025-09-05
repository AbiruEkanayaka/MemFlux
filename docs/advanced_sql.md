# Advanced SQL Features

This document covers advanced SQL features that are implemented in MemFlux but may not yet be fully integrated into the main documentation.

## Additional Data Types

Beyond the basic types, MemFlux supports a wider range of SQL data types in `CREATE TABLE` statements.

| Data Type | Description |
|---|---|
| `SMALLINT` | A 16-bit signed integer. |
| `BIGINT` | A 64-bit signed integer. |
| `NUMERIC(p, s)` | A fixed-point number with precision `p` and scale `s`. |
| `REAL` | A single-precision floating-point number. |
| `DOUBLE PRECISION`| A double-precision floating-point number. |
| `VARCHAR(n)` | A variable-length string with a maximum length of `n`. |
| `CHAR(n)` | A fixed-length string of length `n`. |
| `BYTEA` | A variable-length binary string (hex format, e.g., `\xDEADBEEF`). |
| `JSONB` | A binary representation of a JSON object. |
| `UUID` | A universally unique identifier. |
| `TIMESTAMPTZ` | A timestamp with a time zone. |
| `DATE` | A calendar date (year, month, day). |
| `TIME` | A time of day. |
| `type[]` | An array of any other supported type (e.g., `INTEGER[]`, `TEXT[]`). |

## Views

Views are virtual tables defined by a `SELECT` query. They can be used to simplify complex queries or restrict access to data.

### `CREATE VIEW`
Defines a new view.

**Syntax:**
```sql
CREATE VIEW view_name AS SELECT ...;
```

**Example:**
```sql
CREATE VIEW sf_users AS
SELECT name, age FROM users WHERE city = 'SF';
```

### `DROP VIEW`
Removes an existing view.

**Syntax:**
```sql
DROP VIEW view_name;
```

## Schemas

MemFlux supports namespacing tables using schemas.

### `CREATE SCHEMA`
Defines a new schema.

**Syntax:**
```sql
CREATE SCHEMA schema_name;
```
**Usage:**
```sql
CREATE TABLE my_schema.my_table (...);
```

## Table Constraints

`CREATE TABLE` supports defining constraints to enforce data integrity.

### `PRIMARY KEY`
Uniquely identifies each record in a table. Implicitly `NOT NULL`.

**Syntax:**
```sql
-- Inline
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT
);

-- Table constraint
CREATE TABLE products (
    id INTEGER,
    name TEXT,
    PRIMARY KEY (id)
);
```

### `UNIQUE`
Ensures that all values in a column or a group of columns are unique.

**Syntax:**
```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT UNIQUE
);
```

### `CHECK`
Ensures that all values in a column satisfy a specific condition.

**Syntax:**
```sql
CREATE TABLE products (
    id INTEGER,
    price INTEGER CHECK (price > 0)
);
```

### `FOREIGN KEY` and Referential Actions
Creates a link between two tables and constrains data based on columns in the other table. MemFlux supports `ON DELETE` and `ON UPDATE` actions.

**Supported Actions:**
*   `CASCADE`: If a parent record is deleted/updated, the corresponding child records are also deleted/updated.
*   `SET NULL`: If a parent record is deleted/updated, the foreign key columns in the child records are set to `NULL`.
*   `SET DEFAULT`: If a parent record is deleted/updated, the foreign key columns in the child records are set to their default values.
*   `NO ACTION` / `RESTRICT`: Prevents the deletion/update of the parent record if child records exist. This is the default behavior.

**Syntax:**
```sql
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    user_id INTEGER,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL
);
```

## Enhanced `ALTER TABLE` Commands

The `ALTER TABLE` command is significantly more powerful than previously documented.

| Action | Description | Syntax |
|---|---|---|
| Rename Table | Renames a table. | `ALTER TABLE table_name RENAME TO new_table_name;` |
| Rename Column | Renames a column. | `ALTER TABLE table_name RENAME COLUMN old_name TO new_name;` |
| Add Constraint | Adds a new constraint to a table. | `ALTER TABLE table_name ADD CONSTRAINT constraint_name UNIQUE (column);` |
| Drop Constraint| Removes a constraint from a table. | `ALTER TABLE table_name DROP CONSTRAINT constraint_name;` |
| Set Default | Sets a default value for a column. | `ALTER TABLE table_name ALTER COLUMN column_name SET DEFAULT 'default_value';` |
| Drop Default | Removes the default value for a column. | `ALTER TABLE table_name ALTER COLUMN column_name DROP DEFAULT;` |
| Set Not Null | Adds a `NOT NULL` constraint to a column. | `ALTER TABLE table_name ALTER COLUMN column_name SET NOT NULL;` |
| Drop Not Null | Removes a `NOT NULL` constraint. | `ALTER TABLE table_name ALTER COLUMN column_name DROP NOT NULL;` |
| Change Type | Changes the data type of a column. | `ALTER TABLE table_name ALTER COLUMN column_name TYPE new_data_type;` |

## SQL `CREATE INDEX`

In addition to the Redis-style `IDX.CREATE` command, a standard SQL command is available to create indexes.

**Syntax:**
```sql
CREATE [UNIQUE] INDEX index_name ON table_name (column_name);
```
*   `UNIQUE`: Ensures that the indexed column contains only unique values.
*   This command creates an index that is functionally identical to one created with `IDX.CREATE` and can be managed with `IDX.DROP`.
