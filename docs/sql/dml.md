# SQL: Data Manipulation Language (DML)

DML commands are used to add, modify, and remove data from the database. These commands interact with the virtual schemas defined by DDL to provide type safety and validation.

## `INSERT`

Adds a new row (a new key-value pair) to a table.

### Syntax
```sql
INSERT INTO table_name (column1, column2, ...)
VALUES (value1, value2, ...);
```

### Behavior
- The `table_name` determines the key prefix.
- A primary key is required to construct the full database key. By default, MemFlux looks for an `id` column in the `INSERT` statement. The key is then constructed as `table_name:id`. If no `id` is provided, a new UUID is generated.
- If a virtual schema exists for the table:
    - The provided values are cast to the data type defined in the schema for each column. For example, inserting the string `'42'` into an `INTEGER` column will result in the number `42` being stored.
    - The operation will fail if a value cannot be cast (e.g., inserting `'hello'` into an `INTEGER` column).
- The entire new row is stored as a single JSON object.

### Example
```sql
-- Assumes a 'users' table schema exists
INSERT INTO users (id, name, age, is_active)
VALUES ('user123', 'Alice', 30, true);
```
This creates a key `users:user123` with a JSON value `{"name":"Alice","age":30,"is_active":true}`.

## `UPDATE`

Modifies existing rows in a table.

### Syntax
```sql
UPDATE table_name
SET column1 = value1, column2 = value2, ...
[WHERE condition];
```

### Behavior
- The `UPDATE` command finds all rows matching the `WHERE` clause (or all rows in the table if no `WHERE` is provided).
- For each matching row, it updates the fields specified in the `SET` clause.
- Like `INSERT`, the new values are cast to the types defined in the virtual schema, if one exists.
- The operation is atomic at the row level; the entire JSON document for a key is rewritten.

### Example
```sql
-- Update Alice's age and deactivate her account
UPDATE users
SET age = 31, is_active = false
WHERE name = 'Alice';
```

## `DELETE`

Removes existing rows from a table.

### Syntax
```sql
DELETE FROM table_name [WHERE condition];
```

### Behavior
- The `DELETE` command finds all rows matching the `WHERE` clause.
- If no `WHERE` clause is provided, **all rows in the table are deleted**. This is the same behavior as `TRUNCATE TABLE`.
- For each matching row, the underlying key is deleted from the database.

### Example
```sql
-- Delete all inactive users
DELETE FROM users WHERE is_active = false;
```
