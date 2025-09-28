# SQL: Data Manipulation Language (DML)

DML commands are used to add, modify, and remove data from the database. These commands interact with the virtual schemas defined by DDL to provide type safety and validation.

## `INSERT`

Adds a new row (a new key-value pair) to a table.

### Syntax
```sql
INSERT INTO table_name [(column1, column2, ...)]
(VALUES (value1, value2, ...) | SELECT ...)
[ON CONFLICT (target_column) DO UPDATE SET ... | ON CONFLICT DO NOTHING]
[RETURNING column1, column2 | *];
```

### Behavior
- The `table_name` determines the key prefix.
- A primary key is required to construct the full database key. By default, MemFlux looks for an `id` column in the `INSERT` statement. The key is then constructed as `table_name:id`. If no `id` is provided, a new UUID is generated.
- If a virtual schema exists for the table:
    - The provided values are cast to the data type defined in the schema for each column. For example, inserting the string `'42'` into an `INTEGER` column will result in the number `42` being stored.
    - The operation will fail if a value cannot be cast (e.g., inserting `'hello'` into an `INTEGER` column).
- The entire new row is stored as a single JSON object.

### `ON CONFLICT` Clause

MemFlux supports an `ON CONFLICT` clause for `INSERT` statements to handle unique constraint violations, often referred to as an "upsert" operation.

**Syntax:**
```sql
INSERT INTO table_name (...) VALUES (...)
ON CONFLICT (column_name) DO UPDATE SET ...;

INSERT INTO table_name (...) VALUES (...)
ON CONFLICT (column_name) DO NOTHING;
```

**Behavior:**
- The `(column_name)` specifies the column (or columns) with a `UNIQUE` or `PRIMARY KEY` constraint that might conflict.
- **`DO NOTHING`**: If a conflict occurs, the `INSERT` operation is simply ignored.
- **`DO UPDATE`**: If a conflict occurs, an `UPDATE` operation is performed on the existing row instead.
    - In the `SET` clause of the `DO UPDATE` action, you can reference the values that were proposed for insertion using the special `excluded` table. For example, `SET name = excluded.name`.

### `RETURNING` Clause

The `RETURNING` clause can be used to return data from the row that was inserted or updated (in the case of `ON CONFLICT ... DO UPDATE`). You can return all columns with `*` or specify a list of columns.

### Examples
```sql
-- Assumes a 'users' table schema exists
INSERT INTO users (id, name, age, is_active)
VALUES ('user123', 'Alice', 30, true);
```
This creates a key `users:user123` with a JSON value `{"name":"Alice","age":30,"is_active":true}`.

```sql
-- Insert a new user, or update their city if they already exist, and return the result
INSERT INTO users (id, name, city) VALUES ('user123', 'Alice', 'SF')
ON CONFLICT (id) DO UPDATE SET city = excluded.city
RETURNING *;
```

## `UPDATE`

Modifies existing rows in a table.

### Syntax
```sql
UPDATE table_name
SET column1 = value1, column2 = value2, ...
[FROM from_list]
[WHERE condition]
[RETURNING column1, column2 | *];
```

### Behavior
- The `UPDATE` command finds all rows matching the `WHERE` clause (or all rows in the table if no `WHERE` is provided).
- For each matching row, it updates the fields specified in the `SET` clause.
- Like `INSERT`, the new values are cast to the types defined in the virtual schema, if one exists.
- The operation is atomic at the row level; the entire JSON document for a key is rewritten.

### `FROM` Clause
You can include additional tables in an `UPDATE` statement using the `FROM` clause. This allows you to reference columns from other tables in your `WHERE` clause and `SET` expressions, similar to a `JOIN`.

### `RETURNING` Clause

The `RETURNING` clause can be used to return data from the rows that were updated. You can return all columns with `*` or specify a list of columns.

### Example
```sql
-- Update the age of a user based on information from another table
UPDATE users
SET age = users_metadata.age
FROM users_metadata
WHERE users.id = users_metadata.user_id
RETURNING users.id, users.age;
```

## `DELETE`

Removes existing rows from a table.

### Syntax
```sql
DELETE FROM table_name
[USING using_list]
[WHERE condition]
[RETURNING column1, column2 | *];
```

### Behavior
- The `DELETE` command finds all rows matching the `WHERE` clause.
- If no `WHERE` clause is provided, **all rows in the table are deleted**. This is the same behavior as `TRUNCATE TABLE`.
- For each matching row, the underlying key is deleted from the database.

### `USING` Clause
You can include additional tables in a `DELETE` statement using the `USING` clause. This allows you to reference columns from other tables in your `WHERE` clause to determine which rows to delete.

### `RETURNING` Clause

The `RETURNING` clause can be used to return data from the rows that were deleted. You can return all columns with `*` or specify a list of columns.

### Example
```sql
-- Delete users who are marked for deletion in another table and return their names
DELETE FROM users
USING users_to_delete
WHERE users.id = users_to_delete.user_id
RETURNING users.name;
```
