# SQL: The `SELECT` Statement

The `SELECT` statement is the primary way to query data in MemFlux. It allows for retrieving data, filtering, joining, aggregating, and ordering results.

### Syntax
```sql
SELECT [column1, column2, ...] | *
FROM table_name
[JOIN other_table ON condition]
[WHERE filter_condition]
[GROUP BY column1, ...]
[ORDER BY column1 [ASC | DESC], ...]
[LIMIT count]
[OFFSET start]
[UNION | UNION ALL SELECT ...]
```

## Clauses

### `FROM table_name`
Specifies the primary table for the query. This corresponds to a key prefix (e.g., `FROM users` queries keys starting with `users:`).

### `SELECT` Clause
Specifies the columns (fields) to be returned.
- `SELECT *`: Returns all fields from the JSON object.
- `SELECT column1, column2`: Returns only the specified fields.
- **Aliases:** You can rename columns in the output using `AS`: `SELECT name AS user_name, age FROM users`.
- **Expressions:** You can use functions and expressions: `SELECT name, age * 2 AS doubled_age FROM users`.

### `WHERE` Clause
Filters the rows based on a condition.
- **Operators:** `=`, `!=`, `>`, `<`, `>=`, `<=`.
- **Pattern Matching:** `LIKE` (case-sensitive) and `ILIKE` (case-insensitive). The wildcards are `%` (matches any number of characters) and `_` (matches a single character). To match a literal wildcard character (like `%` or `_`), you can escape it with a backslash (e.g., `\%` matches a literal `%`). To match a literal backslash, use two backslashes (`\\`).
- **Logical Operators:** `AND`, `OR`.
- **Subqueries:** `IN (SELECT ...)` and `= (SELECT ...)` are supported.

**Example:**
```sql
SELECT name, age FROM users WHERE city = 'SF' AND age > 25;
```

### `JOIN` Clause
Combines rows from two or more tables based on a related column.
- **`INNER JOIN` (or `JOIN`):** Returns rows when there is a match in both tables.
- **`LEFT JOIN`:** Returns all rows from the left table, and the matched rows from the right table. If there is no match, the right side is `NULL`.
- **`RIGHT JOIN`:** Returns all rows from the right table, and the matched rows from the left table.
- **`FULL OUTER JOIN`:** Returns rows when there is a match in one of the tables.
- **`CROSS JOIN`:** Returns the Cartesian product of the two tables.

**Example:**
```sql
SELECT users.name, orders.product_id
FROM users
JOIN orders ON users.id = orders.user_id;
```

### `GROUP BY` Clause
Groups rows that have the same values in specified columns into summary rows. It is often used with aggregate functions.

**Example:**
```sql
SELECT city, COUNT(*) AS num_users, AVG(age) AS avg_age
FROM users
GROUP BY city;
```

### Aggregate Functions
- `COUNT(*)` or `COUNT(column)`: Counts the number of rows.
- `SUM(column)`: Calculates the sum of a numeric column.
- `AVG(column)`: Calculates the average of a numeric column.
- `MIN(column)`: Finds the minimum value in a column.
- `MAX(column)`: Finds the maximum value in a column.

### `ORDER BY` Clause
Sorts the result set in ascending or descending order.
- `ASC`: Ascending order (default).
- `DESC`: Descending order.
- **Type-Aware Sorting:** If a virtual schema is present, sorting will be performed based on the column's data type (e.g., `20` comes after `5` for an `INTEGER` column, even if they are stored as strings).
- **NULLs:** `NULL` values are always sorted first, regardless of `ASC` or `DESC` direction.

**Example:**
```sql
SELECT name, age FROM users ORDER BY age DESC, name ASC;
```

### `LIMIT` and `OFFSET` Clauses
Constrains the number of rows returned.
- `LIMIT count`: Returns a maximum of `count` rows.
- `OFFSET start`: Skips the first `start` rows before beginning to return rows.

**Example:**
```sql
-- Paginate results, showing 10 users per page. This is page 3.
SELECT name FROM users ORDER BY name LIMIT 10 OFFSET 20;
```

### `UNION` and `UNION ALL`
Combines the result sets of two or more `SELECT` statements.
- `UNION`: Removes duplicate rows from the combined result set.
- `UNION ALL`: Includes all rows, including duplicates.

**Example:**
```sql
SELECT name FROM table1
UNION ALL
SELECT name FROM table2;
```

### `CASE` Expressions
Allows for conditional logic within a `SELECT` statement.

**Example:**
```sql
SELECT name,
       CASE
           WHEN age < 30 THEN 'Young'
           WHEN age >= 30 AND age < 60 THEN 'Adult'
           ELSE 'Senior'
       END AS age_group
FROM users;
```
