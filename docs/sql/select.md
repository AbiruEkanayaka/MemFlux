# SQL: The `SELECT` Statement

The `SELECT` statement is the primary way to query data in MemFlux. It allows for retrieving data, filtering, joining, aggregating, and ordering results.

### Syntax
```sql
[WITH [RECURSIVE] cte_name AS (subquery), ...]
SELECT [DISTINCT [ON (expression, ...)]] [column1, column2, ...] | *
FROM table_name | (subquery) [AS alias]
[JOIN other_table ON condition]
[WHERE filter_condition]
[GROUP BY column1, ...]
[HAVING having_condition]
[ORDER BY column1 [ASC | DESC], ...]
[LIMIT count]
[OFFSET start]
[UNION | UNION ALL | INTERSECT | EXCEPT SELECT ...]
```

## Clauses

### `WITH` Clause (Common Table Expressions)
The `WITH` clause allows you to define one or more temporary, named result sets, known as Common Table Expressions (CTEs), that can be referenced within the main `SELECT` statement. This is useful for breaking down complex queries into simpler, more readable logical steps.

**Non-Recursive CTE:**
```sql
WITH regional_sales AS (
    SELECT region, SUM(amount) AS total_sales
    FROM orders
    GROUP BY region
)
SELECT region, total_sales
FROM regional_sales
WHERE total_sales > 1000;
```

**Recursive CTE:**
A recursive CTE is defined with `WITH RECURSIVE` and must have a structure that includes a non-recursive "base" member, a `UNION` or `UNION ALL`, and a recursive member that references the CTE's own name. It is useful for querying hierarchical or graph-like data.

```sql
-- Example: Generate a series of numbers from 1 to 5
WITH RECURSIVE counter(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM counter WHERE n < 5
)
SELECT n FROM counter;
```

### `FROM` Clause
Specifies the primary data source for the query.
- **Table:** A table name, which corresponds to a key prefix (e.g., `FROM users` queries keys starting with `users:`).
- **Subquery:** A nested `SELECT` statement in parentheses. A subquery in the `FROM` clause must have an alias.

**Example:**
```sql
-- Query from a derived table
SELECT avg_age FROM (SELECT AVG(age) as avg_age FROM users) AS stats;
```

### `SELECT` Clause
Specifies the columns (fields) to be returned.
- `SELECT *`: Returns all fields from the JSON object.
- `SELECT column1, column2`: Returns only the specified fields.
- **Aliases:** You can rename columns in the output using `AS`: `SELECT name AS user_name, age FROM users`.
- **Expressions:** You can use functions and expressions, including arithmetic (`+`, `-`, `*`, `/`): `SELECT name, age * 2 AS doubled_age FROM users`.
- **Scalar Subqueries:** A subquery that returns a single row and a single column can be used as a value in the `SELECT` list.
- **`DISTINCT`:** Returns only unique rows.
- **`DISTINCT ON (expression, ...)`:** A PostgreSQL-compatible feature that returns the first row for each unique combination of the specified expressions. The "first" row is determined by the `ORDER BY` clause.

**Example:**
```sql
-- Get the youngest user from each city
SELECT DISTINCT ON (city) name, city, age
FROM users
ORDER BY city, age ASC;
```

### `WHERE` Clause
Filters rows based on a condition before any grouping.

| Operator | Description | Example |
|---|---|---|
| `=`, `!=`, `>`, `<`, `>=`, `<=` | Standard comparison operators. | `age > 30` |
| `AND`, `OR` | Logical operators to combine conditions. | `age > 30 AND city = 'SF'` |
| `LIKE`, `ILIKE` | Case-sensitive (`LIKE`) and case-insensitive (`ILIKE`) pattern matching. `%` matches any string, `_` matches any single character. | `name LIKE 'A%'` |
| `BETWEEN` | Checks if a value is within a range (inclusive). | `age BETWEEN 20 AND 30` |
| `IS NULL`, `IS NOT NULL` | Checks if a value is or is not `NULL`. | `email IS NOT NULL` |
| `IN` | Checks if a value is in a list of literals or a subquery. | `city IN ('SF', 'NYC')` or `id IN (SELECT user_id FROM active_sessions)` |
| `EXISTS` | Returns `true` if a subquery returns any rows. | `EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)` |
| `= ANY(...)`, `> ALL(...)` | Compares a value to the results of a subquery. `ANY` returns `true` if the comparison is true for at least one subquery value. `ALL` returns `true` if the comparison is true for all subquery values. | `age > ANY(SELECT age FROM admins)` |


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
Groups rows that have the same values in specified columns into summary rows. It is almost always used with aggregate functions.

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

### `HAVING` Clause
Filters the results of a `GROUP BY` clause. It is like a `WHERE` clause, but it operates on the results of aggregate functions.

**Example:**
```sql
-- Find cities with more than 10 users
SELECT city, COUNT(*)
FROM users
GROUP BY city
HAVING COUNT(*) > 10;
```

### `ORDER BY` Clause
Sorts the result set in ascending or descending order.
- `ASC`: Ascending order (default).
- `DESC`: Descending order.
- **NULLs:** `NULL` values are always sorted first, regardless of `ASC` or `DESC` direction.

**Example:**
```sql
SELECT name, age FROM users ORDER BY age DESC, name ASC;
```

### `LIMIT` and `OFFSET` Clauses
Constrains the number of rows returned, typically for pagination.
- `LIMIT count`: Returns a maximum of `count` rows.
- `OFFSET start`: Skips the first `start` rows before beginning to return rows.

**Example:**
```sql
-- Paginate results, showing 10 users per page. This is page 3.
SELECT name FROM users ORDER BY name LIMIT 10 OFFSET 20;
```

### Set Operators (`UNION`, `INTERSECT`, `EXCEPT`)
Combines the result sets of two or more `SELECT` statements.

- **`UNION`**: Combines results and removes duplicate rows.
- **`UNION ALL`**: Combines results and includes all rows, including duplicates.
- **`INTERSECT`**: Returns only the rows that appear in both result sets.
- **`EXCEPT`**: Returns rows from the first result set that do not appear in the second.

**Example:**
```sql
-- All users who are either in SF or have placed an order
SELECT name FROM users WHERE city = 'SF'
UNION
SELECT name FROM users JOIN orders ON users.id = orders.user_id;
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
