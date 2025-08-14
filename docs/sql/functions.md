# SQL: Built-in Functions

MemFlux provides a variety of built-in scalar functions that can be used in SQL expressions, primarily in the `SELECT` and `WHERE` clauses.

## String Functions

| Function | Description | Example |
|---|---|---|
| `LOWER(string)` | Converts a string to lowercase. | `LOWER('HELLO')` → `'hello'` |
| `UPPER(string)` | Converts a string to uppercase. | `UPPER('hello')` → `'HELLO'` |
| `LENGTH(string)` | Returns the number of characters in a string. | `LENGTH('hello')` → `5` |
| `TRIM(string)` | Removes leading and trailing whitespace. | `TRIM('  hello  ')` → `'hello'` |
| `SUBSTRING(string, start, [length])` | Extracts a substring. `start` is 1-based. If `length` is omitted, it returns the rest of the string. | `SUBSTRING('hello', 2, 3)` → `'ell'` |

## Numeric Functions

| Function | Description | Example |
|---|---|---|
| `ABS(number)` | Returns the absolute value of a number. | `ABS(-10.5)` → `10.5` |
| `ROUND(number)` | Rounds a number to the nearest integer. | `ROUND(10.5)` → `11` |
| `CEIL(number)` | Rounds a number up to the nearest integer. | `CEIL(10.2)` → `11` |
| `FLOOR(number)` | Rounds a number down to the nearest integer. | `FLOOR(10.8)` → `10` |

## Date/Time Functions

| Function | Description | Example |
|---|---|---|
| `NOW()` | Returns the current server date and time as an RFC 3339 string. | `NOW()` → `'2025-08-14T12:34:56Z'` |
| `DATE_PART(part, timestamp)` | Extracts a part from a timestamp string. Supported parts: `year`, `month`, `day`, `hour`, `minute`, `second`, `dow` (day of week), `doy` (day of year). | `DATE_PART('year', '2025-08-14T10:00:00Z')` → `2025` |

## Type Casting Functions

| Function | Description | Example |
|---|---|---|
| `CAST(expression AS data_type)` | Converts an expression to a different data type. Supported types: `INTEGER`, `TEXT`, `BOOLEAN`, `TIMESTAMP`. | `CAST('123' AS INTEGER)` → `123` |
