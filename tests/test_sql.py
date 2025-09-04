from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_sql(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    print("== Comprehensive SQL Test Suite ==")

    # --- Helper Functions ---
    def send_and_parse(cmd_list, description):
        """Sends a command, parses the response, and returns a list of JSON objects."""
        resp = send(cmd_list)
        # The raw response from send might contain multiple bulk strings
        # which are separated by \r\n, and the `send` function decodes it.
        # So `resp` here is a string. `read_resp_response` returns bytes.
        # We need to re-encode to bytes to use `extract_json_from_bulk`.
        resp_bytes = resp.encode('utf-8')

        lines = resp_bytes.split(b'\r\n')
        if not lines or not lines[0].startswith(b'*'):
            # It might be a single-line response like +OK or -ERR
            if resp.startswith(("+-", ":")):
                return resp # Return the simple string directly
            print(f"[WARN] Unexpected response for '{description}': {resp!r}")
            return []
        
        results = []
        # Each result is 2 lines: $len and the json payload
        # Skip the first line (array count)
        i = 1
        while i < len(lines):
            if lines[i].startswith(b'$'):
                try:
                    # Reconstruct the bulk string part for extraction
                    bulk_string = lines[i] + b'\r\n' + lines[i+1] + b'\r\n'
                    row_json = extract_json_from_bulk(bulk_string)
                    if row_json:
                        results.append(row_json)
                    i += 2 # Move to the next element
                except IndexError: # In case the last element is incomplete
                    print(f"[WARN] Incomplete bulk string at end of response for '{description}'")
                    break
            else:
                # This can happen with empty arrays, where lines[i] is empty
                if lines[i]:
                    print(f"[WARN] Unexpected line type in response for '{description}': {lines[i]!r}")
                break
        print(f"[INFO] {description}: {len(results)} rows")
        return results

    def get_column(results, column_name):
        """Extracts a specific column from a list of result rows, preserving None."""
        return [row.get(column_name) for row in results]

    # --- 1. Setup: Clean & Load Data ---
    print("\n-- Phase 1: Data Setup --")
    send(["FLUSHDB"]) # Ensure a clean state for schemas and indexes

    # Create schemas for tables used in SQL tests
    assert_eq(send(["SQL", "CREATE", "TABLE", "user", "(id INTEGER, name TEXT, age INTEGER, city TEXT, dept_id TEXT)"]), "+OK", "CREATE TABLE user")
    assert_eq(send(["SQL", "CREATE", "TABLE", "products", "(id INTEGER, name TEXT, price INTEGER)"]), "+OK", "CREATE TABLE products")
    assert_eq(send(["SQL", "CREATE", "TABLE", "orders", "(id INTEGER, user_id TEXT, product_id TEXT, amount INTEGER)"]), "+OK", "CREATE TABLE orders")
    assert_eq(send(["SQL", "CREATE", "TABLE", "departments", "(id INTEGER, name TEXT)"]), "+OK", "CREATE TABLE departments") # For advanced join tests

    # Clean up all potential keys
    for i in range(1, 11):
        send(["DELETE", f"user:{i}"])
        send(["DELETE", f"orders:{i}"])
        send(["DELETE", f"products:{i}"])
        send(["DELETE", f"departments:{i}"]) # Clean up departments keys

    # Insert users: name, age, city
    assert_eq(send(["JSON.SET", "user:1", '{"name":"Alice","age":30,"city":"SF"}']), "+OK", "Insert user:1")
    assert_eq(send(["JSON.SET", "user:2", '{"name":"Bob","age":25,"city":"SF"}']), "+OK", "Insert user:2")
    assert_eq(send(["JSON.SET", "user:3", '{"name":"Carol","age":30,"city":"NY"}']), "+OK", "Insert user:3")
    assert_eq(send(["JSON.SET", "user:4", '{"name":"David","age":25,"city":"NY"}']), "+OK", "Insert user:4")
    assert_eq(send(["JSON.SET", "user:5", '{"name":"Carol","age":35,"city":"SF"}']), "+OK", "Insert user:5")

    # Insert products: name, price
    assert_eq(send(["JSON.SET", "products:1", '{"name":"Laptop","price":1200}']), "+OK", "Insert product:1")
    assert_eq(send(["JSON.SET", "products:2", '{"name":"Mouse","price":25}']), "+OK", "Insert product:2")
    assert_eq(send(["JSON.SET", "products:3", '{"name":"Keyboard","price":75}']), "+OK", "Insert product:3")

    # Insert orders: user_id, product_id, amount
    assert_eq(send(["JSON.SET", "orders:1", '{"user_id":"1","product_id":"1","amount":1200}']), "+OK", "Insert order:1")
    assert_eq(send(["JSON.SET", "orders:2", '{"user_id":"1","product_id":"2","amount":25}']), "+OK", "Insert order:2")
    assert_eq(send(["JSON.SET", "orders:3", '{"user_id":"2","product_id":"3","amount":75}']), "+OK", "Insert order:3")
    assert_eq(send(["JSON.SET", "orders:4", '{"user_id":"3","product_id":"1","amount":1200}']), "+OK", "Insert order:4")
    assert_eq(send(["JSON.SET", "orders:5", '{"user_id":"4","product_id":"2","amount":50}']), "+OK", "Insert order:5") # 2 mice
    assert_eq(send(["JSON.SET", "orders:6", '{"user_id":"5","product_id":"3","amount":75}']), "+OK", "Insert order:6")

    # Create indexes
    assert_eq(send(["CREATEINDEX", "user:*", "ON", "age"]), "+OK", "Create index on user.age")
    assert_eq(send(["CREATEINDEX", "user:*", "ON", "city"]), "+OK", "Create index on user.city")
    print("[PASS] Data setup complete")

    # --- 2. Basic SELECT and WHERE ---
    print("\n-- Phase 2: SELECT and WHERE Clause Tests --")
    results = send_and_parse(["SQL", "SELECT", "*", "FROM", "user", "WHERE", "age", "=", "30"], "SELECT * WHERE age=30")
    assert_eq(len(results), 2, "Count for age=30")
    assert_eq(sorted(get_column(results, 'name')), ["Alice", "Carol"], "Names for age=30")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "WHERE", "age", "=", "25", "AND", "city", "=", "'NY'"], "SELECT with AND")
    assert_eq(get_column(results, 'name'), ["David"], "Name for age=25 AND city='NY'")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "WHERE", "age", "=", "35", "OR", "city", "=", "'NY'"], "SELECT with OR")
    assert_eq(sorted(get_column(results, 'name')), ["Carol", "Carol", "David"], "Names for age=35 OR city='NY'")
    
    results = send_and_parse(["SQL", "SELECT", "id", "FROM", "user", "WHERE", "name", "=", "'Alice'"], "SELECT id WHERE name='Alice'")
    assert_eq(get_column(results, 'id'), ["1"], "ID for name='Alice'")

    results = send_and_parse(["SQL", "SELECT", "*", "FROM", "user", "WHERE", "age", "=", "99"], "SELECT with no results")
    assert_eq(len(results), 0, "Count for age=99 (no match)")
    print("[PASS] SELECT and WHERE tests complete")

    # --- 2b. LIKE and ILIKE ---
    print("\n-- Phase 2b: LIKE and ILIKE Clause Tests --")
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "WHERE", "name", "LIKE", "'A%'"], "LIKE with trailing wildcard")
    assert_eq(get_column(results, 'name'), ["Alice"], "Name for LIKE 'A%'")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "WHERE", "name", "LIKE", "'%d'"], "LIKE with leading wildcard")
    assert_eq(get_column(results, 'name'), ["David"], "Name for LIKE '%d'")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "WHERE", "name", "LIKE", "'%aro%'"], "LIKE with surrounding wildcard")
    assert_eq(sorted(get_column(results, 'name')), ["Carol", "Carol"], "Names for LIKE '%aro%'" )

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "WHERE", "name", "LIKE", "'B_b'"], "LIKE with single char wildcard")
    assert_eq(get_column(results, 'name'), ["Bob"], "Name for LIKE 'B_b'")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "WHERE", "name", "ILIKE", "'a%'"], "ILIKE case-insensitive")
    assert_eq(get_column(results, 'name'), ["Alice"], "Name for ILIKE 'a%'" )
    
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "WHERE", "name", "ILIKE", "'C%'"], "ILIKE case-insensitive multiple")
    assert_eq(sorted(get_column(results, 'name')), ["Carol", "Carol"], "Names for ILIKE 'C%'" )

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "WHERE", "name", "LIKE", "'Xyz%'"], "LIKE with no matches")
    assert_eq(len(results), 0, "Count for LIKE 'Xyz%' (no match)")
    print("[PASS] LIKE and ILIKE tests complete")

    # --- 3. ORDER BY ---
    print("\n-- Phase 3: ORDER BY Clause Tests --")
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "ORDER", "BY", "name", "ASC"], "ORDER BY name ASC")
    assert_eq(get_column(results, 'name'), ["Alice", "Bob", "Carol", "Carol", "David"], "Names ordered ASC")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "ORDER", "BY", "age", "DESC,", "name", "ASC"], "ORDER BY age DESC, name ASC")
    assert_eq(get_column(results, 'name'), ["Carol", "Alice", "Carol", "Bob", "David"], "Names ordered by age DESC, then name ASC")
    print("[PASS] ORDER BY tests complete")

    # --- 4. GROUP BY ---
    print("\n-- Phase 4: GROUP BY Clause Tests --")
    results = send_and_parse(["SQL", "SELECT", "city", "FROM", "user", "GROUP", "BY", "city"], "GROUP BY city")
    assert_eq(len(results), 2, "Count of cities")
    assert_eq(sorted(get_column(results, 'city')), ["NY", "SF"], "Distinct cities")

    results = send_and_parse(["SQL", "SELECT", "age", "FROM", "user", "GROUP", "BY", "age"], "GROUP BY age")
    assert_eq(len(results), 3, "Count of age groups")
    assert_eq(sorted(get_column(results, 'age')), [25, 30, 35], "Distinct ages")
    print("[PASS] GROUP BY tests complete")

    # --- 5. JOIN ---
    print("\n-- Phase 5: JOIN Clause Tests --")
    # This test now uses INNER JOIN explicitly for clarity
    results = send_and_parse([
        "SQL", "SELECT", "name,amount", "FROM", "user", "INNER", "JOIN", "orders", "ON", "user.id=orders.user_id"
    ], "Two-table INNER JOIN user/orders")
    assert_eq(len(results), 6, "Row count for user/orders INNER JOIN")
    # Check a few values to be sure
    names = sorted(get_column(results, 'name'))
    amounts = sorted(get_column(results, 'amount'))
    assert_eq(names, ["Alice", "Alice", "Bob", "Carol", "Carol", "David"], "Names from user/orders INNER JOIN")
    assert_eq(amounts, [25, 50, 75, 75, 1200, 1200], "Amounts from user/orders INNER JOIN")
    print("[PASS] INNER JOIN tests complete")

    # --- 5b. Advanced JOIN Tests (LEFT, RIGHT, FULL, CROSS) ---
    print("\n-- Phase 5b: Advanced JOIN Tests --")
    # Setup for advanced joins:
    # - Add a user with no orders (user:6, Eve)
    # - Add a department table
    # - Assign some users to departments, leave one user unassigned (David)
    # - Have a department with no users (HR)
    send(["DELETE", "user:6"])
    send(["DELETE", "departments:1"])
    send(["DELETE", "departments:2"])
    send(["DELETE", "departments:3"])
    assert_eq(send(["JSON.SET", "user:6", '{"name":"Eve","age":40,"city":"LA", "dept_id": null}']), "+OK", "Insert user:6 (no orders)")
    assert_eq(send(["JSON.SET", "user:1", '{"name":"Alice","age":30,"city":"SF", "dept_id": "1"}']), "+OK", "Update user:1 with dept_id")
    assert_eq(send(["JSON.SET", "user:2", '{"name":"Bob","age":25,"city":"SF", "dept_id": "1"}']), "+OK", "Update user:2 with dept_id")
    assert_eq(send(["JSON.SET", "user:3", '{"name":"Carol","age":30,"city":"NY", "dept_id": "2"}']), "+OK", "Update user:3 with dept_id")
    # user 4 (David) and 5 (Carol) have no dept_id
    
    assert_eq(send(["JSON.SET", "departments:1", '{"name":"Engineering"}']), "+OK", "Insert dept:1")
    assert_eq(send(["JSON.SET", "departments:2", '{"name":"Sales"}']), "+OK", "Insert dept:2")
    assert_eq(send(["JSON.SET", "departments:3", '{"name":"HR"}']), "+OK", "Insert dept:3 (no users)")

    # LEFT JOIN: All users, with their department if it exists
    results = send_and_parse([
        "SQL", "SELECT", "user.name, departments.name", "FROM", "user", "LEFT", "JOIN", "departments", "ON", "user.dept_id = departments.id", "ORDER", "BY", "user.name", "ASC", ",", "user.id", "ASC"
    ], "LEFT JOIN user/departments")
    assert_eq(len(results), 6, "Row count for LEFT JOIN")
    # Expected: Alice, Bob, Carol, Carol, David, Eve
    # David, Carol(2), Eve should have null department name
    user_names = get_column(results, 'user.name')
    dept_names = get_column(results, 'departments.name')
    assert_eq(user_names, ["Alice", "Bob", "Carol", "Carol", "David", "Eve"], "User names from LEFT JOIN")
    assert_eq(dept_names, ["Engineering", "Engineering", "Sales", None, None, None], "Department names from LEFT JOIN")

    # RIGHT JOIN: All departments, with their users if they exist
    results = send_and_parse([
        "SQL", "SELECT", "user.name, departments.name", "FROM", "user", "RIGHT", "JOIN", "departments", "ON", "user.dept_id = departments.id", "ORDER", "BY", "departments.name"
    ], "RIGHT JOIN user/departments")
    assert_eq(len(results), 4, "Row count for RIGHT JOIN")
    # Expected: Engineering (Alice), Engineering (Bob), HR (null), Sales (Carol)
    user_names = get_column(results, 'user.name')
    dept_names = get_column(results, 'departments.name')
    assert_eq(sorted([d for d in dept_names if d is not None]), ["Engineering", "Engineering", "HR", "Sales"], "Department names from RIGHT JOIN")
    assert_eq(user_names.count(None), 1, "Null user count for RIGHT JOIN (HR)")

    # FULL OUTER JOIN: All users and all departments
    results = send_and_parse([
        "SQL", "SELECT", "user.name, departments.name", "FROM", "user", "FULL", "OUTER", "JOIN", "departments", "ON", "user.dept_id = departments.id"
    ], "FULL OUTER JOIN user/departments")
    # 6 users + 1 unmapped dept (HR) = 7 rows
    assert_eq(len(results), 7, "Row count for FULL OUTER JOIN")
    user_names = get_column(results, 'user.name')
    dept_names = get_column(results, 'departments.name')
    assert_eq(user_names.count(None), 1, "Null user count for FULL JOIN (HR)")
    assert_eq(dept_names.count(None), 3, "Null department count for FULL JOIN (David, Carol, Eve)")

    # CROSS JOIN: Cartesian product
    results = send_and_parse([
        "SQL", "SELECT", "*", "FROM", "products", "CROSS", "JOIN", "departments"
    ], "CROSS JOIN products/departments")
    # 3 products * 3 departments = 9 rows
    assert_eq(len(results), 9, "Row count for CROSS JOIN")
    print("[PASS] Advanced JOIN tests complete")
    
    # Cleanup new data
    send(["DELETE", "user:6"])
    send(["DELETE", "departments:1"])
    send(["DELETE", "departments:2"])
    send(["DELETE", "departments:3"])
    # Reset users that were modified
    assert_eq(send(["JSON.SET", "user:1", '{"name":"Alice","age":30,"city":"SF"}']), "+OK", "Reset user:1")
    assert_eq(send(["JSON.SET", "user:2", '{"name":"Bob","age":25,"city":"SF"}']), "+OK", "Reset user:2")
    assert_eq(send(["JSON.SET", "user:3", '{"name":"Carol","age":30,"city":"NY"}']), "+OK", "Reset user:3")

    # --- 6. Complex Query ---
    print("\n-- Phase 6: Complex Query Test --")
    # Goal: Find the names of all users from SF who bought a Laptop, ordered by age.
    results = send_and_parse([
        "SQL",
        "SELECT", "user.name, user.age, products.name",
        "FROM", "user",
        "JOIN", "orders", "ON", "user.id = orders.user_id",
        "JOIN", "products", "ON", "orders.product_id = products.id",
        "WHERE", "user.city = 'SF'", "AND", "products.name = 'Laptop'",
        "ORDER", "BY", "user.age", "ASC"
    ], "Complex 3-table JOIN with WHERE and ORDER BY")
    
    assert_eq(len(results), 1, "Complex query row count")
    assert_eq(get_column(results, 'user.name'), ["Alice"], "Name from complex query")
    print("[PASS] Complex query test complete")

    # --- 7. Aggregate Functions ---
    print("\n-- Phase 7: Aggregate Function Tests --")

    # Global aggregates (no GROUP BY)
    results = send_and_parse(["SQL", "SELECT", "COUNT(*), SUM(age), AVG(age), MIN(age), MAX(age)", "FROM", "user"], "Global aggregates on user table")
    assert_eq(len(results), 1, "Global aggregate row count")
    if results:
        agg_row = results[0]
        assert_eq(agg_row.get('COUNT(*)'), 5, "COUNT(*) on users")
        assert_eq(agg_row.get('SUM(age)'), 145.0, "SUM(age) on users")
        assert_eq(agg_row.get('AVG(age)'), 29.0, "AVG(age) on users")
        assert_eq(agg_row.get('MIN(age)'), 25.0, "MIN(age) on users")
        assert_eq(agg_row.get('MAX(age)'), 35.0, "MAX(age) on users")

    # Aggregates with GROUP BY
    results = send_and_parse(["SQL", "SELECT", "city, COUNT(*), AVG(age)", "FROM", "user", "GROUP", "BY", "city", "ORDER", "BY", "city"], "Aggregates with GROUP BY city")
    assert_eq(len(results), 2, "GROUP BY city row count")
    
    if len(results) == 2:
        ny_row = results[0]
        sf_row = results[1]
        
        # Assuming order is NY then SF because of ORDER BY city
        assert_eq(ny_row.get('city'), 'NY', "City for first group")
        assert_eq(ny_row.get('COUNT(*)'), 2, "COUNT for NY")
        assert_eq(ny_row.get('AVG(age)'), 27.5, "AVG(age) for NY") # (30+25)/2

        assert_eq(sf_row.get('city'), 'SF', "City for second group")
        assert_eq(sf_row.get('COUNT(*)'), 3, "COUNT for SF")
        # (30+25+35)/3 = 90/3 = 30
        assert_eq(sf_row.get('AVG(age)'), 30.0, "AVG(age) for SF")

    # Test SUM on order amounts
    results = send_and_parse(["SQL", "SELECT", "user_id, SUM(amount)", "FROM", "orders", "GROUP", "BY", "user_id", "ORDER", "BY", "user_id"], "SUM(amount) GROUP BY user_id")
    assert_eq(len(results), 5, "GROUP BY user_id row count")
    # user:1 -> 1200 + 25 = 1225
    # user:2 -> 75
    # user:3 -> 1200
    # user:4 -> 50
    # user:5 -> 75
    expected_sums = {'1': 1225.0, '2': 75.0, '3': 1200.0, '4': 50.0, '5': 75.0}
    for row in results:
        user_id = row.get('user_id')
        actual_sum = row.get('SUM(amount)')
        assert_eq(actual_sum, expected_sums.get(user_id), f"SUM(amount) for user_id={user_id}")

    print("[PASS] Aggregate function tests complete")

    # --- 8. LIMIT and OFFSET ---
    print("\n-- Phase 8: LIMIT and OFFSET Clause Tests --")
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "ORDER", "BY", "name", "ASC", "LIMIT", "2"], "LIMIT 2")
    assert_eq(get_column(results, 'name'), ["Alice", "Bob"], "Names with LIMIT 2")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "ORDER", "BY", "name", "ASC", "OFFSET", "3"], "OFFSET 3")
    assert_eq(get_column(results, 'name'), ["Carol", "David"], "Names with OFFSET 3")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "ORDER", "BY", "name", "ASC", "LIMIT", "2", "OFFSET", "1"], "LIMIT 2 OFFSET 1")
    assert_eq(get_column(results, 'name'), ["Bob", "Carol"], "Names with LIMIT 2 OFFSET 1")
    
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "LIMIT", "10"], "LIMIT 10 (more than rows)")
    assert_eq(len(results), 5, "LIMIT 10 returns all 5 rows")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "ORDER", "BY", "name", "ASC", "LIMIT", "2", "OFFSET", "4"], "LIMIT 2 OFFSET 4")
    assert_eq(get_column(results, 'name'), ["David"], "Names with LIMIT 2 OFFSET 4 (1 result left)")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "user", "LIMIT", "0"], "LIMIT 0")
    assert_eq(len(results), 0, "LIMIT 0 returns no results")
    print("[PASS] LIMIT and OFFSET tests complete")

    # --- 9. DML Operations (INSERT, UPDATE, DELETE) ---
    print("\n-- Phase 9: DML Operations (INSERT, UPDATE, DELETE) --")

    # Test INSERT
    print("[INFO] Testing INSERT...")
    resp = send(["SQL", "INSERT", "INTO", "user", "(id, name, age, city)", "VALUES", "('99', 'Frank', 40, 'LA')"])
    assert_eq(resp, ":1", "INSERT new user 'Frank'")
    results = send_and_parse(["SQL", "SELECT", "name, age", "FROM", "user", "WHERE", "id", "=", "99"], "SELECT new user 'Frank'")
    assert_eq(len(results), 1, "Verify INSERT count")
    if results:
        assert_eq(results[0].get('name'), "Frank", "Verify INSERT name")
        assert_eq(results[0].get('age'), 40, "Verify INSERT age")

    # Test UPDATE
    print("[INFO] Testing UPDATE...")
    resp = send(["SQL", "UPDATE", "user", "SET", "city", "=", "'Los Angeles'", "WHERE", "name", "=", "'Frank'"])
    assert_eq(resp, ":1", "UPDATE single row")
    results = send_and_parse(["SQL", "SELECT", "city", "FROM", "user", "WHERE", "id", "=", "'99'"], "SELECT updated user 'Frank'")
    assert_eq(get_column(results, 'city'), ["Los Angeles"], "Verify UPDATE single row")

    resp = send(["SQL", "UPDATE", "user", "SET", "age", "=", "31", "WHERE", "city", "=", "'NY'"])
    assert_eq(resp, ":2", "UPDATE multiple rows")
    results = send_and_parse(["SQL", "SELECT", "age", "FROM", "user", "WHERE", "city", "=", "'NY'"], "SELECT updated 'NY' users")
    assert_eq(sorted(get_column(results, 'age')), [31, 31], "Verify UPDATE multiple rows")

    # Test DELETE
    print("[INFO] Testing DELETE...")
    resp = send(["SQL", "DELETE", "FROM", "user", "WHERE", "name", "=", "'Frank'"])
    assert_eq(resp, ":1", "DELETE single row")
    results = send_and_parse(["SQL", "SELECT", "*", "FROM", "user", "WHERE", "id", "=", "'99'"], "Verify user 'Frank' is deleted")
    assert_eq(len(results), 0, "Verify DELETE single row")

    resp = send(["SQL", "DELETE", "FROM", "user", "WHERE", "city", "=", "'NY'"])
    assert_eq(resp, ":2", "DELETE multiple rows")
    results = send_and_parse(["SQL", "SELECT", "*", "FROM", "user", "WHERE", "city", "=", "'NY'"], "Verify 'NY' users are deleted")
    assert_eq(len(results), 0, "Verify DELETE multiple rows")
    
    print("[PASS] DML tests complete")

    # Restore users deleted during DML test to ensure test suite consistency
    print("\n[INFO] Restoring users from NY deleted during DML test...")
    assert_eq(send(["JSON.SET", "user:3", '{"name":"Carol","age":30,"city":"NY"}']), "+OK", "Restore user:3")
    assert_eq(send(["JSON.SET", "user:4", '{"name":"David","age":25,"city":"NY"}']), "+OK", "Restore user:4")

    # --- 10. DDL Operations (CREATE TABLE) ---
    print("\n-- Phase 10: DDL Operations --")

    # Test CREATE TABLE
    print("[INFO] Testing CREATE TABLE...")
    # Clean up schema if it exists from a previous failed run
    send(["DELETE", "_internal:schemas:test_table"])

    resp = send(["SQL", "CREATE", "TABLE", "test_table", "(id", "INTEGER,", "name", "TEXT)"])
    assert_eq(resp, "+OK", "CREATE TABLE test_table")

    # Verify the schema was created in the internal storage
    schema_resp = send(["GET", "_internal:schemas:test_table"])
    # The response is a bulk string. We need to parse it.
    schema_json = extract_json_from_bulk(schema_resp.encode('utf-8'))

    assert_eq(schema_json is not None, True, "Schema should be stored as JSON")
    if schema_json:
        assert_eq(schema_json.get('table_name'), 'test_table', "Verify schema table_name")
        assert_eq('columns' in schema_json, True, "Verify schema has 'columns'")
        if 'columns' in schema_json:
            assert_eq(schema_json['columns'].get('id', {}).get('type'), 'INTEGER', "Verify id column type")
            assert_eq(schema_json['columns'].get('name', {}).get('type'), 'TEXT', "Verify name column type")

    # Test creating a table that already exists
    resp = send(["SQL", "CREATE", "TABLE", "test_table", "(id", "INTEGER)"])
    assert_eq(resp.startswith("-ERR"), True, "CREATE TABLE on existing table should fail")
    if not resp.startswith("-ERR"):
         print(f"  NOTE: Expected error, but got {resp!r}")

    # Test DROP TABLE
    print("[INFO] Testing DROP TABLE...")
    # Data for the table to be dropped
    assert_eq(send(["JSON.SET", "drop_test:1", '{"value":100}']), "+OK", "Set data for drop_test")
    
    # Create the table
    resp = send(["SQL", "CREATE", "TABLE", "drop_test", "(value INTEGER)"])
    assert_eq(resp, "+OK", "CREATE TABLE drop_test")
    
    # Verify schema exists
    schema_resp = send(["GET", "_internal:schemas:drop_test"])
    assert_eq(schema_resp.startswith("$"), True, "Schema for drop_test should exist before drop")

    # Drop the table
    resp = send(["SQL", "DROP", "TABLE", "drop_test"])
    assert_eq(resp, "+OK", "DROP TABLE drop_test")

    # Verify schema is gone
    schema_resp = send(["GET", "_internal:schemas:drop_test"])
    assert_eq(schema_resp, "$-1", "Schema for drop_test should NOT exist after drop")

    # Verify data is NOT gone
    data_resp = send(["JSON.GET", "drop_test:1"])
    assert_eq(data_resp, "$13\r\n{\"value\":100}", "Data for drop_test should still exist after drop")

    # Test dropping a non-existent table
    resp = send(["SQL", "DROP", "TABLE", "non_existent_table"])
    assert_eq(resp.startswith("-ERR"), True, "DROP TABLE on non-existent table should fail")

    # Clean up the data
    assert_eq(send(["DELETE", "drop_test:1"]), ":1", "Clean up drop_test data")


    # Clean up the created schema
    assert_eq(send(["DELETE", "_internal:schemas:test_table"]), ":1", "Clean up test_table schema")

    print("[PASS] DDL (CREATE/DROP) tests complete")

    # --- 11. DDL Operations (ALTER TABLE) ---
    print("\n-- Phase 11: ALTER TABLE Operations --")

    # Setup for ALTER
    print("[INFO] Testing ALTER TABLE...")
    send(["DELETE", "_internal:schemas:alter_test"])
    send(["DELETE", "alter_test:1"])
    send(["DELETE", "alter_test:2"])
    resp = send(["SQL", "CREATE", "TABLE", "alter_test", "(id INTEGER, name TEXT)"])
    assert_eq(resp, "+OK", "CREATE TABLE alter_test")
    assert_eq(send(["SQL", "INSERT", "INTO", "alter_test", "(id, name)", "VALUES", "('1', 'initial')"]), ":1", "Insert initial data for alter_test")

    # Test ADD COLUMN
    print("[INFO] Testing ALTER TABLE ADD COLUMN...")
    resp = send(["SQL", "ALTER", "TABLE", "alter_test", "ADD", "COLUMN", "status", "TEXT"])
    assert_eq(resp, "+OK", "ALTER TABLE alter_test ADD COLUMN status")

    # Verify schema update
    schema_resp = send(["GET", "_internal:schemas:alter_test"])
    schema_json = extract_json_from_bulk(schema_resp.encode('utf-8'))
    assert_eq('status' in schema_json.get('columns', {}), True, "Verify 'status' column in schema")
    if schema_json:
        assert_eq(schema_json.get('columns', {}).get('status', {}).get('type'), 'TEXT', "Verify 'status' column type")

    # Test inserting with new column
    assert_eq(send(["SQL", "INSERT", "INTO", "alter_test", "(id, name, status)", "VALUES", "('2', 'with_status', 'active')"]), ":1", "Insert data with new column")
    results = send_and_parse(["SQL", "SELECT", "status", "FROM", "alter_test", "WHERE", "id", "=", "'2'"], "SELECT new column")
    assert_eq(get_column(results, 'status'), ['active'], "Verify new column data")

    # Verify old data is still there (and new column is null)
    results = send_and_parse(["SQL", "SELECT", "name, status", "FROM", "alter_test", "WHERE", "id", "=", "'1'"], "SELECT old data after ADD COLUMN")
    assert_eq(get_column(results, 'name'), ['initial'], "Verify old data name")
    assert_eq(get_column(results, 'status'), [None], "Verify new column is null for old data")

    # Test DROP COLUMN
    print("[INFO] Testing ALTER TABLE DROP COLUMN...")
    resp = send(["SQL", "ALTER", "TABLE", "alter_test", "DROP", "COLUMN", "name"])
    assert_eq(resp, "+OK", "ALTER TABLE alter_test DROP COLUMN name")

    # Verify schema update
    schema_resp = send(["GET", "_internal:schemas:alter_test"])
    schema_json = extract_json_from_bulk(schema_resp.encode('utf-8'))
    assert_eq('name' in schema_json.get('columns', {}), False, "Verify 'name' column is removed from schema")

    # Test error cases
    print("[INFO] Testing ALTER TABLE error cases...")
    resp = send(["SQL", "ALTER", "TABLE", "non_existent_table", "ADD", "COLUMN", "foo", "TEXT"])
    assert_eq(resp.startswith("-ERR"), True, "ALTER TABLE on non-existent table should fail")

    resp = send(["SQL", "ALTER", "TABLE", "alter_test", "ADD", "COLUMN", "id", "INTEGER"])
    assert_eq(resp.startswith("-ERR"), True, "ALTER TABLE ADD existing column should fail")

    resp = send(["SQL", "ALTER", "TABLE", "alter_test", "DROP", "COLUMN", "non_existent_col"])
    assert_eq(resp.startswith("-ERR"), True, "ALTER TABLE DROP non-existent column should fail")

    # Cleanup
    assert_eq(send(["DELETE", "_internal:schemas:alter_test"]), ":1", "Clean up alter_test schema")
    assert_eq(send(["DELETE", "alter_test:1"]), ":1", "Clean up alter_test:1 data")
    assert_eq(send(["DELETE", "alter_test:2"]), ":1", "Clean up alter_test:2 data")
    print("[PASS] ALTER TABLE tests complete")

    # --- 12. DDL Operations (TRUNCATE TABLE) ---
    print("\n-- Phase 12: TRUNCATE TABLE Operations --")

    # Setup for TRUNCATE
    print("[INFO] Testing TRUNCATE TABLE...")
    send(["DELETE", "_internal:schemas:truncate_test"])
    send(["DELETE", "truncate_test:1"])
    send(["DELETE", "truncate_test:2"])
    resp = send(["SQL", "CREATE", "TABLE", "truncate_test", "(id INTEGER, name TEXT)"])
    assert_eq(resp, "+OK", "CREATE TABLE truncate_test")
    assert_eq(send(["SQL", "INSERT", "INTO", "truncate_test", "(id, name)", "VALUES", "('1', 'one')"]), ":1", "Insert data for truncate_test 1")
    assert_eq(send(["SQL", "INSERT", "INTO", "truncate_test", "(id, name)", "VALUES", "('2', 'two')"]), ":1", "Insert data for truncate_test 2")

    # Verify data exists before truncate
    results = send_and_parse(["SQL", "SELECT", "*", "FROM", "truncate_test"], "SELECT from truncate_test before truncate")
    assert_eq(len(results), 2, "Verify 2 rows exist before truncate")

    # Truncate the table
    resp = send(["SQL", "TRUNCATE", "TABLE", "truncate_test"])
    assert_eq(resp, ":2", "TRUNCATE TABLE truncate_test should delete 2 rows")

    # Verify data is gone
    results = send_and_parse(["SQL", "SELECT", "*", "FROM", "truncate_test"], "SELECT from truncate_test after truncate")
    assert_eq(len(results), 0, "Verify 0 rows exist after truncate")

    # Verify schema is NOT gone
    schema_resp = send(["GET", "_internal:schemas:truncate_test"])
    assert_eq(schema_resp.startswith("$"), True, "Schema for truncate_test should still exist after truncate")

    # Test truncating a non-existent table (should behave like DELETE FROM non_existent_table)
    # The current implementation of DELETE returns 0 if no rows are deleted.
    resp = send(["SQL", "TRUNCATE", "TABLE", "non_existent_table"])
    assert_eq(resp, ":0", "TRUNCATE TABLE on non-existent table should affect 0 rows")

    # Cleanup
    assert_eq(send(["DELETE", "_internal:schemas:truncate_test"]), ":1", "Clean up truncate_test schema")
    print("[PASS] TRUNCATE TABLE tests complete")

    # --- 13. CASE Expression Tests ---
    print("\n-- Phase 13: CASE Expression Tests --")
    print("[INFO] Testing CASE expressions...")

    # Test CASE with multiple WHENs and an ELSE
    results = send_and_parse([
        "SQL", "SELECT", "name, age,",
        "CASE",
        "WHEN", "age", "<", "30", "THEN", "'Young'",
        "WHEN", "age", "=", "30", "THEN", "'Prime'",
        "ELSE", "'Veteran'",
        "END", "AS", "age_category",
        "FROM", "user",
        "ORDER", "BY", "name", "ASC"
    ], "SELECT with CASE statement")

    assert_eq(len(results), 5, "CASE statement should return 5 rows")
    if len(results) == 5:
        expected_categories = {
            "Alice": "Prime",   # age 30
            "Bob": "Young",     # age 25
            "Carol": "Prime",   # age 30
            "David": "Young",   # age 25
            "Carol": "Veteran"  # age 35
        }
        # This is a bit tricky because there are two Carols. Let's check the categories.
        categories = sorted(get_column(results, 'age_category'))
        assert_eq(categories, ["Prime", "Prime", "Veteran", "Young", "Young"], "Verify all age categories")

    # Test CASE without an ELSE (should result in NULL)
    results = send_and_parse([
        "SQL", "SELECT", "name, age,",
        "CASE",
        "WHEN", "city", "=", "'SF'", "THEN", "'West Coast'",
        "END", "AS", "coast",
        "FROM", "user",
        "ORDER", "BY", "name", "ASC,", "age", "ASC"
    ], "SELECT with CASE statement without ELSE")
    
    assert_eq(len(results), 5, "CASE without ELSE should return 5 rows")
    if len(results) == 5:
        coasts = get_column(results, 'coast')
        # Alice, Bob, Carol(SF) are West Coast. Carol(NY), David are NULL.
        # Order is Alice, Bob, Carol, Carol, David
        expected_coasts = ["West Coast", "West Coast", "West Coast", None, None]
        # With deterministic order, we can just check the column
        coasts = get_column(results, 'coast')
        # Expected order: Alice, Bob, Carol(age 30), Carol(age 35), David
        expected_coasts = ['West Coast', 'West Coast', None, 'West Coast', None]
        assert_eq(coasts, expected_coasts, "Verify coast categories with NULLs")

    print("[PASS] CASE expression tests complete")
    
    print("\n== All SQL tests passed! ==")








