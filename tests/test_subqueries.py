from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_subqueries(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    def send_and_parse(cmd_list, description):
        """Sends a command, parses the response, and returns a list of JSON objects."""
        resp = send(cmd_list)
        if resp.startswith(("-", ":", "+")):
            return resp

        resp_bytes = resp.encode('utf-8')
        lines = resp_bytes.split(b'\r\n')
        if not lines or not lines[0].startswith(b'*'):
            print(f"[WARN] Unexpected response for '{description}': {resp!r}")
            return []
        
        results = []
        i = 1
        while i < len(lines):
            if lines[i].startswith(b'$'):
                try:
                    bulk_string = lines[i] + b'\r\n' + lines[i+1] + b'\r\n'
                    row_json = extract_json_from_bulk(bulk_string)
                    if row_json:
                        results.append(row_json)
                    i += 2
                except IndexError:
                    break
            else:
                i += 1
        print(f"[INFO] {description}: {len(results)} rows")
        return results

    def get_column(results, column_name):
        """Extracts a specific column from a list of result rows."""
        return sorted([row.get(column_name) for row in results])

    print("== SQL Subquery Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Setup for Subquery Tests --")
    send(["SQL", "DROP", "TABLE", "sq_users"])
    send(["SQL", "DROP", "TABLE", "sq_orders"])
    
    resp = send(["SQL", "CREATE", "TABLE", "sq_users", "(id INTEGER, name TEXT, is_active BOOLEAN)"])
    assert_eq(resp, "+OK", "CREATE TABLE sq_users")
    resp = send(["SQL", "CREATE", "TABLE", "sq_orders", "(id INTEGER, user_id INTEGER, product TEXT)"])
    assert_eq(resp, "+OK", "CREATE TABLE sq_orders")

    send(["SQL", "INSERT", "INTO", "sq_users", "(id, name, is_active)", "VALUES", "(1, 'Alice', true)"])
    send(["SQL", "INSERT", "INTO", "sq_users", "(id, name, is_active)", "VALUES", "(2, 'Bob', true)"])
    send(["SQL", "INSERT", "INTO", "sq_users", "(id, name, is_active)", "VALUES", "(3, 'Charlie', false)"])
    send(["SQL", "INSERT", "INTO", "sq_users", "(id, name, is_active)", "VALUES", "(4, 'David', true)"])

    send(["SQL", "INSERT", "INTO", "sq_orders", "(id, user_id, product)", "VALUES", "(101, 1, 'Laptop')"])
    send(["SQL", "INSERT", "INTO", "sq_orders", "(id, user_id, product)", "VALUES", "(102, 2, 'Mouse')"])
    send(["SQL", "INSERT", "INTO", "sq_orders", "(id, user_id, product)", "VALUES", "(103, 1, 'Keyboard')"])
    send(["SQL", "INSERT", "INTO", "sq_orders", "(id, user_id, product)", "VALUES", "(104, 3, 'Monitor')"])

    # --- 2. Test WHERE ... IN (SELECT ...) ---
    print("\n-- Phase 2: Testing WHERE IN Subqueries --")
    
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "sq_users", "WHERE", "id", "IN", "(SELECT user_id FROM sq_orders)"], "SELECT users with any order")
    assert_eq(len(results), 3, "Row count for users with orders")
    assert_eq(get_column(results, 'name'), ["Alice", "Bob", "Charlie"], "Verify names of users with orders")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "sq_users", "WHERE", "id", "IN", "(SELECT user_id FROM sq_orders WHERE product = 'Laptop')"], "SELECT users who bought a Laptop")
    assert_eq(len(results), 1, "Row count for users who bought a Laptop")
    assert_eq(get_column(results, 'name'), ["Alice"], "Verify name of user who bought a Laptop")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "sq_users", "WHERE", "is_active = true AND id IN (SELECT user_id FROM sq_orders)"], "SELECT active users with orders")
    assert_eq(len(results), 2, "Row count for active users with orders")
    assert_eq(get_column(results, 'name'), ["Alice", "Bob"], "Verify names of active users with orders")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "sq_users", "WHERE", "id", "IN", "(SELECT user_id FROM sq_orders WHERE product = 'NonExistent')"], "SELECT with empty subquery result")
    assert_eq(len(results), 0, "Row count for empty subquery result should be 0")

    # --- 3. Test WHERE ... = (SELECT ...) ---
    print("\n-- Phase 3: Testing WHERE = Subqueries --")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "sq_users", "WHERE", "id", "=", "(SELECT user_id FROM sq_orders WHERE id = 102)"], "SELECT with = and single-row/single-column subquery")
    assert_eq(len(results), 1, "Row count for = subquery")
    assert_eq(get_column(results, 'name'), ["Bob"], "Verify name for = subquery")

    # Error case: subquery returns more than one row
    resp = send(["SQL", "SELECT", "name", "FROM", "sq_users", "WHERE", "id", "=", "(SELECT user_id FROM sq_orders WHERE product LIKE '%o%')"])
    assert_eq(resp.startswith("-ERR"), True, "Subquery with = returning multiple rows should fail")
    assert_eq("Subquery for '=' operator must return exactly one row" in resp, True, "Error message for multi-row = subquery")

    # --- 4. Cleanup ---
    print("\n-- Phase 4: Cleanup for Subquery Tests --")
    send(["SQL", "DROP", "TABLE", "sq_users"])
    send(["SQL", "DROP", "TABLE", "sq_orders"])
    # Data keys are schemaless, so we need to delete them manually
    for i in range(1, 5):
        send(["DELETE", f"sq_users:{i}"])
    for i in range(101, 105):
        send(["DELETE", f"sq_orders:{i}"])
    print("[PASS] Subquery tests complete")


