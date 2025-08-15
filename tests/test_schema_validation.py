from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_schema_validation_and_sorting(sock, reader):
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
            elif lines[i]:
                i += 1
            else:
                i += 1
        print(f"[INFO] {description}: {len(results)} rows")
        return results

    def get_column(results, column_name):
        """Extracts a specific column from a list of result rows, preserving None."""
        return [row.get(column_name) for row in results]

    print("== SQL Schema Validation and Type-Aware Sorting Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Setup for Schema Tests --")
    send(["SQL", "DROP", "TABLE", "schema_test"])
    resp = send(["SQL", "CREATE", "TABLE", "schema_test", "(id INTEGER, name TEXT, price INTEGER)"])
    assert_eq(resp, "+OK", "CREATE TABLE schema_test")

    # --- 2. Schema Validation Tests ---
    print("\n-- Phase 2: Schema Validation in Planner --")

    # SELECT non-existent column
    resp = send(["SQL", "SELECT", "non_existent_col", "FROM", "schema_test"])
    assert_eq(resp.startswith("-ERR"), True, "SELECT non-existent column should fail")
    assert_eq("Column 'non_existent_col' not found" in resp, True, "Error message for SELECT non-existent column")

    # INSERT into non-existent column
    resp = send(["SQL", "INSERT", "INTO", "schema_test", "(id, non_existent_col)", "VALUES", "('1', 'foo')"])
    assert_eq(resp.startswith("-ERR"), True, "INSERT into non-existent column should fail")
    assert_eq("Column 'non_existent_col' does not exist" in resp, True, "Error message for INSERT non-existent column")

    # UPDATE non-existent column
    resp = send(["SQL", "UPDATE", "schema_test", "SET", "non_existent_col", "=", "'bar'", "WHERE", "id=1"])
    assert_eq(resp.startswith("-ERR"), True, "UPDATE non-existent column should fail")
    assert_eq("Column 'non_existent_col' does not exist" in resp, True, "Error message for UPDATE non-existent column")

    # WHERE on non-existent column
    resp = send(["SQL", "SELECT", "*", "FROM", "schema_test", "WHERE", "non_existent_col", "=", "1"])
    assert_eq(resp.startswith("-ERR"), True, "WHERE on non-existent column should fail")
    assert_eq("Column 'non_existent_col' not found" in resp, True, "Error message for WHERE non-existent column")

    # Ambiguous column test setup
    send(["SQL", "DROP", "TABLE", "ambiguous1"])
    send(["SQL", "DROP", "TABLE", "ambiguous2"])
    send(["SQL", "CREATE", "TABLE", "ambiguous1", "(id INTEGER, name TEXT)"])
    send(["SQL", "CREATE", "TABLE", "ambiguous2", "(id INTEGER, value TEXT)"])
    
    resp = send(["SQL", "SELECT", "id", "FROM", "ambiguous1", "JOIN", "ambiguous2", "ON", "ambiguous1.id = ambiguous2.id"])
    assert_eq(resp.startswith("-ERR"), True, "SELECT ambiguous column should fail")
    assert_eq("Column 'id' is ambiguous" in resp, True, "Error message for ambiguous column")
    
    send(["SQL", "DROP", "TABLE", "ambiguous1"])
    send(["SQL", "DROP", "TABLE", "ambiguous2"])
    print("[PASS] Schema validation tests complete")


    # --- 3. Type-Aware Sorting Test ---
    print("\n-- Phase 3: Type-Aware Sorting --")
    # Insert numbers as strings into an INTEGER column
    send(["SQL", "INSERT", "INTO", "schema_test", "(id, name, price)", "VALUES", "('1', 'Item A', '100')"])
    send(["SQL", "INSERT", "INTO", "schema_test", "(id, name, price)", "VALUES", "('2', 'Item B', '20')"])
    send(["SQL", "INSERT", "INTO", "schema_test", "(id, name, price)", "VALUES", "('3', 'Item C', '5')"])
    send(["SQL", "INSERT", "INTO", "schema_test", "(id, name, price)", "VALUES", "('4', 'Item D', null)"]) # Add a null
    
    # Sort ASC
    results = send_and_parse(["SQL", "SELECT", "price", "FROM", "schema_test", "ORDER", "BY", "price", "ASC"], "ORDER BY price ASC")
    prices_asc = get_column(results, 'price')
    # Nulls are sorted first by default in this implementation
    assert_eq(prices_asc, [None, 5, 20, 100], "ORDER BY price ASC should be numeric (nulls first)")

    # Sort DESC
    results = send_and_parse(["SQL", "SELECT", "price", "FROM", "schema_test", "ORDER", "BY", "price", "DESC"], "ORDER BY price DESC")
    prices_desc = get_column(results, 'price')
    assert_eq(prices_desc, [100, 20, 5, None], "ORDER BY price DESC should be numeric")
    print("[PASS] Type-aware sorting tests complete")

    # --- 4. Cleanup ---
    print("\n-- Phase 4: Cleanup for Schema Tests --")
    resp = send(["SQL", "DROP", "TABLE", "schema_test"])
    assert_eq(resp, "+OK", "DROP TABLE schema_test")
    # Also delete the data keys
    send(["DELETE", "schema_test:1"])
    send(["DELETE", "schema_test:2"])
    send(["DELETE", "schema_test:3"])
    send(["DELETE", "schema_test:4"])

    # --- 5. Type-Aware Sorting for Timestamps ---
    print("\n-- Phase 5: Type-Aware Sorting for Timestamps --")
    send(["SQL", "DROP", "TABLE", "timestamp_test"])
    resp = send(["SQL", "CREATE", "TABLE", "timestamp_test", "(id INTEGER, event_time TIMESTAMP)"])
    assert_eq(resp, "+OK", "CREATE TABLE timestamp_test")

    # Insert timestamps that are not in lexicographical order
    send(["SQL", "INSERT", "INTO", "timestamp_test", "(id, event_time)", "VALUES", "(1, '2024-01-10T12:00:00Z')"])
    send(["SQL", "INSERT", "INTO", "timestamp_test", "(id, event_time)", "VALUES", "(2, '2023-12-25T10:00:00Z')"])
    send(["SQL", "INSERT", "INTO", "timestamp_test", "(id, event_time)", "VALUES", "(3, '2024-02-01T09:00:00Z')"])

    # Sort ASC
    results_asc = send_and_parse(["SQL", "SELECT", "event_time", "FROM", "timestamp_test", "ORDER", "BY", "event_time", "ASC"], "ORDER BY event_time ASC")
    times_asc = get_column(results_asc, 'event_time')
    expected_asc = ['2023-12-25T10:00:00Z', '2024-01-10T12:00:00Z', '2024-02-01T09:00:00Z']
    assert_eq(times_asc, expected_asc, "ORDER BY event_time ASC should be chronological")

    # Sort DESC
    results_desc = send_and_parse(["SQL", "SELECT", "event_time", "FROM", "timestamp_test", "ORDER", "BY", "event_time", "DESC"], "ORDER BY event_time DESC")
    times_desc = get_column(results_desc, 'event_time')
    expected_desc = ['2024-02-01T09:00:00Z', '2024-01-10T12:00:00Z', '2023-12-25T10:00:00Z']
    assert_eq(times_desc, expected_desc, "ORDER BY event_time DESC should be chronological")

    # Cleanup
    send(["SQL", "DROP", "TABLE", "timestamp_test"])
    send(["DELETE", "timestamp_test:1", "timestamp_test:2", "timestamp_test:3"])
    print("[PASS] Timestamp sorting tests complete")

    print("[PASS] Schema tests complete")








