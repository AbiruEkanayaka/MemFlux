from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_union(sock, reader):
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

    def get_column_as_set(results, column_name):
        """Extracts a specific column from a list of result rows into a set."""
        return {row.get(column_name) for row in results}

    print("== SQL UNION Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Setup for UNION Tests --")
    send(["SQL", "DROP", "TABLE", "union_test1"])
    send(["SQL", "DROP", "TABLE", "union_test2"])
    
    resp = send(["SQL", "CREATE", "TABLE", "union_test1", "(id INTEGER, name TEXT)"])
    assert_eq(resp, "+OK", "CREATE TABLE union_test1")
    resp = send(["SQL", "CREATE", "TABLE", "union_test2", "(id INTEGER, name TEXT)"])
    assert_eq(resp, "+OK", "CREATE TABLE union_test2")

    send(["SQL", "INSERT", "INTO", "union_test1", "(id, name)", "VALUES", "(1, 'Alice')"])
    send(["SQL", "INSERT", "INTO", "union_test1", "(id, name)", "VALUES", "(2, 'Bob')"])
    send(["SQL", "INSERT", "INTO", "union_test1", "(id, name)", "VALUES", "(3, 'Charlie')"])

    send(["SQL", "INSERT", "INTO", "union_test2", "(id, name)", "VALUES", "(3, 'Charlie')"])
    send(["SQL", "INSERT", "INTO", "union_test2", "(id, name)", "VALUES", "(4, 'David')"])
    send(["SQL", "INSERT", "INTO", "union_test2", "(id, name)", "VALUES", "(5, 'Eve')"])

    # --- 2. Test UNION ALL ---
    print("\n-- Phase 2: Testing UNION ALL --")
    
    results_all = send_and_parse(["SQL", "SELECT", "name", "FROM", "union_test1", "UNION", "ALL", "SELECT", "name", "FROM", "union_test2"], "SELECT with UNION ALL")
    assert_eq(len(results_all), 6, "Row count for UNION ALL should be 6 (3+3)")
    # UNION ALL does not guarantee order, so we check counts of each name
    names_all = [r['name'] for r in results_all]
    assert_eq(names_all.count('Alice'), 1, "UNION ALL should contain one Alice")
    assert_eq(names_all.count('Bob'), 1, "UNION ALL should contain one Bob")
    assert_eq(names_all.count('Charlie'), 2, "UNION ALL should contain two Charlies (duplicate)")
    assert_eq(names_all.count('David'), 1, "UNION ALL should contain one David")
    assert_eq(names_all.count('Eve'), 1, "UNION ALL should contain one Eve")

    # --- 3. Test UNION ---
    print("\n-- Phase 3: Testing UNION (DISTINCT) --")
    
    results_distinct = send_and_parse(["SQL", "SELECT", "name", "FROM", "union_test1", "UNION", "SELECT", "name", "FROM", "union_test2"], "SELECT with UNION")
    assert_eq(len(results_distinct), 5, "Row count for UNION should be 5 (duplicates removed)")
    names_distinct_set = get_column_as_set(results_distinct, 'name')
    assert_eq(names_distinct_set, {'Alice', 'Bob', 'Charlie', 'David', 'Eve'}, "Verify names in UNION result")

    # --- 4. Cleanup ---
    print("\n-- Phase 4: Cleanup for UNION Tests --")
    send(["SQL", "DROP", "TABLE", "union_test1"])
    send(["SQL", "DROP", "TABLE", "union_test2"])
    # Data keys are schemaless, so we need to delete them manually
    for i in range(1, 4):
        send(["DELETE", f"union_test1:{i}"])
    for i in range(3, 6):
        send(["DELETE", f"union_test2:{i}"])
    print("[PASS] UNION tests complete")

