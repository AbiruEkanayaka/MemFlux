from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_sql_cypher_interop(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    def send_and_parse(cmd_list, description):
        """Sends a command, parses the response, and returns a list of JSON objects."""
        resp = send(cmd_list)
        if resp.startswith("-"):
            print(f"[FAIL] {description}: {resp}")
            return []
        if not resp.startswith("*"):
            print(f"[WARN] Unexpected response for '{description}': {resp!r}")
            return []
        
        resp_bytes = resp.encode('utf-8')
        lines = resp_bytes.split(b'\r\n')
        
        results = []
        i = 1
        while i < len(lines):
            if lines[i].startswith(b'$'):
                try:
                    if i + 1 < len(lines):
                        bulk_string = lines[i] + b'\r\n' + lines[i+1] + b'\r\n'
                    else:
                        break
                    row_json = extract_json_from_bulk(bulk_string)
                    if row_json is not None:
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
        return sorted([row.get(column_name) for row in results if row.get(column_name) is not None])

    print("== SQL-Cypher Interoperability Test Suite (GRAPH_MATCH) ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Graph and SQL Table Setup --")
    send(["FLUSHDB"])
    
    # Create SQL table
    send(["SQL", "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, role TEXT)"])
    send(["SQL", "INSERT INTO employees (id, name, role) VALUES (1, 'Alice', 'Engineer'), (2, 'Bob', 'Manager')"])

    # Create Graph data
    alice_id = send(["GRAPH.ADDNODE", "Person", '{"name": "Alice", "employee_id": 1}']).splitlines()[1]
    bob_id = send(["GRAPH.ADDNODE", "Person", '{"name": "Bob", "employee_id": 2}']).splitlines()[1]
    send(["GRAPH.ADDNODE", "Person", '{"name": "Charlie", "employee_id": 3}'])
    send(["GRAPH.ADDREL", alice_id, bob_id, "REPORTS_TO", "{}"])
    
    print("[PASS] Graph and SQL setup complete.")

    # --- 2. Simple GRAPH_MATCH ---
    print("\n-- Phase 2: Simple GRAPH_MATCH Query --")
    query = "SELECT name, emp_id FROM GRAPH_MATCH('MATCH (p:Person) RETURN p.name AS name, p.employee_id AS emp_id') RETURNS (name AS name, emp_id AS emp_id) AS g ORDER BY name"
    results = send_and_parse(["SQL", query], "Simple GRAPH_MATCH to select people")
    
    assert_eq(len(results), 3, "Simple GRAPH_MATCH should return 3 rows")
    expected_names = ["Alice", "Bob", "Charlie"]
    actual_names = get_column(results, 'name')
    assert_eq(actual_names, expected_names, "Verify names from simple GRAPH_MATCH")
    expected_ids = [1, 2, 3]
    actual_ids = get_column(results, 'emp_id')
    assert_eq(actual_ids, expected_ids, "Verify employee IDs from simple GRAPH_MATCH")
    print("[PASS] Simple GRAPH_MATCH tests complete.")

    # --- 3. GRAPH_MATCH with WHERE clause ---
    print("\n-- Phase 3: GRAPH_MATCH with SQL WHERE Clause --")
    query = "SELECT name FROM GRAPH_MATCH('MATCH (p:Person) RETURN p.name AS name, p.employee_id AS emp_id') RETURNS (name AS name, emp_id AS emp_id) AS g WHERE g.emp_id < 3 ORDER BY name"
    results = send_and_parse(["SQL", query], "GRAPH_MATCH with WHERE clause")
    
    assert_eq(len(results), 2, "GRAPH_MATCH with WHERE should return 2 rows")
    expected_names = ["Alice", "Bob"]
    actual_names = get_column(results, 'name')
    assert_eq(actual_names, expected_names, "Verify names from GRAPH_MATCH with WHERE")
    print("[PASS] GRAPH_MATCH with WHERE clause tests complete.")

    # --- 4. GRAPH_MATCH with JOIN ---
    print("\n-- Phase 4: GRAPH_MATCH with SQL JOIN --")
    query = "SELECT e.name, e.role, g.manager_name FROM employees e JOIN GRAPH_MATCH('MATCH (p1:Person)-[:REPORTS_TO]->(p2:Person) RETURN p1.employee_id AS emp_id, p2.name AS manager_name') RETURNS (emp_id AS emp_id, manager_name AS manager_name) AS g ON e.id = g.emp_id"
    results = send_and_parse(["SQL", query], "GRAPH_MATCH with JOIN")

    assert_eq(len(results), 1, "GRAPH_MATCH with JOIN should return 1 row")
    if results:
        expected_row = {'name': 'Alice', 'role': 'Engineer', 'manager_name': 'Bob'}
        # The SQL engine aliases columns with table.column, so we check for that
        result_row_flat = {k.split('.')[-1]: v for k, v in results[0].items()}
        assert_eq(result_row_flat, expected_row, "Verify result of GRAPH_MATCH with JOIN")
    print("[PASS] GRAPH_MATCH with JOIN tests complete.")

    # --- 5. Error Handling ---
    print("\n-- Phase 5: Error Handling --")
    query_no_alias = "SELECT name FROM GRAPH_MATCH('MATCH (p:Person) RETURN p.name AS name') RETURNS (name AS name)"
    resp = send(["SQL", query_no_alias])
    assert_eq(resp.startswith("-ERR"), True, "GRAPH_MATCH without an alias should fail")

    query_bad_cypher = "SELECT name FROM GRAPH_MATCH('MTCH (p:Person) RETURN p.name AS name') RETURNS (name AS name) AS g"
    resp = send(["SQL", query_bad_cypher])
    assert_eq(resp.startswith("-ERR"), True, "GRAPH_MATCH with invalid Cypher should fail")
    print("[PASS] Error handling tests complete.")

    # --- 6. Cleanup ---
    print("\n-- Phase 6: Cleanup --")
    send(["FLUSHDB"])
    print("[PASS] Cleanup complete.")