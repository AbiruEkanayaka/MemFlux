from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_data_interoperability(sock, reader):
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

    def get_column_as_set(results, column_name):
        """Extracts a specific column from a list of result rows into a set."""
        return {row.get(column_name) for row in results}

    print("== Data Interoperability Test Suite ==")

    # --- 1. SQL on Graph Data ---
    print("\n-- Phase 1: SQL on Graph Data --")
    send(["FLUSHDB"])
    
    # Create Graph data
    alice_props = '{"name": "Alice", "age": 30}'
    resp = send(["GRAPH.ADDNODE", "Person", alice_props])
    alice_id = resp.splitlines()[1]

    bob_props = '{"name": "Bob", "age": 25}'
    resp = send(["GRAPH.ADDNODE", "Person", bob_props])
    bob_id = resp.splitlines()[1]

    knows_props = '{"since": 2022}'
    resp = send(["GRAPH.ADDREL", alice_id, bob_id, "KNOWS", knows_props])
    knows_id = resp.splitlines()[1]
    
    print("[INFO] Graph data created. Refreshing schemas and querying via SQL...")
    send(["_REFRESH_GRAPH_SCHEMAS"])

    # Query the virtual 'Person' table
    results_person = send_and_parse(["SQL", "SELECT * FROM Person ORDER BY properties.name"], "SELECT * FROM Person")
    assert_eq(len(results_person), 2, "SQL on Person virtual table should return 2 rows")
    person_names = {p['properties']['name'] for p in results_person}
    assert_eq(person_names, {"Alice", "Bob"}, "Verify names from Person virtual table")

    # Query the virtual 'KNOWS' table
    results_knows = send_and_parse(["SQL", "SELECT * FROM KNOWS"], "SELECT * FROM KNOWS")
    assert_eq(len(results_knows), 1, "SQL on KNOWS virtual table should return 1 row")
    if results_knows:
        knows_row = results_knows[0]
        assert_eq(knows_row.get('_from_id'), alice_id, "Verify _from_id in KNOWS virtual table")
        assert_eq(knows_row.get('_to_id'), bob_id, "Verify _to_id in KNOWS virtual table")
        assert_eq(knows_row.get('properties', {}).get('since'), 2022, "Verify properties in KNOWS virtual table")

    print("[PASS] SQL on Graph Data tests complete.")


    # --- 2. Cypher on SQL Data ---
    print("\n-- Phase 2: Cypher on SQL Data --")
    send(["FLUSHDB"])

    # Create SQL data
    send(["SQL", "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)"])
    send(["SQL", "INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 1200), (2, 'Mouse', 25)"])

    print("[INFO] SQL data created. Querying via Cypher...")

    # Query the virtual 'products' nodes
    results_cypher = send_and_parse(["CYPHER", "MATCH (p:products) RETURN p.name, p.price"], "MATCH (p:products)")
    assert_eq(len(results_cypher), 2, "Cypher on products virtual nodes should return 2 rows")
    
    # Use sets for comparison since order is not guaranteed
    expected_cypher_set = {('Laptop', 1200), ('Mouse', 25)}
    actual_cypher_set = {(r.get('p.name'), r.get('p.price')) for r in results_cypher}
    assert_eq(actual_cypher_set, expected_cypher_set, "Verify data from products virtual nodes")


    # Query with a WHERE clause
    results_cypher_where = send_and_parse(["CYPHER", "MATCH (p:products) WHERE p.price = 1200 RETURN p.name"], "MATCH (p:products) with WHERE")
    assert_eq(len(results_cypher_where), 1, "Cypher WHERE on virtual node should return 1 row")
    assert_eq(results_cypher_where[0]['p.name'], "Laptop", "Verify name from Cypher WHERE clause")

    print("[PASS] Cypher on SQL Data tests complete.")

    # --- 3. Cleanup ---
    print("\n-- Phase 3: Cleanup --")
    send(["FLUSHDB"])
    print("[PASS] Cleanup complete.")
