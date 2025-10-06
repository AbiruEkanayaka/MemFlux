import json
from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_cypher_advanced(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    def send_and_parse(cmd_list, description):
        resp = send(cmd_list)
        if resp.startswith("-"):
            print(f"[FAIL] {description}: {resp}")
            return []
        if not resp.startswith("*"):
            print(f"[WARN] Unexpected response for '{description}': {resp!r}")
            return []
        
        lines = resp.splitlines()
        results = []
        i = 1
        while i < len(lines):
            if lines[i].startswith("$"):
                try:
                    bulk_string = lines[i] + '\r\n' + lines[i+1]
                    row_json = extract_json_from_bulk(bulk_string.encode('utf-8'))
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
        return {row.get(column_name) for row in results}

    print("== Advanced Cypher Test Suite (OPTIONAL MATCH, Var-Length) ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Graph Setup --")
    send(["FLUSHDB"])
    
    # Create a simple path: Alice -> Bob -> Charlie -> David
    # Also add Eve who is not connected to anyone
    alice_id = send(["GRAPH.ADDNODE", "Person", '{"name": "Alice"}']).splitlines()[1]
    bob_id = send(["GRAPH.ADDNODE", "Person", '{"name": "Bob"}']).splitlines()[1]
    charlie_id = send(["GRAPH.ADDNODE", "Person", '{"name": "Charlie"}']).splitlines()[1]
    david_id = send(["GRAPH.ADDNODE", "Person", '{"name": "David"}']).splitlines()[1]
    eve_id = send(["GRAPH.ADDNODE", "Person", '{"name": "Eve"}']).splitlines()[1]

    send(["GRAPH.ADDREL", alice_id, bob_id, "KNOWS", '{}'])
    send(["GRAPH.ADDREL", bob_id, charlie_id, "KNOWS", '{}'])
    send(["GRAPH.ADDREL", charlie_id, david_id, "KNOWS", '{}'])
    print("[PASS] Graph setup complete.")

    # --- 2. OPTIONAL MATCH ---
    print("\n-- Phase 2: OPTIONAL MATCH --")

    # Test where the optional part exists
    results = send_and_parse(["CYPHER", 'MATCH (p:Person {name: "Charlie"}) OPTIONAL MATCH (p)-[:KNOWS]->(b) RETURN p.name, b.name'], "OPTIONAL MATCH with existing path")
    assert_eq(len(results), 1, "OPTIONAL MATCH with path should return 1 row")
    assert_eq(results[0].get('p.name'), "Charlie", "OPTIONAL MATCH start node name is correct")
    assert_eq(results[0].get('b.name'), "David", "OPTIONAL MATCH end node name is correct")

    # Test where the optional part does NOT exist
    results = send_and_parse(["CYPHER", 'MATCH (p:Person {name: "David"}) OPTIONAL MATCH (p)-[:KNOWS]->(b) RETURN p.name, b.name'], "OPTIONAL MATCH with non-existing path")
    assert_eq(len(results), 1, "OPTIONAL MATCH without path should return 1 row")
    assert_eq(results[0].get('p.name'), "David", "OPTIONAL MATCH start node name is correct (no path)")
    assert_eq(results[0].get('b.name'), None, "OPTIONAL MATCH end node name should be null")

    # Test on an isolated node
    results = send_and_parse(["CYPHER", 'MATCH (p:Person {name: "Eve"}) OPTIONAL MATCH (p)-[:KNOWS]->(b) RETURN p.name, b.name'], "OPTIONAL MATCH on isolated node")
    assert_eq(len(results), 1, "OPTIONAL MATCH on isolated node should return 1 row")
    assert_eq(results[0].get('p.name'), "Eve", "Isolated node name is correct")
    assert_eq(results[0].get('b.name'), None, "Isolated node optional path should be null")
    print("[PASS] OPTIONAL MATCH tests complete.")

    # --- 3. Variable-Length Paths ---
    print("\n-- Phase 3: Variable-Length Paths --")

    # Test exact length: *2
    results = send_and_parse(["CYPHER", 'MATCH (a:Person {name:"Alice"})-[:KNOWS*2]->(c:Person) RETURN c.name'], "Variable path of exact length 2")
    assert_eq(len(results), 1, "Exact length path (*2) should find 1 person")
    assert_eq(get_column_as_set(results, 'c.name'), {"Charlie"}, "Path of length 2 should end at Charlie")

    # Test variable length range: *1..2
    results = send_and_parse(["CYPHER", 'MATCH (a:Person {name:"Alice"})-[:KNOWS*1..2]->(b) RETURN b.name'], "Variable path of length 1..2")
    assert_eq(len(results), 2, "Length 1..2 path should find 2 people")
    assert_eq(get_column_as_set(results, 'b.name'), {"Bob", "Charlie"}, "Path of length 1..2 should end at Bob and Charlie")

    # Test unbounded min: *..2
    results = send_and_parse(["CYPHER", 'MATCH (a:Person {name:"Alice"})-[:KNOWS*..2]->(b) RETURN b.name'], "Variable path of length ..2")
    assert_eq(len(results), 2, "Length ..2 path should find 2 people")
    assert_eq(get_column_as_set(results, 'b.name'), {"Bob", "Charlie"}, "Path of length ..2 should end at Bob and Charlie")

    # Test unbounded max: *3..
    results = send_and_parse(["CYPHER", 'MATCH (a:Person {name:"Alice"})-[:KNOWS*3..]->(b) RETURN b.name'], "Variable path of length 3..")
    assert_eq(len(results), 1, "Length 3.. path should find 1 person")
    assert_eq(get_column_as_set(results, 'b.name'), {"David"}, "Path of length 3.. should end at David")

    # Test fully unbounded: *
    results = send_and_parse(["CYPHER", 'MATCH (a:Person {name:"Alice"})-[:KNOWS*]->(b) RETURN b.name'], "Fully unbounded variable path")
    assert_eq(len(results), 3, "Unbounded path should find 3 people")
    assert_eq(get_column_as_set(results, 'b.name'), {"Bob", "Charlie", "David"}, "Unbounded path should find all connected nodes")
    print("[PASS] Variable-length path tests complete.")

    # --- 4. Cleanup ---
    print("\n-- Phase 4: Cleanup --")
    send(["FLUSHDB"])
    print("[PASS] Cleanup complete.")
