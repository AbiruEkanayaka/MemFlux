import json
from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_cypher(sock, reader):
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

    print("== Cypher Query Engine Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Graph Setup --")
    send(["FLUSHDB"])
    
    # Add nodes
    alice_props = '{"name": "Alice", "age": 30}'
    resp = send(["GRAPH.ADDNODE", "Person", alice_props])
    alice_id = resp.splitlines()[1]

    bob_props = '{"name": "Bob", "age": 25}'
    resp = send(["GRAPH.ADDNODE", "Person", bob_props])
    bob_id = resp.splitlines()[1]

    # Add relationship
    knows_props = '{"since": "2022"}'
    send(["GRAPH.ADDREL", alice_id, bob_id, "KNOWS", knows_props])
    print("[PASS] Graph setup complete.")

    # --- 2. Basic Cypher Queries ---
    print("\n-- Phase 2: Basic Cypher Queries --")

    # Test Node Scan
    results = send_and_parse(["CYPHER", 'MATCH (p:Person) RETURN p'], "MATCH (p:Person) RETURN p")
    assert_eq(len(results), 2, "Node scan should return 2 Person nodes")

    # Test Projection
    results = send_and_parse(["CYPHER", 'MATCH (p:Person) RETURN p.name'], "MATCH (p:Person) RETURN p.name")
    names = sorted([r['p.name'] for r in results])
    assert_eq(names, ["Alice", "Bob"], "Projection should return correct names")

    # Test Filter (will use IndexScan due to optimization)
    results = send_and_parse(["CYPHER", 'MATCH (p:Person) WHERE p.name = "Alice" RETURN p.age'], "MATCH with WHERE clause")
    assert_eq(len(results), 1, "Filter should return 1 node")
    assert_eq(results[0]['p.age'], 30, "Filter should return correct property")

    # Test Expand
    results = send_and_parse(["CYPHER", 'MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name'], "MATCH with relationship expand")
    assert_eq(len(results), 1, "Expand should find 1 relationship")
    if results:
        assert_eq(results[0]['a.name'], "Alice", "Expand should return correct start node name")
        assert_eq(results[0]['b.name'], "Bob", "Expand should return correct end node name")

    print("[PASS] Basic Cypher queries complete.")

    # --- 3. Cleanup ---
    print("\n-- Phase 3: Cleanup --")
    send(["FLUSHDB"])
    print("[PASS] Cleanup complete.")

