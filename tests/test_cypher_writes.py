
import json
from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_cypher_writes(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    def send_and_parse(cmd_list, description):
        resp = send(cmd_list)
        if resp.startswith("-ERR"):
            print(f"[FAIL] {description}: {resp}")
            assert_eq(f"query failed: {resp}", "query succeeded", description)
            return []
        if not resp.startswith("*"):
            # Can be an OK from a write query with no RETURN
            if resp == "+OK":
                print(f"[INFO] {description}: OK")
                return "+OK"
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

    print("== Cypher Write Operations Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Graph Setup --")
    send(["FLUSHDB"])
    
    # Add initial nodes and relationships using the low-level commands
    alice_props = '{"name": "Alice", "age": 30}'
    resp = send(["GRAPH.ADDNODE", "Person", alice_props])
    alice_id = resp.splitlines()[1]

    bob_props = '{"name": "Bob", "age": 25}'
    resp = send(["GRAPH.ADDNODE", "Person", bob_props])
    bob_id = resp.splitlines()[1]

    knows_props = '{"since": 2022}'
    send(["GRAPH.ADDREL", alice_id, bob_id, "KNOWS", knows_props])
    print("[PASS] Graph setup complete.")

    # --- 2. CREATE Clause ---
    print("\n-- Phase 2: CREATE Clause --")
    # Create a new node with properties
    results = send_and_parse(["CYPHER", 'CREATE (c:Company {name: "MemFlux"}) RETURN c.name, c._id'], "CREATE node with properties")
    assert_eq(len(results), 1, "CREATE should return 1 row")
    assert_eq(results[0]['c.name'], "MemFlux", "CREATE should return correct property")
    company_id = results[0]['c._id']

    # Create a relationship to an existing node
    send_and_parse(["CYPHER", f'MATCH (p:Person) WHERE p._id = "{alice_id}" CREATE (p)-[:WORKS_AT]->(c:Company {{_id: "{company_id}"}})'], "CREATE relationship to existing node")
    
    # Verify relationship creation
    results = send_and_parse(["CYPHER", f'MATCH (p:Person)-[:WORKS_AT]->(c:Company) WHERE p._id = "{alice_id}" RETURN c.name'], "Verify WORKS_AT relationship by finding company")
    assert_eq(results[0]['c.name'], "MemFlux", "Alice should work at MemFlux")
    print("[PASS] CREATE clause tests complete.")

    # --- 3. SET Clause ---
    print("\n-- Phase 3: SET Clause --")
    send(["BEGIN"]) # Start transaction
    # Set a property on a node
    results = send_and_parse(["CYPHER", f'MATCH (p:Person) WHERE p._id = "{alice_id}" SET p.age = 31 RETURN p.age'], "SET property on a node")
    assert_eq(results[0]['p.age'], 31, "SET should update node property")

    # Set a property on a relationship
    results = send_and_parse(["CYPHER", f'MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE a._id = "{alice_id}" SET r.since = 2023 RETURN r.since'], "SET property on a relationship")
    assert_eq(results[0]['r.since'], 2023, "SET should update relationship property")
    send(["COMMIT"]) # Commit transaction
    print("[PASS] SET clause tests complete.")

    # --- 4. REMOVE Clause ---
    print("\n-- Phase 4: REMOVE Clause --")
    send(["BEGIN"]) # Start transaction
    # Remove a property from a node
    send_and_parse(["CYPHER", f'MATCH (p:Person) WHERE p._id = "{alice_id}" REMOVE p.age'], "REMOVE property from a node")
    send(["COMMIT"]) # Commit transaction
    results = send_and_parse(["CYPHER", f'MATCH (p:Person) WHERE p._id = "{alice_id}" RETURN p'], "Verify REMOVE property")
    assert_eq('age' in results[0]['p'], False, "'age' property should be removed")
    print("[PASS] REMOVE clause tests complete.")

    # --- 5. DELETE and DETACH DELETE ---
    print("\n-- Phase 5: DELETE and DETACH DELETE --")
    # Create a node to delete
    resp = send(["GRAPH.ADDNODE", "Person", '{"name": "ToDelete"}'])
    to_delete_id = resp.splitlines()[1]

    # Test simple DELETE
    send_and_parse(["CYPHER", f'MATCH (p:Person) WHERE p._id = "{to_delete_id}" DELETE p'], "DELETE a node")
    results = send_and_parse(["CYPHER", f'MATCH (p:Person) WHERE p._id = "{to_delete_id}" RETURN p'], "Verify DELETE")
    assert_eq(len(results), 0, "Node should be deleted")

    # Test DETACH DELETE
    # Alice is connected to Bob and MemFlux.
    send_and_parse(["CYPHER", f'MATCH (p:Person) WHERE p._id = "{alice_id}" DETACH DELETE p'], "DETACH DELETE a node")
    results = send_and_parse(["CYPHER", f'MATCH (p:Person) WHERE p._id = "{alice_id}" RETURN p'], "Verify DETACH DELETE")
    assert_eq(len(results), 0, "Alice should be deleted")
    
    # Verify her relationships are also gone
    results = send_and_parse(["CYPHER", f'MATCH (b:Person)<-[r]-() WHERE b._id = "{bob_id}" RETURN r'], "Verify relationships to Bob are gone")
    assert_eq(len(results), 0, "Relationships to Bob should be gone after DETACH DELETE")
    print("[PASS] DELETE and DETACH DELETE tests complete.")

    # --- 6. Cleanup ---
    print("\n-- Phase 6: Cleanup --")
    send(["FLUSHDB"])
    print("[PASS] Cleanup complete.")
