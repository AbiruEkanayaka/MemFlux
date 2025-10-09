import json
from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_cypher_functions_and_paths(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    def send_and_parse(cmd_list, description):
        resp = send(cmd_list)
        if resp.startswith("-ERR"):
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

    print("== Cypher Functions and Paths Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Graph Setup --")
    send(["FLUSHDB"])
    
    # Create nodes for function tests
    alice_id = send(["GRAPH.ADDNODE", "Person", '{"name": "Alice", "age": 30, "tags": ["friendly", "smart"]}']).splitlines()[1]
    bob_id = send(["GRAPH.ADDNODE", "Person", '{"name": "Bob", "age": 25}']).splitlines()[1]
    charlie_id = send(["GRAPH.ADDNODE", "Person", '{"name": "Charlie", "age": 35}']).splitlines()[1]
    david_id = send(["GRAPH.ADDNODE", "Person", '{"name": "David", "age": 40}']).splitlines()[1]
    eve_id = send(["GRAPH.ADDNODE", "Person", '{"name": "Eve", "age": 28}']).splitlines()[1]

    # Create relationships for function tests
    knows_rel_id = send(["GRAPH.ADDREL", alice_id, bob_id, "KNOWS", '{"since": 2022, "strength": 0.8}']).splitlines()[1]
    works_rel_id = send(["GRAPH.ADDREL", bob_id, charlie_id, "WORKS_WITH", '{"project": "Alpha"}']).splitlines()[1]
    friend_rel_id = send(["GRAPH.ADDREL", alice_id, charlie_id, "FRIEND", '{"duration": 5}']).splitlines()[1]

    # Create nodes for shortest path
    # A --(1)--> B --(1)--> C --(1)--> D
    # |           ^           |
    # +----(1)----+           +----(1)----+
    #             E --(1)--> F
    # Shortest path A to D is A-B-C-D (length 3)
    # Longer path A-E-F-C-D (length 4)
    node_a_id = send(["GRAPH.ADDNODE", "Node", '{"name": "A"}']).splitlines()[1]
    node_b_id = send(["GRAPH.ADDNODE", "Node", '{"name": "B"}']).splitlines()[1]
    node_c_id = send(["GRAPH.ADDNODE", "Node", '{"name": "C"}']).splitlines()[1]
    node_d_id = send(["GRAPH.ADDNODE", "Node", '{"name": "D"}']).splitlines()[1]
    node_e_id = send(["GRAPH.ADDNODE", "Node", '{"name": "E"}']).splitlines()[1]
    node_f_id = send(["GRAPH.ADDNODE", "Node", '{"name": "F"}']).splitlines()[1]

    send(["GRAPH.ADDREL", node_a_id, node_b_id, "PATH", '{}'])
    send(["GRAPH.ADDREL", node_b_id, node_c_id, "PATH", '{}'])
    send(["GRAPH.ADDREL", node_c_id, node_d_id, "PATH", '{}'])
    send(["GRAPH.ADDREL", node_a_id, node_e_id, "PATH", '{}'])
    send(["GRAPH.ADDREL", node_e_id, node_f_id, "PATH", '{}'])
    send(["GRAPH.ADDREL", node_f_id, node_c_id, "PATH", '{}'])

    print("[PASS] Graph setup complete.")

    # --- 2. Built-in Functions ---
    print("\n-- Phase 2: Built-in Functions --")

    # id()
    results = send_and_parse(["CYPHER", f'MATCH (p:Person) WHERE p._id = "{alice_id}" RETURN id(p) AS alice_id'], "id(node)")
    assert_eq(results[0]['alice_id'], alice_id, "id(node) should return node ID")
    results = send_and_parse(["CYPHER", f'MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r._id = "{knows_rel_id}" RETURN id(r) AS knows_id'], "id(relationship)")
    assert_eq(results[0]['knows_id'], knows_rel_id, "id(relationship) should return relationship ID")

    # labels()
    results = send_and_parse(["CYPHER", f'MATCH (p:Person) WHERE p._id = "{alice_id}" RETURN labels(p) AS alice_labels'], "labels(node)")
    assert_eq(results[0]['alice_labels'], ["Person"], "labels(node) should return node labels")

    # type()
    results = send_and_parse(["CYPHER", f'MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r._id = "{knows_rel_id}" RETURN type(r) AS rel_type'], "type(relationship)")
    assert_eq(results[0]['rel_type'], "KNOWS", "type(relationship) should return relationship type")

    # properties()
    results = send_and_parse(["CYPHER", f'MATCH (p:Person) WHERE p._id = "{alice_id}" RETURN properties(p) AS alice_props'], "properties(node)")
    expected_alice_props = {"name": "Alice", "age": 30, "tags": ["friendly", "smart"]}
    assert_eq(results[0]['alice_props'], expected_alice_props, "properties(node) should return all user-defined properties")
    results = send_and_parse(["CYPHER", f'MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r._id = "{knows_rel_id}" RETURN properties(r) AS knows_props'], "properties(relationship)")
    expected_knows_props = {"since": 2022, "strength": 0.8}
    assert_eq(results[0]['knows_props'], expected_knows_props, "properties(relationship) should return all user-defined properties")

    # size()
    results = send_and_parse(["CYPHER", f'MATCH (p:Person) WHERE p._id = "{alice_id}" RETURN size(p.name) AS name_len, size(p.tags) AS tags_len'], "size(string) and size(list)")
    assert_eq(results[0]['name_len'], 5, "size(string) should return string length")
    assert_eq(results[0]['tags_len'], 2, "size(list) should return list length")
    
    print("[PASS] Built-in functions tests complete.")

    # --- 3. Path Variables ---
    print("\n-- Phase 3: Path Variables --")

    # Simple path variable
    results = send_and_parse(["CYPHER", f'MATCH p = (a:Person)-[r:KNOWS]->(b:Person) WHERE a._id = "{alice_id}" RETURN p'], "Path variable for A-KNOWS-B")
    assert_eq(len(results), 1, "Path variable query should return 1 row")
    path = results[0].get('p')
    assert_eq(len(path), 3, "Path should contain 3 elements (node, rel, node)")
    assert_eq(path[0].get('name'), "Alice", "Path start node name")
    assert_eq(path[1].get('since'), 2022, "Path relationship property")
    assert_eq(path[2].get('name'), "Bob", "Path end node name")

    # Path variable with variable-length path
    results = send_and_parse(["CYPHER", f'MATCH p = (a:Node {{name: "A"}})-[:PATH*2]->(c:Node {{name: "C"}}) RETURN p'], "Path variable for A-PATH*2-C")
    assert_eq(len(results), 1, "Path variable with var-length query should return 1 row")
    path = results[0].get('p')
    assert_eq(len(path), 5, "Path should contain 5 elements (node, rel, node, rel, node)")
    assert_eq(path[0].get('name'), "A", "Path start node name")
    assert_eq(path[2].get('name'), "B", "Path intermediate node name")
    assert_eq(path[4].get('name'), "C", "Path end node name")

    print("[PASS] Path variables tests complete.")

    # --- 4. Shortest Path ---
    print("\n-- Phase 4: Shortest Path --")

    # Shortest path A to D (A-B-C-D)
    results = send_and_parse(["CYPHER", f'MATCH (start:Node {{name: "A"}}), (end:Node {{name: "D"}}) RETURN shortestPath((start)-[r*]->(end)) AS path'], "Shortest path A to D")
    assert_eq(len(results), 1, "Shortest path query should return 1 row")
    path = results[0].get('path')
    assert_eq(len(path), 7, "Shortest path A-D should have 7 elements (4 nodes, 3 rels)")
    assert_eq(path[0].get('name'), "A", "Shortest path start node")
    assert_eq(path[2].get('name'), "B", "Shortest path intermediate node 1")
    assert_eq(path[4].get('name'), "C", "Shortest path intermediate node 2")
    assert_eq(path[6].get('name'), "D", "Shortest path end node")

    # Shortest path A to A (should return just A)
    results = send_and_parse(["CYPHER", f'MATCH (start:Node {{name: "A"}}), (end:Node {{name: "A"}}) RETURN shortestPath((start)-[r*]->(end)) AS path'], "Shortest path A to A")
    assert_eq(len(results), 1, "Shortest path A to A should return 1 row")
    path = results[0].get('path')
    assert_eq(len(path), 1, "Shortest path A to A should have 1 element (node A)")
    assert_eq(path[0].get('name'), "A", "Shortest path A to A node")

    # Shortest path between disconnected nodes (A to Eve)
    results = send_and_parse(["CYPHER", f'MATCH (start:Node {{name: "A"}}), (end:Person {{name: "Eve"}}) RETURN shortestPath((start)-[r*]->(end)) AS path'], "Shortest path A to Eve (disconnected)")
    assert_eq(len(results), 1, "Shortest path disconnected query should return 1 row")
    path = results[0].get('path')
    assert_eq(path, None, "Shortest path between disconnected nodes should be NULL")

    print("[PASS] Shortest path tests complete.")

    # --- 5. Cleanup ---
    print("\n-- Phase 5: Cleanup --")
    send(["FLUSHDB"])
    print("[PASS] Cleanup complete.")
