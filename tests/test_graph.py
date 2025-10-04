
import json
from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_graph(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    print("== Graph Commands Test Suite ==")

    # --- 1. Cleanup ---
    print("\n-- Phase 1: Cleanup --")
    send(["FLUSHDB"])
    print("[PASS] Cleanup complete.")

    # --- 2. Node Operations ---
    print("\n-- Phase 2: Node Operations (ADDNODE, GETNODE) --")
    
    # Add node 'person:Alice'
    alice_props = '{"name": "Alice", "age": 30}'
    resp = send(["GRAPH.ADDNODE", "Person", alice_props])
    assert_eq(resp.startswith("$"), True, "ADDNODE should return a bulk string (ID)")
    alice_id = resp.splitlines()[1]
    print(f"[INFO] Created Node Alice with ID: {alice_id}")

    # Add node 'person:Bob'
    bob_props = '{"name": "Bob", "age": 25}'
    resp = send(["GRAPH.ADDNODE", "Person", bob_props])
    bob_id = resp.splitlines()[1]
    print(f"[INFO] Created Node Bob with ID: {bob_id}")

    # Get Alice's properties
    resp = send(["GRAPH.GETNODE", alice_id])
    resp_json = extract_json_from_bulk(resp.encode('utf-8'))
    assert_eq(resp_json, json.loads(alice_props), "GETNODE should retrieve correct properties for Alice")

    # Get non-existent node
    resp = send(["GRAPH.GETNODE", "non-existent-id"])
    assert_eq(resp, "$-1", "GETNODE on non-existent ID should be nil")
    print("[PASS] Node operations complete.")

    # --- 3. Relationship Operations ---
    print("\n-- Phase 3: Relationship Operations (ADDREL, GETRELS) --")

    # Add relationship: Alice KNOWS Bob
    knows_props = '{"since": "2022"}'
    resp = send(["GRAPH.ADDREL", alice_id, bob_id, "KNOWS", knows_props])
    assert_eq(resp.startswith("$"), True, "ADDREL should return a bulk string (ID)")
    knows_rel_id = resp.splitlines()[1]
    print(f"[INFO] Created Relationship KNOWS with ID: {knows_rel_id}")

    # Add another relationship: Alice LIKES a new Software node
    software_props = '{"name": "MemFluxDB"}'
    resp = send(["GRAPH.ADDNODE", "Software", software_props])
    software_id = resp.splitlines()[1]
    print(f"[INFO] Created Node Software with ID: {software_id}")
    
    likes_props = '{"weight": 0.9}'
    resp = send(["GRAPH.ADDREL", alice_id, software_id, "LIKES", likes_props])
    likes_rel_id = resp.splitlines()[1]
    print(f"[INFO] Created Relationship LIKES with ID: {likes_rel_id}")

    # GETRELS for Alice (OUT)
    resp = send(["GRAPH.GETRELS", alice_id, "OUT"])
    lines = resp.splitlines()
    assert_eq(lines[0], "*2", "GETRELS OUT should return 2 relationships for Alice")

    all_rels = []
    for i in range(int(lines[0][1:])):
        bulk_string = lines[i*2+1] + '\r\n' + lines[i*2+2]
        all_rels.append(extract_json_from_bulk(bulk_string.encode('utf-8')))
    
    found_knows = any("since" in r and r["since"] == "2022" for r in all_rels)
    found_likes = any("weight" in r and r["weight"] == 0.9 for r in all_rels)
    assert_eq(found_knows and found_likes, True, "GETRELS OUT for Alice returns correct relationships")

    # GETRELS for Bob (IN, type KNOWS)
    resp = send(["GRAPH.GETRELS", bob_id, "IN", "KNOWS"])
    lines = resp.splitlines()
    assert_eq(lines[0], "*1", "GETRELS IN KNOWS for Bob should return 1 relationship")
    rel_json = extract_json_from_bulk((lines[1] + '\r\n' + lines[2]).encode('utf-8'))
    assert_eq("since" in rel_json and rel_json["since"] == "2022", True, "GETRELS IN KNOWS for Bob returns correct relationship")

    # GETRELS for Bob (BOTH)
    resp = send(["GRAPH.GETRELS", bob_id, "BOTH"])
    lines = resp.splitlines()
    assert_eq(lines[0], "*1", "GETRELS BOTH for Bob should return 1 relationship")

    print("[PASS] Relationship operations complete.")

    # --- 4. Deletion ---
    print("\n-- Phase 4: Deletion (DELETE) --")

    # Delete relationship
    resp = send(["GRAPH.DELETE", knows_rel_id])
    assert_eq(resp, ":1", "DELETE on relationship ID should return 1")
    
    # Verify relationship is gone
    resp = send(["GRAPH.GETRELS", alice_id, "OUT", "KNOWS"])
    assert_eq(resp, "*0", "KNOWS relationship should be gone")
    resp = send(["GRAPH.GETRELS", bob_id, "IN", "KNOWS"])
    assert_eq(resp, "*0", "Incoming KNOWS relationship should be gone")

    # Delete node
    resp = send(["GRAPH.DELETE", alice_id])
    assert_eq(resp, ":1", "DELETE on node ID should return 1")

    # Verify node is gone
    resp = send(["GRAPH.GETNODE", alice_id])
    assert_eq(resp, "$-1", "Alice node should be gone")

    # Verify associated relationships are also gone (dangling edge check)
    # The current implementation of GRAPH.DELETE does not cascade.
    resp = send(["GRAPH.GETRELS", software_id, "IN"])
    assert_eq(resp.splitlines()[0], "*1", "LIKES relationship should still exist (dangling)")
    
    # Delete non-existent ID
    resp = send(["GRAPH.DELETE", "non-existent-id"])
    assert_eq(resp, ":0", "DELETE on non-existent ID should return 0")

    print("[PASS] Deletion operations complete.")
