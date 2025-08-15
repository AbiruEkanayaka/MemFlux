from .common import send_resp_command, assert_eq

def test_advanced_commands(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    print("== Advanced Commands Test Suite ==")

    # --- 1. FLUSHDB ---
    print("\n-- Phase 1: FLUSHDB --")
    send(["SET", "key_before_flush", "1"])
    send(["LPUSH", "list_before_flush", "1"])
    assert_eq(send(["FLUSHDB"]), "+OK", "FLUSHDB command")
    assert_eq(send(["GET", "key_before_flush"]), "$-1", "Key should be gone after FLUSHDB")
    assert_eq(send(["LLEN", "list_before_flush"]), ":0", "List should be gone after FLUSHDB")
    print("[PASS] FLUSHDB tests complete")

    # --- 2. KEYS ---
    print("\n-- Phase 2: KEYS --")
    send(["SET", "keys_test:1", "a"])
    send(["SET", "keys_test:2", "b"])
    send(["SET", "keys_another:3", "c"])
    send(["SET", "keys_test:10", "d"])

    resp = send(["KEYS", "keys_test:*"])
    lines = resp.splitlines()
    assert_eq(lines[0], "*3", "KEYS keys_test:* count")
    members = set(lines[2::2])
    assert_eq(members, {"keys_test:1", "keys_test:2", "keys_test:10"}, "KEYS keys_test:* members")

    resp = send(["KEYS", "keys_test:?"])
    lines = resp.splitlines()
    assert_eq(lines[0], "*2", "KEYS keys_test:? count")
    members = set(lines[2::2])
    assert_eq(members, {"keys_test:1", "keys_test:2"}, "KEYS keys_test:? members")

    send(["DELETE", "keys_test:1", "keys_test:2", "keys_another:3", "keys_test:10"])
    print("[PASS] KEYS tests complete")

    # --- 3. JSON.DEL ---
    print("\n-- Phase 3: JSON.DEL --")
    send(["JSON.SET", "json_del_test", '{\"a\": 1, \"b\": {\"c\": 2}, \"d\": 3}']) # Corrected escaping for JSON string
    assert_eq(send(["JSON.DEL", "json_del_test.b.c"]), ":1", "JSON.DEL existing nested key")
    assert_eq(send(["JSON.GET", "json_del_test"]), '$20\r\n{\"a\":1,\"b\":{},\"d\":3}', "Verify nested key deleted") # Corrected escaping for JSON string
    assert_eq(send(["JSON.DEL", "json_del_test.d"]), ":1", "JSON.DEL existing top-level key")
    assert_eq(send(["JSON.GET", "json_del_test"]), '$14\r\n{"a":1,"b":{}}', "Verify top-level key deleted") # Corrected escaping for JSON string
    assert_eq(send(["JSON.DEL", "json_del_test.nonexistent"]), ":0", "JSON.DEL non-existent key")
    send(["DELETE", "json_del_test"])
    print("[PASS] JSON.DEL tests complete")

    # --- 4. List advanced POP and LLEN ---
    print("\n-- Phase 4: Advanced List Commands --")
    send(["DELETE", "list_adv_test"])
    send(["RPUSH", "list_adv_test", "a", "b", "c", "d", "e"])
    assert_eq(send(["LLEN", "list_adv_test"]), ":5", "LLEN should be 5")
    
    resp = send(["LPOP", "list_adv_test", "2"])
    lines = resp.splitlines()
    assert_eq(lines[0], "*2", "LPOP count 2 returns 2 elements")
    popped = [lines[2], lines[4]]
    assert_eq(popped, ["a", "b"], "LPOP count 2 returns correct elements")
    assert_eq(send(["LLEN", "list_adv_test"]), ":3", "LLEN should be 3 after LPOP 2")

    resp = send(["RPOP", "list_adv_test", "2"])
    lines = resp.splitlines()
    assert_eq(lines[0], "*2", "RPOP count 2 returns 2 elements")
    popped = [lines[2], lines[4]]
    assert_eq(popped, ["e", "d"], "RPOP count 2 returns correct elements")
    assert_eq(send(["LLEN", "list_adv_test"]), ":1", "LLEN should be 1 after RPOP 2")
    send(["DELETE", "list_adv_test"])
    print("[PASS] Advanced List command tests complete")

    # --- 5. Set SCARD ---
    print("\n-- Phase 5: Set SCARD --")
    send(["DELETE", "set_scard_test"])
    send(["SADD", "set_scard_test", "a", "b", "c"])
    assert_eq(send(["SCARD", "set_scard_test"]), ":3", "SCARD should be 3")
    send(["SREM", "set_scard_test", "c"])
    assert_eq(send(["SCARD", "set_scard_test"]), ":2", "SCARD should be 2 after SREM")
    assert_eq(send(["SCARD", "nonexistent_set"]), ":0", "SCARD on non-existent set is 0")
    send(["DELETE", "set_scard_test"])
    print("[PASS] SCARD tests complete")

    # --- 6. Index Management ---
    print("\n-- Phase 6: Index Management (IDX.LIST, IDX.DROP) --")
    send(["DELETE", "idx_mgmt_test:1"])
    send(["JSON.SET", "idx_mgmt_test:1", '{\"city\": \"SF\"}']) # Corrected escaping for JSON string
    send(["CREATEINDEX", "idx_mgmt_test:*", "ON", "city"])
    
    resp = send(["IDX.LIST"])
    assert_eq("idx_mgmt_test:*|city" in resp, True, "IDX.LIST should contain the new index")

    # The name for IDX.DROP is fabricated in commands.rs from the prefix and path
    fabricated_name = "idx_mgmt_test:_city"
    assert_eq(send(["IDX.DROP", fabricated_name]), ":1", "IDX.DROP should return 1 for a successful drop")
    
    resp_after_drop = send(["IDX.LIST"])
    assert_eq("idx_mgmt_test:*|city" not in resp_after_drop, True, "IDX.LIST should not contain the dropped index")
    
    assert_eq(send(["IDX.DROP", "nonexistent_index"]), ":0", "IDX.DROP on non-existent index should return 0")
    send(["DELETE", "idx_mgmt_test:1"])
    print("[PASS] Index Management tests complete")

    # --- 7. Trivial Server Commands ---
    print("\n-- Phase 7: Trivial Server Commands (PING, SAVE) --")
    assert_eq(send(["PING"]), "+PONG", "PING should return PONG")
    assert_eq(send(["SAVE"]), "+OK", "SAVE should return OK (no-op)")
    print("[PASS] Trivial server command tests complete")
