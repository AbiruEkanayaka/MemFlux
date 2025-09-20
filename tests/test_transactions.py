from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_transactions(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    def get_value(key):
        resp = send(["GET", key])
        if resp == "$-1":
            return "$-1"
        if resp.startswith("$"):
            return resp.split("\r\n")[1]
        return resp

    def get_json_value(key, path):
        # JSON.GET expects a single argument in the form "key.path"
        full_path = f"{key}{path}" if path.startswith('.') else f"{key}.{path}"
        if path == ".":
            full_path = key

        resp = send(["JSON.GET", full_path])
        if resp == "$-1":
            return "$-1"
        if resp.startswith("$"):
            return resp.split("\r\n")[1]
        return resp

    def get_list_len(key):
        resp = send(["LLEN", key])
        if resp.startswith(":"):
            return int(resp[1:])
        return -1

    def get_set_members(key):
        resp = send(["SMEMBERS", key])
        if resp.startswith("*"):
            lines = resp.split("\r\n")
            members = set()
            # Start from index 2 to skip the array count and first bulk string length
            i = 1
            while i < len(lines) -1:
                if lines[i].startswith("$"):
                    # The actual member is on the next line
                    members.add(lines[i+1])
                i += 2
            return members
        return set()

    print("== Transaction Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Setup --")
    send(["FLUSHDB"])
    print("[PASS] Setup complete.")

    # --- 2. Test BEGIN, COMMIT, and DML ---
    print("\n-- Phase 2: BEGIN, COMMIT, and DML --")
    assert_eq(send(["BEGIN"]), "+OK", "BEGIN a new transaction")
    assert_eq(send(["SET", "tx_key1", "value1"]), "+OK", "SET tx_key1 inside transaction")
    assert_eq(send(["LPUSH", "tx_list1", "item1", "item2"]), "+OK", "LPUSH tx_list1 inside transaction")
    assert_eq(send(["SADD", "tx_set1", "member1"]), "+OK", "SADD tx_set1 inside transaction")
    assert_eq(send(["JSON.SET", "tx_json1", '{"a":1}']), "+OK", "JSON.SET tx_json1 inside transaction")

    # Verify changes are not visible outside the transaction yet
    assert_eq(get_value("tx_key1"), "$-1", "tx_key1 should not be visible before COMMIT")
    assert_eq(get_list_len("tx_list1"), 0, "tx_list1 should not be visible before COMMIT")
    assert_eq(len(get_set_members("tx_set1")), 0, "tx_set1 should not be visible before COMMIT")
    assert_eq(get_json_value("tx_json1", "."), "$-1", "tx_json1 should not be visible before COMMIT")

    assert_eq(send(["COMMIT"]), "+OK", "COMMIT the transaction")

    # Verify changes are visible after COMMIT
    assert_eq(get_value("tx_key1"), "value1", "tx_key1 should be visible after COMMIT")
    assert_eq(get_list_len("tx_list1"), 2, "tx_list1 should be visible after COMMIT")
    assert_eq(get_set_members("tx_set1"), {"member1"}, "tx_set1 should be visible after COMMIT")
    assert_eq(get_json_value("tx_json1", "."), '{"a":1}', "tx_json1 should be visible after COMMIT")
    print("[PASS] BEGIN, COMMIT, and DML tests complete.")

    # --- 3. Test BEGIN, ROLLBACK, and DML ---
    print("\n-- Phase 3: BEGIN, ROLLBACK, and DML --")
    send(["FLUSHDB"]) # Clean state for rollback test
    assert_eq(send(["BEGIN"]), "+OK", "BEGIN another transaction for ROLLBACK")
    assert_eq(send(["SET", "tx_key2", "value2"]), "+OK", "SET tx_key2 inside transaction")
    assert_eq(send(["LPUSH", "tx_list2", "itemA"]), "+OK", "LPUSH tx_list2 inside transaction")
    assert_eq(send(["DELETE", "tx_key1"]), "+OK", "DELETE tx_key1 (should not exist)") # Delete a non-existent key

    # Verify changes are not visible outside the transaction yet
    assert_eq(get_value("tx_key2"), "$-1", "tx_key2 should not be visible before ROLLBACK")

    assert_eq(send(["ROLLBACK"]), "+OK", "ROLLBACK the transaction")

    # Verify changes are NOT visible after ROLLBACK
    assert_eq(get_value("tx_key2"), "$-1", "tx_key2 should NOT be visible after ROLLBACK")
    assert_eq(get_list_len("tx_list2"), 0, "tx_list2 should NOT be visible after ROLLBACK")
    print("[PASS] BEGIN, ROLLBACK, and DML tests complete.")

    # --- 4. Test Nested Transactions (should fail) ---
    print("\n-- Phase 4: Nested Transactions --")
    assert_eq(send(["BEGIN"]), "+OK", "BEGIN outer transaction")
    assert_eq(send(["BEGIN"]).startswith("-ERR"), True, "BEGIN inside an active transaction should fail")
    assert_eq(send(["ROLLBACK"]), "+OK", "ROLLBACK outer transaction") # Clean up
    print("[PASS] Nested transaction tests complete.")

    # --- 5. Test COMMIT/ROLLBACK without active transaction ---
    print("\n-- Phase 5: COMMIT/ROLLBACK without active transaction --")
    assert_eq(send(["COMMIT"]).startswith("-ERR"), True, "COMMIT without active transaction should fail")
    assert_eq(send(["ROLLBACK"]).startswith("-ERR"), True, "ROLLBACK without active transaction should fail")
    print("[PASS] COMMIT/ROLLBACK without active transaction tests complete.")

    # --- 6. Test DML operations outside a transaction (auto-commit) ---
    print("\n-- Phase 6: DML outside transaction (auto-commit) --")
    send(["FLUSHDB"])
    assert_eq(send(["SET", "auto_key", "auto_value"]), "+OK", "SET auto_key outside transaction")
    assert_eq(get_value("auto_key"), "auto_value", "auto_key should be immediately visible (auto-commit)")
    print("[PASS] DML outside transaction tests complete.")

    # --- 7. Cleanup ---
    print("\n-- Phase 7: Cleanup --")
    send(["FLUSHDB"])
    print("[PASS] Cleanup complete.")
