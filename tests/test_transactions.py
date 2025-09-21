import sys
from .common import send_resp_command, assert_eq, create_connection

class ConnHelper:
    """A helper class to wrap a connection or a cursor and its commands."""
    def __init__(self, conn_or_cursor, reader, name):
        self.conn_or_cursor = conn_or_cursor
        self.reader = reader
        self.name = name

    def send(self, parts):
        resp, *_ = send_resp_command(self.conn_or_cursor, self.reader, parts)
        return resp.strip()

    def get_value(self, key):
        resp = self.send(["GET", key])
        if resp == "$-1":
            return "$-1"
        if resp.startswith("$"):
            return resp.split("\r\n")[1]
        return resp

    def get_list_len(self, key):
        resp = self.send(["LLEN", key])
        if resp.startswith(":"):
            return int(resp[1:])
        return -1

def test_transactions(sock, reader, ffi_path=None):
    c1 = None
    c2 = None
    conn1 = None
    conn2 = None
    cursor1 = None
    cursor2 = None

    try:
        if ffi_path:
            print("[INFO] FFI mode: creating one connection and two cursors.")
            conn1 = sock # Use the connection passed in from the test runner
            cursor1 = conn1.cursor()
            cursor2 = conn1.cursor()
            c1 = ConnHelper(cursor1, None, "C1")
            c2 = ConnHelper(cursor2, None, "C2")
        else:
            print("[INFO] Server mode: creating two connections.")
            conn1, reader1 = sock, reader
            c1 = ConnHelper(conn1, reader1, "C1")
            conn2, reader2 = create_connection()
            if not conn2:
                print("[FAIL] Could not create a second connection for transaction tests.")
                sys.exit(1)
            c2 = ConnHelper(conn2, reader2, "C2")

        print("== Transaction Isolation Test Suite ==")

        # --- 1. Setup ---
        print("\n-- Phase 1: Setup --")
        c1.send(["FLUSHDB"])
        print("[PASS] Setup complete.")

        # --- 2. Test BEGIN, COMMIT, and DML ---
        print("\n-- Phase 2: BEGIN, COMMIT, and DML --")
        assert_eq(c1.send(["BEGIN"]), "+OK", f"{c1.name}: BEGIN a new transaction")
        assert_eq(c1.send(["SET", "tx_key1", "value1"]), "+OK", f"{c1.name}: SET tx_key1 inside transaction")
        assert_eq(c1.send(["LPUSH", "tx_list1", "item1"]), ":1", f"{c1.name}: LPUSH tx_list1 inside transaction")

        # Verify changes are visible inside the transaction (C1)
        print(f"[INFO] Verifying changes are visible inside the transaction ({c1.name})...")
        assert_eq(c1.get_value("tx_key1"), "value1", f"{c1.name}: tx_key1 should be visible inside transaction")
        assert_eq(c1.get_list_len("tx_list1"), 1, f"{c1.name}: tx_list1 should be visible inside transaction")

        # Verify changes are NOT visible outside the transaction (C2)
        print(f"[INFO] Verifying changes are NOT visible outside the transaction ({c2.name})...")
        assert_eq(c2.get_value("tx_key1"), "$-1", f"{c2.name}: tx_key1 should NOT be visible before COMMIT")
        assert_eq(c2.get_list_len("tx_list1"), 0, f"{c2.name}: tx_list1 should NOT be visible before COMMIT")

        assert_eq(c1.send(["COMMIT"]), "+OK", f"{c1.name}: COMMIT the transaction")

        # Verify changes are visible on the second connection/cursor after COMMIT
        print(f"[INFO] Verifying changes are visible on {c2.name} after COMMIT...")
        assert_eq(c2.get_value("tx_key1"), "value1", f"{c2.name}: tx_key1 should be visible after COMMIT")
        assert_eq(c2.get_list_len("tx_list1"), 1, f"{c2.name}: tx_list1 should be visible after COMMIT")
        print("[PASS] BEGIN, COMMIT, and DML tests complete.")

        # --- 3. Test BEGIN, ROLLBACK, and DML ---
        print("\n-- Phase 3: BEGIN, ROLLBACK, and DML --")
        c1.send(["FLUSHDB"]) # Clean state for rollback test
        assert_eq(c1.send(["SET", "tx_key_persist", "should_stay"]), "+OK", f"{c1.name}: SET a key that should persist")
        
        assert_eq(c1.send(["BEGIN"]), "+OK", f"{c1.name}: BEGIN another transaction for ROLLBACK")
        assert_eq(c1.send(["SET", "tx_key2", "value2"]), "+OK", f"{c1.name}: SET tx_key2 inside transaction")
        assert_eq(c1.send(["DELETE", "tx_key_persist"]), ":1", f"{c1.name}: DELETE the persistent key inside transaction")

        # Verify changes are visible inside the transaction (C1)
        assert_eq(c1.get_value("tx_key2"), "value2", f"{c1.name}: tx_key2 should be visible inside transaction")
        assert_eq(c1.get_value("tx_key_persist"), "$-1", f"{c1.name}: tx_key_persist should appear deleted inside transaction")

        # Verify changes are not visible outside the transaction (C2)
        assert_eq(c2.get_value("tx_key2"), "$-1", f"{c2.name}: tx_key2 should NOT be visible before ROLLBACK")
        assert_eq(c2.get_value("tx_key_persist"), "should_stay", f"{c2.name}: tx_key_persist should still be visible before ROLLBACK")

        assert_eq(c1.send(["ROLLBACK"]), "+OK", f"{c1.name}: ROLLBACK the transaction")

        # Verify state is reverted after ROLLBACK
        print(f"[INFO] Verifying state is reverted on {c2.name} after ROLLBACK...")
        assert_eq(c2.get_value("tx_key2"), "$-1", f"{c2.name}: tx_key2 should NOT be visible after ROLLBACK")
        assert_eq(c2.get_value("tx_key_persist"), "should_stay", f"{c2.name}: tx_key_persist should still be visible after ROLLBACK")
        print("[PASS] BEGIN, ROLLBACK, and DML tests complete.")

        # --- 4. Test Nested Transactions (should fail) ---
        print("\n-- Phase 4: Nested Transactions --")
        assert_eq(c1.send(["BEGIN"]), "+OK", f"{c1.name}: BEGIN outer transaction")
        assert_eq(c1.send(["BEGIN"]).startswith("-ERR"), True, f"{c1.name}: BEGIN inside an active transaction should fail")
        assert_eq(c1.send(["ROLLBACK"]), "+OK", f"{c1.name}: ROLLBACK outer transaction") # Clean up
        print("[PASS] Nested transaction tests complete.")

        # --- 5. Test COMMIT/ROLLBACK without active transaction ---
        print("\n-- Phase 5: COMMIT/ROLLBACK without active transaction --")
        assert_eq(c1.send(["COMMIT"]).startswith("-ERR"), True, f"{c1.name}: COMMIT without active transaction should fail")
        assert_eq(c1.send(["ROLLBACK"]).startswith("-ERR"), True, f"{c1.name}: ROLLBACK without active transaction should fail")
        print("[PASS] COMMIT/ROLLBACK without active transaction tests complete.")

        # --- 6. Test DML operations outside a transaction (auto-commit) ---
        print("\n-- Phase 6: DML outside transaction (auto-commit) --")
        c1.send(["FLUSHDB"])
        assert_eq(c1.send(["SET", "auto_key", "auto_value"]), "+OK", f"{c1.name}: SET auto_key outside transaction")
        assert_eq(c2.get_value("auto_key"), "auto_value", f"{c2.name}: auto_key should be immediately visible (auto-commit)")
        print("[PASS] DML outside transaction tests complete.")

        # --- 7. Cleanup ---
        print("\n-- Phase 7: Cleanup --")
        c1.send(["FLUSHDB"])
        print("[PASS] Cleanup complete.")

    finally:
        if cursor1: cursor1.close()
        if cursor2: cursor2.close()
        if conn1 and ffi_path: conn1.close()
        if conn2: conn2.close()
