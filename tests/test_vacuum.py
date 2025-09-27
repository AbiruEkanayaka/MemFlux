import time
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

def test_vacuum(sock, reader, ffi_path=None):
    c1 = None
    c2 = None
    conn1 = None
    conn2 = None
    cursor1 = None
    cursor2 = None

    try:
        if ffi_path:
            print("[INFO] FFI mode: creating one connection and two cursors for vacuum test.")
            conn1 = sock
            cursor1 = conn1.cursor()
            cursor2 = conn1.cursor()
            c1 = ConnHelper(cursor1, None, "C1")
            c2 = ConnHelper(cursor2, None, "C2")
        else:
            print("[INFO] Server mode: creating two connections for vacuum test.")
            conn1, reader1 = sock, reader
            c1 = ConnHelper(conn1, reader1, "C1")
            conn2, reader2 = create_connection(retry=True)
            if not conn2:
                print("[FAIL] Could not create a second connection for vacuum tests.")
                return
            c2 = ConnHelper(conn2, reader2, "C2")

        print("== MVCC Vacuum Test Suite ==")

        # --- 1. Setup ---
        print("\n-- Phase 1: Setup --")
        c1.send(["FLUSHDB"])
        time.sleep(0.1) # Give server a moment
        print("[PASS] Setup complete.")

        # --- 2. Test vacuuming a dead version ---
        print("\n-- Phase 2: Vacuuming a dead version --")
        c1.send(["SET", "key1", "value1"]) # txid 1 (auto-committed)
        c1.send(["SET", "key2", "value2"]) # txid 2 (auto-committed)
        c1.send(["SET", "key1", "value1_new"]) # txid 3 (auto-committed), expires version from txid 1
        
        # At this point, the old version of key1 is dead.
        resp = c1.send(["VACUUM"])
        assert_eq(resp, "+Removed 1 versions and 0 keys", "VACUUM should remove one dead version")
        assert_eq(c1.get_value("key1"), "value1_new", "key1 should have its new value")
        assert_eq(c1.get_value("key2"), "value2", "key2 should be unaffected")
        print("[PASS] Dead version vacuumed successfully.")

        # --- 3. Test vacuuming a dead key ---
        print("\n-- Phase 3: Vacuuming a dead key --")
        c1.send(["DELETE", "key2"]) # txid 4, expires version from txid 2
        
        # The key 'key2' is now dead.
        resp = c1.send(["VACUUM"])
        assert_eq(resp, "+Removed 1 versions and 1 keys", "VACUUM should remove one dead key")
        assert_eq(c1.get_value("key2"), "$-1", "key2 should be gone after vacuum")
        print("[PASS] Dead key vacuumed successfully.")

        # --- 4. Test vacuum with an active transaction ---
        print("\n-- Phase 4: Vacuum with an active transaction --")
        c1.send(["SET", "key3", "value3"]) # txid 5

        # Start a transaction on the second connection. It establishes a snapshot.
        c2.send(["BEGIN"]) # txid 6

        # Update a key on the first connection. This creates a version that the second connection can't see.
        c1.send(["SET", "key3", "value3_new"]) # txid 7, expires version from txid 5

        # The old version of key3 is expired by txid 7.
        # However, transaction c2 (txid 6) is still active, and its snapshot horizon (xmin) is 6.
        # Since 7 >= 6, the expired version is NOT dead yet.
        resp = c1.send(["VACUUM"])
        assert_eq(resp, "+Removed 0 versions and 0 keys", "VACUUM should not remove anything while other transaction is active")
        
        # Verify the other transaction can still see the old value
        assert_eq(c2.get_value("key3"), "value3", "Active transaction C2 should still see the old value")
        print("[PASS] Vacuum correctly blocked by active transaction.")

        # --- 5. Test vacuum after the transaction commits ---
        print("\n-- Phase 5: Vacuum after transaction commits --")
        c2.send(["COMMIT"]) # txid 6 commits.

        # Now there are no active transactions. The next vacuum run should clean up the old version of key3.
        resp = c1.send(["VACUUM"])
        assert_eq(resp, "+Removed 1 versions and 0 keys", "VACUUM should now remove the dead version of key3")
        assert_eq(c1.get_value("key3"), "value3_new", "key3 should have its new value after vacuum")
        print("[PASS] Vacuum successful after transaction commit.")

    finally:
        if cursor1: cursor1.close()
        if cursor2: cursor2.close()
        if conn2: conn2.close()
