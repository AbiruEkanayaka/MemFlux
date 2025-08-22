import os
import time
import socket
from .common import send_resp_command, assert_eq, create_connection

def test_recovery(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    print("== Persistence and Recovery Test Suite ==")
    
    # --- Phase 1: Setup and Initial Data ---
    print("\n-- Phase 1: Setup --")
    print("[INFO] This test requires a manual server restart.")
    
    # Clean up from previous runs
    if os.path.exists("memflux.snapshot"):
        os.remove("memflux.snapshot")
    if os.path.exists("memflux.snapshot.tmp"):
        os.remove("memflux.snapshot.tmp")
    if os.path.exists("memflux.wal"):
        # Can't delete if server has it open, but we can truncate it
        # by sending a FLUSHDB command.
        send(["FLUSHDB"])
        # Give it a moment to process
        time.sleep(0.5)

    print("[INFO] Writing initial data (will be in snapshot)...")
    assert_eq(send(["SET", "key_in_snapshot", "snapshot_value"]), "+OK", "Set snapshot key")
    assert_eq(send(["JSON.SET", "user:snapshot", '{"name": "snap"}' ]), "+OK", "Set snapshot JSON key")
    
    # --- Phase 2: Trigger Snapshot ---
    print("\n-- Phase 2: Trigger Snapshot --")
    # Write enough data to trigger a snapshot (threshold is 128MB)
    large_value = 's' * (256 * 1024) # 256 KB
    num_writes = 70
    print(f"[INFO] Writing {num_writes} * 256KB to trigger snapshot...")
    for i in range(num_writes):
        send(["SET", f"snapshot_trigger_{i}", large_value])
    
    print("[INFO] Writes sent. Waiting up to 10 seconds for snapshot file...")
    snapshot_created = False
    for _ in range(10):
        if os.path.exists("memflux.snapshot"):
            snapshot_created = True
            break
        time.sleep(1)
    assert_eq(snapshot_created, True, "Snapshot file was created")
    if not snapshot_created:
        print("[FAIL] Snapshot was not created. Aborting recovery test.")
        return

    # --- Phase 3: Write WAL-only data ---
    print("\n-- Phase 3: Write WAL-only Data --")
    print("[INFO] Writing data that should only exist in the WAL after the snapshot...")
    assert_eq(send(["SET", "key_in_wal", "wal_value"]), "+OK", "Set WAL key")
    assert_eq(send(["JSON.SET", "user:wal", '{"name": "wal"}' ]), "+OK", "Set WAL JSON key")
    # Also test a deletion
    assert_eq(send(["DELETE", "key_in_snapshot"]), ":1", "Delete snapshot key (logged in WAL)")
    print("[PASS] WAL data written.")

    # --- Phase 4: Manual Restart and Verification ---
    print("\n" + "="*50)
    print("!! ACTION REQUIRED !!")
    print("1. Stop the MemFlux server now (e.g., with Ctrl+C).")
    print("2. Restart the MemFlux server with the same command (e.g., ./target/release/memflux).")
    print("3. The test will automatically continue once the server is back online.")
    print("="*50)
    
    # Close the current connection
    sock.close()

    # Wait for server to come back online
    while True:
        sock, reader = create_connection(retry=True)
        if sock:
            print("\n[INFO] Reconnected to server. Proceeding with verification...")
            break
        time.sleep(1)
        print(".", end="", flush=True)

    # --- Phase 5: Verification ---
    print("\n-- Phase 5: Verifying Recovered State --")
    
    # Check WAL-only key
    resp_wal = send(["GET", "key_in_wal"])
    assert_eq(resp_wal, "$9\r\nwal_value", "Key from WAL should exist")

    # Check WAL-only JSON key
    resp_wal_json = send(["JSON.GET", "user:wal.name"])
    assert_eq(resp_wal_json, "$3\r\nwal", "JSON key from WAL should exist")

    # Check snapshot key (that was NOT deleted)
    resp_snap_json = send(["JSON.GET", "user:snapshot.name"])
    assert_eq(resp_snap_json, "$4\r\nsnap", "JSON key from snapshot should exist")

    # Check key that was deleted in WAL
    resp_deleted = send(["GET", "key_in_snapshot"])
    assert_eq(resp_deleted, "$-1", "Key deleted in WAL should NOT exist")

    print("\n[PASS] Data recovery test successful!")

    # --- Final Cleanup ---
    print("\n-- Final Cleanup --")
    send(["DELETE", "key_in_wal", "user:wal", "user:snapshot"])
    for i in range(num_writes):
        send(["DELETE", f"snapshot_trigger_{i}"])
    
    return sock, reader # Return the new connection objects
