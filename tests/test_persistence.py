import os
import time
from .common import send_resp_command, assert_eq

def test_snapshot(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    print("== Snapshot Trigger Test (2 Snapshots) ==")
    
    print("[INFO] This test works best if the server was started fresh.")
    print("[INFO] It will attempt to fill the WAL twice to trigger two snapshots.")

    # Clean up from previous runs
    if os.path.exists("memflux.snapshot"):
        print("[WARN] Snapshot file already exists. Deleting it for a clean test run.")
        os.remove("memflux.snapshot")
    if os.path.exists("memflux.wal"):
        # We can't just delete the WAL as the server has it open.
        # But we can check its size and hope it's small.
        # A proper test would restart the server.
        print(f"[WARN] WAL file exists (size: {os.path.getsize('memflux.wal')}). Test results may vary.")

    # The WAL threshold is 16MB. Let's write more than that.
    # A large value to write repeatedly. 256KB.
    # 16 * 1024 * 1024 / (256 * 1024) = 64 writes. Let's do 70 to be safe.
    large_value = 'x' * (256 * 1024) # 256 KB
    num_writes = 70
    all_keys_to_clean = []

    # --- Trigger First Snapshot ---
    print(f"\n-- Phase 1: Triggering first snapshot --")
    print(f"[INFO] Writing {num_writes} keys with a 256KB value each...")
    keys_phase1 = []
    for i in range(num_writes):
        key = f"snapshot_test_key_p1_{i}"
        keys_phase1.append(key)
        send(["SET", key, large_value])
        if (i + 1) % 10 == 0:
            print(f"[INFO] ... {i+1}/{num_writes} writes completed.")
    all_keys_to_clean.extend(keys_phase1)
    
    print("[INFO] All writes for phase 1 sent. Waiting up to 10 seconds for snapshot...")
    
    snapshot_created = False
    for _ in range(10): # Wait up to 10 seconds
        if os.path.exists("memflux.snapshot"):
            snapshot_created = True
            break
        time.sleep(1)

    assert_eq(snapshot_created, True, "Phase 1: Snapshot file 'memflux.snapshot' was created")
    
    # Give a moment for the WAL truncation to complete
    time.sleep(2)
    first_snapshot_mtime = os.path.getmtime("memflux.snapshot")
    print(f"[INFO] First snapshot created at timestamp: {first_snapshot_mtime}")
    
    if os.path.exists("memflux.wal"):
        wal_size_after_snap1 = os.path.getsize("memflux.wal")
        print(f"[INFO] WAL size after first snapshot is {wal_size_after_snap1} bytes.")
        # The WAL might not be perfectly zero if other background tasks or timing issues occur
        assert_eq(wal_size_after_snap1 < 1024, True, "Phase 1: WAL file was compacted (size is small)")
    else:
        print("[INFO] WAL file does not exist after first snapshot, which is acceptable.")

    # --- Trigger Second Snapshot ---
    print(f"\n-- Phase 2: Triggering second snapshot --")
    print(f"[INFO] Writing {num_writes} new keys with a 256KB value each...")
    keys_phase2 = []
    for i in range(num_writes):
        key = f"snapshot_test_key_p2_{i}"
        keys_phase2.append(key)
        send(["SET", key, large_value])
        if (i + 1) % 10 == 0:
            print(f"[INFO] ... {i+1}/{num_writes} writes completed.")
    all_keys_to_clean.extend(keys_phase2)

    print("[INFO] All writes for phase 2 sent. Waiting up to 10 seconds for new snapshot...")

    second_snapshot_created = False
    second_snapshot_mtime = 0
    for _ in range(10): # Wait up to 10 seconds
        if os.path.exists("memflux.snapshot"):
            second_snapshot_mtime = os.path.getmtime("memflux.snapshot")
            if second_snapshot_mtime > first_snapshot_mtime:
                second_snapshot_created = True
                break
        time.sleep(1)

    assert_eq(second_snapshot_created, True, "Phase 2: Snapshot file was updated (new modification time)")
    if second_snapshot_created:
        print(f"[INFO] Second snapshot created at timestamp: {second_snapshot_mtime}")

    # --- Cleanup ---
    print("\n-- Phase 3: Cleanup --")
    print(f"[INFO] Cleaning up {len(all_keys_to_clean)} test keys...")
    deleted_count = 0
    for key in all_keys_to_clean:
        resp = send(["DELETE", key])
        if resp == ":1":
            deleted_count += 1
    print(f"[INFO] Deleted {deleted_count}/{len(all_keys_to_clean)} keys.")








