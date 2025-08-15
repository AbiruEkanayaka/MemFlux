from .common import send_resp_command, assert_eq

def test_index_maintenance(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    print("== Index Maintenance Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Data and Index Setup --")
    send(["DELETE", "idx_maint:1", "idx_maint:2", "idx_maint:3"])
    # Drop index if it exists from a previous failed run
    send(["IDX.DROP", "idx_maint_city"])
    
    send(["JSON.SET", "idx_maint:1", '{"city": "SF", "state": "CA"}'])
    send(["JSON.SET", "idx_maint:2", '{"city": "SF", "state": "CA"}'])
    send(["JSON.SET", "idx_maint:3", '{"city": "NY", "state": "NY"}'])
    
    resp = send(["CREATEINDEX", "idx_maint:*", "ON", "city"])
    assert_eq(resp, "+OK", "CREATEINDEX on city")
    print("[PASS] Setup complete")

    # --- 2. Verify Backfill and Initial State ---
    print("\n-- Phase 2: Verify Index Backfill --")
    # We can't inspect the index directly, but we can query it
    resp = send(["SQL", "SELECT", "id", "FROM", "idx_maint", "WHERE", "city='SF'"])
    assert_eq(resp.count("$"), 2, "Query for 'SF' should return 2 results from backfilled index")
    
    resp = send(["SQL", "SELECT", "id", "FROM", "idx_maint", "WHERE", "city='NY'"])
    assert_eq(resp.count("$"), 1, "Query for 'NY' should return 1 result from backfilled index")
    print("[PASS] Backfill verification complete")

    # --- 3. Test Index Update on JSON.SET ---
    print("\n-- Phase 3: Verify Index Update on JSON.SET --")
    # Change a value from SF to NY
    send(["JSON.SET", "idx_maint:1.city", '"NY"'])
    
    # Now, SF should have 1 result and NY should have 2
    resp = send(["SQL", "SELECT", "id", "FROM", "idx_maint", "WHERE", "city='SF'"])
    assert_eq(resp.count("$"), 1, "Query for 'SF' should return 1 result after update")
    
    resp = send(["SQL", "SELECT", "id", "FROM", "idx_maint", "WHERE", "city='NY'"])
    assert_eq(resp.count("$"), 2, "Query for 'NY' should return 2 results after update")
    print("[PASS] Index update on JSON.SET verified")

    # --- 4. Test Index Update on DELETE ---
    print("\n-- Phase 4: Verify Index Update on DELETE --")
    # Delete one of the NY keys
    send(["DELETE", "idx_maint:3"])
    
    # Now, NY should have only 1 result
    resp = send(["SQL", "SELECT", "id", "FROM", "idx_maint", "WHERE", "city='NY'"])
    assert_eq(resp.count("$"), 1, "Query for 'NY' should return 1 result after DELETE")
    print("[PASS] Index update on DELETE verified")

    # --- 5. Cleanup ---
    print("\n-- Phase 5: Cleanup --")
    send(["IDX.DROP", "idx_maint_city"])
    send(["DELETE", "idx_maint:1", "idx_maint:2"])
    print("[PASS] Cleanup complete")

    # --- 6. Verify Correctness of Non-Equality Queries on Indexed Fields ---
    print("\n-- Phase 6: Verify Non-Equality Queries on Indexed Fields --")
    send(["DELETE", "idx_lim:1", "idx_lim:2", "idx_lim:3"])
    send(["IDX.DROP", "idx_lim_age"])

    send(["JSON.SET", "idx_lim:1", '{"age": 25, "name": "Alice"}'])
    send(["JSON.SET", "idx_lim:2", '{"age": 35, "name": "Bob"}'])
    send(["JSON.SET", "idx_lim:3", '{"age": 45, "name": "Carol"}'])
    send(["CREATEINDEX", "idx_lim:*", "ON", "age"])

    # Test a range query. This will use a table scan, but should still be correct.
    resp_gt = send(["SQL", "SELECT", "name", "FROM", "idx_lim", "WHERE", "age", ">", "30"])
    assert_eq("Bob" in resp_gt and "Carol" in resp_gt, True, "Query with '>' on indexed field returns correct data")
    assert_eq(resp_gt.count("$"), 2, "Query with '>' on indexed field returns correct count")

    # Test a LIKE query on an indexed string field
    send(["IDX.DROP", "idx_lim_name"])
    send(["CREATEINDEX", "idx_lim:*", "ON", "name"])
    resp_like = send(["SQL", "SELECT", "name", "FROM", "idx_lim", "WHERE", "name", "LIKE", "'A%'"])
    assert_eq("Alice" in resp_like, True, "Query with 'LIKE' on indexed field returns correct data")
    assert_eq(resp_like.count("$"), 1, "Query with 'LIKE' on indexed field returns correct count")

    # Cleanup
    send(["IDX.DROP", "idx_lim_age"])
    send(["IDX.DROP", "idx_lim_name"])
    send(["DELETE", "idx_lim:1", "idx_lim:2", "idx_lim:3"])
    print("[PASS] Non-equality query correctness tests complete")
