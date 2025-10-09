from .common import send_resp_command, assert_eq, extract_json_from_bulk
import json

def test_table_commands(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    def send_and_parse_sql(sql, description):
        resp = send(["SQL", sql])
        if resp.startswith("-"):
            print(f"[FAIL] {description}: {resp}")
            return []
        if not resp.startswith("*"):
            return []
        lines = resp.splitlines()
        return [extract_json_from_bulk((lines[i] + '\r\n' + lines[i+1]).encode('utf-8')) for i in range(1, len(lines), 2)]

    print("== Low-Level Table and Row Commands Test Suite ==")

    # --- 1. Cleanup ---
    print("\n-- Phase 1: Cleanup --")
    send(["FLUSHDB"])
    print("[PASS] Cleanup complete.")

    # --- 2. TABLE.CREATE ---
    print("\n-- Phase 2: TABLE.CREATE --")
    schema_json = '''{
        "table_name": "test_table",
        "columns": {
            "id": {"type": "INTEGER", "nullable": false},
            "name": {"type": "TEXT", "nullable": true}
        },
        "constraints": [
            {"PrimaryKey": {"name": "pk_test_table", "columns": ["id"]}}
        ]
    }'''
    assert_eq(send(["TABLE.CREATE", "test_table", schema_json]), "+OK", "TABLE.CREATE should succeed")
    assert_eq(send(["TABLE.CREATE", "test_table", schema_json]).startswith("-ERR"), True, "TABLE.CREATE on existing table should fail")
    
    # SQL Verification
    sql_results = send_and_parse_sql("SELECT * FROM test_table", "SQL check on newly created table")
    assert_eq(len(sql_results), 0, "SQL SELECT on new table should return 0 rows")
    print("[PASS] TABLE.CREATE tests complete.")

    # --- 3. TABLE.DESCRIBE ---
    print("\n-- Phase 3: TABLE.DESCRIBE --")
    resp = send(["TABLE.DESCRIBE", "test_table"])
    described_schema = extract_json_from_bulk(resp.encode('utf-8'))
    assert_eq(described_schema['table_name'], "test_table", "TABLE.DESCRIBE returns correct table name")
    assert_eq("id" in described_schema['columns'], True, "TABLE.DESCRIBE includes id column")
    assert_eq(send(["TABLE.DESCRIBE", "non_existent_table"]).startswith("-ERR"), True, "TABLE.DESCRIBE on non-existent table should fail")
    print("[PASS] TABLE.DESCRIBE tests complete.")

    # --- 4. ROW Commands ---
    print("\n-- Phase 4: ROW.* Commands --")
    row1_json = '''{"id": 1, "name": "Alice"}'''
    row2_json = '''{"id": 2, "name": "Bob"}'''
    assert_eq(send(["ROW.SET", "test_table", "1", row1_json]), "+OK", "ROW.SET for row 1")
    assert_eq(send(["ROW.SET", "test_table", "2", row2_json]), "+OK", "ROW.SET for row 2")

    # SQL Verification for ROW.SET
    sql_results_1 = send_and_parse_sql("SELECT name FROM test_table WHERE id = 1", "SQL check for ROW.SET id=1")
    assert_eq(sql_results_1[0]['name'], "Alice", "SQL verifies ROW.SET for name=Alice")

    resp = send(["ROW.GET", "test_table", "1"])
    retrieved_row1 = extract_json_from_bulk(resp.encode('utf-8'))
    assert_eq(retrieved_row1, json.loads(row1_json), "ROW.GET should retrieve correct data for row 1")
    
    assert_eq(send(["ROW.GET", "test_table", "99"]), "$-1", "ROW.GET for non-existent row should be nil")

    assert_eq(send(["ROW.SETPROP", "test_table", "1", "name", '"Alice-updated"']), "+OK", "ROW.SETPROP should succeed")
    
    # SQL Verification for ROW.SETPROP
    sql_results_updated = send_and_parse_sql("SELECT name FROM test_table WHERE id = 1", "SQL check for ROW.SETPROP")
    assert_eq(sql_results_updated[0]['name'], "Alice-updated", "SQL verifies ROW.SETPROP update")
    print("[PASS] ROW.* command tests complete.")

    # --- 5. TABLE.SCAN ---
    print("\n-- Phase 5: TABLE.SCAN --")
    resp = send(["TABLE.SCAN", "test_table"])
    lines = resp.splitlines()
    assert_eq(lines[0], "*2", "TABLE.SCAN should return 2 rows")
    scan_results = [extract_json_from_bulk((lines[i] + '\r\n' + lines[i+1]).encode('utf-8')) for i in range(1, len(lines), 2)]
    names = {r['name'] for r in scan_results}
    assert_eq(names, {"Alice-updated", "Bob"}, "TABLE.SCAN should return the correct data")

    # SQL Verification for TABLE.SCAN
    sql_results_scan = send_and_parse_sql("SELECT name FROM test_table", "SQL check to verify TABLE.SCAN")
    sql_names = {r['name'] for r in sql_results_scan}
    assert_eq(sql_names, {"Alice-updated", "Bob"}, "SQL SELECT * results match TABLE.SCAN")
    print("[PASS] TABLE.SCAN tests complete.")

    # --- 6. ROW.DELETE ---
    print("\n-- Phase 6: ROW.DELETE --")
    assert_eq(send(["ROW.DELETE", "test_table", "1"]), ":1", "ROW.DELETE should return 1 for success")
    assert_eq(send(["ROW.GET", "test_table", "1"]), "$-1", "ROW.GET on deleted row should be nil")
    
    # SQL Verification for ROW.DELETE
    sql_results_delete = send_and_parse_sql("SELECT * FROM test_table WHERE id = 1", "SQL check for deleted row")
    assert_eq(len(sql_results_delete), 0, "SQL SELECT on deleted row should return 0 rows")
    sql_count = send_and_parse_sql("SELECT COUNT(*) FROM test_table", "SQL COUNT after delete")
    assert_eq(sql_count[0]['COUNT(*)'], 1, "SQL COUNT(*) should be 1 after delete")
    print("[PASS] ROW.DELETE tests complete.")

    # --- 7. TABLE.DROP ---
    print("\n-- Phase 7: TABLE.DROP --")
    resp = send(["TABLE.DROP", "test_table"])
    assert_eq(resp.startswith("+OK"), True, "TABLE.DROP should succeed")
    assert_eq(send(["TABLE.DESCRIBE", "test_table"]).startswith("-ERR"), True, "TABLE.DESCRIBE on dropped table should fail")
    
    # SQL Verification for TABLE.DROP
    # Since the SQL engine can fall back to a schemaless scan, a SELECT on a dropped
    # table will not error, but it should return no data since TABLE.DROP also deletes the data.
    sql_after_drop = send(["SQL", "SELECT * FROM test_table"])
    assert_eq(sql_after_drop, "*0", "SQL SELECT on dropped table should return 0 rows")

    assert_eq(send(["TABLE.DROP", "test_table"]).startswith("-ERR"), True, "TABLE.DROP on non-existent table should fail")
    print("[PASS] TABLE.DROP tests complete.")