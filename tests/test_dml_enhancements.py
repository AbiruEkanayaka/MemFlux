from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_dml_enhancements(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    def send_and_parse(cmd_list, description):
        resp = send(cmd_list)
        if resp.startswith(("-", ":", "+")):
            return resp
        resp_bytes = resp.encode('utf-8')
        lines = resp_bytes.split(b'\r\n')
        if not lines or not lines[0].startswith(b'*'):
            print(f"[WARN] Unexpected response for '{description}': {resp!r}")
            return []
        
        results = []
        i = 1
        while i < len(lines):
            if lines[i].startswith(b'$'):
                try:
                    if i + 1 < len(lines):
                        bulk_string = lines[i] + b'\r\n' + lines[i+1] + b'\r\n'
                    else:
                        print(f"[WARN] Incomplete bulk string at line {i}")
                        break
                    row_json = extract_json_from_bulk(bulk_string)
                    if row_json is not None:
                        results.append(row_json)
                    i += 2
                except IndexError:
                    break
            else:
                i += 1
        print(f"[INFO] {description}: {len(results)} rows")
        return results

    def get_column(results, column_name):
        return sorted([row.get(column_name) for row in results if row.get(column_name) is not None])

    print("== DML Enhancements Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Setup --")
    send(["FLUSHDB"])
    send(["SQL", "CREATE", "TABLE", "dml_users", "(id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"])
    send(["SQL", "CREATE", "TABLE", "dml_updates", "(user_id INTEGER, new_age INTEGER)"])
    send(["SQL", "CREATE", "TABLE", "dml_archive", "(id INTEGER, name TEXT)"])
    send(["SQL", "INSERT", "INTO", "dml_users", "(id, name, age)", "VALUES", "(1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)"])
    print("[PASS] Setup complete.")

    # --- 2. RETURNING Clause ---
    print("\n-- Phase 2: RETURNING Clause --")
    # INSERT ... RETURNING
    results = send_and_parse(["SQL", "INSERT", "INTO", "dml_users", "(id, name, age)", "VALUES", "(4, 'David', 40)", "RETURNING", "id, name"], "INSERT ... RETURNING")
    assert_eq(len(results), 1, "INSERT RETURNING should return 1 row")
    assert_eq(results[0], {'id': 4, 'name': 'David'}, "INSERT RETURNING correct data")

    # UPDATE ... RETURNING
    results = send_and_parse(["SQL", "UPDATE", "dml_users", "SET", "age = 31", "WHERE", "id = 1", "RETURNING", "age"], "UPDATE ... RETURNING")
    assert_eq(len(results), 1, "UPDATE RETURNING should return 1 row")
    assert_eq(results[0], {'age': 31}, "UPDATE RETURNING correct data")

    # DELETE ... RETURNING
    results = send_and_parse(["SQL", "DELETE", "FROM", "dml_users", "WHERE", "id = 2", "RETURNING", "*"], "DELETE ... RETURNING")
    assert_eq(len(results), 1, "DELETE RETURNING should return 1 row")
    assert_eq(results[0], {'id': 2, 'name': 'Bob', 'age': 25}, "DELETE RETURNING correct data")
    print("[PASS] RETURNING clause tests complete.")

    # --- 3. INSERT ... SELECT ---
    print("\n-- Phase 3: INSERT ... SELECT ---")
    send(["SQL", "INSERT", "INTO", "dml_archive", "(id, name)", "SELECT", "id, name", "FROM", "dml_users", "WHERE", "age > 30"])
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "dml_archive", "ORDER", "BY", "name"], "SELECT from archive table")
    assert_eq(get_column(results, 'name'), ['Alice', 'Charlie', 'David'], "INSERT ... SELECT correct data")
    print("[PASS] INSERT ... SELECT tests complete.")

    # --- 4. ON CONFLICT (UPSERT) ---
    print("\n-- Phase 4: ON CONFLICT (UPSERT) ---")
    # ON CONFLICT DO NOTHING
    resp = send(["SQL", "INSERT", "INTO", "dml_users", "(id, name, age)", "VALUES", "(1, 'Alice V2', 99)", "ON", "CONFLICT", "(id)", "DO", "NOTHING"])
    assert_eq(resp, ":0", "ON CONFLICT DO NOTHING should affect 0 rows")
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "dml_users", "WHERE", "id = 1"], "SELECT after DO NOTHING")
    assert_eq(results[0].get('name'), 'Alice', "Name should be unchanged after DO NOTHING")

    # ON CONFLICT DO UPDATE
    resp = send(["SQL", "INSERT", "INTO", "dml_users", "(id, name, age)", "VALUES", "(1, 'Alice V2', 99)", "ON", "CONFLICT", "(id)", "DO", "UPDATE", "SET", "name = 'Alice V2', age = excluded.age"])
    assert_eq(resp, ":1", "ON CONFLICT DO UPDATE should affect 1 row")
    results = send_and_parse(["SQL", "SELECT", "name, age", "FROM", "dml_users", "WHERE", "id = 1"], "SELECT after DO UPDATE")
    assert_eq(results[0], {'name': 'Alice V2', 'age': 99}, "Row should be updated after DO UPDATE")
    print("[PASS] ON CONFLICT tests complete.")

    # --- 5. UPDATE ... FROM ---
    print("\n-- Phase 5: UPDATE ... FROM ---")
    send(["SQL", "INSERT", "INTO", "dml_updates", "(user_id, new_age)", "VALUES", "(3, 36), (4, 41)"])
    
    # Use RETURNING to see what the executor thinks it's setting the age to.
    update_results = send_and_parse(["SQL", "UPDATE", "dml_users", "SET", "age = dml_updates.new_age", "FROM", "dml_updates", "WHERE", "dml_users.id = dml_updates.user_id", "RETURNING", "age"], "UPDATE with RETURNING")
    returned_ages = get_column(update_results, 'age')
    assert_eq(returned_ages, [36, 41], "UPDATE...RETURNING should show the new ages")

    # Now, verify the data was actually persisted in the table.
    results = send_and_parse(["SQL", "SELECT", "age", "FROM", "dml_users", "WHERE", "id = 3 OR id = 4", "ORDER", "BY", "id"], "SELECT after UPDATE FROM")
    assert_eq(get_column(results, 'age'), [36, 41], "Ages should be updated from another table")
    print("[PASS] UPDATE ... FROM tests complete.")

    # --- 6. DELETE ... USING ---
    print("\n-- Phase 6: DELETE ... USING ---")
    resp = send(["SQL", "DELETE", "FROM", "dml_users", "USING", "dml_updates", "WHERE", "dml_users.id = dml_updates.user_id"])
    assert_eq(resp, ":2", "DELETE USING should affect 2 rows")
    results = send_and_parse(["SQL", "SELECT", "id", "FROM", "dml_users"], "SELECT after DELETE USING")
    assert_eq(get_column(results, 'id'), [1], "Only user 1 should remain")
    print("[PASS] DELETE ... USING tests complete.")

    # --- 7. Cleanup ---
    print("\n-- Phase 7: Cleanup --")
    send(["SQL", "DROP", "TABLE", "dml_users"])
    send(["SQL", "DROP", "TABLE", "dml_updates"])
    send(["SQL", "DROP", "TABLE", "dml_archive"])
    print("[PASS] Cleanup complete.")
