from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_type_casting(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()
        
    print("== SQL Type Casting Test Suite ==")

    # --- Helper ---
    def send_and_parse(cmd_list, description):
        """Sends a command, parses the response, and returns a list of JSON objects."""
        resp = send(cmd_list)
        if resp.startswith(("-", ":", "+")):
            return resp # Return DML/error responses directly

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
                    bulk_string = lines[i] + b'\r\n' + lines[i+1] + b'\r\n'
                    row_json = extract_json_from_bulk(bulk_string)
                    if row_json:
                        results.append(row_json)
                    i += 2
                except IndexError:
                    break
            elif lines[i]:
                print(f"[WARN] Unexpected line type in response for '{description}': {lines[i]!r}")
                break
            else: # Empty line might indicate end of multi-bulk
                i += 1

        print(f"[INFO] {description}: {len(results)} rows")
        return results

    # --- 1. Setup ---
    print("\n-- Phase 1: Setup for Type Casting --")
    send(["SQL", "DROP", "TABLE", "type_test"])
    resp = send(["SQL", "CREATE", "TABLE", "type_test", "(id TEXT, name TEXT, age INTEGER, is_active BOOLEAN)"])
    assert_eq(resp, "+OK", "CREATE TABLE type_test")

    # --- 2. INSERT Tests ---
    print("\n-- Phase 2: INSERT with Type Casting --")
    # Successful casts
    resp = send(["SQL", "INSERT", "INTO", "type_test", "(id, name, age, is_active)", "VALUES", "('1', 'Alice', '30', 'true')"])
    assert_eq(resp, ":1", "INSERT with string-to-int and string-to-bool cast")
    
    resp = send(["SQL", "INSERT", "INTO", "type_test", "(id, name, age, is_active)", "VALUES", "('2', 'Bob', 25.7, 1)"])
    assert_eq(resp, ":1", "INSERT with float-to-int and number-to-bool cast")

    # Verify successful casts
    results = send_and_parse(["SQL", "SELECT", "age, is_active", "FROM", "type_test", "WHERE", "id = '1'"], "SELECT casted row 1")
    assert_eq(len(results), 1, "Row 1 should exist")
    if results:
        assert_eq(results[0].get('age'), 30, "Verify 'age' was cast to INTEGER for row 1")
        assert_eq(results[0].get('is_active'), True, "Verify 'is_active' was cast to BOOLEAN for row 1")

    results = send_and_parse(["SQL", "SELECT", "age, is_active", "FROM", "type_test", "WHERE", "id = '2'"], "SELECT casted row 2")
    assert_eq(len(results), 1, "Row 2 should exist")
    if results:
        assert_eq(results[0].get('age'), 25, "Verify 'age' (float) was cast to INTEGER for row 2")
        assert_eq(results[0].get('is_active'), True, "Verify 'is_active' (1) was cast to BOOLEAN for row 2")

    # Failing casts
    resp = send(["SQL", "INSERT", "INTO", "type_test", "(id, age)", "VALUES", "('3', 'not-a-number')"])
    assert_eq(resp.startswith("-ERR"), True, "INSERT with invalid string-to-int cast should fail")
    
    resp = send(["SQL", "INSERT", "INTO", "type_test", "(id, is_active)", "VALUES", "('4', 'maybe')"])
    assert_eq(resp.startswith("-ERR"), True, "INSERT with invalid string-to-bool cast should fail")

    # --- 3. UPDATE Tests ---
    print("\n-- Phase 3: UPDATE with Type Casting --")
    # Successful cast
    resp = send(["SQL", "UPDATE", "type_test", "SET", "age", "=", "'45', is_active", "=", "'f'", "WHERE", "id", "=", "'1'"])
    assert_eq(resp, ":1", "UPDATE with string-to-int and string-to-bool cast")

    # Verify successful cast
    results = send_and_parse(["SQL", "SELECT", "age, is_active", "FROM", "type_test", "WHERE", "id = '1'"], "SELECT updated row 1")
    assert_eq(len(results), 1, "Updated row 1 should exist")
    if results:
        assert_eq(results[0].get('age'), 45, "Verify 'age' was updated and cast to INTEGER")
        assert_eq(results[0].get('is_active'), False, "Verify 'is_active' was updated and cast to BOOLEAN")

    # Failing cast
    resp = send(["SQL", "UPDATE", "type_test", "SET", "age", "=", "'invalid'", "WHERE", "id", "=", "'2'"])
    assert_eq(resp.startswith("-ERR"), True, "UPDATE with invalid string-to-int cast should fail")

    # Verify original data was not changed on failed update
    results = send_and_parse(["SQL", "SELECT", "age", "FROM", "type_test", "WHERE", "id = '2'"], "SELECT row 2 after failed update")
    assert_eq(len(results), 1, "Row 2 should still exist")
    if results:
        assert_eq(results[0].get('age'), 25, "Verify 'age' was not changed after failed update")

    # --- 4. Cleanup ---
    print("\n-- Phase 4: Cleanup for Type Casting --")
    resp = send(["SQL", "DROP", "TABLE", "type_test"])
    assert_eq(resp, "+OK", "DROP TABLE type_test")
    # Also delete the data keys
    send(["DELETE", "type_test:1"])
    send(["DELETE", "type_test:2"])
    print("[PASS] Type Casting tests complete")








