from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_like(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    print("== SQL LIKE/ILIKE Expression Test Suite ==")

    # --- Helper Functions ---
    def send_and_parse(cmd_list, description):
        """Sends a command, parses the response, and returns a list of JSON objects."""
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
                    bulk_string = lines[i] + b'\r\n' + lines[i+1] + b'\r\n'
                    row_json = extract_json_from_bulk(bulk_string)
                    if row_json:
                        results.append(row_json)
                    i += 2
                except IndexError:
                    break
            else:
                if lines[i]:
                    print(f"[WARN] Unexpected line type in response for '{description}': {lines[i]!r}")
                break
        print(f"[INFO] {description}: {len(results)} rows")
        return results

    def get_column(results, column_name):
        """Extracts a specific column from a list of result rows, preserving None."""
        return [row.get(column_name) for row in results]

    # --- 1. Setup: Clean & Load Data ---
    print("\n-- Phase 1: Data Setup --")
    send(["DELETE", "like_test:1"])
    send(["DELETE", "like_test:2"])
    send(["DELETE", "like_test:3"])
    send(["DELETE", "like_test:4"])
    send(["DELETE", "like_test:5"])
    send(["DELETE", "like_test:6"])

    assert_eq(send(["JSON.SET", "like_test:1", '{"name":"Alice"}']), "+OK", "Insert 1")
    assert_eq(send(["JSON.SET", "like_test:2", '{"name":"Bob"}']), "+OK", "Insert 2")
    assert_eq(send(["JSON.SET", "like_test:3", '{"name":"carol"}']), "+OK", "Insert 3")
    assert_eq(send(["JSON.SET", "like_test:4", '{"name":"David"}']), "+OK", "Insert 4")
    assert_eq(send(["JSON.SET", "like_test:5", '{"name":"a_b"}']), "+OK", "Insert 5")
    assert_eq(send(["JSON.SET", "like_test:6", '{"name":"a%c"}']), "+OK", "Insert 6")
    print("[PASS] Data setup complete")

    # --- 2. LIKE/ILIKE Tests ---
    print("\n-- Phase 2: LIKE and ILIKE Clause Tests --")
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "like_test", "WHERE", "name", "LIKE", "'A%'"], "LIKE with trailing wildcard")
    assert_eq(get_column(results, 'name'), ["Alice"], "Name for LIKE 'A%'" )

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "like_test", "WHERE", "name", "LIKE", "'%d'"], "LIKE with leading wildcard")
    assert_eq(get_column(results, 'name'), ["David"], "Name for LIKE '%d'")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "like_test", "WHERE", "name", "LIKE", "'%o%'"], "LIKE with surrounding wildcard")
    assert_eq(sorted(get_column(results, 'name')), ["Bob", "carol"], "Names for LIKE '%o%'" )

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "like_test", "WHERE", "name", "LIKE", "'B_b'"], "LIKE with single char wildcard")
    assert_eq(get_column(results, 'name'), ["Bob"], "Name for LIKE 'B_b'")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "like_test", "WHERE", "name", "ILIKE", "'a%'"], "ILIKE case-insensitive")
    assert_eq(sorted(get_column(results, 'name')), ["Alice", "a%c", "a_b"], "Names for ILIKE 'a%'" )
    
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "like_test", "WHERE", "name", "ILIKE", "'C%'"], "ILIKE case-insensitive multiple")
    assert_eq(sorted(get_column(results, 'name')), ["carol"], "Names for ILIKE 'C%'" )

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "like_test", "WHERE", "name", "LIKE", "'Xyz%'"], "LIKE with no matches")
    assert_eq(len(results), 0, "Count for LIKE 'Xyz%' (no match)")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "like_test", "WHERE", "name", "LIKE", "'a\\_b'"], "LIKE with escaped underscore")
    assert_eq(get_column(results, 'name'), ["a_b"], "Name for LIKE 'a\\_b'")

    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "like_test", "WHERE", "name", "LIKE", "'a\\%c'"], "LIKE with escaped percent")
    assert_eq(get_column(results, 'name'), ["a%c"], "Name for LIKE 'a\\%c'")

    print("[PASS] LIKE and ILIKE tests complete")

    # --- 3. Cleanup ---
    print("\n-- Phase 3: Cleanup --")
    send(["DELETE", "like_test:1"])
    send(["DELETE", "like_test:2"])
    send(["DELETE", "like_test:3"])
    send(["DELETE", "like_test:4"])
    send(["DELETE", "like_test:5"])
    send(["DELETE", "like_test:6"])
    print("[PASS] Cleanup complete")



