from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_case(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    print("== SQL CASE Expression Test Suite ==")

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
    # Clean up all potential keys
    for i in range(1, 6):
        send(["DELETE", f"user:{i}"])

    # Insert users: name, age, city
    assert_eq(send(["JSON.SET", "user:1", '{"name":"Alice","age":30,"city":"SF"}']), "+OK", "Insert user:1")
    assert_eq(send(["JSON.SET", "user:2", '{"name":"Bob","age":25,"city":"SF"}']), "+OK", "Insert user:2")
    assert_eq(send(["JSON.SET", "user:3", '{"name":"Carol","age":30,"city":"NY"}']), "+OK", "Insert user:3")
    assert_eq(send(["JSON.SET", "user:4", '{"name":"David","age":25,"city":"NY"}']), "+OK", "Insert user:4")
    assert_eq(send(["JSON.SET", "user:5", '{"name":"Carol","age":35,"city":"SF"}']), "+OK", "Insert user:5")
    print("[PASS] Data setup complete")

    # --- 2. CASE Expression Tests ---
    print("\n-- Phase 2: CASE Expression Tests --")
    print("[INFO] Testing CASE expressions...")

    # Test CASE with multiple WHENs and an ELSE
    results = send_and_parse([
        "SQL", "SELECT", "name, age,",
        "CASE",
        "WHEN", "age", "<", "30", "THEN", "'Young'",
        "WHEN", "age", "=", "30", "THEN", "'Prime'",
        "ELSE", "'Veteran'",
        "END", "AS", "age_category",
        "FROM", "user",
        "ORDER", "BY", "name", "ASC", ",", "age", "ASC"
    ], "SELECT with CASE statement")

    assert_eq(len(results), 5, "CASE statement should return 5 rows")
    if len(results) == 5:
        # Order is now deterministic: Alice, Bob, Carol(30), Carol(35), David
        expected_categories = ["Prime", "Young", "Prime", "Veteran", "Young"]
        actual_categories = get_column(results, 'age_category')
        assert_eq(actual_categories, expected_categories, "Verify all age categories")

    # Test CASE without an ELSE (should result in NULL)
    results = send_and_parse([
        "SQL", "SELECT", "name, age,",
        "CASE",
        "WHEN", "city", "=", "'SF'", "THEN", "'West Coast'",
        "END", "AS", "coast",
        "FROM", "user",
        "ORDER", "BY", "name", "ASC", ",", "age", "ASC"
    ], "SELECT with CASE statement without ELSE")
    
    assert_eq(len(results), 5, "CASE without ELSE should return 5 rows")
    if len(results) == 5:
        # Order: Alice, Bob, Carol(30), Carol(35), David
        # Alice(SF), Bob(SF), Carol(NY), Carol(SF), David(NY)
        expected_coasts = ["West Coast", "West Coast", None, "West Coast", None]
        actual_coasts = get_column(results, 'coast')
        assert_eq(actual_coasts, expected_coasts, "Verify coast categories with NULLs")

    print("[PASS] CASE expression tests complete")

    # --- 3. Cleanup ---
    print("\n-- Phase 3: Cleanup --")
    for i in range(1, 6):
        send(["DELETE", f"user:{i}"])
    print("[PASS] Cleanup complete")





