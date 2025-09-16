from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_sql_comparison_operators(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    def send_and_parse(cmd_list, description):
        resp = send(cmd_list)
        resp_bytes = resp.encode('utf-8')
        lines = resp_bytes.split(b'\r\n')
        if not lines or not lines[0].startswith(b'*'):
            if resp.startswith(("+-", ":")):
                return resp
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
                i += 1
        print(f"[INFO] {description}: {len(results)} rows")
        return results

    def get_column_as_set(results, column_name):
        return {row.get(column_name) for row in results}

    print("== SQL Comparison Operators Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Data Setup --")
    send(["DELETE", "op_test:1", "op_test:2", "op_test:3", "op_test:4"])
    send(["JSON.SET", "op_test:1", '{"name": "A", "value": 10}'])
    send(["JSON.SET", "op_test:2", '{"name": "B", "value": 20}'])
    send(["JSON.SET", "op_test:3", '{"name": "C", "value": 20}'])
    send(["JSON.SET", "op_test:4", '{"name": "D", "value": 30}'])
    print("[PASS] Data setup complete")

    # --- 2. Operator Tests ---
    print("\n-- Phase 2: Testing Operators --")
    
    # !=
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "op_test", "WHERE", "value", "!=", "20"], "WHERE value != 20")
    assert_eq(get_column_as_set(results, 'name'), {"A", "D"}, "Names for value != 20")

    # >
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "op_test", "WHERE", "value", ">", "20"], "WHERE value > 20")
    assert_eq(get_column_as_set(results, 'name'), {"D"}, "Names for value > 20")

    # >=
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "op_test", "WHERE", "value", ">=", "20"], "WHERE value >= 20")
    assert_eq(get_column_as_set(results, 'name'), {"B", "C", "D"}, "Names for value >= 20")

    # <
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "op_test", "WHERE", "value", "<", "20"], "WHERE value < 20")
    assert_eq(get_column_as_set(results, 'name'), {"A"}, "Names for value < 20")

    # <=
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "op_test", "WHERE", "value", "<=", "20"], "WHERE value <= 20")
    assert_eq(get_column_as_set(results, 'name'), {"A", "B", "C"}, "Names for value <= 20")
    
    print("[PASS] Operator tests complete")

    # --- 3. Arithmetic Operator Tests ---
    print("\n-- Phase 3: Testing Arithmetic Operators --")
    
    # In SELECT clause
    results = send_and_parse(["SQL", "SELECT", "value", "+", "5", "as", "v", "FROM", "op_test", "WHERE", "name", "=", "'A'"], "SELECT with +")
    assert_eq(results[0]['v'], 15.0, "SELECT with + operator")

    results = send_and_parse(["SQL", "SELECT", "value", "-", "5", "as", "v", "FROM", "op_test", "WHERE", "name", "=", "'B'"], "SELECT with -")
    assert_eq(results[0]['v'], 15.0, "SELECT with - operator")

    results = send_and_parse(["SQL", "SELECT", "value", "*", "2", "as", "v", "FROM", "op_test", "WHERE", "name", "=", "'C'"], "SELECT with *")
    assert_eq(results[0]['v'], 40.0, "SELECT with * operator")

    results = send_and_parse(["SQL", "SELECT", "value", "/", "2", "as", "v", "FROM", "op_test", "WHERE", "name", "=", "'D'"], "SELECT with /")
    assert_eq(results[0]['v'], 15.0, "SELECT with / operator")

    # In WHERE clause
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "op_test", "WHERE", "value", "*", "2", ">", "30"], "WHERE with arithmetic")
    assert_eq(get_column_as_set(results, 'name'), {"B", "C", "D"}, "Names for WHERE with arithmetic")

    # In ORDER BY clause
    results = send_and_parse(["SQL", "SELECT", "name", "FROM", "op_test", "ORDER", "BY", "value", "*", "-1", "DESC", ",", "name", "ASC"], "ORDER BY with arithmetic")
    assert_eq([r['name'] for r in results], ["A", "B", "C", "D"], "Names for ORDER BY with arithmetic")
    
    print("[PASS] Arithmetic operator tests complete")

    # --- 4. Cleanup ---
    print("\n-- Phase 4: Cleanup --")
    send(["DELETE", "op_test:1", "op_test:2", "op_test:3", "op_test:4"])
    print("[PASS] Cleanup complete")
