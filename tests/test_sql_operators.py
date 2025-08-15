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

    # --- 3. Cleanup ---
    print("\n-- Phase 3: Cleanup --")
    send(["DELETE", "op_test:1", "op_test:2", "op_test:3", "op_test:4"])
    print("[PASS] Cleanup complete")
