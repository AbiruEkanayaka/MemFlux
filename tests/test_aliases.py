from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_sql_aliases(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

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
                i += 1
        print(f"[INFO] {description}: {len(results)} rows")
        return results

    def get_column(results, column_name):
        """Extracts a specific column from a list of result rows."""
        return [row.get(column_name) for row in results]

    print("== SQL Aliases Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Setup for Alias Tests --")
    send(["SQL", "DROP", "TABLE", "alias_test"])
    resp = send(["SQL", "CREATE", "TABLE", "alias_test", "(id INTEGER, name TEXT, value INTEGER)"])
    assert_eq(resp, "+OK", "CREATE TABLE alias_test")
    send(["SQL", "INSERT", "INTO", "alias_test", "(id, name, value)", "VALUES", "('1', 'item-a', 100)"])
    send(["SQL", "INSERT", "INTO", "alias_test", "(id, name, value)", "VALUES", "('2', 'item-b', 200)"])

    # --- 2. Test Explicit Aliases (AS) ---
    print("\n-- Phase 2: Testing Explicit Aliases (AS) --")
    results = send_and_parse(["SQL", "SELECT", "name", "AS", "product_name", "FROM", "alias_test", "ORDER", "BY", "id"], "SELECT with explicit alias")
    assert_eq(len(results), 2, "Row count for explicit alias")
    assert_eq('product_name' in results[0], True, "Check for alias 'product_name' in results")
    assert_eq(get_column(results, 'product_name'), ['item-a', 'item-b'], "Verify column values via alias")

    results = send_and_parse(["SQL", "SELECT", "COUNT(*)", "AS", "total_count", "FROM", "alias_test"], "SELECT with aggregate alias")
    assert_eq(len(results), 1, "Row count for aggregate alias")
    assert_eq(results[0].get('total_count'), 2, "Verify aggregate value via alias")

    # --- 3. Test Implicit Aliases ---
    print("\n-- Phase 3: Testing Implicit Aliases --")
    results = send_and_parse(["SQL", "SELECT", "name", "product_name_implicit", "FROM", "alias_test", "ORDER", "BY", "id"], "SELECT with implicit alias")
    assert_eq(len(results), 2, "Row count for implicit alias")
    assert_eq('product_name_implicit' in results[0], True, "Check for implicit alias in results")
    assert_eq(get_column(results, 'product_name_implicit'), ['item-a', 'item-b'], "Verify column values via implicit alias")

    # --- 4. Cleanup ---
    print("\n-- Phase 4: Cleanup for Alias Tests --")
    send(["SQL", "DROP", "TABLE", "alias_test"])
    send(["DELETE", "alias_test:1"])
    send(["DELETE", "alias_test:2"])
    print("[PASS] Alias tests complete")





