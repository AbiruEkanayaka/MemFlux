from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_string_functions(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    def send_and_parse(cmd_list, description):
        """Sends a command, parses the response, and returns a list of JSON objects."""
        resp = send(cmd_list)
        if resp.startswith(("-", ":", "+")):
            print(f"[WARN] Unexpected response for '{description}': {resp!r}")
            return []

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

    print("== SQL String Functions Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Setup for Function Tests --")
    send(["DELETE", "func_test:1"])
    send(["DELETE", "func_test:2"])
    send(["DELETE", "func_test:3"])
    
    send(["JSON.SET", "func_test:1", '{"name":"  Test User 1  ", "email":"TEST@EXAMPLE.COM"}'])
    send(["JSON.SET", "func_test:2", '{"name":"Another User", "email":"another@example.com"}'])
    send(["JSON.SET", "func_test:3", '{"name":"  Third  ", "email":"THIRD@EXAMPLE.COM"}'])
    
    # --- 2. Test String Functions ---
    print("\n-- Phase 2: Testing String Functions --")

    # Test LOWER()
    results = send_and_parse(["SQL", "SELECT", "LOWER(email)", "AS", "lower_email", "FROM", "func_test", "WHERE", "id='1'"], "SELECT with LOWER()")
    assert_eq(get_column(results, 'lower_email'), ["test@example.com"], "LOWER() function test")

    # Test UPPER()
    results = send_and_parse(["SQL", "SELECT", "UPPER(name)", "AS", "upper_name", "FROM", "func_test", "WHERE", "id='2'"], "SELECT with UPPER()")
    assert_eq(get_column(results, 'upper_name'), ["ANOTHER USER"], "UPPER() function test")

    # Test LENGTH()
    results = send_and_parse(["SQL", "SELECT", "LENGTH(name)", "AS", "name_len", "FROM", "func_test", "WHERE", "id='2'"], "SELECT with LENGTH()")
    assert_eq(get_column(results, 'name_len'), [12], "LENGTH() function test")

    # Test TRIM()
    results = send_and_parse(["SQL", "SELECT", "TRIM(name)", "AS", "trimmed_name", "FROM", "func_test", "WHERE", "id='1'"], "SELECT with TRIM()")
    assert_eq(get_column(results, 'trimmed_name'), ["Test User 1"], "TRIM() function test")

    # Test SUBSTRING() with 2 arguments
    results = send_and_parse(["SQL", "SELECT", "SUBSTRING(email, 1, 4)", "AS", "sub", "FROM", "func_test", "WHERE", "id='1'"], "SELECT with SUBSTRING(str, start, len)")
    assert_eq(get_column(results, 'sub'), ["TEST"], "SUBSTRING(str, start, len) function test")

    # Test SUBSTRING() with 1 argument
    results = send_and_parse(["SQL", "SELECT", "SUBSTRING(name, 9)", "AS", "sub", "FROM", "func_test", "WHERE", "id='2'"], "SELECT with SUBSTRING(str, start)")
    assert_eq(get_column(results, 'sub'), ["User"], "SUBSTRING(str, start) function test")

    # Test nested functions
    results = send_and_parse(["SQL", "SELECT", "LENGTH(TRIM(name))", "AS", "len_trim_name", "FROM", "func_test", "WHERE", "id='3'"], "SELECT with nested LENGTH(TRIM())")
    assert_eq(get_column(results, 'len_trim_name'), [5], "Nested LENGTH(TRIM()) function test")

    # --- 3. Cleanup ---
    print("\n-- Phase 3: Cleanup for Function Tests --")
    send(["DELETE", "func_test:1"])
    send(["DELETE", "func_test:2"])
    send(["DELETE", "func_test:3"])
    print("[PASS] Function tests complete")


def test_numeric_functions(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    def send_and_parse(cmd_list, description):
        """Sends a command, parses the response, and returns a list of JSON objects."""
        resp = send(cmd_list)
        if resp.startswith(("-", ":", "+")):
            print(f"[WARN] Unexpected response for '{description}': {resp!r}")
            return []

        resp_bytes = resp.encode('utf-8')
        lines = resp_bytes.split(b'\r\n')
        if not lines or not lines[0].startswith(b'*'):
            print(f"[WARN] Unexpected response for '{description}': {resp!r}")
            return []
        
        results = []
        i = 1
        while i < len(lines):
            if lines[i].startswith(b''):
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

    print("== SQL Numeric Functions Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Setup for Numeric Function Tests --")
    send(["DELETE", "num_test:1"])
    send(["DELETE", "num_test:2"])
    
    send(["JSON.SET", "num_test:1", '{"value": -10.5}'])
    send(["JSON.SET", "num_test:2", '{"value": 15.2}'])
    
    # --- 2. Test Numeric Functions ---
    print("\n-- Phase 2: Testing Numeric Functions --")

    # Test ABS()
    results = send_and_parse(["SQL", "SELECT", "ABS(value)", "AS", "abs_val", "FROM", "num_test", "WHERE", "id='1'"], "SELECT with ABS()")
    assert_eq(get_column(results, 'abs_val'), [10.5], "ABS() on negative float")

    # Test ROUND()
    results = send_and_parse(["SQL", "SELECT", "ROUND(value)", "AS", "round_val", "FROM", "num_test", "WHERE", "id='2'"], "SELECT with ROUND()")
    assert_eq(get_column(results, 'round_val'), [15.0], "ROUND() on 15.2")

    # Test CEIL()
    results = send_and_parse(["SQL", "SELECT", "CEIL(value)", "AS", "ceil_val", "FROM", "num_test", "WHERE", "id='1'"], "SELECT with CEIL()")
    assert_eq(get_column(results, 'ceil_val'), [-10.0], "CEIL() on -10.5")

    # Test FLOOR()
    results = send_and_parse(["SQL", "SELECT", "FLOOR(value)", "AS", "floor_val", "FROM", "num_test", "WHERE", "id='2'"], "SELECT with FLOOR()")
    assert_eq(get_column(results, 'floor_val'), [15.0], "FLOOR() on 15.2")

    # --- 3. Cleanup ---
    print("\n-- Phase 3: Cleanup for Numeric Function Tests --")
    send(["DELETE", "num_test:1"])
    send(["DELETE", "num_test:2"])
    print("[PASS] Numeric Function tests complete")


def test_datetime_functions(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    def send_and_parse(cmd_list, description):
        """Sends a command, parses the response, and returns a list of JSON objects."""
        resp = send(cmd_list)
        if resp.startswith(("-", ":", "+")):
            print(f"[WARN] Unexpected response for '{description}': {resp!r}")
            return []

        resp_bytes = resp.encode('utf-8')
        lines = resp_bytes.split(b'\r\n')
        if not lines or not lines[0].startswith(b'*'):
            print(f"[WARN] Unexpected response for '{description}': {resp!r}")
            return []
        
        results = []
        i = 1
        while i < len(lines):
            if lines[i].startswith(b''):
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

    print("== SQL DateTime Functions Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Setup for DateTime Function Tests --")
    send(["DELETE", "time_test:1"])
    send(["JSON.SET", "time_test:1", '{"event_time": "2023-03-14T10:30:00Z"}'])
    
    # --- 2. Test DateTime Functions ---
    print("\n-- Phase 2: Testing DateTime Functions --")

    # Test NOW()
    results = send_and_parse(["SQL", "SELECT", "NOW()", "AS", "current_time", "FROM", "time_test", "LIMIT", "1"], "SELECT with NOW()")
    assert_eq(len(results), 1, "NOW() should return one row")
    # We can't check the exact time, but we can check if it's a string of a certain length
    now_val = get_column(results, 'current_time')[0]
    assert_eq(isinstance(now_val, str) and len(now_val) > 15, True, "NOW() returns a string timestamp")

    # Test DATE_PART()
    results = send_and_parse(["SQL", "SELECT", "DATE_PART('year', event_time)", "AS", "year", "FROM", "time_test"], "DATE_PART() for year")
    assert_eq(get_column(results, 'year'), [2023], "DATE_PART('year', ...)")

    # --- 3. Cleanup ---
    print("\n-- Phase 3: Cleanup for DateTime Function Tests --")
    send(["DELETE", "time_test:1"])
    print("[PASS] DateTime Function tests complete")


def test_cast_function(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    def send_and_parse(cmd_list, description):
        """Sends a command, parses the response, and returns a list of JSON objects."""
        resp = send(cmd_list)
        if resp.startswith(("-", ":", "+")):
            print(f"[WARN] Unexpected response for '{description}': {resp!r}")
            return []

        resp_bytes = resp.encode('utf-8')
        lines = resp_bytes.split(b'\r\n')
        if not lines or not lines[0].startswith(b'*'):
            print(f"[WARN] Unexpected response for '{description}': {resp!r}")
            return []
        
        results = []
        i = 1
        while i < len(lines):
            if lines[i].startswith(b''):
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

    print("== SQL CAST Function Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Setup for CAST Function Tests --")
    send(["DELETE", "cast_test:1"])
    send(["JSON.SET", "cast_test:1", '{"age": 42, "age_str": "42"}'])
    
    # --- 2. Test CAST Function ---
    print("\n-- Phase 2: Testing CAST Function --")

    # Test CAST to TEXT
    results = send_and_parse(["SQL", "SELECT", "CAST(age AS TEXT)", "AS", "age_as_text", "FROM", "cast_test"], "CAST to TEXT")
    assert_eq(get_column(results, 'age_as_text'), ["42"], "CAST(age AS TEXT)")

    # Test CAST to INTEGER
    results = send_and_parse(["SQL", "SELECT", "CAST(age_str AS INTEGER)", "AS", "age_as_int", "FROM", "cast_test"], "CAST to INTEGER")
    assert_eq(get_column(results, 'age_as_int'), [42], "CAST(age_str AS INTEGER)")

    # --- 3. Cleanup ---
    print("\n-- Phase 3: Cleanup for CAST Function Tests --")
    send(["DELETE", "cast_test:1"])
    print("[PASS] CAST Function tests complete")



