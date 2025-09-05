from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_data_types_and_constraints(sock, reader):
    """
    Run an end-to-end test suite that exercises data types, constraints, and foreign-key behavior over the SQL-like socket protocol.
    
    The test performs three phases:
    1) Data Types: creates a table covering many SQL types, inserts a valid row, verifies returned values (including CHAR padding, BYTEA, UUID and arrays), and checks that invalid inserts (e.g., out-of-range SMALLINT, oversized VARCHAR, malformed UUID/DATE) return errors.
    2) Constraints: creates a table with PRIMARY KEY, NOT NULL, UNIQUE, DEFAULT and CHECK constraints and verifies expected enforcement for NOT NULL, DEFAULT behavior, UNIQUE/PRIMARY KEY violations, and CHECK constraints.
    3) Foreign Keys: creates parent/child tables with a FOREIGN KEY, verifies valid and invalid child inserts, and checks that DELETE/UPDATE on a referenced parent is rejected until dependent children are removed.
    
    Side effects: sends SQL commands over the provided socket/reader, modifies server state (creates/drops tables, inserts/deletes rows) and uses assertions to validate responses.
    """
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    def send_and_parse(cmd_list, description):
        """
        Send a RESP command list, parse the server response, and return either a raw RESP reply or a list of JSON row dicts extracted from a bulk-array reply.
        
        If the server reply begins with '-', ':', or '+' the raw response string is returned unchanged. Otherwise the function expects a RESP bulk-array (lines starting with '*') where each bulk string contains a JSON row; it extracts each JSON object using extract_json_from_bulk and returns a list of parsed row dictionaries. If the response is not in the expected bulk format, an empty list is returned.
        
        Parameters:
            cmd_list: list
                RESP command parts to be sent (passed through to send()).
            description: str
                Human-readable description used in warning/info messages to identify the command context.
        
        Returns:
            str or list[dict]:
                - A raw RESP reply string when the response is a simple error/ok/integer (starts with '-', ':', '+').
                - Otherwise, a list of parsed JSON row dictionaries extracted from a RESP bulk-array. An empty list is returned if the reply is not a bulk-array or contains no valid JSON rows.
        """
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

    print("== Data Types and Constraints Test Suite ==")
    # --- 1. Data Types ---
    print("\n-- Phase 1: Data Type Validation --")
    send(["SQL", "DROP", "TABLE", "dt_test"]) 
    create_sql = [
        "SQL", "CREATE", "TABLE", "dt_test", "(",
        "c_smallint SMALLINT,",
        "c_integer INTEGER,",
        "c_bigint BIGINT,",
        "c_numeric NUMERIC(10, 2),",
        "c_real REAL,",
        "c_double DOUBLE PRECISION,",
        "c_varchar VARCHAR(10),",
        "c_char CHAR(10),",
        "c_bytea BYTEA,",
        "c_uuid UUID,",
        "c_date DATE,",
        "c_time TIME,",
        "c_timestamptz TIMESTAMPTZ,",
        "c_array INTEGER[]",
        ")"
    ]
    assert_eq(send(create_sql), "+OK", "CREATE TABLE with various data types")

    # Test valid inserts
    valid_insert_sql = [
        "SQL", "INSERT", "INTO", "dt_test",
        "(c_smallint, c_integer, c_bigint, c_numeric, c_real, c_double, c_varchar, c_char, c_bytea, c_uuid, c_date, c_time, c_timestamptz, c_array)",
        "VALUES",
        "(32767, 2147483647, 9223372036854775807, 12345.67, 123.45, 123.456789, 'varchar', 'char', '\\xDEADBEEF', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2025-08-30', '12:34:56', '2025-08-30T12:34:56Z', '{1,2,3}')"
    ]
    assert_eq(send(valid_insert_sql), ":1", "INSERT with valid data for all types")

    # Verify the inserted data
    results = send_and_parse(["SQL", "SELECT", "*", "FROM", "dt_test"], "SELECT inserted data types")
    assert_eq(len(results), 1, "Should retrieve the inserted row")
    if results:
        row = results[0]
        assert_eq(row.get('c_smallint'), 32767, "Verify SMALLINT")
        assert_eq(row.get('c_numeric'), 12345.67, "Verify NUMERIC")
        assert_eq(row.get('c_varchar'), "varchar", "Verify VARCHAR")
        assert_eq(row.get('c_char'), "char      ", "Verify CHAR (padded)") # CHAR is padded
        assert_eq(row.get('c_bytea'), "\\xdeadbeef", "Verify BYTEA")
        assert_eq(row.get('c_uuid'), "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "Verify UUID")
        assert_eq(row.get('c_array'), [1, 2, 3], "Verify ARRAY")

    # Test invalid inserts
    assert_eq(send(["SQL", "INSERT", "INTO", "dt_test", "(c_smallint)", "VALUES", "(32768)"]).startswith("-ERR"), True, "INSERT invalid SMALLINT (too large)")
    assert_eq(send(["SQL", "INSERT", "INTO", "dt_test", "(c_varchar)", "VALUES", "('12345678901')"]).startswith("-ERR"), True, "INSERT invalid VARCHAR (too long)")
    assert_eq(send(["SQL", "INSERT", "INTO", "dt_test", "(c_uuid)", "VALUES", "('not-a-uuid')"]).startswith("-ERR"), True, "INSERT invalid UUID")
    assert_eq(send(["SQL", "INSERT", "INTO", "dt_test", "(c_date)", "VALUES", "('2025/08/30')"]).startswith("-ERR"), True, "INSERT invalid DATE format")

    send(["SQL", "DROP", "TABLE", "dt_test"])
    send(["DELETE", "dt_test:1"])  
    print("[PASS] Data Type tests complete")


    # --- 2. Constraints ---
    print("\n-- Phase 2: Constraint Validation --")
    send(["FLUSHDB"])
    constraints_create_sql = [
        "SQL", "CREATE", "TABLE", "constraints_test", "(",
        "id INTEGER PRIMARY KEY,",
        "name TEXT NOT NULL,",
        "email TEXT UNIQUE,",
        "status TEXT DEFAULT 'pending',",
        "value INTEGER CHECK (value > 0)",
        ")"
    ]
    assert_eq(send(constraints_create_sql), "+OK", "CREATE TABLE with constraints")

    # Test NOT NULL
    assert_eq(send(["SQL", "INSERT", "INTO", "constraints_test", "(id, email)", "VALUES", "(1, 'a@b.com')"]).startswith("-ERR"), True, "INSERT should fail with NOT NULL violation")

    # Test DEFAULT
    assert_eq(send(["SQL", "INSERT", "INTO", "constraints_test", "(id, name, email, value)", "VALUES", "(1, 'Alice', 'a@b.com', 10)"]), ":1", "INSERT with explicit status")
    results = send_and_parse(["SQL", "SELECT", "status", "FROM", "constraints_test", "WHERE", "id=1"], "SELECT status for default test")
    assert_eq(results[0].get('status'), 'pending', "Verify DEFAULT value")

    # Test UNIQUE
    assert_eq(send(["SQL", "INSERT", "INTO", "constraints_test", "(id, name, email, value)", "VALUES", "(2, 'Bob', 'a@b.com', 20)"]).startswith("-ERR"), True, "INSERT should fail with UNIQUE violation")

    # Test PRIMARY KEY (which is also UNIQUE and NOT NULL)
    assert_eq(send(["SQL", "INSERT", "INTO", "constraints_test", "(id, name, email, value)", "VALUES", "(1, 'Carol', 'c@b.com', 30)"]).startswith("-ERR"), True, "INSERT should fail with PRIMARY KEY violation")
    assert_eq(send(["SQL", "INSERT", "INTO", "constraints_test", "(name, email, value)", "VALUES", "('David', 'd@b.com', 40)"]).startswith("-ERR"), True, "INSERT should fail with PRIMARY KEY (NOT NULL) violation")

    # Test CHECK
    assert_eq(send(["SQL", "INSERT", "INTO", "constraints_test", "(id, name, email, value)", "VALUES", "(2, 'Bob', 'b@b.com', 0)"]).startswith("-ERR"), True, "INSERT should fail with CHECK constraint violation")

    print("[PASS] Constraint tests complete")


    # --- 3. Foreign Keys ---
    print("\n-- Phase 3: Foreign Key Constraint Validation --")
    send(["FLUSHDB"])

    assert_eq(send(["SQL", "CREATE", "TABLE", "fk_parent", "(id INTEGER PRIMARY KEY, name TEXT)"]), "+OK", "CREATE TABLE fk_parent")
    fk_create_sql = [
        "SQL", "CREATE", "TABLE", "fk_child", "(",
        "id INTEGER PRIMARY KEY,",
        "parent_id INTEGER,",
        "FOREIGN KEY (parent_id) REFERENCES fk_parent(id)",
        ")"
    ]
    assert_eq(send(fk_create_sql), "+OK", "CREATE TABLE fk_child with FOREIGN KEY")

    # Setup parent data
    assert_eq(send(["SQL", "INSERT", "INTO", "fk_parent", "(id, name)", "VALUES", "(10, 'Parent Ten')"]), ":1", "Insert parent row")

    # Test valid FK insert
    assert_eq(send(["SQL", "INSERT", "INTO", "fk_child", "(id, parent_id)", "VALUES", "(101, 10)"]), ":1", "INSERT into child with valid FK")

    # Test invalid FK insert
    assert_eq(send(["SQL", "INSERT", "INTO", "fk_child", "(id, parent_id)", "VALUES", "(102, 99)"]).startswith("-ERR"), True, "INSERT into child with invalid FK should fail")

    # Test DELETE on parent with existing child (RESTRICT)
    assert_eq(send(["SQL", "DELETE", "FROM", "fk_parent", "WHERE", "id=10"]).startswith("-ERR"), True, "DELETE from parent with child reference should fail")

    # Test UPDATE on parent key with existing child (RESTRICT)
    assert_eq(send(["SQL", "UPDATE", "fk_parent", "SET", "id=11", "WHERE", "id=10"]).startswith("-ERR"), True, "UPDATE parent key with child reference should fail")

    # Test successful DELETE after child is removed
    assert_eq(send(["SQL", "DELETE", "FROM", "fk_child", "WHERE", "id=101"]), ":1", "Delete child row")
    assert_eq(send(["SQL", "DELETE", "FROM", "fk_parent", "WHERE", "id=10"]), ":1", "DELETE from parent should now succeed")

    print("[PASS] Foreign Key tests complete")

