from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_ddl_enhancements(sock, reader):
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
        return [row.get(column_name) for row in results]

    print("== DDL Enhancements Test Suite ==")

    # --- Cleanup from previous runs ---
    print("\n-- Cleanup: Ensuring a clean slate --")
    send(["FLUSHDB"])
    send(["DELETE", "_internal:schemas:if_not_exists_test"])
    send(["DELETE", "_internal:schemas:my_schema"])
    send(["DELETE", "_internal:schemas:view_base_table"])
    send(["DELETE", "_internal:schemas:my_view"])
    send(["DELETE", "_internal:schemas:old_table_name"])
    send(["DELETE", "_internal:schemas:rename_col_test"])
    send(["DELETE", "_internal:schemas:add_constraint_test"])
    send(["DELETE", "_internal:schemas:drop_constraint_test"])
    send(["DELETE", "_internal:schemas:alter_default_test"])
    send(["DELETE", "_internal:schemas:alter_not_null_test"])
    send(["DELETE", "_internal:schemas:alter_type_test"])
    send(["DELETE", "if_not_exists_test:1"])
    send(["DELETE", "view_base_table:1"])
    send(["DELETE", "old_table_name:1"])
    send(["DELETE", "rename_col_test:1"])
    send(["DELETE", "add_constraint_test:1"])
    send(["DELETE", "drop_constraint_test:1"])
    send(["DELETE", "alter_default_test:1"])
    send(["DELETE", "alter_not_null_test:1"])
    send(["DELETE", "alter_type_test:1"])
    print("[PASS] Cleanup complete.")

    # --- 1. CREATE TABLE ... IF NOT EXISTS ---
    print("\n-- Phase 1: CREATE TABLE ... IF NOT EXISTS --")
    assert_eq(send(["SQL", "CREATE", "TABLE", "if_not_exists_test", "(id INTEGER)"]), "+OK", "CREATE TABLE normally")
    assert_eq(send(["SQL", "CREATE", "TABLE", "IF", "NOT", "EXISTS", "if_not_exists_test", "(id INTEGER)"]), "+OK", "CREATE TABLE IF NOT EXISTS (should succeed)")
    assert_eq(send(["SQL", "CREATE", "TABLE", "if_not_exists_test", "(id INTEGER, name TEXT)"]).startswith("-ERR"), True, "CREATE TABLE existing (should fail)")
    send(["SQL", "DROP", "TABLE", "if_not_exists_test"])
    print("[PASS] CREATE TABLE IF NOT EXISTS tests complete.")

    # --- 2. CREATE SCHEMA ---
    print("\n-- Phase 2: CREATE SCHEMA --")
    assert_eq(send(["SQL", "CREATE", "SCHEMA", "my_schema"]), "+OK", "CREATE SCHEMA my_schema")
    assert_eq(send(["SQL", "CREATE", "SCHEMA", "my_schema"]).startswith("-ERR"), True, "CREATE SCHEMA existing (should fail)")
    assert_eq(send(["SQL", "CREATE", "TABLE", "my_schema.users", "(id INTEGER, name TEXT)"]), "+OK", "CREATE TABLE in new schema")
    assert_eq(send(["SQL", "DROP", "TABLE", "my_schema.users"]), "+OK", "DROP TABLE in schema")
    print("[PASS] CREATE SCHEMA tests complete.")

    # --- 3. CREATE VIEW ---
    print("\n-- Phase 3: CREATE VIEW --")
    assert_eq(send(["SQL", "CREATE", "TABLE", "view_base_table", "(id INTEGER, value TEXT)"]), "+OK", "CREATE base table for view")
    send(["SQL", "INSERT", "INTO", "view_base_table", "(id, value)", "VALUES", "(1, 'test_value')"])
    assert_eq(send(["SQL", "CREATE", "VIEW", "my_view", "AS", "SELECT", "id,", "value", "FROM", "view_base_table", "WHERE", "id", "=", "1"]), "+OK", "CREATE VIEW my_view")
    
    results = send_and_parse(["SQL", "SELECT", "id, value", "FROM", "my_view"], "Query my_view")
    assert_eq(len(results), 1, "View should return 1 row")
    if results:
        assert_eq(results[0].get('id'), 1, "View row ID")
        assert_eq(results[0].get('value'), "test_value", "View row value")
    
    assert_eq(send(["SQL", "CREATE", "VIEW", "my_view", "AS", "SELECT", "id", "FROM", "view_base_table"]).startswith("-ERR"), True, "CREATE VIEW existing (should fail)")
    assert_eq(send(["SQL", "DROP", "VIEW", "my_view"]), "+OK", "DROP VIEW my_view")
    assert_eq(send(["SQL", "DROP", "TABLE", "view_base_table"]), "+OK", "DROP base table for view")
    print("[PASS] CREATE VIEW tests complete.")

    # --- 4. ALTER TABLE RENAME TABLE ---
    print("\n-- Phase 4: ALTER TABLE RENAME TABLE --")
    assert_eq(send(["SQL", "CREATE", "TABLE", "old_table_name", "(id INTEGER, data TEXT)"]), "+OK", "CREATE TABLE old_table_name")
    send(["SQL", "INSERT", "INTO", "old_table_name", "(id, data)", "VALUES", "(1, 'some_data')"])
    assert_eq(send(["SQL", "ALTER", "TABLE", "old_table_name", "RENAME", "TO", "new_table_name"]), "+OK", "ALTER TABLE RENAME TO")
    
    results = send_and_parse(["SQL", "SELECT", "id, data", "FROM", "new_table_name"], "Query new_table_name")
    assert_eq(len(results), 1, "Renamed table should return 1 row")
    if results:
        assert_eq(results[0].get('id'), 1, "Renamed table row ID")
        assert_eq(results[0].get('data'), "some_data", "Renamed table row data")
    
    assert_eq(send(["SQL", "SELECT", "id", "FROM", "old_table_name"]), "*0", "Query old_table_name (should be empty)")
    send(["SQL", "DROP", "TABLE", "new_table_name"])
    print("[PASS] ALTER TABLE RENAME TABLE tests complete.")

    # --- 5. ALTER TABLE RENAME COLUMN ---
    print("\n-- Phase 5: ALTER TABLE RENAME COLUMN --")
    assert_eq(send(["SQL", "CREATE", "TABLE", "rename_col_test", "(id INTEGER, old_col TEXT)"]), "+OK", "CREATE TABLE rename_col_test")
    send(["SQL", "INSERT", "INTO", "rename_col_test", "(id, old_col)", "VALUES", "(1, 'old_value')"])
    assert_eq(send(["SQL", "ALTER", "TABLE", "rename_col_test", "RENAME", "COLUMN", "old_col", "TO", "new_col"]), "+OK", "ALTER TABLE RENAME COLUMN")
    
    results = send_and_parse(["SQL", "SELECT", "id, new_col", "FROM", "rename_col_test"], "Query new_col")
    assert_eq(len(results), 1, "Table with renamed column should return 1 row")
    if results:
        assert_eq(results[0].get('id'), 1, "Renamed column row ID")
        assert_eq(results[0].get('new_col'), "old_value", "Renamed column row data")
    
    assert_eq(send(["SQL", "SELECT", "id", "FROM", "rename_col_test", "WHERE", "old_col", "=", "'old_value'"]).startswith("-ERR"), True, "Query old_col (should fail)")
    send(["SQL", "DROP", "TABLE", "rename_col_test"])
    print("[PASS] ALTER TABLE RENAME COLUMN tests complete.")

    # --- 6. ALTER TABLE ADD CONSTRAINT ---
    print("\n-- Phase 6: ALTER TABLE ADD CONSTRAINT --")
    assert_eq(send(["SQL", "CREATE", "TABLE", "add_constraint_test", "(id INTEGER, value INTEGER)"]), "+OK", "CREATE TABLE add_constraint_test")
    send(["SQL", "INSERT", "INTO", "add_constraint_test", "(id, value)", "VALUES", "(1, 10)"])
    send(["SQL", "INSERT", "INTO", "add_constraint_test", "(id, value)", "VALUES", "(2, 20)"])

    # Add UNIQUE constraint
    assert_eq(send(["SQL", "ALTER", "TABLE", "add_constraint_test", "ADD", "CONSTRAINT", "unique_value", "UNIQUE", "(value)"]), "+OK", "ALTER TABLE ADD UNIQUE CONSTRAINT")
    assert_eq(send(["SQL", "INSERT", "INTO", "add_constraint_test", "(id, value)", "VALUES", "(3, 10)"]).startswith("-ERR"), True, "INSERT violating new UNIQUE constraint (should fail)")

    # Add CHECK constraint
    assert_eq(send(["SQL", "ALTER", "TABLE", "add_constraint_test", "ADD", "CONSTRAINT", "check_positive", "CHECK", "(value > 0)"]), "+OK", "ALTER TABLE ADD CHECK CONSTRAINT")
    assert_eq(send(["SQL", "INSERT", "INTO", "add_constraint_test", "(id, value)", "VALUES", "(4, -5)"]).startswith("-ERR"), True, "INSERT violating new CHECK constraint (should fail)")
    
    send(["SQL", "DROP", "TABLE", "add_constraint_test"])
    print("[PASS] ALTER TABLE ADD CONSTRAINT tests complete.")

    # --- 7. ALTER TABLE DROP CONSTRAINT ---
    print("\n-- Phase 7: ALTER TABLE DROP CONSTRAINT --")
    assert_eq(send(["SQL", "CREATE", "TABLE", "drop_constraint_test", "(id INTEGER PRIMARY KEY, value INTEGER, CONSTRAINT my_unique_value UNIQUE (value))"]), "+OK", "CREATE TABLE drop_constraint_test with PK and named UNIQUE")
    send(["SQL", "INSERT", "INTO", "drop_constraint_test", "(id, value)", "VALUES", "(1, 10)"])
    send(["SQL", "INSERT", "INTO", "drop_constraint_test", "(id, value)", "VALUES", "(2, 20)"])
    assert_eq(send(["SQL", "INSERT", "INTO", "drop_constraint_test", "(id, value)", "VALUES", "(3, 10)"]).startswith("-ERR"), True, "INSERT violating UNIQUE (should fail)")

    # Drop UNIQUE constraint
    assert_eq(send(["SQL", "ALTER", "TABLE", "drop_constraint_test", "DROP", "CONSTRAINT", "my_unique_value"]), "+OK", "ALTER TABLE DROP UNIQUE CONSTRAINT") # Constraint name is auto-generated as <column_name>_unique
    assert_eq(send(["SQL", "INSERT", "INTO", "drop_constraint_test", "(id, value)", "VALUES", "(3, 10)"]), ":1", "INSERT previously violating UNIQUE (should succeed)")
    
    send(["SQL", "DROP", "TABLE", "drop_constraint_test"])
    print("[PASS] ALTER TABLE DROP CONSTRAINT tests complete.")

    # --- 8. ALTER TABLE ALTER COLUMN SET DEFAULT / DROP DEFAULT ---
    print("\n-- Phase 8: ALTER TABLE ALTER COLUMN SET DEFAULT / DROP DEFAULT --")
    assert_eq(send(["SQL", "CREATE", "TABLE", "alter_default_test", "(id INTEGER, data TEXT)"]), "+OK", "CREATE TABLE alter_default_test")
    send(["SQL", "INSERT", "INTO", "alter_default_test", "(id)", "VALUES", "(1)"]) # No default yet
    
    results = send_and_parse(["SQL", "SELECT", "data", "FROM", "alter_default_test", "WHERE", "id", "=", "1"], "SELECT data (should be null)")
    assert_eq(get_column(results, 'data'), [None], "Data should be NULL initially")

    # SET DEFAULT
    assert_eq(send(["SQL", "ALTER", "TABLE", "alter_default_test", "ALTER", "COLUMN", "data", "SET", "DEFAULT", "'new_default'"]), "+OK", "ALTER COLUMN SET DEFAULT")
    send(["SQL", "INSERT", "INTO", "alter_default_test", "(id)", "VALUES", "(2)"])
    results = send_and_parse(["SQL", "SELECT", "data", "FROM", "alter_default_test", "WHERE", "id", "=", "2"], "SELECT data (should be new_default)")
    assert_eq(get_column(results, 'data'), ["new_default"], "Data should have new default")

    # DROP DEFAULT
    assert_eq(send(["SQL", "ALTER", "TABLE", "alter_default_test", "ALTER", "COLUMN", "data", "DROP", "DEFAULT"]), "+OK", "ALTER COLUMN DROP DEFAULT")
    send(["SQL", "INSERT", "INTO", "alter_default_test", "(id)", "VALUES", "(3)"])
    results = send_and_parse(["SQL", "SELECT", "data", "FROM", "alter_default_test", "WHERE", "id", "=", "3"], "SELECT data (should be null again)")
    assert_eq(get_column(results, 'data'), [None], "Data should be NULL again after dropping default")

    send(["SQL", "DROP", "TABLE", "alter_default_test"])
    print("[PASS] ALTER TABLE ALTER COLUMN SET DEFAULT / DROP DEFAULT tests complete.")

    # --- 9. ALTER TABLE ALTER COLUMN SET NOT NULL / DROP NOT NULL ---
    print("\n-- Phase 9: ALTER TABLE ALTER COLUMN SET NOT NULL / DROP NOT NULL --")
    assert_eq(send(["SQL", "CREATE", "TABLE", "alter_not_null_test", "(id INTEGER, data TEXT)"]), "+OK", "CREATE TABLE alter_not_null_test")
    send(["SQL", "INSERT", "INTO", "alter_not_null_test", "(id, data)", "VALUES", "(1, 'value')"])
    send(["SQL", "INSERT", "INTO", "alter_not_null_test", "(id, data)", "VALUES", "(2, NULL)"]) # Insert a null value

    # SET NOT NULL (should fail due to existing NULLs)
    assert_eq(send(["SQL", "ALTER", "TABLE", "alter_not_null_test", "ALTER", "COLUMN", "data", "SET", "NOT", "NULL"]).startswith("-ERR"), True, "ALTER COLUMN SET NOT NULL (should fail with existing NULLs)")
    
    # Delete the NULL value and try again
    send(["SQL", "DELETE", "FROM", "alter_not_null_test", "WHERE", "id", "=", "2"])
    assert_eq(send(["SQL", "ALTER", "TABLE", "alter_not_null_test", "ALTER", "COLUMN", "data", "SET", "NOT", "NULL"]), "+OK", "ALTER COLUMN SET NOT NULL (should succeed after removing NULLs)")
    assert_eq(send(["SQL", "INSERT", "INTO", "alter_not_null_test", "(id)", "VALUES", "(3)"]).startswith("-ERR"), True, "INSERT violating new NOT NULL (should fail)")

    # DROP NOT NULL
    assert_eq(send(["SQL", "ALTER", "TABLE", "alter_not_null_test", "ALTER", "COLUMN", "data", "DROP", "NOT", "NULL"]), "+OK", "ALTER COLUMN DROP NOT NULL")
    send(["SQL", "INSERT", "INTO", "alter_not_null_test", "(id)", "VALUES", "(3)"]) # Should now succeed
    results = send_and_parse(["SQL", "SELECT", "data", "FROM", "alter_not_null_test", "WHERE", "id", "=", "3"], "SELECT data (should be null again)")
    assert_eq(get_column(results, 'data'), [None], "Data should be NULL again after dropping NOT NULL")

    send(["SQL", "DROP", "TABLE", "alter_not_null_test"])
    print("[PASS] ALTER TABLE ALTER COLUMN SET NOT NULL / DROP NOT NULL tests complete.")

    # --- 10. ALTER TABLE ALTER COLUMN TYPE ---
    print("\n-- Phase 10: ALTER TABLE ALTER COLUMN TYPE --")
    assert_eq(send(["SQL", "CREATE", "TABLE", "alter_type_test", "(id INTEGER, value TEXT)"]), "+OK", "CREATE TABLE alter_type_test")
    send(["SQL", "INSERT", "INTO", "alter_type_test", "(id, value)", "VALUES", "(1, '123')"])
    send(["SQL", "INSERT", "INTO", "alter_type_test", "(id, value)", "VALUES", "(2, '456')"])
    send(["SQL", "INSERT", "INTO", "alter_type_test", "(id, value)", "VALUES", "(3, 'abc')"]) # Invalid for int conversion

    # ALTER COLUMN TYPE TEXT to INTEGER
    assert_eq(send(["SQL", "ALTER", "TABLE", "alter_type_test", "ALTER", "COLUMN", "value", "TYPE", "INTEGER"]).startswith("-ERR"), True, "ALTER COLUMN TYPE (should fail due to 'abc')")
    
    # Delete invalid data and try again
    send(["SQL", "DELETE", "FROM", "alter_type_test", "WHERE", "id", "=", "3"])
    assert_eq(send(["SQL", "ALTER", "TABLE", "alter_type_test", "ALTER", "COLUMN", "value", "TYPE", "INTEGER"]), "+OK", "ALTER COLUMN TYPE TEXT to INTEGER (should succeed)")
    
    results = send_and_parse(["SQL", "SELECT", "value", "FROM", "alter_type_test", "ORDER", "BY", "id"], "SELECT values (should be integers)")
    assert_eq(get_column(results, 'value'), [123, 456], "Values should now be integers")

    # Verify schema update
    schema_resp = send(["GET", "_internal:schemas:alter_type_test"])
    schema_json = extract_json_from_bulk(schema_resp.encode('utf-8'))
    if schema_json:
        assert_eq(schema_json.get('columns', {}).get('value', {}).get('type'), 'INTEGER', "Verify 'value' column type is INTEGER in schema")

    send(["SQL", "DROP", "TABLE", "alter_type_test"])
    print("[PASS] ALTER TABLE ALTER COLUMN TYPE tests complete.")

    print("\n== All DDL Enhancements tests passed! ==")
