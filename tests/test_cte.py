from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_cte(sock, reader):
    def send(parts):
        resp, *_ = send_resp_command(sock, reader, parts)
        return resp.strip()

    def send_and_parse(cmd_list, description):
        """Sends a command, parses the response, and returns a list of JSON objects."""
        resp = send(cmd_list)
        if resp.startswith(("-", ":", "+")):
            print(f"[WARN] Received non-array response for '{description}': {resp!r}")
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
                    if i + 1 < len(lines):
                        bulk_string = lines[i] + b'\r\n' + lines[i+1] + b'\r\n'
                    else:
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

    def get_column_as_set(results, column_name):
        return {row.get(column_name) for row in results}

    print("== SQL CTE (Common Table Expressions) Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Setup for CTE Tests --")
    send(["FLUSHDB"])
    send(["SQL", "CREATE", "TABLE", "cte_employees", "(id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER, salary INTEGER)"])
    send(["SQL", "CREATE", "TABLE", "cte_departments", "(id INTEGER PRIMARY KEY, name TEXT)"])
    
    send(["SQL", "INSERT", "INTO", "cte_departments", "VALUES", "(1, 'Engineering'), (2, 'Sales')"])
    send(["SQL", "INSERT", "INTO", "cte_employees", "VALUES", 
          "(1, 'Alice', NULL, 120000),",
          "(2, 'Bob', 1, 100000),",
          "(3, 'Charlie', 1, 110000),",
          "(4, 'David', 2, 80000),",
          "(5, 'Eve', 2, 90000)"])
    print("[PASS] Setup complete.")

    # --- 2. Simple CTE ---
    print("\n-- Phase 2: Simple CTE --")
    results = send_and_parse([
        "SQL", 
        "WITH top_earners AS (SELECT name, salary FROM cte_employees WHERE salary > 100000)",
        "SELECT name FROM top_earners ORDER BY name"
    ], "Simple CTE to find top earners")
    assert_eq(len(results), 2, "Simple CTE should return 2 rows")
    assert_eq(get_column_as_set(results, 'name'), {'Alice', 'Charlie'}, "Verify names of top earners")
    print("[PASS] Simple CTE tests complete.")

    # --- 3. CTE with a JOIN in the main query ---
    print("\n-- Phase 3: CTE used in a JOIN --")
    results = send_and_parse([
        "SQL",
        "WITH managers AS (SELECT id, name FROM cte_employees WHERE manager_id IN (SELECT DISTINCT manager_id FROM cte_employees))",
        "SELECT e.name as employee_name, m.name as manager_name",
        "FROM cte_employees e JOIN managers m ON e.manager_id = m.id",
        "ORDER BY e.name"
    ], "CTE identifying managers, joined back to employees")
    assert_eq(len(results), 4, "CTE in JOIN should return 4 rows")
    # Expected: Bob->Alice, Charlie->Alice, David->Bob, Eve->Bob
    employee_manager_pairs = {(r.get('employee_name'), r.get('manager_name')) for r in results}
    expected_pairs = {('Bob', 'Alice'), ('Charlie', 'Alice'), ('David', 'Bob'), ('Eve', 'Bob')}
    assert_eq(employee_manager_pairs, expected_pairs, "Verify employee-manager pairs from CTE JOIN")
    print("[PASS] CTE in JOIN tests complete.")

    # --- 4. Multiple CTEs (Chained) ---
    print("\n-- Phase 4: Multiple Chained CTEs --")
    results = send_and_parse([
        "SQL",
        "WITH",
        "  managers_chained AS (SELECT id FROM cte_employees WHERE manager_id IS NULL),",
        "  direct_reports_chained AS (SELECT name, salary FROM cte_employees WHERE manager_id IN (SELECT id FROM managers_chained))",
        "SELECT name FROM direct_reports_chained WHERE salary > 100000 ORDER BY name"
    ], "Multiple chained CTEs")
    assert_eq(len(results), 1, "Chained CTEs should return 1 row")
    assert_eq(get_column_as_set(results, 'name'), {'Charlie'}, "Verify chained CTE result")
    print("[PASS] Multiple chained CTEs tests complete.")

    # --- 5. CTE with aggregate functions ---
    print("\n-- Phase 5: CTE with Aggregates ---")
    results = send_and_parse([
        "SQL",
        "WITH salary_summary AS (",
        "  SELECT manager_id, AVG(salary) as avg_salary",
        "  FROM cte_employees",
        "  WHERE manager_id IS NOT NULL",
        "  GROUP BY manager_id",
        ")",
        "SELECT e.name, s.avg_salary",
        "FROM salary_summary s JOIN cte_employees e ON s.manager_id = e.id",
        "ORDER BY e.name"
    ], "CTE with aggregates")
    assert_eq(len(results), 2, "CTE with aggregates should return 2 rows (one for each manager)")
    # Manager 1 (Alice) has reports with salaries 100k, 110k -> avg 105k
    # Manager 2 (Bob) has reports with salaries 80k, 90k -> avg 85k
    manager_avg_salaries = {r.get('e.name'): r.get('s.avg_salary') for r in results}
    expected_salaries = {'Alice': 105000.0, 'Bob': 85000.0}
    assert_eq(manager_avg_salaries, expected_salaries, "Verify manager average salaries from aggregate CTE")
    print("[PASS] CTE with aggregates tests complete.")

    # --- 6. Recursive CTE (UNION ALL) ---
    print("\n-- Phase 6: Recursive CTE (UNION ALL) --")
    results = send_and_parse([
        "SQL",
        "WITH RECURSIVE employee_hierarchy (id, name, manager_id, level) AS (",
        "  SELECT id, name, manager_id, 0 FROM cte_employees WHERE manager_id IS NULL",
        "  UNION ALL",
        "  SELECT e.id, e.name, e.manager_id, eh.level + 1",
        "  FROM cte_employees e JOIN employee_hierarchy eh ON e.manager_id = eh.id",
        ")",
        "SELECT name, level FROM employee_hierarchy ORDER BY level, name"
    ], "Recursive CTE to build employee hierarchy")
    
    assert_eq(len(results), 5, "Recursive CTE should return all 5 employees")
    expected_hierarchy = [
        {'name': 'Alice', 'level': 0},
        {'name': 'Bob', 'level': 1},
        {'name': 'Charlie', 'level': 1},
        {'name': 'David', 'level': 2},
        {'name': 'Eve', 'level': 2}
    ]
    assert_eq(results, expected_hierarchy, "Verify employee hierarchy and levels")
    print("[PASS] Recursive CTE (UNION ALL) tests complete.")

    # --- 7. Recursive CTE (UNION) with Cycles ---
    print("\n-- Phase 7: Recursive CTE (UNION) with Cycles --")
    send(["SQL", "CREATE TABLE cyclic_graph (id INTEGER, parent_id INTEGER)"])
    send(["SQL", "INSERT INTO cyclic_graph VALUES (1, 3), (2, 1), (3, 2)"]) # Cycle: 1 -> 3 -> 2 -> 1

    results_union = send_and_parse([
        "SQL",
        "WITH RECURSIVE path (id, parent_id) AS (",
        "  SELECT id, parent_id FROM cyclic_graph WHERE id = 1",
        "  UNION", # Using UNION (distinct) should cause termination
        "  SELECT c.id, c.parent_id",
        "  FROM cyclic_graph c JOIN path p ON c.id = p.parent_id",
        ")",
        "SELECT id FROM path"
    ], "Recursive CTE with UNION on a cyclic graph")

    assert_eq(len(results_union), 3, "Recursive CTE with UNION on a cycle should terminate and return 3 rows")
    assert_eq(get_column_as_set(results_union, 'id'), {1, 2, 3}, "Verify all nodes in cycle are visited once with UNION")
    send(["SQL", "DROP TABLE cyclic_graph"])
    print("[PASS] Recursive CTE (UNION) with cycles tests complete.")

    # --- 8. Cleanup ---
    print("\n-- Phase 8: Cleanup --")
    send(["SQL", "DROP", "TABLE", "cte_employees"])
    send(["SQL", "DROP", "TABLE", "cte_departments"])
    print("[PASS] Cleanup complete.")

