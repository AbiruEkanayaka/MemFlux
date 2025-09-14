from .common import send_resp_command, assert_eq, extract_json_from_bulk

def test_dql_enhancements(sock, reader):
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
                    if i + 1 < len(lines):
                        bulk_string = lines[i] + b'\r\n' + lines[i+1] + b'\r\n'
                    else:
                        print(f"[WARN] Incomplete bulk string at line {i}")
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

    def get_column(results, column_name):
        """Extracts a specific column from a list of result rows, preserving None."""
        return sorted([row.get(column_name) for row in results if row.get(column_name) is not None])

    print("== DQL Enhancements Test Suite ==")

    # --- 1. Setup ---
    print("\n-- Phase 1: Setup --")
    send(["FLUSHDB"])
    send(["SQL", "CREATE", "TABLE", "dql_sales", "(id INTEGER PRIMARY KEY, city TEXT, product TEXT, quantity INTEGER, price INTEGER)"])
    send(["SQL", "CREATE", "TABLE", "dql_locations", "(city TEXT PRIMARY KEY, country TEXT)"])
    
    send(["SQL", "INSERT", "INTO", "dql_locations", "(city, country)", "VALUES", "('SF', 'USA'), ('NY', 'USA'), ('London', 'UK')"])
    
    send(["SQL", "INSERT", "INTO", "dql_sales", "(id, city, product, quantity, price)", "VALUES", 
          "(1, 'SF', 'Laptop', 1, 1200),",
          "(2, 'SF', 'Mouse', 2, 25),",
          "(3, 'NY', 'Laptop', 1, 1250),",
          "(4, 'NY', 'Mouse', 5, 20),",
          "(5, 'London', 'Keyboard', 10, 75),",
          "(6, 'London', 'Mouse', 8, 22),",
          "(7, 'SF', 'Keyboard', 5, 80)"])
    
    # Add a sale with a NULL city for IS NULL tests
    send(["SQL", "INSERT", "INTO", "dql_sales", "(id, city, product, quantity, price)", "VALUES", "(8, NULL, 'Webcam', 1, 100)"])
    print("[PASS] Setup complete.")

    # --- 2. HAVING Clause ---
    print("\n-- Phase 2: HAVING Clause --")
    # Find cities with total sales quantity > 10
    results = send_and_parse(["SQL", "SELECT", "city, SUM(quantity) as total_quantity", "FROM", "dql_sales", "GROUP", "BY", "city", "HAVING", "SUM(quantity) > 10"], "HAVING SUM(quantity) > 10")
    assert_eq(len(results), 1, "HAVING should return 1 city")
    assert_eq(results[0].get('city'), 'London', "City with total quantity > 10 should be London")
    assert_eq(results[0].get('total_quantity'), 18.0, "Total quantity for London should be 18")
    print("[PASS] HAVING clause tests complete.")

    # --- 3. DISTINCT ON ---
    print("\n-- Phase 3: DISTINCT ON ---")
    # Find one sale for each product, ordered by quantity DESC (i.e., the largest sale for each product)
    results = send_and_parse(["SQL", "SELECT", "DISTINCT", "ON", "(product)", "product, quantity", "FROM", "dql_sales", "ORDER", "BY", "product, quantity DESC"], "DISTINCT ON product")
    assert_eq(len(results), 4, "DISTINCT ON should return 4 rows (one for each product)")
    expected_distinct = sorted([{'product': 'Keyboard', 'quantity': 10}, {'product': 'Laptop', 'quantity': 1}, {'product': 'Mouse', 'quantity': 8}, {'product': 'Webcam', 'quantity': 1}], key=lambda x: x['product'])
    actual_distinct = sorted(results, key=lambda x: x['product'])
    assert_eq(actual_distinct, expected_distinct, "DISTINCT ON results should be correct")
    print("[PASS] DISTINCT ON tests complete.")

    # --- 4. INTERSECT and EXCEPT ---
    print("\n-- Phase 4: INTERSECT and EXCEPT --")
    # INTERSECT: Cities that are in both tables (should be SF, NY, London)
    results = send_and_parse(["SQL", "SELECT", "city", "FROM", "dql_sales", "INTERSECT", "SELECT", "city", "FROM", "dql_locations"], "INTERSECT cities")
    assert_eq(len(results), 3, "INTERSECT should return 3 cities")
    assert_eq(get_column(results, 'city'), ['London', 'NY', 'SF'], "INTERSECT results")

    # EXCEPT: Cities in locations that have no sales
    send(["SQL", "INSERT", "INTO", "dql_locations", "(city, country)", "VALUES", "('Paris', 'France')"])
    results = send_and_parse(["SQL", "SELECT", "city", "FROM", "dql_locations", "EXCEPT", "SELECT", "city", "FROM", "dql_sales"], "EXCEPT cities after adding Paris")
    assert_eq(len(results), 1, "EXCEPT should return 1 city")
    assert_eq(results[0].get('city'), 'Paris', "EXCEPT result should be Paris")
    print("[PASS] INTERSECT and EXCEPT tests complete.")

    # --- 5. Expanded WHERE Predicates ---
    print("\n-- Phase 5: Expanded WHERE Predicates --")
    # BETWEEN
    results = send_and_parse(["SQL", "SELECT", "id", "FROM", "dql_sales", "WHERE", "price", "BETWEEN", "75", "AND", "100"], "WHERE price BETWEEN 75 AND 100")
    assert_eq(get_column(results, 'id'), [5, 7, 8], "BETWEEN results")
    
    # IS NULL / IS NOT NULL
    results = send_and_parse(["SQL", "SELECT", "id", "FROM", "dql_sales", "WHERE", "city", "IS", "NULL"], "WHERE city IS NULL")
    assert_eq(get_column(results, 'id'), [8], "IS NULL result")
    results = send_and_parse(["SQL", "SELECT", "COUNT(*)", "FROM", "dql_sales", "WHERE", "city", "IS", "NOT", "NULL"], "WHERE city IS NOT NULL")
    assert_eq(results[0].get('COUNT(*)'), 7, "IS NOT NULL result count")

    # EXISTS
    results = send_and_parse(["SQL", "SELECT", "city", "FROM", "dql_locations", "l", "WHERE", "EXISTS", "(SELECT 1 FROM dql_sales s WHERE s.city = l.city)"], "WHERE EXISTS")
    assert_eq(get_column(results, 'city'), ['London', 'NY', 'SF'], "EXISTS results")

    # IN (value_list)
    results = send_and_parse(["SQL", "SELECT", "id", "FROM", "dql_sales", "WHERE", "product", "IN", "('Laptop', 'Webcam')"], "WHERE IN (value_list)")
    assert_eq(get_column(results, 'id'), [1, 3, 8], "IN (value_list) results")

    # ANY / ALL
    results = send_and_parse(["SQL", "SELECT", "id", "FROM", "dql_sales", "WHERE", "price", ">", "ANY", "(SELECT price FROM dql_sales WHERE product = 'Mouse')"], "WHERE > ANY (subquery)")
    assert_eq(len(results), 7, "> ANY should return 7 rows (price > 20)")
    results = send_and_parse(["SQL", "SELECT", "id", "FROM", "dql_sales", "WHERE", "price", ">", "ALL", "(SELECT price FROM dql_sales WHERE product = 'Mouse')"], "WHERE > ALL (subquery)")
    assert_eq(len(results), 5, "> ALL should return 5 rows (price > 25)")
    print("[PASS] Expanded WHERE predicate tests complete.")

    # --- 6. Subqueries in SELECT and FROM ---
    print("\n-- Phase 6: Subqueries in SELECT and FROM --")
    # Subquery in FROM (derived table)
    results = send_and_parse(["SQL", "SELECT", "s.product, s.total_quantity", "FROM", "(SELECT product, SUM(quantity) as total_quantity FROM dql_sales GROUP BY product) AS s", "WHERE", "s.total_quantity > 8"], "Subquery in FROM")
    assert_eq(len(results), 2, "Subquery in FROM should return 2 products")
    assert_eq(get_column(results, 's.product'), ['Keyboard', 'Mouse'], "Products from derived table")

    # Scalar subquery in SELECT list
    results = send_and_parse(["SQL", "SELECT", "product,", "(SELECT", "country", "FROM", "dql_locations", "WHERE", "city", "=", "dql_sales.city)", "AS", "country", "FROM", "dql_sales", "WHERE", "product", "=", "'Laptop'"], "Scalar subquery in SELECT")
    assert_eq(len(results), 2, "Scalar subquery should return 2 rows")
    assert_eq(get_column(results, 'country'), ['USA', 'USA'], "Countries from scalar subquery")
    print("[PASS] Subquery tests complete.")

    # --- 7. Cleanup ---
    print("\n-- Phase 7: Cleanup --")
    send(["SQL", "DROP", "TABLE", "dql_sales"])
    send(["SQL", "DROP", "TABLE", "dql_locations"])
    print("[PASS] Cleanup complete.")
