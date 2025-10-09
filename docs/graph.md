# Graph Data Model & Cypher Queries

MemFlux includes a complete, high-performance property graph database model, deeply integrated with its other data models. You can interact with the graph via low-level commands or, more powerfully, through the built-in **Cypher Query Language** engine.

## The Property Graph Model

A property graph consists of two primary elements:

*   **Nodes:** The entities or objects in your data model. Each node has:
    *   A **Label:** A string that categorizes the node (e.g., `Person`, `Company`, `Product`).
    *   **Properties:** A JSON object containing arbitrary data about the node (e.g., `{"name": "Alice", "age": 30}`).

*   **Relationships (or Edges):** The directed connections between nodes. Each relationship has:
    *   A **Type:** A string that defines the nature of the connection (e.g., `KNOWS`, `WORKS_AT`, `BOUGHT`).
    *   **Properties:** A JSON object containing data about the relationship itself (e.g., `{"since": 2022, "weight": 0.8}`).

This model is ideal for representing complex, interconnected data like social networks, dependency trees, and recommendation systems.

## Cypher Query Language

Cypher is a declarative query language for property graphs. It uses ASCII-art patterns to describe the graph structures you want to find or create. MemFlux implements a powerful subset of Cypher.

All Cypher queries are executed using the `CYPHER` command:
`CYPHER "MATCH (p:Person) WHERE p.age > 30 RETURN p.name"`

### Reading Data: `MATCH`

The `MATCH` clause is used to specify the patterns to search for in the graph.

*   Nodes are represented by parentheses: `(p:Person)`
*   Relationships are represented by arrows: `-[r:KNOWS]->`

```cypher
-- Find all nodes with the label 'Person'
MATCH (p:Person)
RETURN p;

-- Find all people named 'Alice'
MATCH (p:Person {name: 'Alice'})
RETURN p;

-- Find who Alice knows
MATCH (alice:Person {name: 'Alice'})-[:KNOWS]->(friend:Person)
RETURN friend.name;

-- Optional matches (like a LEFT JOIN)
MATCH (p:Person {name: 'Bob'})
OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
RETURN p.name, c.name; -- c.name will be null if Bob doesn't work anywhere
```

### Writing Data: `CREATE`, `MERGE`, `SET`, `DELETE`

*   **`CREATE`**: Creates new nodes and relationships.
    ```cypher
    -- Create a new person
    CREATE (p:Person {name: 'Charlie', age: 40})
    RETURN p;

    -- Connect two existing people
    MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
    CREATE (a)-[:FRIENDS_WITH {since: 2023}]->(b);
    ```

*   **`MERGE`**: A combination of `MATCH` and `CREATE`. It finds a pattern if it exists, or creates it if it doesn't. This is useful for "upsert" operations.
    ```cypher
    -- Find the 'Product' node with name 'Laptop', or create it if it doesn't exist.
    MERGE (p:Product {name: 'Laptop'})
      ON CREATE SET p.created_at = timestamp()
      ON MATCH SET p.last_seen = timestamp()
    RETURN p;
    ```

*   **`SET` & `REMOVE`**: Modifies properties on existing nodes and relationships.
    ```cypher
    MATCH (p:Person {name: 'Alice'})
    SET p.age = 31, p.city = 'San Francisco'; -- Add or update properties

    MATCH (p:Person {name: 'Alice'})
    REMOVE p.city; -- Remove a property
    ```

*   **`DELETE` & `DETACH DELETE`**: Removes nodes and relationships.
    ```cypher
    -- Delete a specific relationship
    MATCH (a:Person)-[r:FRIENDS_WITH]->(b:Person)
    WHERE a.name = 'Alice' AND b.name = 'Bob'
    DELETE r;

    -- Delete a node and all relationships connected to it
    MATCH (p:Person {name: 'Charlie'})
    DETACH DELETE p;
    ```

### Advanced Patterns & Functions

*   **Variable-Length Paths:** Find paths of varying lengths.
    ```cypher
    -- Find people who are 1 to 3 steps away from Alice in the KNOWS graph
    MATCH (a:Person {name:'Alice'})-[:KNOWS*1..3]->(p:Person)
    RETURN p.name;
    ```

*   **`shortestPath()`:** Finds the shortest path between two nodes.
    ```cypher
    MATCH (start:Person {name:'Alice'}), (end:Person {name:'David'})
    RETURN shortestPath((start)-[:KNOWS*]->(end));
    ```

*   **Path Variables:** Assign an entire path to a variable.
    ```cypher
    MATCH p = (a:Person)-[:WORKS_AT]->(c:Company)
    RETURN p;
    ```

## SQL / Graph Interoperability

MemFlux allows you to seamlessly query between the SQL and Graph models.

1.  **Query Graph Data with SQL:**
    Node labels and relationship types are automatically exposed as virtual SQL tables. You can query them directly.
    ```sql
    -- This SQL query works because 'Person' is a node label
    SELECT * FROM Person WHERE properties.age > 30;

    -- This SQL query works because 'KNOWS' is a relationship type
    SELECT _from_id, _to_id FROM KNOWS;
    ```

2.  **Use Cypher Queries inside SQL:**
    The `GRAPH_MATCH` function in SQL lets you embed a Cypher query in your `FROM` clause, treating its results as a table you can join with.
    ```sql
    -- Find the SQL employee records for people who are managers in the graph
    SELECT e.name, e.role
    FROM employees AS e
    JOIN GRAPH_MATCH(
        'MATCH (mgr:Person)<-[:REPORTS_TO]-(:Person) RETURN mgr.employee_id AS id'
    ) RETURNS (id AS id) AS managers ON e.id = managers.id;
    ```
