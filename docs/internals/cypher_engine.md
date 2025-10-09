# Internals: The Cypher Query Engine

The MemFlux Cypher engine is a custom-built, streaming query engine designed to execute graph queries. It follows a traditional multi-stage pipeline, similar to the SQL engine, to process Cypher queries.

**Source Directory:** `src/cypher_engine/`

## 1. Parsing (`parser.rs`)

The first step is to transform the raw Cypher query string into a structured format.

*   **Tokenizer:** The query string is broken down into a sequence of tokens (e.g., `MATCH`, `(`, `p`, `:`, `Person`, `)`, `RETURN`, `p`). This is done by a simple, hand-written tokenizer.
*   **Parser:** The stream of tokens is parsed into a **Cypher Abstract Syntax Tree (AST)**, defined in `ast.rs`. The AST is a tree structure that represents the grammatical structure of the query, with enums for clauses like `Match`, `Create`, `Return`, etc., and structs for patterns, expressions, and properties.

## 2. Logical Planning (`logical_plan.rs`)

The AST is then converted into a **Logical Plan**. This plan represents the high-level, declarative steps needed to fulfill the query.

*   **AST to Logical Plan:** The `ast_to_logical_plan` function traverses the AST and builds a tree of `LogicalPlan` enums. For example, a `MATCH (a)-[r]->(b) WHERE a.name = 'Alice'` clause becomes a `Filter` node on top of an `Expand` node, which is on top of a `NodeByLabelScan` node.
*   **Simple Cost-Based Optimization:** The planner performs a rudimentary cost analysis to decide the order of pattern matching. For instance, if a `MATCH` clause contains multiple disconnected patterns, the planner will try to start with the one that is likely to be cheapest (e.g., a pattern that can use an index).

## 3. Physical Planning (`physical_plan.rs`)

The Physical Planner converts the Logical Plan into a **Physical Plan**, which describes the specific algorithms to be used.

*   **Logical to Physical Plan:** The `logical_to_physical_plan` function traverses the logical plan and selects physical operators.
*   **Index Scan Optimization:** The most important optimization is **Index Scan Selection**. If the planner encounters a `Filter` on a `NodeByLabelScan` that corresponds to an indexed property (e.g., `WHERE n.name = 'value'`), it will replace the `NodeScan` -> `Filter` sequence with a much faster `IndexScan` operator.

## 4. Execution (`execution.rs`)

The final stage executes the physical plan using a streaming, Volcano-style iterator model.

*   **`execute` function:** This is the entry point for the executor. It takes a `PhysicalPlan` and a `TransactionHandle` and returns a `Stream` of `Result<Row>`. The executor contains the implementation for all physical operators (`NodeScan`, `Expand`, `Filter`, `Create`, `Set`, etc.).
*   **Row Context:** Each yielded item is a `Row`, which is a JSON object mapping bound variable names (e.g., `p`, `r`) to their graph entity values (the node or relationship properties).
*   **Expression Evaluation:** The `evaluate_expression` function is used to compute the values of expressions in `WHERE`, `RETURN`, and `SET` clauses within the context of the current `Row`.
*   **Transactional Context:** All data access is performed through the provided `TransactionHandle`, ensuring that all reads respect the transaction's snapshot isolation and all writes are staged correctly.
*   **Write Operations:** For write clauses like `CREATE`, `SET`, and `DELETE`, the executor calls the appropriate `graph_*` methods on the `StorageExecutor` to stage the changes within the active transaction.
