# Internals: The SQL Query Engine

The MemFlux SQL query engine is a custom-built, streaming-first engine designed to operate directly on in-memory JSON data. It follows a traditional multi-stage pipeline to process SQL queries. This document details each stage of that pipeline.

**Source Files:**
*   `src/query_engine/simple_parser.rs`
*   `src/query_engine/ast.rs`
*   `src/query_engine/logical_plan.rs`
*   `src/query_engine/physical_plan.rs`
*   `src/query_engine/execution.rs`
*   `src/query_engine/functions.rs`

## 1. Parsing (`simple_parser.rs`)

The first step is to transform the raw SQL query string into a structured format.

*   **Tokenizer:** The query string is broken down into a sequence of tokens (e.g., `SELECT`, `*`, `FROM`, `users`, `WHERE`, `age`, `>`, `30`). This is done by a simple, hand-written tokenizer that recognizes keywords, identifiers, literals, and operators.
*   **Parser:** The stream of tokens is then parsed into an **Abstract Syntax Tree (AST)**. The AST is a tree structure that represents the grammatical structure of the query. For example, a `SELECT` statement is represented by a `SelectStatement` struct containing fields for its columns, `FROM` clause, `WHERE` clause, etc.

The AST is defined in `src/query_engine/ast.rs`.

## 2. Logical Planning (`logical_plan.rs`)

The AST represents *what* the user asked for, but not how to get it. The next step is to convert the AST into a **Logical Plan**.

*   **AST to Logical Plan:** The `ast_to_logical_plan` function traverses the AST and builds a tree of `LogicalPlan` enums. The logical plan is a higher-level representation of the operations needed to fulfill the query. For example, a `SELECT ... WHERE ...` statement becomes a `Filter` node on top of a `TableScan` node.
*   **Schema Validation:** During this phase, the planner validates the query against any existing virtual schemas (from the `SchemaCache`). It checks if tables and columns exist, resolving column names and flagging ambiguities.
*   **Expression Transformation:** SQL expressions (like `age > 30` or `LOWER(name)`) are converted from their AST representation (`SimpleExpression`) into a more type-aware `Expression` enum, which is used by the executor.

The logical plan is still declarative; it describes the logical steps but not the specific algorithms to use.

## 3. Physical Planning (`physical_plan.rs`)

The Physical Planner converts the Logical Plan into a **Physical Plan**. This plan describes the specific algorithms and data access methods that will be used to execute the query.

*   **Logical to Physical Plan:** The `logical_to_physical_plan` function traverses the logical plan and selects physical operators.
*   **Optimization:** This is where query optimization occurs. The current implementation has a simple but important optimization:
    *   **Index Scan Selection:** If a `Filter` node is filtering on a column with an equality check (e.g., `WHERE city = 'SF'`) and a suitable index exists, the planner will replace the `TableScan` -> `Filter` sequence with a much more efficient `IndexScan` operator. This allows the engine to jump directly to the relevant keys instead of scanning the entire table.

The output is a tree of `PhysicalPlan` enums, which is directly executable.

## 4. Execution (`execution.rs`)

The final stage is to execute the physical plan.

*   **Volcano/Iterator Model:** The executor uses a streaming, iterator-based model (often called a Volcano model). Each node in the physical plan is an "operator" that pulls rows from the node(s) below it, processes them, and yields the results to the node above it.
*   **Streaming Results:** This model is highly efficient for memory usage, as the full result set does not need to be materialized in memory at once. Rows are processed and streamed back to the client one by one.
*   **`execute` function:** This is the entry point for the executor. It takes a `PhysicalPlan` and returns a `Stream` of `Result<Row>`.
*   **Context-Aware Evaluation:** Expressions are evaluated using the `evaluate_with_context` method, which has access to the current row, the application context (database, schemas, etc.), and even the outer row's context for executing correlated subqueries.
*   **DML/DDL Execution:** For commands like `INSERT`, `UPDATE`, `DELETE`, and `CREATE TABLE`, the executor performs the modifications to the in-memory database and logs the changes to the WAL for persistence. It typically yields a single row with the number of affected rows.
