use anyhow::{anyhow, Result};

use super::ast::{AlterTableAction, AstStatement, CaseExpression, ColumnDef, CreateIndexStatement,
    CreateSchemaStatement, CreateTableStatement, CreateViewStatement, DeleteStatement,
    DropTableStatement, DropViewStatement, ForeignKeyClause, InsertStatement, JoinClause,
    OrderByExpression, SelectColumn, SelectStatement, SimpleExpression, SimpleValue,
    TableConstraint, TruncateTableStatement, UnionClause, UpdateStatement, AlterTableStatement};

// Very simple tokenizer
/// Splits a SQL string into a sequence of lexical tokens.
///
/// Tokenization rules:
/// - Whitespace and semicolons separate tokens.
/// - Single-quoted string literals are preserved as single tokens (including surrounding quotes).
/// - Operators and punctuation are emitted as separate tokens: `=`, `,`, `(`, `)`, `>`, `<`, `!`, `[`, `]`, and `.` (dot is usually separate).
/// - Combined comparison operators `>=`, `<=`, and `!=` are recognized and returned as two-character tokens.
/// - A dot between digits is merged into the current numeric token (so `123.45` becomes one token).
///
/// # Examples
///
/// ```
/// let toks = tokenize("SELECT id, name FROM users WHERE price >= 12.50 AND name = 'O\\'Reilly';");
/// assert!(toks.contains(&"SELECT".to_string()));
/// assert!(toks.contains(&"price".to_string()));
/// assert!(toks.contains(&">=".to_string()));
/// assert!(toks.contains(&"12.50".to_string()));
/// assert!(toks.contains(&"'O\\'Reilly'".to_string()));
/// ```
fn tokenize(sql: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current_token = String::new();
    let mut in_string = false;
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                if !in_string {
                    if !current_token.is_empty() {
                        tokens.push(current_token.clone());
                        current_token.clear();
                    }
                    in_string = true;
                    current_token.push(ch);
                } else {
                    current_token.push(ch);
                    tokens.push(current_token.clone());
                    current_token.clear();
                    in_string = false;
                }
            }
            _ if in_string => {
                current_token.push(ch);
            }
            ' ' | '\n' | '\r' | '\t' | ';' => {
                if !current_token.is_empty() {
                    tokens.push(current_token.clone());
                    current_token.clear();
                }
            }
            '=' | ',' | '(' | ')' | '>' | '<' | '!' | '[' | ']'=> {
                if !current_token.is_empty() {
                    tokens.push(current_token.clone());
                    current_token.clear();
                }
                let mut op = ch.to_string();
                if let Some('=') = chars.peek() {
                    if op == ">" || op == "<" || op == "!" {
                        op.push('=');
                        chars.next(); // consume the '='
                    }
                }
                tokens.push(op);
            }

            '.'=> {
                if !current_token.is_empty() && current_token.chars().all(|c| c.is_ascii_digit()) {
                    current_token.push('.');
                } else {
                    if !current_token.is_empty() {
                        tokens.push(current_token.clone());
                        current_token.clear();
                    }
                    tokens.push(".".to_string());
                }
            }
            _ => {
                current_token.push(ch);
            }
        }
    }
    if !current_token.is_empty() {
        tokens.push(current_token);
    }
    tokens
}

struct Parser {
    tokens: Vec<String>,
    pos: usize,
}

impl Parser {
    fn new(sql: &str) -> Self {
        Parser {
            tokens: tokenize(sql),
            pos: 0,
        }
    }

    fn current(&self) -> Option<&str> {
        self.tokens.get(self.pos).map(|s| s.as_str())
    }

    fn advance(&mut self) {
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
    }

    /// Consume the current token if it matches `expected` (case-insensitive), otherwise return an error.
    ///
    /// This checks the parser's current token without consuming it first; if it equals `expected`
    /// ignoring ASCII case, the token is consumed and `Ok(())` is returned. On mismatch or end of
    /// input an `anyhow::Error` is returned describing the expected token and what was found.
    /// The token position is not advanced when a non-matching token is encountered.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut parser = Parser::new("SELECT * FROM t");
    /// // current token is "SELECT"
    /// parser.expect("select").unwrap(); // case-insensitive match consumes "SELECT"
    /// ```
    fn expect(&mut self, expected: &str) -> Result<()> {
        match self.current() {
            Some(token) if token.eq_ignore_ascii_case(expected) => {
                self.advance();
                Ok(())
            }
            Some(token) => Err(anyhow!("Expected '{}', found '{}'", expected, token)),
            None => Err(anyhow!("Expected '{}', found end of input", expected)),
        }
    }

    /// Parse the current token as an unquoted SQL identifier and advance the parser.
    ///
    /// This accepts only simple, unquoted identifiers composed of ASCII alphanumerics and underscores (`[A-Za-z0-9_]`).
    /// On success returns the identifier as a `String` and consumes the token by advancing the parser position.
    /// Returns an error if there is no current token or if the token contains characters outside the allowed set.
    /// Note: quoted identifiers (e.g., double-quoted) are not supported by this helper.
    ///
    /// # Examples
    ///
    /// ```
    /// // Given a parser positioned at the token `column_name`, this will consume it and return the string.
    /// let mut p = Parser::new("column_name");
    /// let ident = p.parse_identifier().unwrap();
    /// assert_eq!(ident, "column_name");
    /// ```
    fn parse_identifier(&mut self) -> Result<String> {
        let token = self.current().ok_or_else(|| anyhow!("Expected an identifier"))?;
        // Very basic check, doesn't handle quoted identifiers
        if token.chars().all(|c| c.is_alphanumeric() || c == '_') {
            let identifier = token.to_string();
            self.advance();
            Ok(identifier)
        } else {
            Err(anyhow!("Invalid identifier: {}", token))
        }
    }

    /// Parses a simple qualified name: either an identifier or a dotted qualified identifier (`schema.table`).
    ///
    /// Returns the parsed name as a single `String`. If a dot is present after the first identifier,
    /// the function consumes it and parses a second identifier, returning `"first.second"`.
    /// Propagates any error from `parse_identifier` (e.g., on invalid/empty identifier).
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = Parser::new("public.users");
    /// let name = p.parse_qualified_name().unwrap();
    /// assert_eq!(name, "public.users");
    /// ```
    fn parse_qualified_name(&mut self) -> Result<String> {
        let mut name = self.parse_identifier()?;
        if self.current() == Some(".") {
            self.advance(); // consume dot
            name.push('.');
            name.push_str(&self.parse_identifier()?);
        }
        Ok(name)
    }

    /// Parse the comma-separated column list of a SELECT clause.
    ///
    /// Continues consuming tokens until the `FROM` keyword or end of input. Supports:
    /// - a single `*` column,
    /// - arbitrary expressions,
    /// - explicit aliases using `AS`,
    /// - implicit aliases (a bare identifier following an expression, unless it is a SQL keyword like `FROM`, `WHERE`, `JOIN`, `GROUP`, `ORDER`, `LIMIT`, `OFFSET`, or a comma).
    ///
    /// Returns a non-empty `Vec<SelectColumn>`; returns an error if no columns are found.
    ///
    /// # Examples
    ///
    /// ```
    /// // Position the parser at the start of the select list (after "SELECT")
    /// let mut p = Parser::new("a AS x, b c, COUNT(*) FROM t");
    /// let cols = p.parse_column_list().unwrap();
    /// assert_eq!(cols.len(), 3);
    /// assert_eq!(cols[0].alias.as_deref(), Some("x"));
    /// assert_eq!(cols[1].alias.as_deref(), Some("c"));
    /// ```
    fn parse_column_list(&mut self) -> Result<Vec<SelectColumn>> {
        let mut columns = Vec::new();
        loop {
            match self.current() {
                Some("FROM") | None => break,
                Some("*") => {
                    columns.push(SelectColumn {
                        expr: SimpleExpression::Column("*".to_string()),
                        alias: None,
                    });
                    self.advance();
                    break;
                }
                Some(_) => {
                    let expr = self.parse_expression()?;

                    let mut alias = None;
                    if self.current().map_or(false, |t| t.eq_ignore_ascii_case("AS")) {
                        self.advance(); // consume "AS"
                        alias = Some(self.parse_identifier()?);
                    } else if let Some(token) = self.current() {
                        // Implicit alias (e.g. "SELECT col1 c1")
                        if !["FROM", ",", "WHERE", "ORDER", "GROUP", "LIMIT", "OFFSET", "JOIN"]
                            .iter()
                            .any(|k| k.eq_ignore_ascii_case(token)) {
                            alias = Some(self.parse_identifier()?);
                        }
                    }

                    columns.push(SelectColumn { expr, alias });

                    if self.current() == Some(",") {
                        self.advance();
                    } else {
                        break;
                    }
                }
            }
        }
        if columns.is_empty() {
            return Err(anyhow!("No columns found in SELECT"));
        }
        Ok(columns)
    }

    fn parse_expression(&mut self) -> Result<SimpleExpression> {
        self.parse_or_expression()
    }

    fn parse_or_expression(&mut self) -> Result<SimpleExpression> {
        let mut left = self.parse_and_expression()?;
        while self.current().map_or(false, |t| t.eq_ignore_ascii_case("OR")) {
            self.advance(); // consume "OR"
            let right = self.parse_and_expression()?;
            left = SimpleExpression::LogicalOp {
                left: Box::new(left),
                op: "OR".to_string(),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and_expression(&mut self) -> Result<SimpleExpression> {
        let mut left = self.parse_comparison_expression()?;
        while self.current().map_or(false, |t| t.eq_ignore_ascii_case("AND")) {
            self.advance(); // consume "AND"
            let right = self.parse_comparison_expression()?;
            left = SimpleExpression::LogicalOp {
                left: Box::new(left),
                op: "AND".to_string(),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    /// Parse a comparison expression (binary comparison or fallback to a primary expression).
    ///
    /// This consumes a primary expression on the left, then optionally parses a comparison
    /// operator (`=`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `ILIKE`, `IN`) followed by either
    /// a primary expression or a parenthesized expression/subquery on the right. When a
    /// comparison operator is present a `SimpleExpression::BinaryOp` is returned; otherwise
    /// the left primary expression is returned unchanged.
    ///
    /// Examples
    ///
    /// ```
    /// let mut p = Parser::new("col = 42");
    /// let expr = p.parse_comparison_expression().unwrap();
    /// match expr {
    ///     SimpleExpression::BinaryOp { op, .. } => assert_eq!(op, "="),
    ///     _ => panic!("expected binary op"),
    /// }
    /// ```
    fn parse_comparison_expression(&mut self) -> Result<SimpleExpression> {
        let left = self.parse_primary_expression()?;

        if let Some(op) = self.current() {
            if ["=", "!=", ">", "<", ">=", "<=", "LIKE", "ILIKE", "IN"]
                .iter()
                .any(|k| k.eq_ignore_ascii_case(op)) {
                let op_str = op.to_uppercase();
                self.advance();

                let right = if self.current() == Some("(") {
                    self.advance(); // consume "("
                    let expr = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("SELECT")) {
                        let subquery = self.parse_select()?;
                        SimpleExpression::Subquery(Box::new(subquery))
                    } else {
                        self.parse_expression()? 
                    };
                    self.expect(")")?;
                    expr
                } else {
                    self.parse_primary_expression()? 
                };

                return Ok(SimpleExpression::BinaryOp {
                    left: Box::new(left),
                    op: op_str,
                    right: Box::new(right),
                });
            }
        }
        Ok(left)
    }

    /// Parse a "primary" SQL expression: literals, NULL/BOOLEAN, CASE, identifiers (columns),
    /// function calls (including aggregate functions and `CAST`), and qualified names.
    ///
    /// This is the lowest-level expression parser that recognizes:
    /// - String literals surrounded by single quotes, numeric literals, `TRUE`/`FALSE`, and `NULL`.
    /// - `CASE ... WHEN ... THEN ... [ELSE ...] END` expressions (delegates to `parse_case_expression`).
    /// - `CAST(expr AS type)` expressions.
    /// - Function calls and aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`). `COUNT(DISTINCT ...)`
    ///   is supported. Aggregate arguments must be simple column names or `*`; complex expressions in
    ///   aggregate calls are not supported and will return an error.
    /// - Bare identifiers or qualified names (e.g., `schema.table` or `table.column`) are returned as columns.
    ///
    /// Returns Ok(SimpleExpression) on success or an error when input ends unexpectedly or when encountering
    /// unsupported constructs (e.g., complex aggregate arguments).
    ///
    /// # Examples
    ///
    /// ```
    /// # use crate::parser::Parser;
    /// # use crate::ast::{SimpleExpression, SimpleValue};
    /// let mut p = Parser::new("'hello'");
    /// let expr = p.parse_primary_expression().unwrap();
    /// assert!(matches!(expr, SimpleExpression::Literal(SimpleValue::String(s)) if s == "hello"));
    /// ```
    fn parse_primary_expression(&mut self) -> Result<SimpleExpression> {
        match self.current() {
            Some(token) => {
                if token.eq_ignore_ascii_case("CASE") {
                    return self.parse_case_expression();
                }
                if token.starts_with('\'') && token.ends_with('\'') {
                    let value = token[1..token.len() - 1].to_string();
                    self.advance();
                    Ok(SimpleExpression::Literal(SimpleValue::String(value)))
                } else if token.parse::<f64>().is_ok() {
                    let value = token.to_string();
                    self.advance();
                    Ok(SimpleExpression::Literal(SimpleValue::Number(value)))
                } else if token.eq_ignore_ascii_case("true")
                    || token.eq_ignore_ascii_case("false")
                {
                    let value = token.eq_ignore_ascii_case("true");
                    self.advance();
                    Ok(SimpleExpression::Literal(SimpleValue::Boolean(value)))
                } else if token.eq_ignore_ascii_case("null") {
                    self.advance();
                    Ok(SimpleExpression::Literal(SimpleValue::Null))
                } else {
                    let identifier = token.to_string();
                    if self.tokens.get(self.pos + 1) == Some(&"(".to_string()) {
                        self.advance(); // consume identifier
                        self.advance(); // consume "("

                        if identifier.eq_ignore_ascii_case("CAST") {
                            let expr_to_cast = self.parse_expression()?;
                            self.expect("AS")?;
                            let data_type = self.parse_data_type()?;
                            self.expect(")")?;
                            return Ok(SimpleExpression::Cast { expr: Box::new(expr_to_cast), data_type });
                        }

                        let func_name = identifier.to_uppercase();

                        if ["COUNT", "SUM", "AVG", "MIN", "MAX"].contains(&func_name.as_str()) {
                            // Handle DISTINCT modifier for COUNT
                            let distinct = if func_name == "COUNT"
                                && self.current().map_or(false, |t| t.eq_ignore_ascii_case("DISTINCT"))
                            {
                                self.advance();
                                true
                            } else {
                                false
                            };

                            let arg_expr = if self.current() == Some("*") {
                                self.advance();
                                SimpleExpression::Column("*".to_string())
                            } else {
                                self.parse_expression()?
                            };
                            self.expect(")")?;

                            // For now, convert back to string for backward compatibility
                            // Consider updating AggregateFunction to accept SimpleExpression
                            let arg = match arg_expr {
                                SimpleExpression::Column(col) => {
                                    if distinct {
                                        format!("DISTINCT {}", col)
                                    } else {
                                        col
                                    }
                                },
                                _ => return Err(anyhow!("Complex expressions in aggregate functions not yet supported")),
                            };

                            return Ok(SimpleExpression::AggregateFunction { func: func_name, arg });
                        }

                        let args = self.parse_argument_list()?;
                        self.expect(")")?;
                        return Ok(SimpleExpression::FunctionCall {
                            func: func_name,
                            args,
                        });
                    }

                    // It's a column name, potentially qualified
                    let col_name = self.parse_qualified_name()?;
                    Ok(SimpleExpression::Column(col_name))
                }
            }
            None => Err(anyhow!("Unexpected end of input in expression")),
        }
    }

    fn parse_argument_list(&mut self) -> Result<Vec<SimpleExpression>> {
        let mut args = Vec::new();
        if self.current() == Some(")") {
            return Ok(args);
        }
        loop {
            args.push(self.parse_expression()?);
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        Ok(args)
    }

    fn parse_case_expression(&mut self) -> Result<SimpleExpression> {
        self.expect("CASE")?;
        let mut when_then_pairs = Vec::new();
        while self.current().map_or(false, |t| t.eq_ignore_ascii_case("WHEN")) {
            self.advance(); // consume WHEN
            let condition = self.parse_expression()?;
            self.expect("THEN")?;
            let result = self.parse_expression()?;
            when_then_pairs.push((condition, result));
        }

        let else_expression = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("ELSE")) {
            self.advance(); // consume ELSE
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        self.expect("END")?;

        Ok(SimpleExpression::Case(Box::new(CaseExpression {
            when_then_pairs,
            else_expression,
        })))
    }

    fn parse_expression_list(&mut self) -> Result<Vec<SimpleExpression>> {
        let mut exprs = Vec::new();
        loop {
            exprs.push(self.parse_primary_expression()?);
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        Ok(exprs)
    }

    /// Parses a comma-separated ORDER BY expression list, consuming tokens until the list ends.
    ///
    /// Each entry is a primary expression optionally followed by `ASC` or `DESC`.
    /// If the direction is omitted the entry defaults to ascending (`ASC`).
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = Parser::new("col1 DESC, col2, func(col3) ASC");
    /// let list = p.parse_order_by_list().unwrap();
    /// assert_eq!(list.len(), 3);
    /// assert_eq!(list[0].asc, false); // DESC
    /// assert_eq!(list[1].asc, true);  // default ASC
    /// assert_eq!(list[2].asc, true);  // explicit ASC
    /// ```
    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByExpression>> {
        let mut exprs = Vec::new();
        loop {
            let expr = self.parse_primary_expression()?;
            let asc = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("DESC")) {
                self.advance();
                false
            } else {
                if self.current().map_or(false, |t| t.eq_ignore_ascii_case("ASC")) {
                    self.advance();
                }
                true
            };
            exprs.push(OrderByExpression {
                expression: expr,
                asc,
            });
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        Ok(exprs)
    }

    /// Parse a SQL data type (with optional parameters and array notation) and return it as a single string.
    ///
    /// Recognizes:
    /// - Parameterized types: `NUMERIC(precision,scale)`, `VARCHAR(n)`, `CHAR(n)`
    /// - `DOUBLE PRECISION` (two-token type)
    /// - Postfixed array notation `[]`
    ///
    /// Returns an error if the input is truncated or expected tokens (parentheses, precision/scale, or closing brackets) are missing.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = Parser::new("NUMERIC(10,2)");
    /// let dt = p.parse_data_type().unwrap();
    /// assert_eq!(dt, "NUMERIC(10,2)");
    ///
    /// let mut p2 = Parser::new("DOUBLE PRECISION[]");
    /// let dt2 = p2.parse_data_type().unwrap();
    /// assert_eq!(dt2, "DOUBLE PRECISION[]");
    /// ```
    fn parse_data_type(&mut self) -> Result<String> {
        let mut data_type = self.current().ok_or_else(|| anyhow!("Expected data type"))?.to_string();
        self.advance();

        if (data_type.eq_ignore_ascii_case("NUMERIC") || data_type.eq_ignore_ascii_case("VARCHAR") || data_type.eq_ignore_ascii_case("CHAR")) && self.current() == Some("(") {
            self.advance(); // consume (
            data_type.push('(');
            let precision = self.current().ok_or_else(|| anyhow!("Expected precision for {}", data_type))?.to_string();
            self.advance();
            data_type.push_str(&precision);
            if self.current() == Some(",") {
                self.advance(); // consume ,
                data_type.push(',');
                let scale = self.current().ok_or_else(|| anyhow!("Expected scale for NUMERIC"))?.to_string();
                self.advance();
                data_type.push_str(&scale);
            }
            data_type.push(')');
            self.expect(")")?;
        } else if data_type.eq_ignore_ascii_case("DOUBLE") && self.current().map_or(false, |t| t.eq_ignore_ascii_case("PRECISION")) {
            self.advance(); // consume PRECISION
            data_type.push_str(" PRECISION");
        }

        if self.current() == Some("[") {
            self.advance();
            self.expect("]")?;
            data_type.push_str("[]");
        }

        Ok(data_type)
    }

    /// Parses a CREATE INDEX clause of the form:
    /// `INDEX <index_name> ON <table_name>(<expr>, ...)` and returns a CreateIndexStatement.
    ///
    /// The parser expects the caller to have already consumed any leading `CREATE` and optional
    /// `UNIQUE` token; this method parses the `INDEX` keyword, the index name, the `ON` table
    /// reference, and the parenthesized, comma-separated list of index expressions.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = Parser::new("INDEX idx_user_on_email ON users(email)");
    /// let idx = p.parse_create_index().unwrap();
    /// assert_eq!(idx.index_name, "idx_user_on_email");
    /// assert_eq!(idx.table_name, "users");
    /// assert_eq!(idx.columns.len(), 1);
    /// assert!(!idx.unique);
    /// ```
    fn parse_create_index(&mut self) -> Result<CreateIndexStatement> {
        self.expect("INDEX")?;
        let index_name = self.parse_identifier()?;
        self.expect("ON")?;
        let table_name = self.parse_qualified_name()?;
        self.expect("(")?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_expression()?);
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        self.expect(")")?;

        Ok(CreateIndexStatement {
            index_name,
            table_name,
            columns,
            unique: false, // Placeholder, UNIQUE is parsed before this
        })
    }

    /// Parse the next top-level SQL statement into an AST node.
    ///
    /// This method dispatches on the first token and parses one of the supported
    /// top-level statements, returning the corresponding `AstStatement`:
    /// - SELECT, INSERT, DELETE, UPDATE
    /// - CREATE TABLE / VIEW / SCHEMA / INDEX (an optional `UNIQUE` token before `INDEX` is honored)
    /// - DROP TABLE / VIEW
    /// - ALTER (table-level ALTER TABLE variants)
    /// - TRUNCATE (table)
    ///
    /// Returns an error for an empty input or for unsupported/unknown top-level commands.
    ///
    /// # Examples
    ///
    /// ```
    /// let ast = parse_sql("SELECT 1").unwrap();
    /// assert!(matches!(ast, AstStatement::Select(_)));
    /// ```
    pub fn parse(&mut self) -> Result<AstStatement> {
        match self.current().map(|s| s.to_uppercase()).as_deref() {
            Some("SELECT") => Ok(AstStatement::Select(self.parse_select()?)),
            Some("INSERT") => Ok(AstStatement::Insert(self.parse_insert()?)),
            Some("DELETE") => Ok(AstStatement::Delete(self.parse_delete()?)),
            Some("UPDATE") => Ok(AstStatement::Update(self.parse_update()?)),
            Some("CREATE") => {
                self.advance(); // consume CREATE
                let unique = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("UNIQUE")) {
                    self.advance();
                    true
                } else {
                    false
                };

                match self.current().map(|s| s.to_uppercase()).as_deref() {
                    Some("TABLE") => Ok(AstStatement::CreateTable(self.parse_create_table()?)),
                    Some("VIEW") => Ok(AstStatement::CreateView(self.parse_create_view()?)),
                    Some("SCHEMA") => Ok(AstStatement::CreateSchema(self.parse_create_schema()?)),
                    Some("INDEX") => {
                        let mut stmt = self.parse_create_index()?;
                        stmt.unique = unique;
                        Ok(AstStatement::CreateIndex(stmt))
                    }
                    _ => Err(anyhow!("Unsupported CREATE statement: {:?}", self.current())),
                }
            }
            Some("DROP") => {
                self.advance(); // consume DROP
                match self.current().map(|s| s.to_uppercase()).as_deref() {
                    Some("TABLE") => Ok(AstStatement::DropTable(self.parse_drop_table()?)),
                    Some("VIEW") => Ok(AstStatement::DropView(self.parse_drop_view()?)),
                    _ => Err(anyhow!("Unsupported DROP statement: {:?}", self.current())),
                }
            }
            Some("ALTER") => Ok(AstStatement::AlterTable(self.parse_alter_table()?)),
            Some("TRUNCATE") => Ok(AstStatement::TruncateTable(self.parse_truncate_table()?)),
            Some(other) => Err(anyhow!("Unsupported SQL command: {}", other)),
            None => Err(anyhow!("Empty SQL query")),
        }
    }

    /// Parse a `CREATE SCHEMA` statement.
    ///
    /// Parses the `SCHEMA` keyword followed by an identifier and returns a
    /// `CreateSchemaStatement` containing the schema name. Returns an error if the
    /// next token is not `SCHEMA` or if the following identifier is invalid.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = Parser::new("CREATE SCHEMA myschema");
    /// let stmt = p.parse_create_schema().unwrap();
    /// assert_eq!(stmt.schema_name, "myschema");
    /// ```
    fn parse_create_schema(&mut self) -> Result<CreateSchemaStatement> {
        self.expect("SCHEMA")?;
        let schema_name = self.parse_identifier()?;
        Ok(CreateSchemaStatement { schema_name })
    }

    /// Parse a `VIEW` creation statement of the form `VIEW <name> AS <select>` and return its AST.
    ///
    /// This consumes the `VIEW` token, a qualified view name, the `AS` token, and a following `SELECT`
    /// statement, producing a `CreateViewStatement { view_name, query }`.
    ///
    /// Returns an error if the expected tokens or a valid SELECT query are missing or malformed.
    ///
    /// # Examples
    ///
    /// ```
    /// let res = parse_sql("CREATE VIEW my_view AS SELECT 1;");
    /// assert!(res.is_ok());
    /// ```
    fn parse_create_view(&mut self) -> Result<CreateViewStatement> {
        self.expect("VIEW")?;
        let view_name = self.parse_qualified_name()?;
        self.expect("AS")?;
        let query = self.parse_select()?;
        Ok(CreateViewStatement { view_name, query })
    }

    /// Parse a `CREATE TABLE` statement into a `CreateTableStatement`.
    ///
    /// Supports optional `IF NOT EXISTS`, column definitions with data types and
    /// inline modifiers (`NOT NULL`, `DEFAULT`, `PRIMARY KEY`, `UNIQUE`, `CHECK`),
    /// and top-level table constraints (`PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY`,
    /// `CHECK`, optionally named via `CONSTRAINT <name>`). Inline `PRIMARY KEY`
    /// and `UNIQUE` column modifiers are converted into equivalent table
    /// constraints. Returns an error for malformed syntax (unexpected tokens,
    /// missing parentheses, empty column/constraint definitions, or invalid
    /// identifiers).
    ///
    /// # Examples
    ///
    /// ```
    /// let mut parser = Parser::new("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, CONSTRAINT u_name UNIQUE (name))");
    /// let create = parser.parse_create_table().unwrap();
    /// assert_eq!(create.table_name, "users");
    /// assert!(create.if_not_exists);
    /// assert!(!create.columns.is_empty());
    /// assert!(!create.constraints.is_empty());
    /// ```
    fn parse_create_table(&mut self) -> Result<CreateTableStatement> {
        self.expect("TABLE")?;
        let if_not_exists = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("IF")) {
            self.advance();
            self.expect("NOT")?;
            self.expect("EXISTS")?;
            true
        } else {
            false
        };
        let table_name = self.parse_qualified_name()?;

        self.expect("(")?;

        let mut columns = Vec::new();
        let mut constraints = Vec::new();

        loop {
            if self.current() == Some(")") {
                break;
            }

            let token = self.current().map(|s| s.to_uppercase());
            if let Some(t) = token {
                if ["PRIMARY", "FOREIGN", "CHECK", "CONSTRAINT", "UNIQUE"].contains(&t.as_str()) {
                    // It's a table constraint
                    let constraint = self.parse_table_constraint_definition()?;
                    constraints.push(constraint);
                } else {
                    // It's a column definition
                    let column_name = self.parse_identifier()?;
                    let data_type = self.parse_data_type()?;

                    let mut nullable = true;
                    let mut default = None;
                    let mut is_primary_key = false;
                    let mut is_unique = false;

                    'column_modifiers: loop {
                        match self.current().map(|s| s.to_uppercase()).as_deref() {
                            Some("NOT") => {
                                self.advance();
                                self.expect("NULL")?;
                                nullable = false;
                            }
                            Some("DEFAULT") => {
                                self.advance();
                                default = Some(self.parse_expression()?);
                            }
                            Some("PRIMARY") => {
                                self.advance();
                                self.expect("KEY")?;
                                is_primary_key = true;
                                nullable = false; // Primary key implies NOT NULL
                            }
                            Some("UNIQUE") => {
                                self.advance();
                                is_unique = true;
                            }
                            Some("CHECK") => {
                                self.advance(); // consume CHECK
                                self.expect("(")?;
                                let expression = self.parse_expression()?;
                                self.expect(")")?;
                                constraints.push(TableConstraint::Check {
                                    name: None, // No explicit name for inline CHECK
                                    expression,
                                });
                            }
                            _ => break 'column_modifiers,
                        }
                    }

                    columns.push(ColumnDef {
                        name: column_name.clone(), // Clone for use in constraints
                        data_type,
                        nullable,
                        default,
                    });

                    // If it was a column-level PRIMARY KEY or UNIQUE, add it to table constraints
                    if is_primary_key {
                        constraints.push(TableConstraint::PrimaryKey {
                            name: None, // No explicit name for inline PK
                            columns: vec![column_name.clone()],
                        });
                    }
                    if is_unique {
                        constraints.push(TableConstraint::Unique {
                            name: None, // No explicit name for inline UNIQUE
                            columns: vec![column_name.clone()],
                        });
                    }
                }
            } else {
                return Err(anyhow!("Expected column definition or table constraint, found end of input"));
            }

            if self.current() == Some(",") {
                self.advance();
            } else if self.current() == Some(")") {
                break;
            } else {
                return Err(anyhow!("Expected ',', or ')', found '{}'", self.current().unwrap_or("end of input")));
            }
        }

        self.expect(")")?;

        Ok(CreateTableStatement {
            table_name,
            columns,
            if_not_exists,
            constraints, // New field
        })
    }

    // New helper function to parse a table constraint definition
    /// Parse a single table constraint definition from the current token stream.
    ///
    /// Supports an optional leading `CONSTRAINT <name>` clause, followed by one of:
    /// - `PRIMARY KEY (col, ...)`
    /// - `FOREIGN KEY (...) REFERENCES ...` (delegates to `parse_foreign_key_clause`)
    /// - `CHECK (<expression>)`
    /// - `UNIQUE (col, ...)`
    ///
    /// On success returns the corresponding `TableConstraint` with the optional constraint name populated.
    /// Consumes the tokens that make up the constraint; returns an error for unexpected tokens or unsupported constraint keywords.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = Parser::new("CONSTRAINT pk_users PRIMARY KEY (id)");
    /// let constraint = p.parse_table_constraint_definition().unwrap();
    /// match constraint {
    ///     TableConstraint::PrimaryKey { name: Some(n), columns } => {
    ///         assert_eq!(n, "pk_users");
    ///         assert_eq!(columns, vec!["id".to_string()]);
    ///     }
    ///     _ => panic!("expected primary key constraint"),
    /// }
    /// ```
    fn parse_table_constraint_definition(&mut self) -> Result<TableConstraint> {
        let constraint_name = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("CONSTRAINT")) {
            self.advance();
            let name = self.parse_identifier()?;
            Some(name)
        } else {
            None
        };

        let constraint_keyword = self.current().ok_or_else(|| anyhow!("Expected constraint keyword"))?.to_uppercase();
        self.advance(); // Consume the constraint keyword (e.g., "PRIMARY", "FOREIGN", "CHECK", "UNIQUE")

        match constraint_keyword.as_str() {
            "PRIMARY" => {
                self.expect("KEY")?;
                self.expect("(")?;
                let columns = self.parse_identifier_list()?;
                self.expect(")")?;
                Ok(TableConstraint::PrimaryKey { name: constraint_name, columns })
            }
            "FOREIGN" => {
                let mut fk = self.parse_foreign_key_clause()?;
                fk.name = constraint_name;
                Ok(TableConstraint::ForeignKey(fk))
            }
            "CHECK" => {
                self.expect("(")?;
                let expression = self.parse_expression()?;
                self.expect(")")?;
                Ok(TableConstraint::Check { name: constraint_name, expression })
            }
            "UNIQUE" => {
                self.expect("(")?;
                let columns = self.parse_identifier_list()?;
                self.expect(")")?;
                Ok(TableConstraint::Unique { name: constraint_name, columns })
            }
            _ => Err(anyhow!("Unsupported constraint type: {:?}", constraint_keyword)),
        }
    }

    /// Parses a FOREIGN KEY clause and returns a ForeignKeyClause AST node.
    ///
    /// Expects the current token to be "KEY" (typically called after consuming an optional
    /// leading "FOREIGN"). Parses the referencing column list, the referenced table and
    /// column list, and optional ON DELETE / ON UPDATE actions. Supports actions like
    /// RESTRICT, NO ACTION, CASCADE, SET NULL, and SET DEFAULT (the latter two must be
    /// written as `SET NULL` / `SET DEFAULT` and are returned as a single string, e.g. `"SET NULL"`).
    ///
    /// Returns an error if required tokens (parentheses, identifiers, REFERENCES, or action
    /// targets) are missing or malformed.
    ///
    /// # Examples
    ///
    /// ```
    /// // Caller should position the parser so that the current token is "KEY".
    /// let mut p = Parser::new("KEY (col) REFERENCES other_table(other_col) ON DELETE SET NULL");
    /// // consume the leading KEY token to demonstrate typical callsite
    /// p.expect("KEY").unwrap();
    /// let fk = p.parse_foreign_key_clause().unwrap();
    /// assert_eq!(fk.columns, vec!["col".to_string()]);
    /// assert_eq!(fk.references_table, "other_table".to_string());
    /// assert_eq!(fk.references_columns, vec!["other_col".to_string()]);
    /// assert_eq!(fk.on_delete.as_deref(), Some("SET NULL"));
    /// ```
    fn parse_foreign_key_clause(&mut self) -> Result<ForeignKeyClause> {
        self.expect("KEY")?;
        self.expect("(")?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_identifier()?);
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        self.expect(")")?;

        self.expect("REFERENCES")?;
        let references_table = self.parse_qualified_name()?;

        self.expect("(")?;
        let mut references_columns = Vec::new();
        loop {
            references_columns.push(self.parse_identifier()?);
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        self.expect(")")?;

        let mut on_delete = None;
        let mut on_update = None;

        loop {
            if self.current().map_or(false, |t| t.eq_ignore_ascii_case("ON")) {
                self.advance(); // consume ON
                if self.current().map_or(false, |t| t.eq_ignore_ascii_case("DELETE")) {
                    self.advance(); // consume DELETE
                    let mut action = self.current().ok_or_else(|| anyhow!("Expected ON DELETE action"))?.to_string();
                    self.advance();
                    if action.eq_ignore_ascii_case("SET") {
                        if let Some(next_token) = self.current() {
                            if next_token.eq_ignore_ascii_case("NULL") || next_token.eq_ignore_ascii_case("DEFAULT") {
                                action.push(' ');
                                action.push_str(next_token);
                                self.advance();
                            }
                        }
                    }
                    on_delete = Some(action);
                } else if self.current().map_or(false, |t| t.eq_ignore_ascii_case("UPDATE")) {
                    self.advance(); // consume UPDATE
                    let mut action = self.current().ok_or_else(|| anyhow!("Expected ON UPDATE action"))?.to_string();
                    self.advance();
                    if action.eq_ignore_ascii_case("SET") {
                        if let Some(next_token) = self.current() {
                            if next_token.eq_ignore_ascii_case("NULL") || next_token.eq_ignore_ascii_case("DEFAULT") {
                                action.push(' ');
                                action.push_str(next_token);
                                self.advance();
                            }
                        }
                    }
                    on_update = Some(action);
                } else {
                    return Err(anyhow!("Expected DELETE or UPDATE after ON"));
                }
            } else {
                break;
            }
        }

        Ok(ForeignKeyClause {
            name: None, // Will be filled in by the caller if a CONSTRAINT name was parsed
            columns,
            references_table,
            references_columns,
            on_delete,
            on_update,
        })
    }

    /// Parse the remainder of a `DROP` statement when the next token is `TABLE`.
    ///
    /// Expects the next token to be the keyword `TABLE` (case-insensitive) followed by a qualified
    /// table name, and returns a `DropTableStatement` containing that name.
    ///
    /// # Examples
    ///
    /// ```
    /// let ast = parse_sql("DROP TABLE schema.my_table;").unwrap();
    /// match ast {
    ///     AstStatement::DropTable(dt) => assert_eq!(dt.table_name, "schema.my_table"),
    ///     _ => panic!("expected DropTable"),
    /// }
    /// ```
    fn parse_drop_table(&mut self) -> Result<DropTableStatement> {
        self.expect("TABLE")?;
        let table_name = self.parse_qualified_name()?;
        Ok(DropTableStatement { table_name })
    }

    /// Parses a `DROP VIEW` clause and returns a `DropViewStatement`.
    ///
    /// Expects the current token to be `VIEW` (case-insensitive) and then parses a qualified
    /// name for the view (e.g., `schema.view` or `view`). Returns a `DropViewStatement` with
    /// the parsed view name.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = Parser::new("DROP VIEW public.my_view;");
    /// let stmt = p.parse_drop_view().unwrap();
    /// assert_eq!(stmt.view_name, "public.my_view");
    /// ```
    fn parse_drop_view(&mut self) -> Result<DropViewStatement> {
        self.expect("VIEW")?;
        let view_name = self.parse_qualified_name()?;
        Ok(DropViewStatement { view_name })
    }

    /// Parses a `TRUNCATE TABLE` statement and returns a `TruncateTableStatement` containing the target table name.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut parser = Parser::new("TRUNCATE TABLE my_schema.my_table;");
    /// let stmt = parser.parse_truncate_table().unwrap();
    /// assert_eq!(stmt.table_name, "my_schema.my_table");
    /// ```
    fn parse_truncate_table(&mut self) -> Result<TruncateTableStatement> {
        self.expect("TRUNCATE")?;
        self.expect("TABLE")?;
        let table_name = self.parse_qualified_name()?;
        Ok(TruncateTableStatement { table_name })
    }

    /// Parse an ALTER TABLE statement and return its AST representation.
    ///
    /// Supported ALTER TABLE actions:
    /// - RENAME TO <table>
    /// - RENAME COLUMN <old> TO <new>
    /// - ADD [CONSTRAINT <name>] <constraint>
    /// - ADD [COLUMN] <name> <datatype>
    /// - DROP COLUMN <name>
    /// - DROP CONSTRAINT <name>
    /// - ALTER [COLUMN] <name> SET DEFAULT <expr>
    /// - ALTER [COLUMN] <name> SET NOT NULL
    /// - ALTER [COLUMN] <name> DROP DEFAULT
    /// - ALTER [COLUMN] <name> DROP NOT NULL
    /// - ALTER [COLUMN] <name> TYPE <new_datatype>
    ///
    /// Returns an AlterTableStatement with the parsed table name and an AlterTableAction
    /// describing the requested modification.
    ///
    /// Errors:
    /// Returns an Err if the statement is syntactically invalid, if required tokens or
    /// identifiers are missing, or if an unsupported ALTER action is encountered.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut parser = Parser::new("ALTER TABLE users RENAME TO accounts");
    /// let stmt = parser.parse_alter_table().unwrap();
    /// match stmt.action {
    ///     AlterTableAction::RenameTable { new_table_name } => {
    ///         assert_eq!(new_table_name, "accounts");
    ///     }
    ///     _ => panic!("expected RenameTable"),
    /// }
    /// ```
    fn parse_alter_table(&mut self) -> Result<AlterTableStatement> {
        self.expect("ALTER")?;
        self.expect("TABLE")?;
        let table_name = self.parse_qualified_name()?;

        let action = match self.current().map(|s| s.to_uppercase()).as_deref() {
            Some("RENAME") => {
                self.advance(); // consume RENAME
                match self.current().map(|s| s.to_uppercase()).as_deref() {
                    Some("TO") => {
                        self.advance(); // consume TO
                        let new_table_name = self.parse_qualified_name()?;
                        AlterTableAction::RenameTable { new_table_name }
                    }
                    Some("COLUMN") => {
                        self.advance(); // consume COLUMN
                        let old_column_name = self.parse_identifier()?;
                        self.expect("TO")?;
                        let new_column_name = self.parse_identifier()?;
                        AlterTableAction::RenameColumn {
                            old_column_name,
                            new_column_name,
                        }
                    }
                    _ => return Err(anyhow!("Expected TO or COLUMN after RENAME")),
                }
            }
            Some("ADD") => {
                self.advance(); // consume ADD
                if self.current().map_or(false, |t| t.eq_ignore_ascii_case("CONSTRAINT")) {
                    self.advance();
                    let name = Some(self.parse_identifier()?);
                    let constraint = self.parse_table_constraint(name)?;
                    AlterTableAction::AddConstraint(constraint)
                } else {
                    if self.current().map_or(false, |t| t.eq_ignore_ascii_case("COLUMN")) {
                        self.advance(); // consume optional COLUMN
                    }
                    let column_name = self.parse_identifier()?;
                    let data_type = self.parse_data_type()?;
                    AlterTableAction::AddColumn(ColumnDef {
                        name: column_name,
                        data_type,
                        nullable: true,
                        default: None,
                    })
                }
            }
            Some("DROP") => {
                self.advance(); // consume DROP
                match self.current().map(|s| s.to_uppercase()).as_deref() {
                    Some("COLUMN") => {
                        self.advance(); // consume COLUMN
                        let column_name = self.parse_identifier()?;
                        AlterTableAction::DropColumn { column_name }
                    }
                    Some("CONSTRAINT") => {
                        self.advance(); // consume CONSTRAINT
                        let constraint_name = self.parse_identifier()?;
                        AlterTableAction::DropConstraint { constraint_name }
                    }
                    _ => return Err(anyhow!("Expected COLUMN or CONSTRAINT after DROP")),
                }
            }
            Some("ALTER") => {
                self.advance(); // consume ALTER
                if self.current().map_or(false, |t| t.eq_ignore_ascii_case("COLUMN")) {
                    self.advance(); // consume optional COLUMN
                }
                let column_name = self.parse_identifier()?;
                match self.current().map(|s| s.to_uppercase()).as_deref() {
                    Some("SET") => {
                        self.advance(); // consume SET
                        if self.current().map_or(false, |t| t.eq_ignore_ascii_case("DEFAULT")) {
                            self.advance(); // consume DEFAULT
                            let default_expr = self.parse_expression()?;
                            AlterTableAction::AlterColumnSetDefault {
                                column_name,
                                default_expr,
                            }
                        } else if self.current().map_or(false, |t| t.eq_ignore_ascii_case("NOT")) {
                            self.advance(); // consume NOT
                            self.expect("NULL")?;
                            AlterTableAction::AlterColumnSetNotNull {
                                column_name,
                            }
                        } else {
                            return Err(anyhow!("Expected DEFAULT or NOT NULL after SET"));
                        }
                    }
                    Some("DROP") => {
                        self.advance(); // consume DROP
                        match self.current().map(|s| s.to_uppercase()).as_deref() {
                            Some("DEFAULT") => {
                                self.advance();
                                AlterTableAction::AlterColumnDropDefault { column_name }
                            }
                            Some("NOT") => {
                                self.advance();
                                self.expect("NULL")?;
                                AlterTableAction::AlterColumnDropNotNull { column_name }
                            }
                            _ => return Err(anyhow!("Expected DEFAULT or NOT NULL after DROP")),
                        }
                    }
                    Some("TYPE") => {
                        self.advance(); // consume TYPE
                        let new_data_type = self.parse_data_type()?;
                        AlterTableAction::AlterColumnType { column_name, new_data_type }
                    }
                    _ => return Err(anyhow!("Unsupported ALTER COLUMN action")),
                }
            }
            Some(other) => return Err(anyhow!("Unsupported ALTER TABLE action: {}", other)),
            None => return Err(anyhow!("Expected action for ALTER TABLE")),
        };

        Ok(AlterTableStatement { table_name, action })
    }

    /// Parse a table constraint at the current token position and return a `TableConstraint`.
    ///
    /// Supports the following constraint syntaxes (case-insensitive):
    /// - `UNIQUE (col1, col2, ...)`
    /// - `PRIMARY KEY (col1, col2, ...)`
    /// - `FOREIGN KEY ...` (delegates to `parse_foreign_key_clause`, the provided `name` is applied)
    /// - `CHECK (<expression>)`
    ///
    /// If `name` is `Some`, the parsed constraint will use that name (for `FOREIGN KEY` the name is set on the parsed clause).
    /// Returns an error for unsupported constraint types or on unexpected tokens.
    ///
    /// # Examples
    ///
    /// ```
    /// # use crate::parser::Parser;
    /// # use crate::ast::TableConstraint;
    /// let mut p = Parser::new("UNIQUE (id, other)");
    /// let constraint = p.parse_table_constraint(None).unwrap();
    /// match constraint {
    ///     TableConstraint::Unique { name, columns } => {
    ///         assert!(name.is_none());
    ///         assert_eq!(columns, vec!["id".to_string(), "other".to_string()]);
    ///     }
    ///     _ => panic!("expected UNIQUE constraint"),
    /// }
    /// ```
    fn parse_table_constraint(&mut self, name: Option<String>) -> Result<TableConstraint> {
        match self.current().map(|s| s.to_uppercase()).as_deref() {
            Some("UNIQUE") => {
                self.advance();
                self.expect("(")?;
                let columns = self.parse_identifier_list()?;
                self.expect(")")?;
                Ok(TableConstraint::Unique { name, columns })
            }
            Some("PRIMARY") => {
                self.advance();
                self.expect("KEY")?;
                self.expect("(")?;
                let columns = self.parse_identifier_list()?;
                self.expect(")")?;
                Ok(TableConstraint::PrimaryKey { name, columns })
            }
            Some("FOREIGN") => {
                let mut fk = self.parse_foreign_key_clause()?;
                fk.name = name;
                Ok(TableConstraint::ForeignKey(fk))
            }
            Some("CHECK") => {
                self.advance();
                self.expect("(")?;
                let expression = self.parse_expression()?;
                self.expect(")")?;
                Ok(TableConstraint::Check { name, expression })
            }
            _ => Err(anyhow!("Unsupported constraint type")),
        }
    }

    /// Parses a comma-separated list of unquoted identifiers and returns them as `Vec<String>`.
    ///
    /// Continues parsing identifiers while commas are present; returns an error if any individual
    /// identifier is invalid or if the input ends unexpectedly.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = Parser::new("col1, col2, col3");
    /// let ids = p.parse_identifier_list().unwrap();
    /// assert_eq!(ids, vec!["col1".to_string(), "col2".to_string(), "col3".to_string()]);
    /// ```
    fn parse_identifier_list(&mut self) -> Result<Vec<String>> {
        let mut identifiers = Vec::new();
        loop {
            identifiers.push(self.parse_identifier()?);
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        Ok(identifiers)
    }

    /// Parses an UPDATE statement.
    ///
    /// Expects the current token to be `UPDATE`, then parses:
    /// - a qualified table name,
    /// - the `SET` clause with one or more `column = expression` assignments (comma-separated),
    /// - an optional `WHERE` expression.
    ///
    /// Returns an `UpdateStatement` containing the table name, list of (column, expression) assignments,
    /// and an optional where-clause. Errors if required tokens are missing or expressions/identifiers are invalid.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = Parser::new("UPDATE users SET name = 'Alice', age = 30 WHERE id = 1");
    /// let stmt = p.parse_update().unwrap();
    /// assert_eq!(stmt.table, "users");
    /// assert_eq!(stmt.set.len(), 2);
    /// assert!(stmt.where_clause.is_some());
    /// ```
    fn parse_update(&mut self) -> Result<UpdateStatement> {
        self.expect("UPDATE")?;
        let table = self.parse_qualified_name()?;

        self.expect("SET")?;
        let mut set_clauses = Vec::new();
        loop {
            let column = self.parse_identifier()?;
            self.expect("=")?;
            let value = self.parse_expression()?;
            set_clauses.push((column, value));
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }

        let where_clause = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("WHERE")) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(UpdateStatement {
            table,
            set: set_clauses,
            where_clause,
        })
    }

    /// Parses a SQL DELETE statement at the current parser position and returns a DeleteStatement.
    ///
    /// Expects the next tokens to start with `DELETE FROM <table>` and optionally a `WHERE` clause
    /// following the table name. The returned DeleteStatement contains the target table (qualified
    /// name) and an optional where_clause expression.
    ///
    /// # Examples
    ///
    /// ```
    /// # use crate::parse_sql;
    /// let ast = parse_sql("DELETE FROM users WHERE id = 1").unwrap();
    /// match ast {
    ///     crate::ast::AstStatement::Delete(stmt) => {
    ///         assert_eq!(stmt.from_table, "users");
    ///         assert!(stmt.where_clause.is_some());
    ///     }
    ///     _ => panic!("expected DELETE statement"),
    /// }
    /// ```
    fn parse_delete(&mut self) -> Result<DeleteStatement> {
        self.expect("DELETE")?;
        self.expect("FROM")?;
        let from_table = self.parse_qualified_name()?;

        let where_clause = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("WHERE")) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(DeleteStatement {
            from_table,
            where_clause,
        })
    }

    /// Parse an `INSERT ... VALUES` statement and return an `InsertStatement`.
    ///
    /// This method expects the current position to be at the start of an `INSERT` statement
    /// and consumes tokens for the `INSERT INTO <table> [(col, ...)] VALUES (expr, ...)[, ...]` form.
    /// It returns an `InsertStatement` with:
    /// - `table`: the target table name (qualified name),
    /// - `columns`: optional column list (empty when omitted),
    /// - `values`: a vector of rows, each row being a `Vec<SimpleExpression>`.
    ///
    /// Validation and errors:
    /// - Empty column list `()` is rejected.
    /// - Empty values row `()` is rejected.
    /// - If a column list is provided, every row must have the same number of values as the column count.
    /// - If no column list is provided and multiple rows are supplied, all rows must have the same number of values.
    /// - Errors are returned via `anyhow::Result` for invalid syntax or mismatched counts.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = Parser::new("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')");
    /// let stmt = p.parse_insert().unwrap();
    /// assert_eq!(stmt.table, "users");
    /// assert_eq!(stmt.columns, vec!["id".to_string(), "name".to_string()]);
    /// assert_eq!(stmt.values.len(), 2);
    /// ```
    fn parse_insert(&mut self) -> Result<InsertStatement> {
        self.expect("INSERT")?;
        self.expect("INTO")?;
        let table = self.parse_qualified_name()?;

        let mut columns = Vec::new();
        if self.current() == Some("(") {
            self.advance(); // consume "("
            // Handle empty column list `()` which is invalid.
            if self.current() == Some(")") {
                return Err(anyhow!("Column list cannot be empty"));
            }
            loop {
                columns.push(self.parse_identifier()?);
                if self.current() == Some(",") {
                    self.advance();
                } else {
                    break;
                }
            }
            self.expect(")")?;
        }

        self.expect("VALUES")?;

        let mut all_values = Vec::new();
        loop {
            self.expect("(")?;
            let mut single_row_values = Vec::new();
            if self.current() == Some(")") {
                return Err(anyhow!("Values list cannot be empty"));
            }
            loop {
                single_row_values.push(self.parse_expression()?);
                if self.current() == Some(",") {
                    self.advance();
                } else {
                    break;
                }
            }
            self.expect(")")?;
            all_values.push(single_row_values);

            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }

        if !columns.is_empty() {
            for row_values in &all_values {
                if columns.len() != row_values.len() {
                    return Err(anyhow!(
                        "INSERT has mismatch between number of columns ({}) and values ({})",
                        columns.len(),
                        row_values.len()
                    ));
                }
            }
        } else if all_values.len() > 1 {
            // If no columns are specified, all rows must have the same number of values.
            let first_row_len = all_values[0].len();
            if all_values.iter().any(|row| row.len() != first_row_len) {
                return Err(anyhow!(
                    "INSERT with multiple rows must have the same number of values in each row"
                ));
            }
        }


        Ok(InsertStatement {
            table,
            columns,
            values: all_values,
        })
    }

    /// Parses a SELECT statement into a SelectStatement AST node.
    ///
    /// Supports:
    /// - SELECT <columns> FROM <table>
    /// - chained JOINs (INNER, LEFT [OUTER], RIGHT [OUTER], FULL [OUTER], CROSS); CROSS JOINs do not require an ON clause, other joins do
    /// - optional WHERE, GROUP BY, ORDER BY
    /// - optional LIMIT and OFFSET (either order; OFFSET may appear before LIMIT)
    /// - optional UNION [ALL] <SELECT> (parsed recursively)
    ///
    /// The parser expects qualified names for table references (e.g., `schema.table`) and builds join clauses with the join type string, target table, and ON condition. For CROSS JOIN, the ON condition is a literal TRUE expression.
    ///
    /// # Examples
    ///
    /// ```
    /// // use the public entry to parse a full SQL string; this demonstrates basic usage
    /// let ast = parse_sql("SELECT id, name FROM users WHERE active = true LIMIT 10").unwrap();
    /// // ensure parsing succeeded
    /// match ast {
    ///     AstStatement::Select(_) => {},
    ///     _ => panic!("expected SELECT statement"),
    /// }
    /// ```
    pub fn parse_select(&mut self) -> Result<SelectStatement> {
        self.expect("SELECT")?;
        let columns = self.parse_column_list()?;

        self.expect("FROM")?;
        let from_table = self.parse_qualified_name()?;

        let mut joins = Vec::new();
        loop {
            let join_type_str;
            let on_expected;

            let token = match self.current() {
                Some(t) => t.to_uppercase(),
                None => break,
            };

            if token == "LEFT" || token == "RIGHT" || token == "FULL" {
                self.advance(); // consume LEFT/RIGHT/FULL
                let mut jt = token;
                if self.current().map_or(false, |t| t.eq_ignore_ascii_case("OUTER")) {
                    self.advance();
                    jt.push_str(" OUTER");
                }
                self.expect("JOIN")?;
                jt.push_str(" JOIN");
                join_type_str = jt;
                on_expected = true;
            } else if token == "INNER" {
                self.advance();
                self.expect("JOIN")?;
                join_type_str = "INNER JOIN".to_string();
                on_expected = true;
            } else if token == "CROSS" {
                self.advance();
                self.expect("JOIN")?;
                join_type_str = "CROSS JOIN".to_string();
                on_expected = false;
            } else if token == "JOIN" {
                self.advance();
                join_type_str = "INNER JOIN".to_string();
                on_expected = true;
            } else {
                break;
            }

            let table = self.parse_qualified_name()?;

            let on_condition = if on_expected {
                self.expect("ON")?;
                self.parse_expression()? 
            } else {
                SimpleExpression::Literal(SimpleValue::Boolean(true))
            };

            joins.push(JoinClause {
                join_type: join_type_str,
                table,
                on_condition,
            });
        }

        let where_clause = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("WHERE")) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        let group_by = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("GROUP")) {
            self.advance();
            self.expect("BY")?;
            self.parse_expression_list()? 
        } else {
            Vec::new()
        };

        let order_by = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("ORDER")) {
            self.advance();
            self.expect("BY")?;
            self.parse_order_by_list()? 
        } else {
            Vec::new()
        };

        let mut limit = None;
        if self.current().map_or(false, |t| t.eq_ignore_ascii_case("LIMIT")) {
            self.advance();
            let val_str = self
                .current()
                .ok_or_else(|| anyhow!("Expected value for LIMIT"))?;
            limit = Some(val_str.parse::<usize>()?);
            self.advance();
        }

        let mut offset = None;
        if self.current().map_or(false, |t| t.eq_ignore_ascii_case("OFFSET")) {
            self.advance();
            let val_str = self
                .current()
                .ok_or_else(|| anyhow!("Expected value for OFFSET"))?;
            offset = Some(val_str.parse::<usize>()?);
            self.advance();
        }

        if limit.is_none() && self.current().map_or(false, |t| t.eq_ignore_ascii_case("LIMIT")) {
            self.advance();
            let val_str = self
                .current()
                .ok_or_else(|| anyhow!("Expected value for LIMIT"))?;
            limit = Some(val_str.parse::<usize>()?);
            self.advance();
        }

        let union = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("UNION")) {
            self.advance(); // consume UNION
            let all = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("ALL")) {
                self.advance();
                true
            } else {
                false
            };
            Some(Box::new(UnionClause {
                all,
                select: Box::new(self.parse_select()?),
            }))
        } else {
            None
        };

        Ok(SelectStatement {
            columns,
            from_table,
            joins,
            where_clause,
            group_by,
            order_by,
            limit,
            offset,
            union,
        })
    }
}

/// Parse a SQL string into the parser's AST representation.
///
/// Returns an `AstStatement` representing the first SQL statement parsed from `sql`,
/// or an error if the input is syntactically invalid or contains unsupported constructs.
///
/// # Examples
///
/// ```
/// // parse a simple SELECT statement
/// let ast = parse_sql("SELECT 1").unwrap();
/// ```
pub fn parse_sql(sql: &str) -> Result<AstStatement> {
    let mut parser = Parser::new(sql);
    parser.parse()
}
