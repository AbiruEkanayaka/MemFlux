use anyhow::{anyhow, Result};

use super::ast::*;

// Very simple tokenizer
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
            ' ' | '\n' | '\r' | '\t' => {
                if !current_token.is_empty() {
                    tokens.push(current_token.clone());
                    current_token.clear();
                }
            }
            '=' | ',' | '(' | ')' | '>' | '<' | '!' | '[' | ']' => {
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
                        alias = Some(
                            self.current()
                                .ok_or_else(|| anyhow!("Expected alias after AS"))?
                                .to_string(),
                        );
                        self.advance();
                    } else if let Some(token) = self.current() {
                        // Implicit alias (e.g. "SELECT col1 c1")
                        if !["FROM", ",", "WHERE", "ORDER", "GROUP", "LIMIT", "OFFSET", "JOIN"]
                            .iter()
                            .any(|k| k.eq_ignore_ascii_case(token)) {
                            alias = Some(token.to_string());
                            self.advance();
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
                        // This would be for a list of values, e.g. IN (1, 2, 3)
                        // For now, we only support subqueries as per the plan.
                        // We can just parse a single expression for now for `= (...)`
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
                    // This could be a column, or a function call
                    let identifier = token.to_string();
                    if self.tokens.get(self.pos + 1) == Some(&"(".to_string()) {
                        // Function call or CAST
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

                        // Special case for aggregate functions for now
                        if ["COUNT", "SUM", "AVG", "MIN", "MAX"].contains(&func_name.as_str()) {
                            let arg = self
                                .current()
                                .ok_or_else(|| anyhow!("Expected argument for {}", func_name))? 
                                .to_string();
                            self.advance(); // consume argument
                            self.expect(")")?;
                            return Ok(SimpleExpression::AggregateFunction { func: func_name, arg });
                        }

                        // Generic function call
                        let args = self.parse_argument_list()?;
                        self.expect(")")?;
                        return Ok(SimpleExpression::FunctionCall {
                            func: func_name,
                            args,
                        });
                    }

                    // Column name
                    let mut col_name = identifier;
                    self.advance();
                    if self.current() == Some(".") {
                        self.advance();
                        col_name.push('.');
                        if let Some(col_part) = self.current() {
                            col_name.push_str(col_part);
                            self.advance();
                        } else {
                            return Err(anyhow!("Expected column part after '.'"));
                        }
                    }
                    Ok(SimpleExpression::Column(col_name))
                }
            }
            None => Err(anyhow!("Unexpected end of input in expression"))
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

    pub fn parse(&mut self) -> Result<AstStatement> {
        match self.current().map(|s| s.to_uppercase()).as_deref() {
            Some("SELECT") => Ok(AstStatement::Select(self.parse_select()?)),
            Some("INSERT") => Ok(AstStatement::Insert(self.parse_insert()?)),
            Some("DELETE") => Ok(AstStatement::Delete(self.parse_delete()?)),
            Some("UPDATE") => Ok(AstStatement::Update(self.parse_update()?)),
            Some("CREATE") => Ok(AstStatement::CreateTable(self.parse_create_table()?)),
            Some("DROP") => {
                self.advance(); // consume DROP
                Ok(AstStatement::DropTable(self.parse_drop_table()?))
            }
            Some("ALTER") => Ok(AstStatement::AlterTable(self.parse_alter_table()?)),
            Some("TRUNCATE") => Ok(AstStatement::TruncateTable(self.parse_truncate_table()?)),
            Some(other) => Err(anyhow!("Unsupported SQL command: {}", other)),
            None => Err(anyhow!("Empty SQL query")),
        }
    }

    fn parse_create_table(&mut self) -> Result<CreateTableStatement> {
        self.expect("CREATE")?;
        self.expect("TABLE")?;
        let table_name = self
            .current()
            .ok_or_else(|| anyhow!("Expected table name after CREATE TABLE"))?
            .to_string();
        self.advance();

        self.expect("(")?;

        let mut columns = Vec::new();
        let mut primary_key = Vec::new();
        let mut foreign_keys = Vec::new();
        let mut check = None;

        loop {
            if self.current() == Some(")") {
                break;
            }

            // Check for table-level constraints
            if let Some(token) = self.current() {
                let upper_token = token.to_uppercase();
                if upper_token == "PRIMARY" || upper_token == "FOREIGN" || upper_token == "CHECK" || upper_token == "CONSTRAINT" || upper_token == "UNIQUE" {
                    break;
                }
            }

            let column_name = self
                .current()
                .ok_or_else(|| anyhow!("Expected column name or constraint definition"))?
                .to_string();
            self.advance();
            let data_type = self.parse_data_type()?;

            let mut nullable = true;
            let mut unique = false;
            let mut default = None;

            // Loop for inline constraints
            'inline: loop {
                match self.current().map(|s| s.to_uppercase()).as_deref() {
                    Some("NOT") => {
                        self.advance();
                        self.expect("NULL")?;
                        nullable = false;
                    }
                    Some("UNIQUE") => {
                        self.advance();
                        unique = true;
                    }
                    Some("PRIMARY") => {
                        self.advance();
                        self.expect("KEY")?;
                        if !primary_key.is_empty() {
                            return Err(anyhow!("Multiple primary keys defined"));
                        }
                        primary_key.push(column_name.clone());
                        nullable = false;
                        unique = true;
                    }
                    Some("DEFAULT") => {
                        self.advance();
                        default = Some(self.parse_expression()?);
                    }
                    _ => break 'inline,
                }
            }

            columns.push(ColumnDef {
                name: column_name,
                data_type,
                nullable,
                unique,
                default,
            });

            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }

        // Now parse table-level constraints
        loop {
            if self.current() == Some(")") {
                break;
            }
            if self.current() == Some(",") {
                self.advance();
            }

            let constraint_name = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("CONSTRAINT")) {
                self.advance();
                let name = self.current().ok_or_else(|| anyhow!("Expected constraint name"))?.to_string();
                self.advance();
                Some(name)
            } else {
                None
            };

            match self.current().map(|s| s.to_uppercase()).as_deref() {
                Some("PRIMARY") => {
                    self.advance();
                    self.expect("KEY")?;
                    self.expect("(")?;
                    if !primary_key.is_empty() {
                        return Err(anyhow!("Multiple primary keys defined"));
                    }
                    loop {
                        primary_key.push(self.current().ok_or_else(|| anyhow!("Expected column name in PRIMARY KEY"))?.to_string());
                        self.advance();
                        if self.current() == Some(",") {
                            self.advance();
                        } else {
                            break;
                        }
                    }
                    self.expect(")")?;
                }
                Some("FOREIGN") => {
                    let mut fk = self.parse_foreign_key_clause()?;
                    fk.name = constraint_name;
                    foreign_keys.push(fk);
                }
                Some("CHECK") => {
                    self.advance();
                    self.expect("(")?;
                    check = Some(self.parse_expression()?);
                    self.expect(")")?;
                }
                Some("UNIQUE") => {
                    self.advance();
                    self.expect("(")?;
                    loop {
                        let col_name = self.current().ok_or_else(|| anyhow!("Expected column name in UNIQUE constraint"))?.to_string();
                        if let Some(col) = columns.iter_mut().find(|c| c.name == col_name) {
                            col.unique = true;
                        } else {
                            return Err(anyhow!("Column '{}' in UNIQUE constraint not found in table definition", col_name));
                        }
                        self.advance();
                        if self.current() == Some(",") {
                            self.advance();
                        } else {
                            break;
                        }
                    }
                    self.expect(")")?;
                }
                _ => {
                    if self.current() == Some(")") {
                        break;
                    }
                    return Err(anyhow!("Unexpected token in CREATE TABLE: {:?}", self.current()));
                }
            }
        }

        self.expect(")")?;

        Ok(CreateTableStatement {
            table_name,
            columns,
            primary_key,
            foreign_keys,
            check,
        })
    }

    fn parse_foreign_key_clause(&mut self) -> Result<ForeignKeyClause> {
        self.expect("FOREIGN")?;
        self.expect("KEY")?;
        self.expect("(")?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.current().ok_or_else(|| anyhow!("Expected column name in FOREIGN KEY"))?.to_string());
            self.advance();
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        self.expect(")")?;

        self.expect("REFERENCES")?;
        let references_table = self.current().ok_or_else(|| anyhow!("Expected referenced table name"))?.to_string();
        self.advance();

        self.expect("(")?;
        let mut references_columns = Vec::new();
        loop {
            references_columns.push(self.current().ok_or_else(|| anyhow!("Expected referenced column name"))?.to_string());
            self.advance();
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        self.expect(")")?;

        let mut on_delete = None;
        if self.current().map_or(false, |t| t.eq_ignore_ascii_case("ON")) {
            self.advance();
            self.expect("DELETE")?;
            let action = self.current().ok_or_else(|| anyhow!("Expected ON DELETE action"))?.to_string();
            self.advance();
            on_delete = Some(action);
        }

        let mut on_update = None;
        if self.current().map_or(false, |t| t.eq_ignore_ascii_case("ON")) {
            self.advance();
            self.expect("UPDATE")?;
            let action = self.current().ok_or_else(|| anyhow!("Expected ON UPDATE action"))?.to_string();
            self.advance();
            on_update = Some(action);
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

    fn parse_drop_table(&mut self) -> Result<DropTableStatement> {
        self.expect("TABLE")?;
        let table_name = self
            .current()
            .ok_or_else(|| anyhow!("Expected table name after DROP TABLE"))?
            .to_string();
        self.advance();

        Ok(DropTableStatement { table_name })
    }

    fn parse_truncate_table(&mut self) -> Result<TruncateTableStatement> {
        self.expect("TRUNCATE")?;
        self.expect("TABLE")?;
        let table_name = self
            .current()
            .ok_or_else(|| anyhow!("Expected table name after TRUNCATE TABLE"))?
            .to_string();
        self.advance();

        Ok(TruncateTableStatement { table_name })
    }

    fn parse_alter_table(&mut self) -> Result<AlterTableStatement> {
        self.expect("ALTER")?;
        self.expect("TABLE")?;
        let table_name = self
            .current()
            .ok_or_else(|| anyhow!("Expected table name after ALTER TABLE"))?
            .to_string();
        self.advance();

        let action = match self.current().map(|s| s.to_uppercase()).as_deref() {
            Some("ADD") => {
                self.advance(); // consume ADD
                if self.current().map_or(false, |t| t.eq_ignore_ascii_case("COLUMN")) {
                    self.advance(); // consume optional COLUMN
                }
                let column_name = self
                    .current()
                    .ok_or_else(|| anyhow!("Expected column name after ADD"))?
                    .to_string();
                self.advance();
                let data_type = self.parse_data_type()?;
                AlterTableAction::AddColumn(ColumnDef {
                    name: column_name,
                    data_type,
                    nullable: true,
                    unique: false,
                    default: None,
                })
            }
            Some("DROP") => {
                self.advance(); // consume DROP
                if self.current().map_or(false, |t| t.eq_ignore_ascii_case("COLUMN")) {
                    self.advance(); // consume optional COLUMN
                }
                let column_name = self
                    .current()
                    .ok_or_else(|| anyhow!("Expected column name after DROP"))?
                    .to_string();
                self.advance();
                AlterTableAction::DropColumn { column_name }
            }
            Some(other) => return Err(anyhow!("Unsupported ALTER TABLE action: {}", other)),
            None => return Err(anyhow!("Expected action for ALTER TABLE")),
        };

        Ok(AlterTableStatement { table_name, action })
    }

    fn parse_update(&mut self) -> Result<UpdateStatement> {
        self.expect("UPDATE")?;
        let table = self
            .current()
            .ok_or_else(|| anyhow!("Expected table name after UPDATE"))?
            .to_string();
        self.advance();

        self.expect("SET")?;
        let mut set_clauses = Vec::new();
        loop {
            let column = self
                .current()
                .ok_or_else(|| anyhow!("Expected column name for SET"))?
                .to_string();
            self.advance();
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

    fn parse_delete(&mut self) -> Result<DeleteStatement> {
        self.expect("DELETE")?;
        self.expect("FROM")?;
        let from_table = self
            .current()
            .ok_or_else(|| anyhow!("Expected table name after FROM"))?
            .to_string();
        self.advance();

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

    fn parse_insert(&mut self) -> Result<InsertStatement> {
        self.expect("INSERT")?;
        self.expect("INTO")?;
        let table = self
            .current()
            .ok_or_else(|| anyhow!("Expected table name after INTO"))?
            .to_string();
        self.advance();

        self.expect("(")?;
        let mut columns = Vec::new();
        loop {
            columns.push(
                self.current()
                    .ok_or_else(|| anyhow!("Expected column name"))?
                    .to_string(),
            );
            self.advance();
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        self.expect(")")?;

        self.expect("VALUES")?;

        self.expect("(")?;
        let mut values = Vec::new();
        loop {
            values.push(self.parse_expression()?);
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        self.expect(")")?;

        if !columns.is_empty() && columns.len() != values.len() {
            return Err(anyhow!(
                "INSERT has mismatch between number of columns and values"
            ));
        }

        Ok(InsertStatement {
            table,
            columns,
            values,
        })
    }

    pub fn parse_select(&mut self) -> Result<SelectStatement> {
        self.expect("SELECT")?;
        let columns = self.parse_column_list()?;

        self.expect("FROM")?;
        let from_table = self
            .current()
            .ok_or_else(|| anyhow!("Expected table name after FROM"))?
            .to_string();
        self.advance();

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

            let table = self
                .current()
                .ok_or_else(|| anyhow!("Expected table name after JOIN"))?
                .to_string();
            self.advance();

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
                .ok_or_else(|| anyhow!("Expected value for LIMIT"))?
                .to_string();
            limit = Some(val_str.parse::<usize>()?);
            self.advance();
        }

        let mut offset = None;
        if self.current().map_or(false, |t| t.eq_ignore_ascii_case("OFFSET")) {
            self.advance();
            let val_str = self
                .current()
                .ok_or_else(|| anyhow!("Expected value for OFFSET"))?
                .to_string();
            offset = Some(val_str.parse::<usize>()?);
            self.advance();
        }

        // Some SQL dialects allow OFFSET before LIMIT. Let's check for LIMIT again.
        if limit.is_none() && self.current().map_or(false, |t| t.eq_ignore_ascii_case("LIMIT")) {
            self.advance();
            let val_str = self
                .current()
                .ok_or_else(|| anyhow!("Expected value for LIMIT"))?
                .to_string();
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

pub fn parse_sql(sql: &str) -> Result<AstStatement> {
    let mut parser = Parser::new(sql);
    parser.parse()
}