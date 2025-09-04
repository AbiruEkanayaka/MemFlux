use anyhow::{anyhow, Result};

use super::ast::{AlterTableAction, AstStatement, CaseExpression, ColumnDef, CreateIndexStatement,
    CreateSchemaStatement, CreateTableStatement, CreateViewStatement, DeleteStatement,
    DropTableStatement, DropViewStatement, ForeignKeyClause, InsertStatement, JoinClause,
    OrderByExpression, SelectColumn, SelectStatement, SimpleExpression, SimpleValue,
    TableConstraint, TruncateTableStatement, UnionClause, UpdateStatement, AlterTableStatement};

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

    fn parse_qualified_name(&mut self) -> Result<String> {
        let mut name = self.parse_identifier()?;
        if self.current() == Some(".") {
            self.advance(); // consume dot
            name.push('.');
            name.push_str(&self.parse_identifier()?);
        }
        Ok(name)
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
                            let arg = if self.current() == Some("*") {
                                self.advance();
                                "*".to_string()
                            } else {
                                self.parse_identifier()?
                            };
                            self.expect(")")?;
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

    fn parse_create_schema(&mut self) -> Result<CreateSchemaStatement> {
        self.expect("SCHEMA")?;
        let schema_name = self.parse_identifier()?;
        Ok(CreateSchemaStatement { schema_name })
    }

    fn parse_create_view(&mut self) -> Result<CreateViewStatement> {
        self.expect("VIEW")?;
        let view_name = self.parse_qualified_name()?;
        self.expect("AS")?;
        let query = self.parse_select()?;
        Ok(CreateViewStatement { view_name, query })
    }

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
        if self.current().map_or(false, |t| t.eq_ignore_ascii_case("ON")) {
            self.advance();
            self.expect("DELETE")?;
            let action = self.current().ok_or_else(|| anyhow!("Expected ON DELETE action"))?.to_string();
            self.advance();
            on_delete = Some(action.to_string());
        }

        let mut on_update = None;
        if self.current().map_or(false, |t| t.eq_ignore_ascii_case("ON")) {
            self.advance();
            self.expect("UPDATE")?;
            let action = self.current().ok_or_else(|| anyhow!("Expected ON UPDATE action"))?.to_string();
            self.advance();
            on_update = Some(action.to_string());
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
        let table_name = self.parse_qualified_name()?;
        Ok(DropTableStatement { table_name })
    }

    fn parse_drop_view(&mut self) -> Result<DropViewStatement> {
        self.expect("VIEW")?;
        let view_name = self.parse_qualified_name()?;
        Ok(DropViewStatement { view_name })
    }

    fn parse_truncate_table(&mut self) -> Result<TruncateTableStatement> {
        self.expect("TRUNCATE")?;
        self.expect("TABLE")?;
        let table_name = self.parse_qualified_name()?;
        Ok(TruncateTableStatement { table_name })
    }

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

pub fn parse_sql(sql: &str) -> Result<AstStatement> {
    let mut parser = Parser::new(sql);
    parser.parse()
}
