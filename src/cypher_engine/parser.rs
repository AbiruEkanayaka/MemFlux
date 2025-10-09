use crate::cypher_engine::ast::*;
use anyhow::{anyhow, Result};

// Simple tokenizer for Cypher subset
fn tokenize(sql: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current_token = String::new();
    let mut in_string = false;
    let mut quote_char = '\0';
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_string {
            if ch == quote_char {
                current_token.push(ch);
                tokens.push(current_token.clone());
                current_token.clear();
                in_string = false;
            } else {
                current_token.push(ch);
            }
            continue;
        }

        match ch {
            '\'' | '"' => {
                if !current_token.is_empty() {
                    tokens.push(current_token.clone());
                    current_token.clear();
                }
                in_string = true;
                quote_char = ch;
                current_token.push(ch);
            }
            ' ' | '\n' | '\r' | '\t' => {
                if !current_token.is_empty() {
                    tokens.push(current_token.clone());
                    current_token.clear();
                }
            }
            '=' | ',' | '(' | ')' | '{' | '}' | ':' | '[' | ']' | '.' | '-' | '>' | '<' | '*' => {
                if !current_token.is_empty() {
                    tokens.push(current_token.clone());
                    current_token.clear();
                }
                tokens.push(ch.to_string());
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

struct CypherParser {
    tokens: Vec<String>,
    pos: usize,
}

impl CypherParser {
    fn new(sql: &str) -> Self {
        CypherParser {
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
        if token.chars().all(|c| c.is_alphanumeric() || c == '_') {
            let identifier = token.to_string();
            self.advance();
            Ok(identifier)
        } else {
            Err(anyhow!("Invalid identifier: {}", token))
        }
    }

    fn parse_optional_match_clause(&mut self) -> Result<MatchQuery> {
        self.expect("OPTIONAL")?;
        self.parse_match_clause()
    }

    fn parse_match_clause(&mut self) -> Result<MatchQuery> {
        self.expect("MATCH")?;
        let mut patterns = Vec::new();
        patterns.push(self.parse_pattern()?);
        while self.current() == Some(",") {
            self.advance(); // consume comma
            patterns.push(self.parse_pattern()?);
        }

        let where_clause = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("WHERE")) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(MatchQuery { patterns, where_clause })
    }

    fn parse_merge_clause(&mut self) -> Result<MergeClause> {
        self.expect("MERGE")?;
        let pattern = self.parse_pattern()?;
        let mut on_match = None;
        let mut on_create = None;

        loop {
            if self.current().map_or(false, |t| t.eq_ignore_ascii_case("ON")) {
                self.advance(); // Consume ON
                if self.current().map_or(false, |t| t.eq_ignore_ascii_case("MATCH")) {
                    self.advance(); // Consume MATCH
                    if on_match.is_some() {
                        return Err(anyhow!("Cannot specify ON MATCH more than once"));
                    }
                    on_match = Some(self.parse_set_clause()?);
                } else if self.current().map_or(false, |t| t.eq_ignore_ascii_case("CREATE")) {
                    self.advance(); // Consume CREATE
                    if on_create.is_some() {
                        return Err(anyhow!("Cannot specify ON CREATE more than once"));
                    }
                    on_create = Some(self.parse_set_clause()?);
                } else {
                    return Err(anyhow!("Expected MATCH or CREATE after ON"));
                }
            } else {
                break;
            }
        }

        Ok(MergeClause { pattern, on_match, on_create })
    }

    fn parse_create_clause(&mut self) -> Result<Pattern> {
        self.expect("CREATE")?;
        self.parse_pattern()
    }

    fn parse_set_clause(&mut self) -> Result<SetClause> {
        self.expect("SET")?;
        let mut items = Vec::new();
        loop {
            let property = self.parse_property_access_expression()?;
            self.expect("=")?;
            let expression = self.parse_expression()?;
            items.push(SetItem { property, expression });
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        Ok(SetClause { items })
    }

    fn parse_delete_clause(&mut self) -> Result<DeleteClause> {
        let detach = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("DETACH")) {
            self.advance();
            true
        } else {
            false
        };
        self.expect("DELETE")?;

        let mut expressions = Vec::new();
        loop {
            expressions.push(self.parse_expression()?);
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        Ok(DeleteClause { expressions, detach })
    }

    fn parse_remove_clause(&mut self) -> Result<RemoveClause> {
        self.expect("REMOVE")?;
        let mut items = Vec::new();
        loop {
            items.push(self.parse_expression()?);
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        Ok(RemoveClause { items })
    }

    fn parse_map_literal(&mut self) -> Result<Expression> {
        self.expect("{")?;
        let mut props = Vec::new();
        if self.current() == Some("}") {
            self.advance();
            return Ok(Expression::Map(props));
        }
        loop {
            let key = self.parse_identifier()?;
            self.expect(":")?;
            let value = self.parse_expression()?;
            props.push((key, value));
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        self.expect("}")?;
        Ok(Expression::Map(props))
    }

    fn parse_pattern(&mut self) -> Result<Pattern> {
        let variable = if self.tokens.get(self.pos + 1) == Some(&"=".to_string()) {
            let var = self.parse_identifier()?;
            self.expect("=")?;
            Some(var)
        } else {
            None
        };

        let mut parts = vec![self.parse_node_pattern()?];
        while self.current() == Some("-") || self.current() == Some("<") {
            parts.push(self.parse_relationship_pattern()?);
            parts.push(self.parse_node_pattern()?);
        }
        Ok(Pattern { parts, variable })
    }

    fn parse_node_pattern(&mut self) -> Result<PatternPart> {
        self.expect("(")?;
        let variable = if self.current() != Some(":") && self.current() != Some(")") && self.current() != Some("{") {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let mut labels = Vec::new();
        if self.current() == Some(":") {
            self.advance();
            labels.push(self.parse_identifier()?);
        }
        let properties = if self.current() == Some("{") {
            Some(self.parse_map_literal()?)
        } else {
            None
        };
        self.expect(")")?;
        Ok(PatternPart::Node(NodePattern { variable, labels, properties }))
    }

    fn parse_relationship_pattern(&mut self) -> Result<PatternPart> {
        let has_left_arrow = if self.current() == Some("<") {
            self.advance();
            true
        } else {
            false
        };

        self.expect("-")?;

        self.expect("[")?;
        let variable = if self.current() != Some(":") && self.current() != Some("]") && self.current() != Some("{") && self.current() != Some("*") {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let mut types = Vec::new();
        if self.current() == Some(":") {
            self.advance();
            types.push(self.parse_identifier()?);
        }
        let mut properties = None;
        let mut range: Option<(Option<u32>, Option<u32>)> = None;
        loop {
            if self.current() == Some("*") && range.is_none() {
                self.advance(); // consume *
                let mut min = None;
                let mut max = None;
                // Cases: * | *.. | *N | *N.. | *..M | *M..N
                if self.current().map_or(false, |t| t.chars().all(char::is_numeric) || t == ".") {
                    if self.current().map_or(false, |t| t.chars().all(char::is_numeric)) {
                        let num = self.current().unwrap().parse::<u32>()?;
                        self.advance();
                        min = Some(num);
                        if self.current() == Some(".") {
                            self.advance();
                            self.expect(".")?;
                            if self.current().map_or(false, |t| t.chars().all(char::is_numeric)) {
                                max = Some(self.current().unwrap().parse::<u32>()?);
                                self.advance();
                            }
                        } else {
                            max = Some(num); // *N exact
                        }
                    } else if self.current() == Some(".") {
                        self.advance();
                        self.expect(".")?;
                        max = Some(self.current().ok_or_else(|| anyhow!("Expected a number after *.."))?.parse::<u32>()?);
                        self.advance();
                    }
                }
                range = Some((min, max));
                continue;
            }
            if self.current() == Some("{") && properties.is_none() {
                properties = Some(self.parse_map_literal()?);
                continue;
            }
            break;
        }

        self.expect("]")?;

        self.expect("-")?;

        let has_right_arrow = if self.current() == Some(">") {
            self.advance();
            true
        } else {
            false
        };

        let direction = match (has_left_arrow, has_right_arrow) {
            (false, true) => RelationshipDirection::Outgoing,
            (true, false) => RelationshipDirection::Incoming,
            (false, false) => RelationshipDirection::Both,
            (true, true) => return Err(anyhow!("Invalid relationship pattern: <-->")),
        };

        Ok(PatternPart::Relationship(RelationshipPattern { direction, variable, types, properties, range }))
    }

    fn parse_return_clause(&mut self) -> Result<ReturnClause> {
        self.expect("RETURN")?;
        let mut items = Vec::new();
        loop {
            items.push(self.parse_return_item()?);
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        Ok(ReturnClause { items })
    }

    fn parse_order_by_clause(&mut self) -> Result<OrderByClause> {
        self.expect("ORDER")?;
        self.expect("BY")?;
        let mut items = Vec::new();
        loop {
            items.push(self.parse_order_by_item()?);
            if self.current() == Some(",") {
                self.advance();
            } else {
                break;
            }
        }
        Ok(OrderByClause { items })
    }

    fn parse_order_by_item(&mut self) -> Result<OrderByItem> {
        let expression = self.parse_expression()?;
        let asc = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("DESC")) {
            self.advance();
            false
        } else {
            if self.current().map_or(false, |t| t.eq_ignore_ascii_case("ASC")) {
                self.advance();
            }
            true
        };
        Ok(OrderByItem { expression, asc })
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem> {
        let expression = self.parse_expression()?;
        let alias = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("AS")) {
            self.advance();
            Some(self.parse_identifier()?)
        } else {
            None
        };
        Ok(ReturnItem { expression, alias })
    }

    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_comparison_expression()
    }

    fn parse_comparison_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_property_access_expression()?;
        if self.current() == Some("=") {
            self.advance();
            let right = self.parse_comparison_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: "=".to_string(),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_property_access_expression(&mut self) -> Result<Expression> {
        let mut expr = self.parse_primary_expression()?;
        while self.current() == Some(".") {
            self.advance();
            let prop = self.parse_identifier()?;
            expr = Expression::Property(Box::new(expr), prop);
        }
        Ok(expr)
    }

    fn parse_primary_expression(&mut self) -> Result<Expression> {
        let token = self.current().ok_or_else(|| anyhow!("Unexpected end of expression"))?;
        if (token.starts_with('\'') && token.ends_with('\'')) || (token.starts_with('"') && token.ends_with('"')) {
            let value = token[1..token.len() - 1].to_string();
            self.advance();
            return Ok(Expression::Literal(LiteralValue::String(value)));
        } else if let Ok(num) = token.parse::<i64>() {
            self.advance();
            return Ok(Expression::Literal(LiteralValue::Integer(num)));
        } else if token.eq_ignore_ascii_case("true") {
            self.advance();
            return Ok(Expression::Literal(LiteralValue::Boolean(true)));
        } else if token.eq_ignore_ascii_case("false") {
            self.advance();
            return Ok(Expression::Literal(LiteralValue::Boolean(false)));
        } else if token == "{" {
            return self.parse_map_literal();
        } else {
            let identifier = self.parse_identifier()?;
            if self.current() == Some("(") {
                self.advance(); // consume (

                if identifier.eq_ignore_ascii_case("shortestPath") {
                    let pattern = self.parse_pattern()?;
                    self.expect(")")?;
                    return Ok(Expression::ShortestPath(Box::new(pattern)));
                }

                let args = self.parse_argument_list()?;
                self.expect(")")?;
                Ok(Expression::FunctionCall {
                    func: identifier,
                    args,
                })
            } else {
                Ok(Expression::Variable(identifier))
            }
        }
    }

    fn parse_argument_list(&mut self) -> Result<Vec<Expression>> {
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



    pub fn parse(&mut self) -> Result<CypherQuery> {
        let mut query = CypherQuery::default();
        loop {
            let token = self.current().map(|s| s.to_uppercase());
            match token.as_deref() {
                Some("OPTIONAL") => query.clauses.push(Clause::OptionalMatch(self.parse_optional_match_clause()?)),
                Some("MATCH") => query.clauses.push(Clause::Match(self.parse_match_clause()?)),
                Some("CREATE") => query.clauses.push(Clause::Create(self.parse_create_clause()?)),
                Some("MERGE") => query.clauses.push(Clause::Merge(self.parse_merge_clause()?)),
                Some("REMOVE") => query.clauses.push(Clause::Remove(self.parse_remove_clause()?)),
                Some("SET") => query.clauses.push(Clause::Set(self.parse_set_clause()?)),
                Some("DELETE") | Some("DETACH") => query.clauses.push(Clause::Delete(self.parse_delete_clause()?)),
                Some("RETURN") => query.clauses.push(Clause::Return(self.parse_return_clause()?)),
                Some("ORDER") => query.clauses.push(Clause::OrderBy(self.parse_order_by_clause()?)),
                Some(other) => return Err(anyhow!("Unsupported clause: {}", other)),
                None => break,
            }
        }
        if query.clauses.is_empty() {
            return Err(anyhow!("Query is empty"));
        }
        Ok(query)
    }
}

pub fn parse_cypher(sql: &str) -> Result<CypherQuery> {
    let mut parser = CypherParser::new(sql);
    parser.parse()
}
