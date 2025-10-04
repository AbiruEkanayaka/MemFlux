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
            '=' | ',' | '(' | ')' | '{' | '}' | ':' | '[' | ']' | '.' | '-' | '>' | '<' => {
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

    fn parse_match_query(&mut self) -> Result<MatchQuery> {
        self.expect("MATCH")?;
        let pattern = self.parse_pattern()?;

        let where_clause = if self.current().map_or(false, |t| t.eq_ignore_ascii_case("WHERE")) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        let return_clause = self.parse_return_clause()?;

        Ok(MatchQuery { pattern, where_clause, return_clause })
    }

    fn parse_pattern(&mut self) -> Result<Pattern> {
        let mut parts = vec![self.parse_node_pattern()?];
        while self.current() == Some("-") || self.current() == Some("<") {
            parts.push(self.parse_relationship_pattern()?);
            parts.push(self.parse_node_pattern()?);
        }
        Ok(Pattern { parts })
    }

    fn parse_node_pattern(&mut self) -> Result<PatternPart> {
        self.expect("(")?;
        let variable = if self.current() != Some(":") && self.current() != Some(")") {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let mut labels = Vec::new();
        if self.current() == Some(":") {
            self.advance();
            labels.push(self.parse_identifier()?);
        }
        self.expect(")")?;
        Ok(PatternPart::Node(NodePattern { variable, labels, properties: None }))
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
        let variable = if self.current() != Some(":") && self.current() != Some("]") {
            Some(self.parse_identifier()?)
        } else {
            None
        };
        let mut types = Vec::new();
        if self.current() == Some(":") {
            self.advance();
            types.push(self.parse_identifier()?);
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

        Ok(PatternPart::Relationship(RelationshipPattern { direction, variable, types, properties: None }))
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
        let mut expr = self.parse_primary_expression()?;
        while self.current() == Some(".") {
            self.advance();
            let prop = self.parse_identifier()?;
            expr = Expression::Property(Box::new(expr), prop);
        }
        if self.current() == Some("=") {
            self.advance();
            let right = self.parse_primary_expression()?;
            expr = Expression::BinaryOp { left: Box::new(expr), op: "=".to_string(), right: Box::new(right) };
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
        } else {
            let var = self.parse_identifier()?;
            return Ok(Expression::Variable(var));
        }
    }

    pub fn parse(&mut self) -> Result<CypherQuery> {
        match self.current().map(|s| s.to_uppercase()).as_deref() {
            Some("MATCH") => Ok(CypherQuery::Match(self.parse_match_query()?)),
            _ => Err(anyhow!("Unsupported query type, expected MATCH"))
        }
    }
}

pub fn parse_cypher(sql: &str) -> Result<CypherQuery> {
    let mut parser = CypherParser::new(sql);
    parser.parse()
}
