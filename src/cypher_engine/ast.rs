use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CypherQuery {
    Match(MatchQuery),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MatchQuery {
    pub pattern: Pattern,
    pub where_clause: Option<Expression>,
    pub return_clause: ReturnClause,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Pattern {
    pub parts: Vec<PatternPart>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PatternPart {
    Node(NodePattern),
    Relationship(RelationshipPattern),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Option<Expression>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RelationshipPattern {
    pub direction: RelationshipDirection,
    pub variable: Option<String>,
    pub types: Vec<String>,
    pub properties: Option<Expression>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RelationshipDirection {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReturnClause {
    pub items: Vec<ReturnItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReturnItem {
    pub expression: Expression,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Expression {
    Variable(String),
    Property(Box<Expression>, String),
    Literal(LiteralValue),
    BinaryOp {
        left: Box<Expression>,
        op: String,
        right: Box<Expression>,
    },
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Variable(s) => write!(f, "{}", s),
            Expression::Property(expr, prop) => write!(f, "{}.{}", expr, prop),
            Expression::Literal(lit) => write!(f, "{}", lit),
            Expression::BinaryOp { left, op, right } => write!(f, "{} {} {}", left, op, right),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LiteralValue {
    String(String),
    Integer(i64),
}

impl fmt::Display for LiteralValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LiteralValue::String(s) => write!(f, "'{}'", s),
            LiteralValue::Integer(i) => write!(f, "{}", i),
        }
    }
}