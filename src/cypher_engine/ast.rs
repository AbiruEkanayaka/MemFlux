use serde::{Deserialize, Serialize};
use std::fmt;

pub type Row = serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct CypherQuery {
    pub clauses: Vec<Clause>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Clause {
    Match(MatchQuery),
    OptionalMatch(MatchQuery),
    Create(Pattern),
    Merge(MergeClause),
    Set(SetClause),
    Remove(RemoveClause),
    Delete(DeleteClause),
    Return(ReturnClause),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MergeClause {
    pub pattern: Pattern,
    pub on_match: Option<SetClause>,
    pub on_create: Option<SetClause>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RemoveClause {
    pub items: Vec<Expression>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MatchQuery {
    pub pattern: Pattern,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SetClause {
    pub items: Vec<SetItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SetItem {
    pub property: Expression,
    pub expression: Expression,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeleteClause {
    pub expressions: Vec<Expression>,
    pub detach: bool,
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
    pub range: Option<(Option<u32>, Option<u32>)>, // Added for variable-length paths
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
    Map(Vec<(String, Expression)>),
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
            Expression::Map(props) => {
                let items: Vec<String> = props.iter().map(|(k, v)| format!("{}: {}", k, v)).collect();
                write!(f, "{{{}}}", items.join(", "))
            }
            Expression::BinaryOp { left, op, right } => write!(f, "{} {} {}", left, op, right),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LiteralValue {
    String(String),
    Integer(i64),
    Boolean(bool),
}

impl fmt::Display for LiteralValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LiteralValue::String(s) => write!(f, "'{}'", s),
            LiteralValue::Integer(i) => write!(f, "{}", i),
            LiteralValue::Boolean(b) => write!(f, "{}", b),
        }
    }
}