
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SimpleValue {
    String(String),
    Number(String),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SimpleExpression {
    Column(String),
    Literal(SimpleValue),
    BinaryOp {
        left: Box<SimpleExpression>,
        op: String,
        right: Box<SimpleExpression>,
    },
    LogicalOp {
        left: Box<SimpleExpression>,
        op: String, // "AND" or "OR"
        right: Box<SimpleExpression>,
    },
    AggregateFunction {
        func: String,
        arg: String,
    },
    FunctionCall {
        func: String,
        args: Vec<SimpleExpression>,
    },
    Case(Box<CaseExpression>),
    Cast {
        expr: Box<SimpleExpression>,
        data_type: String,
    },
    Subquery(Box<SelectStatement>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CaseExpression {
    pub when_then_pairs: Vec<(SimpleExpression, SimpleExpression)>,
    pub else_expression: Option<Box<SimpleExpression>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrderByExpression {
    pub expression: SimpleExpression,
    pub asc: bool, // true for ASC, false for DESC
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JoinClause {
    pub join_type: String,
    pub table: String,
    pub on_condition: SimpleExpression,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnionClause {
    pub all: bool, // true for UNION ALL
    pub select: Box<SelectStatement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SelectColumn {
    pub expr: SimpleExpression,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SelectStatement {
    pub columns: Vec<SelectColumn>,
    pub from_table: String,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<SimpleExpression>,
    pub group_by: Vec<SimpleExpression>,
    pub order_by: Vec<OrderByExpression>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub union: Option<Box<UnionClause>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<SimpleExpression>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeleteStatement {
    pub from_table: String,
    pub where_clause: Option<SimpleExpression>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub unique: bool,
    pub default: Option<SimpleExpression>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ForeignKeyClause {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub references_table: String,
    pub references_columns: Vec<String>,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<String>,
    pub foreign_keys: Vec<ForeignKeyClause>,
    pub check: Option<SimpleExpression>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DropTableStatement {
    pub table_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AlterTableAction {
    AddColumn(ColumnDef),
    DropColumn { column_name: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AlterTableStatement {
    pub table_name: String,
    pub action: AlterTableAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TruncateTableStatement {
    pub table_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AstStatement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
    Update(UpdateStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    AlterTable(AlterTableStatement),
    TruncateTable(TruncateTableStatement),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateStatement {
    pub table: String,
    pub set: Vec<(String, SimpleExpression)>,
    pub where_clause: Option<SimpleExpression>,
}









