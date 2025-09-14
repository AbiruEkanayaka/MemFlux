
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum SimpleValue {
    String(String),
    Number(String),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum TableReference {
    Table { name: String, alias: Option<String> },
    Subquery(Box<SelectStatement>, String), // Subquery and its alias
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
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
    List(Vec<SimpleExpression>),
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct CaseExpression {
    pub when_then_pairs: Vec<(SimpleExpression, SimpleExpression)>,
    pub else_expression: Option<Box<SimpleExpression>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct OrderByExpression {
    pub expression: SimpleExpression,
    pub asc: bool, // true for ASC, false for DESC
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct JoinClause {
    pub join_type: String,
    pub table: TableReference,
    pub on_condition: SimpleExpression,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum SetOperatorType {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct SetOperatorClause {
    pub operator: SetOperatorType,
    pub all: bool,
    pub select: Box<SelectStatement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct SelectColumn {
    pub expr: SimpleExpression,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct SelectStatement {
    pub columns: Vec<SelectColumn>,
    pub distinct_on: Vec<SimpleExpression>,
    pub from: TableReference,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<SimpleExpression>,
    pub group_by: Vec<SimpleExpression>,
    pub having_clause: Option<SimpleExpression>,
    pub order_by: Vec<OrderByExpression>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub set_operator: Option<Box<SetOperatorClause>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OnConflict {
    DoNothing,
    DoUpdate(Vec<(String, SimpleExpression)>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum InsertSource {
    Values(Vec<Vec<SimpleExpression>>),
    Select(Box<SelectStatement>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub source: InsertSource,
    pub on_conflict: Option<(Vec<String>, OnConflict)>,
    pub returning: Vec<SelectColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeleteStatement {
    pub from_table: String,
    pub using_list: Vec<String>,
    pub where_clause: Option<SimpleExpression>,
    pub returning: Vec<SelectColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
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
    pub if_not_exists: bool,
    pub constraints: Vec<TableConstraint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DropTableStatement {
    pub table_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DropViewStatement {
    pub view_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AlterTableAction {
    RenameTable {
        new_table_name: String,
    },
    RenameColumn {
        old_column_name: String,
        new_column_name: String,
    },
    AddColumn(ColumnDef),
    DropColumn {
        column_name: String,
    },
    AddConstraint(TableConstraint),
    DropConstraint {
        constraint_name: String,
    },
    AlterColumnSetDefault {
        column_name: String,
        default_expr: SimpleExpression,
    },
    AlterColumnDropDefault {
        column_name: String,
    },
    AlterColumnSetNotNull {
        column_name: String,
    },
    AlterColumnDropNotNull {
        column_name: String,
    },
    AlterColumnType {
        column_name: String,
        new_data_type: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TableConstraint {
    Unique { name: Option<String>, columns: Vec<String> },
    PrimaryKey { name: Option<String>, columns: Vec<String> },
    ForeignKey(ForeignKeyClause),
    Check { name: Option<String>, expression: SimpleExpression },
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
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<SimpleExpression>, // To support expression-based indexes
    pub unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AstStatement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Delete(DeleteStatement),
    Update(UpdateStatement),
    CreateTable(CreateTableStatement),
    CreateView(CreateViewStatement),
    CreateSchema(CreateSchemaStatement),
    DropTable(DropTableStatement),
    DropView(DropViewStatement),
    AlterTable(AlterTableStatement),
    TruncateTable(TruncateTableStatement),
    CreateIndex(CreateIndexStatement),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateSchemaStatement {
    pub schema_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CreateViewStatement {
    pub view_name: String,
    pub query: SelectStatement,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateStatement {
    pub table: String,
    pub set: Vec<(String, SimpleExpression)>,
    pub from_list: Vec<String>,
    pub where_clause: Option<SimpleExpression>,
    pub returning: Vec<SelectColumn>,
}









