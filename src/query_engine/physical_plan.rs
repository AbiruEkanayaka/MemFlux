use anyhow::Result;
use serde_json::Value;

use super::ast::{AlterTableAction, ColumnDef, ForeignKeyClause, CreateIndexStatement, SelectStatement};
use super::logical_plan::*;
use crate::indexing::IndexManager;

#[derive(Debug)]
pub enum PhysicalPlan {
    TableScan {
        prefix: String,
    },
    IndexScan {
        index_name: String,
        key: Value,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expression,
    },
    Projection {
        input: Box<PhysicalPlan>,
        expressions: Vec<(Expression, Option<String>)>,
    },
    Join {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        condition: Expression,
        join_type: JoinType,
    },
    HashAggregate {
        input: Box<PhysicalPlan>,
        group_expressions: Vec<Expression>,
        agg_expressions: Vec<Expression>,
    },
    Sort {
        input: Box<PhysicalPlan>,
        sort_expressions: Vec<(Expression, bool)>,
    },
    Limit {
        input: Box<PhysicalPlan>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDef>,
        primary_key: Vec<String>,
        foreign_keys: Vec<ForeignKeyClause>,
        check: Option<Expression>,
        if_not_exists: bool,
    },
    CreateView {
        view_name: String,
        query: SelectStatement,
    },
    CreateSchema {
        schema_name: String,
    },
    DropTable {
        table_name: String,
    },
    DropView {
        view_name: String,
    },
    AlterTable {
        table_name: String,
        action: AlterTableAction,
    },
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<Expression>,
    },
    Delete {
        from: Box<PhysicalPlan>,
    },
    Update {
        table_name: String,
        from: Box<PhysicalPlan>,
        set: Vec<(String, Expression)>,
    },
    Union {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        all: bool,
    },
    CreateIndex {
        statement: CreateIndexStatement,
    },
}

pub fn logical_to_physical_plan(
    plan: LogicalPlan,
    index_manager: &IndexManager,
) -> Result<PhysicalPlan> {
    match plan {
        LogicalPlan::TableScan { table_name } => Ok(PhysicalPlan::TableScan {
            prefix: format!("{}:", table_name),
        }),
        LogicalPlan::Filter { input, predicate } => {
            if let Some((col, val)) = extract_col_eq_literal(&predicate) {
                if let LogicalPlan::TableScan { ref table_name } = *input {
                    let prefix = format!("{}:*", table_name);
                    let index_name = format!("{}|{}", prefix, col);
                    if index_manager.indexes.contains_key(&index_name) {
                        return Ok(PhysicalPlan::IndexScan {
                            index_name,
                            key: val,
                        });
                    }
                }
            }
            Ok(PhysicalPlan::Filter {
                input: Box::new(logical_to_physical_plan(*input, index_manager)?),
                predicate,
            })
        }
        LogicalPlan::Projection {
            input,
            expressions,
        } => Ok(PhysicalPlan::Projection {
            input: Box::new(logical_to_physical_plan(*input, index_manager)?),
            expressions,
        }),
        LogicalPlan::Join {
            left,
            right,
            condition,
            join_type,
        } => Ok(PhysicalPlan::Join {
            left: Box::new(logical_to_physical_plan(*left, index_manager)?),
            right: Box::new(logical_to_physical_plan(*right, index_manager)?),
            condition,
            join_type,
        }),
        LogicalPlan::Aggregate {
            input,
            group_expressions,
            agg_expressions,
        } => Ok(PhysicalPlan::HashAggregate {
            input: Box::new(logical_to_physical_plan(*input, index_manager)?),
            group_expressions,
            agg_expressions,
        }),
        LogicalPlan::Sort {
            input,
            sort_expressions,
        } => Ok(PhysicalPlan::Sort {
            input: Box::new(logical_to_physical_plan(*input, index_manager)?),
            sort_expressions,
        }),
        LogicalPlan::Limit {
            input,
            limit,
            offset,
        } => Ok(PhysicalPlan::Limit {
            input: Box::new(logical_to_physical_plan(*input, index_manager)?),
            limit,
            offset,
        }),
        LogicalPlan::CreateTable {
            table_name,
            columns,
            primary_key,
            foreign_keys,
            check,
            if_not_exists,
        } => Ok(PhysicalPlan::CreateTable {
            table_name,
            columns,
            primary_key,
            foreign_keys,
            check,
            if_not_exists,
        }),
        LogicalPlan::CreateView {
            view_name,
            query,
        } => Ok(PhysicalPlan::CreateView {
            view_name,
            query,
        }),
        LogicalPlan::CreateSchema {
            schema_name,
        } => Ok(PhysicalPlan::CreateSchema {
            schema_name,
        }),
        LogicalPlan::DropTable {
            table_name,
        } => Ok(PhysicalPlan::DropTable {
            table_name,
        }),
        LogicalPlan::DropView {
            view_name,
        } => Ok(PhysicalPlan::DropView {
            view_name,
        }),
        LogicalPlan::Insert {
            table_name,
            columns,
            values,
        } => Ok(PhysicalPlan::Insert {
            table_name,
            columns,
            values,
        }),
        LogicalPlan::Delete { from } => Ok(PhysicalPlan::Delete {
            from: Box::new(logical_to_physical_plan(*from, index_manager)?),
        }),
        LogicalPlan::Update {
            table_name,
            from,
            set,
        } => Ok(PhysicalPlan::Update {
            table_name,
            from: Box::new(logical_to_physical_plan(*from, index_manager)?),
            set,
        }),
        LogicalPlan::AlterTable { table_name, action } => {
            Ok(PhysicalPlan::AlterTable { table_name, action })
        }
        LogicalPlan::Union { left, right, all } => Ok(PhysicalPlan::Union {
            left: Box::new(logical_to_physical_plan(*left, index_manager)?),
            right: Box::new(logical_to_physical_plan(*right, index_manager)?),
            all,
        }),
        LogicalPlan::CreateIndex { statement } => Ok(PhysicalPlan::CreateIndex { statement }),
    }
}

fn extract_col_eq_literal(predicate: &Expression) -> Option<(String, Value)> {
    if let Expression::BinaryOp { left, op, right } = predicate {
        if *op == Operator::Eq {
            if let (Expression::Column(col), Expression::Literal(val)) = (&**left, &**right) {
                return Some((col.clone(), val.clone()));
            }
            if let (Expression::Literal(val), Expression::Column(col)) = (&**left, &**right) {
                return Some((col.clone(), val.clone()));
            }
        }
    }
    None
}









