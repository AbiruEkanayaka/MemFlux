use anyhow::Result;
use serde_json::Value;

use super::ast::{AlterTableAction, ColumnDef, CreateIndexStatement, SelectStatement, TableConstraint};
use super::logical_plan::{self, *};
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
    SubqueryScan {
        alias: String,
        input: Box<PhysicalPlan>,
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
        if_not_exists: bool,
        constraints: Vec<TableConstraint>,
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
        source: Box<PhysicalPlan>,
        source_column_names: Vec<String>,
        on_conflict: Option<(Vec<String>, OnConflictAction)>,
        returning: Vec<(Expression, Option<String>)>
    },
    Delete {
        table_name: String,
        from: Box<PhysicalPlan>,
        returning: Vec<(Expression, Option<String>)>,
    },
    Update {
        table_name: String,
        from: Box<PhysicalPlan>,
        set: Vec<(String, Expression)>,
        returning: Vec<(Expression, Option<String>)>,
    },
    UnionAll {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
    },
    Intersect {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
    },
    Except {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
    },
    CreateIndex {
        statement: CreateIndexStatement,
    },
    Values {
        values: Vec<Vec<Expression>>,
    },
    DistinctOn {
        input: Box<PhysicalPlan>,
        expressions: Vec<Expression>,
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
            if_not_exists,
            constraints,
        } => Ok(PhysicalPlan::CreateTable {
            table_name,
            columns,
            if_not_exists,
            constraints,
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
            source,
            on_conflict,
            returning,
        } => {
            let mut current_source = &*source;
            loop {
                match current_source {
                    LogicalPlan::Sort { input, .. } => current_source = input,
                    LogicalPlan::Limit { input, .. } => current_source = input,
                    _ => break,
                }
            }

            let source_column_names = if let LogicalPlan::Projection { expressions, .. } = current_source {
                expressions.iter().map(|(expr, alias)| {
                    alias.clone().unwrap_or_else(|| expr.to_string())
                }).collect()
            } else if let LogicalPlan::Values { values } = current_source {
                if let Some(first_row) = values.get(0) {
                    (0..first_row.len()).map(|i| format!("column_{}", i)).collect()
                } else {
                    vec![]
                }
            } else {
                // This case can be hit for INSERT ... SELECT * FROM ...
                // where the projection is not explicit. We can try to get columns from the underlying scan.
                // For now, we'll rely on the logical plan having an explicit projection.
                vec![]
            };

            Ok(PhysicalPlan::Insert {
                table_name,
                columns,
                source: Box::new(logical_to_physical_plan(*source, index_manager)?),
                source_column_names,
                on_conflict,
                returning,
            })
        }
        LogicalPlan::Delete { table_name, from, returning } => Ok(PhysicalPlan::Delete {
            table_name,
            from: Box::new(logical_to_physical_plan(*from, index_manager)?),
            returning,
        }),
        LogicalPlan::Update {
            table_name,
            from,
            set,
            returning,
        } => Ok(PhysicalPlan::Update {
            table_name,
            from: Box::new(logical_to_physical_plan(*from, index_manager)?),
            set,
            returning,
        }),
        LogicalPlan::AlterTable { table_name, action } => {
            Ok(PhysicalPlan::AlterTable { table_name, action })
        }
        LogicalPlan::UnionAll { left, right } => Ok(PhysicalPlan::UnionAll {
            left: Box::new(logical_to_physical_plan(*left, index_manager)?),
            right: Box::new(logical_to_physical_plan(*right, index_manager)?),
        }),
        LogicalPlan::Intersect { left, right } => Ok(PhysicalPlan::Intersect {
            left: Box::new(logical_to_physical_plan(*left, index_manager)?),
            right: Box::new(logical_to_physical_plan(*right, index_manager)?),
        }),
        LogicalPlan::Except { left, right } => Ok(PhysicalPlan::Except {
            left: Box::new(logical_to_physical_plan(*left, index_manager)?),
            right: Box::new(logical_to_physical_plan(*right, index_manager)?),
        }),
        LogicalPlan::CreateIndex { statement } => Ok(PhysicalPlan::CreateIndex { statement }),
        LogicalPlan::Values { values } => Ok(PhysicalPlan::Values { values }),
        LogicalPlan::SubqueryScan { alias, input } => Ok(PhysicalPlan::SubqueryScan {
            alias,
            input: Box::new(logical_to_physical_plan(*input, index_manager)?),
        }),
        LogicalPlan::DistinctOn { input, expressions } => Ok(PhysicalPlan::DistinctOn {
            input: Box::new(logical_to_physical_plan(*input, index_manager)?),
            expressions,
        }),
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









