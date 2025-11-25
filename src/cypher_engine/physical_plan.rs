use crate::cypher_engine::ast::{self, Row};
use crate::cypher_engine::logical_plan::{JoinType, LogicalPlan};
use crate::indexing::IndexManager;
use anyhow::Result;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalPlan {
    NodeScan {
        variable: String,
        label: String,
    },
    IndexScan {
        // The result of an optimization
        variable: String,
        label: String,
        property: String,
        value: Value,
    },
    Expand {
        start_node_var: String,
        rel_var: String,
        end_node_var: String,
        rel_type: String,
        direction: ast::RelationshipDirection,
        path_variable: Option<String>,
        input: Box<PhysicalPlan>,
        is_optional: bool,
        range: Option<(Option<u32>, Option<u32>)>,
    },
    Filter {
        predicate: ast::Expression,
        input: Box<PhysicalPlan>,
    },
    Projection {
        expressions: Vec<(ast::Expression, Option<String>)>, // expr, alias
        input: Box<PhysicalPlan>,
    },
    Join {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        condition: ast::Expression,
        join_type: JoinType,
    },
    Create {
        pattern: ast::Pattern,
        input: Box<PhysicalPlan>,
    },
    Merge {
        pattern: ast::Pattern,
        on_create: Option<ast::SetClause>,
        on_match: Option<ast::SetClause>,
        input: Box<PhysicalPlan>,
    },
    Remove {
        items: Vec<ast::Expression>,
        input: Box<PhysicalPlan>,
    },
    Set {
        items: Vec<ast::SetItem>,
        input: Box<PhysicalPlan>,
    },
    Delete {
        expressions: Vec<ast::Expression>,
        detach: bool,
        input: Box<PhysicalPlan>,
    },
    Sort {
        input: Box<PhysicalPlan>,
        sort_expressions: Vec<(ast::Expression, bool)>,
    },
    Dummy,
    Values(Vec<Row>),
}

pub fn logical_to_physical_plan(
    plan: LogicalPlan,
    index_manager: &IndexManager,
) -> Result<PhysicalPlan> {
    match plan {
        LogicalPlan::NodeByLabelScan { variable, label } => {
            Ok(PhysicalPlan::NodeScan { variable, label })
        }
        LogicalPlan::Filter { predicate, input } => {
            // Optimization: Check if this is a filter on an indexed property
            if let LogicalPlan::NodeByLabelScan {
                ref variable,
                ref label,
            } = *input
            {
                if let ast::Expression::BinaryOp { left, op, right } = &predicate {
                    if op == "=" {
                        if let (
                            ast::Expression::Property(var_expr, prop),
                            ast::Expression::Literal(val),
                        ) = (&**left, &**right)
                        {
                            if let ast::Expression::Variable(var_name) = &**var_expr {
                                if var_name == variable {
                                    let json_val = match val {
                                        ast::LiteralValue::String(s) => Value::String(s.clone()),
                                        ast::LiteralValue::Integer(i) => serde_json::json!(i),
                                        ast::LiteralValue::Boolean(b) => serde_json::json!(b),
                                    };

                                    // We found a pattern like: MATCH (n:Label) WHERE n.prop = value
                                    // Check if an index exists. For graphs, the prefix is `_node:Label:*`
                                    let index_prefix = format!("_node:{}:*", label);
                                    let internal_index_name = format!("{}|{}", index_prefix, prop);

                                    if index_manager.indexes.contains_key(&internal_index_name) {
                                        // Convert to an IndexScan
                                        return Ok(PhysicalPlan::IndexScan {
                                            variable: variable.clone(),
                                            label: label.clone(),
                                            property: prop.clone(),
                                            value: json_val,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // If optimization doesn't apply, create a normal Filter plan
            Ok(PhysicalPlan::Filter {
                predicate,
                input: Box::new(logical_to_physical_plan(*input, index_manager)?),
            })
        }
        LogicalPlan::Expand {
            start_node_var,
            rel_var,
            end_node_var,
            rel_type,
            direction,
            path_variable,
            input,
            is_optional,
            range,
        } => Ok(PhysicalPlan::Expand {
            start_node_var,
            rel_var,
            end_node_var,
            rel_type,
            direction,
            path_variable,
            input: Box::new(logical_to_physical_plan(*input, index_manager)?),
            is_optional,
            range,
        }),
        LogicalPlan::Projection { expressions, input } => Ok(PhysicalPlan::Projection {
            expressions,
            input: Box::new(logical_to_physical_plan(*input, index_manager)?),
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
        LogicalPlan::Create { pattern, input } => Ok(PhysicalPlan::Create {
            pattern,
            input: Box::new(logical_to_physical_plan(*input, index_manager)?),
        }),
        LogicalPlan::Merge {
            pattern,
            on_create,
            on_match,
            input,
        } => Ok(PhysicalPlan::Merge {
            pattern,
            on_create,
            on_match,
            input: Box::new(logical_to_physical_plan(*input, index_manager)?),
        }),
        LogicalPlan::Remove { items, input } => Ok(PhysicalPlan::Remove {
            items,
            input: Box::new(logical_to_physical_plan(*input, index_manager)?),
        }),
        LogicalPlan::Set { items, input } => Ok(PhysicalPlan::Set {
            items,
            input: Box::new(logical_to_physical_plan(*input, index_manager)?),
        }),
        LogicalPlan::Delete {
            expressions,
            detach,
            input,
        } => Ok(PhysicalPlan::Delete {
            expressions,
            detach,
            input: Box::new(logical_to_physical_plan(*input, index_manager)?),
        }),
        LogicalPlan::Sort {
            input,
            sort_expressions,
        } => Ok(PhysicalPlan::Sort {
            input: Box::new(logical_to_physical_plan(*input, index_manager)?),
            sort_expressions,
        }),
        LogicalPlan::Dummy => Ok(PhysicalPlan::Dummy),
    }
}
