use crate::cypher_engine::ast;
use crate::cypher_engine::logical_plan::{LogicalPlan};
use anyhow::Result;
use crate::indexing::IndexManager;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalPlan {
    NodeScan {
        variable: String,
        label: String,
    },
    IndexScan { // The result of an optimization
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
        input: Box<PhysicalPlan>,
    },
    Filter {
        predicate: ast::Expression,
        input: Box<PhysicalPlan>,
    },
    Projection {
        expressions: Vec<(ast::Expression, Option<String>)>, // expr, alias
        input: Box<PhysicalPlan>,
    },
}

pub fn logical_to_physical_plan(
    plan: LogicalPlan,
    index_manager: &IndexManager,
) -> Result<PhysicalPlan> {
    match plan {
        LogicalPlan::NodeByLabelScan { variable, label } => {
            Ok(PhysicalPlan::NodeScan { variable, label })
        }
        LogicalPlan::Filter {
            predicate,
            input,
        } => {
            // Optimization: Check if this is a filter on an indexed property
            if let LogicalPlan::NodeByLabelScan { ref variable, ref label } = *input {
                if let ast::Expression::BinaryOp { left, op, right } = &predicate {
                    if op == "=" {
                        if let (ast::Expression::Property(var_expr, prop), ast::Expression::Literal(val)) = (&**left, &**right) {
                            if let ast::Expression::Variable(var_name) = &**var_expr {
                                if var_name == variable {
                                    let json_val = match val {
                                        ast::LiteralValue::String(s) => Value::String(s.clone()),
                                        ast::LiteralValue::Integer(i) => serde_json::json!(i),
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
        LogicalPlan::Expand { start_node_var, rel_var, end_node_var, rel_type, direction, input } => {
            Ok(PhysicalPlan::Expand {
                start_node_var,
                rel_var,
                end_node_var,
                rel_type,
                direction,
                input: Box::new(logical_to_physical_plan(*input, index_manager)?),
            })
        }
        LogicalPlan::Projection { expressions, input } => {
            Ok(PhysicalPlan::Projection {
                expressions,
                input: Box::new(logical_to_physical_plan(*input, index_manager)?),
            })
        }
    }
}
