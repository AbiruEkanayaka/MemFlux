use crate::cypher_engine::ast::{self, CypherQuery, MatchQuery, PatternPart};
use anyhow::{anyhow, Result};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    NodeByLabelScan {
        variable: String,
        label: String,
    },
    Expand {
        start_node_var: String,
        rel_var: String,
        end_node_var: String,
        rel_type: String,
        direction: ast::RelationshipDirection,
        input: Box<LogicalPlan>,
    },
    Filter {
        predicate: ast::Expression,
        input: Box<LogicalPlan>,
    },
    Projection {
        expressions: Vec<(ast::Expression, Option<String>)>, // expr, alias
        input: Box<LogicalPlan>,
    },
}

pub fn ast_to_logical_plan(query: CypherQuery) -> Result<LogicalPlan> {
    match query {
        CypherQuery::Match(match_query) => build_plan_from_match(match_query),
    }
}

fn build_plan_from_match(query: MatchQuery) -> Result<LogicalPlan> {
    let mut plan: Option<LogicalPlan> = None;
    let mut bound_variables = HashMap::new();

    for part in &query.pattern.parts {
        match part {
            PatternPart::Node(node_pattern) => {
                let var = node_pattern.variable.clone().ok_or_else(|| anyhow!("All nodes in MATCH must have a variable"))?;
                let label = node_pattern.labels.get(0).cloned().ok_or_else(|| anyhow!("All nodes in MATCH must have a label"))?;
                bound_variables.insert(var.clone(), label.clone());

                if plan.is_none() {
                    plan = Some(LogicalPlan::NodeByLabelScan { variable: var, label });
                }
            }
            PatternPart::Relationship(rel_pattern) => {
                let start_node_var = bound_variables.keys().last().cloned().ok_or_else(|| anyhow!("Relationship must follow a node"))?;
                let end_node_pattern = query.pattern.parts.get(bound_variables.len() * 2).and_then(|p| if let PatternPart::Node(n) = p { Some(n) } else { None }).ok_or_else(|| anyhow!("Relationship must be followed by a node"))?;
                let end_node_var = end_node_pattern.variable.clone().ok_or_else(|| anyhow!("End node must have a variable"))?;
                let end_node_label = end_node_pattern.labels.get(0).cloned().ok_or_else(|| anyhow!("End node must have a label"))?;
                bound_variables.insert(end_node_var.clone(), end_node_label);

                let rel_var = rel_pattern.variable.clone().unwrap_or_else(|| format!("_rel_{}", bound_variables.len()));
                let rel_type = rel_pattern.types.get(0).cloned().unwrap_or_else(|| "".to_string());

                plan = Some(LogicalPlan::Expand {
                    start_node_var,
                    rel_var,
                    end_node_var,
                    rel_type,
                    direction: rel_pattern.direction.clone(),
                    input: Box::new(plan.take().unwrap()),
                });
            }
        }
    }

    let mut final_plan = plan.ok_or_else(|| anyhow!("Could not build a plan from the MATCH clause"))?;

    if let Some(predicate) = query.where_clause {
        final_plan = LogicalPlan::Filter {
            predicate,
            input: Box::new(final_plan),
        };
    }

    let projection_expressions = query.return_clause.items.into_iter()
        .map(|item| (item.expression, item.alias))
        .collect();

    final_plan = LogicalPlan::Projection {
        expressions: projection_expressions,
        input: Box::new(final_plan),
    };

    Ok(final_plan)
}
