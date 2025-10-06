use crate::cypher_engine::ast::{self, CypherQuery};
use anyhow::{anyhow, Result};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    FullOuter,
    Cross,
}

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
        path_variable: Option<String>,
        input: Box<LogicalPlan>,
        is_optional: bool,
        range: Option<(Option<u32>, Option<u32>)>
    },
    Filter {
        predicate: ast::Expression,
        input: Box<LogicalPlan>,
    },
    Projection {
        expressions: Vec<(ast::Expression, Option<String>)>, // expr, alias
        input: Box<LogicalPlan>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        condition: ast::Expression,
        join_type: JoinType,
    },
    Create {
        pattern: ast::Pattern,
        input: Box<LogicalPlan>,
    },
    Merge {
        pattern: ast::Pattern,
        on_create: Option<ast::SetClause>,
        on_match: Option<ast::SetClause>,
        input: Box<LogicalPlan>,
    },
    Remove {
        items: Vec<ast::Expression>,
        input: Box<LogicalPlan>,
    },
    Set {
        items: Vec<ast::SetItem>,
        input: Box<LogicalPlan>,
    },
    Delete {
        expressions: Vec<ast::Expression>,
        detach: bool,
        input: Box<LogicalPlan>,
    },
    Dummy,
}

pub fn ast_to_logical_plan(query: CypherQuery) -> Result<LogicalPlan> {
    let mut plan: LogicalPlan = LogicalPlan::Dummy;

    for clause in query.clauses {
        plan = match clause {
            ast::Clause::Match(match_query) => {
                build_plan_from_match(match_query, plan, false)?
            }
            ast::Clause::OptionalMatch(match_query) => {
                build_plan_from_match(match_query, plan, true)?
            }
            ast::Clause::Create(pattern) => {
                LogicalPlan::Create {
                    pattern,
                    input: Box::new(plan),
                }
            }
            ast::Clause::Merge(merge_clause) => {
                // A MERGE is like an optional MATCH, followed by a conditional CREATE.
                // First, build a plan to find the things that already exist.
                let match_plan = build_plan_from_match(ast::MatchQuery { patterns: vec![merge_clause.pattern.clone()], where_clause: None }, plan, true)?;

                // The Merge plan itself will contain the logic to either use the matched data
                // or create the new data.
                LogicalPlan::Merge {
                    pattern: merge_clause.pattern,
                    on_create: merge_clause.on_create,
                    on_match: merge_clause.on_match,
                    input: Box::new(match_plan),
                }
            }
            ast::Clause::Remove(remove_clause) => {
                LogicalPlan::Remove {
                    items: remove_clause.items,
                    input: Box::new(plan),
                }
            }
            ast::Clause::Set(set_clause) => {
                LogicalPlan::Set {
                    items: set_clause.items,
                    input: Box::new(plan),
                }
            }
            ast::Clause::Delete(delete_clause) => {
                LogicalPlan::Delete {
                    expressions: delete_clause.expressions,
                    detach: delete_clause.detach,
                    input: Box::new(plan),
                }
            }
            ast::Clause::Return(return_clause) => {
                let projection_expressions = return_clause.items.into_iter()
                    .map(|item| (item.expression, item.alias))
                    .collect();
                LogicalPlan::Projection {
                    expressions: projection_expressions,
                    input: Box::new(plan),
                }
            }
        };
    }

    Ok(plan)
}

fn build_plan_from_match(query: ast::MatchQuery, input_plan: LogicalPlan, is_optional: bool) -> Result<LogicalPlan> {
    let mut current_plan: Option<LogicalPlan> = if matches!(input_plan, LogicalPlan::Dummy) {
        None
    } else {
        Some(input_plan)
    };
    let mut bound_variables = HashMap::new();
    let mut predicates: Vec<ast::Expression> = Vec::new();

    for (_pattern_idx, pattern) in query.patterns.into_iter().enumerate() {
        let mut pattern_plan: Option<LogicalPlan> = None;
        let path_variable = pattern.variable.clone();

        for part_idx in 0..pattern.parts.len() {
            let part = &pattern.parts[part_idx];
            match part {
                ast::PatternPart::Node(node_pattern) => {
                    let var = node_pattern.variable.clone().unwrap_or_else(|| format!("_anon_node_{}", bound_variables.len()));
                    let label = node_pattern.labels.get(0).cloned().unwrap_or_else(|| "".to_string());
                    bound_variables.insert(var.clone(), label.clone());

                    if pattern_plan.is_none() {
                        pattern_plan = Some(LogicalPlan::NodeByLabelScan { variable: var.clone(), label });
                    }

                    if let Some(ast::Expression::Map(props)) = &node_pattern.properties {
                        for (prop_name, prop_expr) in props {
                            let predicate = ast::Expression::BinaryOp {
                                left: Box::new(ast::Expression::Property(
                                    Box::new(ast::Expression::Variable(var.clone())),
                                    prop_name.clone(),
                                )),
                                op: "=".to_string(),
                                right: Box::new(prop_expr.clone()),
                            };
                            predicates.push(predicate);
                        }
                    }
                }
                ast::PatternPart::Relationship(rel_pattern) => {
                    let start_node_var = bound_variables.keys().last().cloned().ok_or_else(|| anyhow!("Relationship must follow a node"))?;
                    
                    let end_node_pattern = pattern.parts.get(part_idx + 1)
                        .and_then(|p| if let ast::PatternPart::Node(n) = p { Some(n) } else { None })
                        .ok_or_else(|| anyhow!("Relationship must be followed by a node"))?;

                    let end_node_var = end_node_pattern.variable.clone().unwrap_or_else(|| format!("_anon_node_{}", bound_variables.len()));
                    let end_node_label = end_node_pattern.labels.get(0).cloned().unwrap_or_else(|| "".to_string());
                    bound_variables.insert(end_node_var.clone(), end_node_label);

                    let rel_var = rel_pattern.variable.clone().unwrap_or_else(|| format!("_anon_rel_{}", bound_variables.len()));
                    let rel_type = rel_pattern.types.get(0).cloned().unwrap_or_else(|| "".to_string());

                    pattern_plan = Some(LogicalPlan::Expand {
                        start_node_var,
                        rel_var,
                        end_node_var: end_node_var.clone(),
                        rel_type,
                        direction: rel_pattern.direction.clone(),
                        path_variable: path_variable.clone(),
                        input: Box::new(pattern_plan.take().unwrap()),
                        is_optional,
                        range: rel_pattern.range.clone(),
                    });

                    // Add filters for the end node of the relationship pattern
                    if let Some(ast::Expression::Map(props)) = &end_node_pattern.properties {
                        for (prop_name, prop_expr) in props {
                            let predicate = ast::Expression::BinaryOp {
                                left: Box::new(ast::Expression::Property(
                                    Box::new(ast::Expression::Variable(end_node_var.clone())),
                                    prop_name.clone(),
                                )),
                                op: "=".to_string(),
                                right: Box::new(prop_expr.clone()),
                            };
                            predicates.push(predicate);
                        }
                    }
                }
            }
        }

        if let Some(p_plan) = pattern_plan {
            if let Some(c_plan) = current_plan {
                // If there's an existing plan, join with it
                current_plan = Some(LogicalPlan::Join {
                    left: Box::new(c_plan),
                    right: Box::new(p_plan),
                    // For now, assume a CROSS JOIN if no explicit ON clause
                    // This is a simplification; a proper Cypher implementation would handle implicit joins.
                    condition: ast::Expression::Literal(ast::LiteralValue::Boolean(true)),
                    join_type: JoinType::Cross,
                });
            } else {
                current_plan = Some(p_plan);
            }
        }
    }

    let mut final_plan = current_plan.ok_or_else(|| anyhow!("Could not build a plan from the MATCH clause"))?;

    for p in predicates {
        final_plan = LogicalPlan::Filter {
            predicate: p,
            input: Box::new(final_plan),
        };
    }

    if let Some(predicate) = query.where_clause {
        final_plan = LogicalPlan::Filter {
            predicate,
            input: Box::new(final_plan),
        };
    }

    Ok(final_plan)
}
