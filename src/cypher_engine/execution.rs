use crate::cypher_engine::ast;
use crate::cypher_engine::physical_plan::PhysicalPlan;
use crate::storage_executor::get_visible_db_value;
use crate::transaction::TransactionHandle;
use crate::types::{AppContext, DbValue};
use anyhow::{anyhow, Result};
use async_stream::try_stream;
use futures::stream::{Stream, StreamExt};
use serde_json::{json, Value};
use std::sync::Arc;

type Row = Value;

async fn evaluate_expression(expr: &ast::Expression, row: &Row) -> Result<Value> {
    match expr {
        ast::Expression::Literal(lit) => match lit {
            ast::LiteralValue::String(s) => Ok(Value::String(s.clone())),
            ast::LiteralValue::Integer(i) => Ok(json!(i)),
        },
        ast::Expression::Variable(var) => Ok(row.get(var).cloned().unwrap_or(Value::Null)),
        ast::Expression::Property(expr, prop_name) => {
            let obj = Box::pin(evaluate_expression(expr, row)).await?;
            Ok(obj.get(prop_name).cloned().unwrap_or(Value::Null))
        }
        ast::Expression::BinaryOp { left, op, right } => {
            let left_val = Box::pin(evaluate_expression(left, row)).await?;
            let right_val = Box::pin(evaluate_expression(right, row)).await?;
            match op.as_str() {
                "=" => Ok(json!(left_val == right_val)),
                _ => Err(anyhow!("Unsupported operator: {}", op)),
            }
        }
    }
}

pub fn execute<'a>(
    plan: PhysicalPlan,
    ctx: Arc<AppContext>,
    transaction_handle: TransactionHandle,
) -> impl Stream<Item = Result<Row>> + Send + 'a {
    try_stream! {
        match plan {
            PhysicalPlan::NodeScan { variable, label } => {
                let prefix = format!("_node:{}:", label);
                let tx_guard = transaction_handle.read().await;

                for entry in ctx.db.iter() {
                    if entry.key().starts_with(&prefix) {
                        if let Some(db_val) = get_visible_db_value(entry.key(), &ctx, tx_guard.as_ref()).await {
                             if let DbValue::JsonB(bytes) = db_val {
                                if let Ok(mut props) = serde_json::from_slice::<Value>(&bytes) {
                                    let id = entry.key().split(':').last().unwrap_or("");
                                    if let Some(obj) = props.as_object_mut() {
                                        obj.insert("_id".to_string(), json!(id));
                                    }

                                    let mut row = json!({});
                                    row[variable.clone()] = props;
                                    yield row;
                                }
                            }
                        }
                    }
                }
            }
            PhysicalPlan::IndexScan { variable, label, property, value } => {
                let index_prefix = format!("_node:{}:*", label);
                let internal_index_name = format!("{}|{}", index_prefix, property);

                if let Some(index) = ctx.index_manager.indexes.get(&internal_index_name) {
                    let index_key = serde_json::to_string(&value)?;
                    let index_data = index.read().await;
                    if let Some(keys) = index_data.get(&index_key) {
                        let tx_guard = transaction_handle.read().await;
                        for db_key in keys {
                            if let Some(db_val) = get_visible_db_value(db_key, &ctx, tx_guard.as_ref()).await {
                                if let DbValue::JsonB(bytes) = db_val {
                                    if let Ok(mut props) = serde_json::from_slice::<Value>(&bytes) {
                                        if let Some(obj) = props.as_object_mut() {
                                            obj.insert("_id".to_string(), json!(db_key.split(':').last().unwrap_or("")));
                                        }
                                        let mut row = json!({});
                                        row[variable.clone()] = props;
                                        yield row;
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // Fallback to a full scan if index doesn't exist, though planner should prevent this.
                }
            }
            PhysicalPlan::Expand { start_node_var, rel_var: _rel_var, end_node_var, rel_type, direction, input } => {
                let mut stream = Box::pin(execute(*input, ctx.clone(), transaction_handle.clone()));
                while let Some(start_row_result) = stream.next().await {
                    let start_row = start_row_result?;
                    let start_node_obj = start_row.get(&start_node_var).ok_or_else(|| anyhow!("Start node variable '{} ' not found", start_node_var))?;
                    let start_node_id = start_node_obj.get("_id").and_then(|v| v.as_str()).ok_or_else(|| anyhow!("Start node ID not found"))?;

                    let edge_prefix = match direction {
                        ast::RelationshipDirection::Outgoing => format!("_edge:out:{}:{}:", start_node_id, rel_type),
                        ast::RelationshipDirection::Incoming => format!("_edge:in:{}:{}:", start_node_id, rel_type),
                        ast::RelationshipDirection::Both => format!("_edge:out:{}:{}:", start_node_id, rel_type), // Simplified for now
                    };

                    let tx_guard = transaction_handle.read().await;
                    for entry in ctx.db.iter() {
                        if entry.key().starts_with(&edge_prefix) {
                            let parts: Vec<&str> = entry.key().split(':').collect();
                            if parts.len() < 4 { continue; }

                            let end_node_id = match direction {
                                ast::RelationshipDirection::Outgoing => parts.last().unwrap(),
                                ast::RelationshipDirection::Incoming => parts.last().unwrap(),
                                ast::RelationshipDirection::Both => parts.last().unwrap(),
                            };

                            // Fetch the end node
                            let pk_key = format!("_pk_node:{}", end_node_id);
                            if let Some(DbValue::Bytes(label_bytes)) = get_visible_db_value(&pk_key, &ctx, tx_guard.as_ref()).await {
                                let end_node_label = String::from_utf8(label_bytes)?;
                                let end_node_key = format!("_node:{}:{}", end_node_label, end_node_id);

                                if let Some(DbValue::JsonB(end_node_bytes)) = get_visible_db_value(&end_node_key, &ctx, tx_guard.as_ref()).await {
                                    let mut end_node_props = serde_json::from_slice::<Value>(&end_node_bytes)?;
                                    if let Some(obj) = end_node_props.as_object_mut() {
                                        obj.insert("_id".to_string(), json!(end_node_id));
                                    }

                                    let mut new_row = start_row.clone();
                                    if let Some(obj) = new_row.as_object_mut() {
                                        obj.insert(end_node_var.clone(), end_node_props);
                                        // We could insert rel properties here too if needed
                                    }
                                    yield new_row;
                                }
                            }
                        }
                    }
                }
            }
            PhysicalPlan::Filter { predicate, input } => {
                let mut stream = Box::pin(execute(*input, ctx.clone(), transaction_handle.clone()));
                while let Some(row_result) = stream.next().await {
                    let row = row_result?;
                    if evaluate_expression(&predicate, &row).await?.as_bool().unwrap_or(false) {
                        yield row;
                    }
                }
            }
            PhysicalPlan::Projection { expressions, input } => {
                let mut stream = Box::pin(execute(*input, ctx.clone(), transaction_handle.clone()));
                while let Some(row_result) = stream.next().await {
                    let row = row_result?;
                    let mut new_row = json!({});
                    for (expr, alias) in &expressions {
                        let value = evaluate_expression(expr, &row).await?;
                        let key = match alias {
                            Some(a) => a.clone(),
                            None => match expr {
                                ast::Expression::Variable(v) => v.clone(),
                                ast::Expression::Property(e, p) => format!("{}.{}", e, p),
                                _ => "expr".to_string(),
                            }
                        };
                        new_row[key] = value;
                    }
                    yield new_row;
                }
            }
        }
    }
}