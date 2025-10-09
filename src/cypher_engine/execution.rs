use crate::cypher_engine::ast::{self, Row};
use crate::cypher_engine::physical_plan::PhysicalPlan;
use crate::storage_executor::get_visible_db_value;
use crate::transaction::TransactionHandle;
use crate::types::{AppContext, DbValue};
use anyhow::{anyhow, Result};
use async_stream::try_stream;
use futures::stream::{Stream, StreamExt, TryStreamExt};
use serde_json::{json, Value};
use std::sync::Arc;

async fn evaluate_expression(expr: &ast::Expression, row: &Row, ctx: Arc<AppContext>, transaction_handle: TransactionHandle) -> Result<Value> {
    match expr {
        ast::Expression::Literal(lit) => match lit {
            ast::LiteralValue::String(s) => Ok(Value::String(s.clone())),
            ast::LiteralValue::Integer(i) => Ok(json!(i)),
            ast::LiteralValue::Boolean(b) => Ok(json!(b)),
        },
        ast::Expression::Variable(var) => Ok(row.get(var).cloned().unwrap_or(Value::Null)),
        ast::Expression::Property(expr, prop_name) => {
            let obj = Box::pin(evaluate_expression(expr, row, ctx, transaction_handle)).await?;
            Ok(obj.get(prop_name).cloned().unwrap_or(Value::Null))
        }
        ast::Expression::Map(props) => {
            let mut map = serde_json::Map::new();
            for (key, value_expr) in props {
                let value = Box::pin(evaluate_expression(value_expr, row, ctx.clone(), transaction_handle.clone())).await?;
                map.insert(key.clone(), value);
            }
            Ok(Value::Object(map))
        }
        ast::Expression::BinaryOp { left, op, right } => {
            let left_val = Box::pin(evaluate_expression(left, row, ctx.clone(), transaction_handle.clone())).await?;
            let right_val = Box::pin(evaluate_expression(right, row, ctx, transaction_handle)).await?;
            match op.as_str() {
                "=" => Ok(json!(left_val == right_val)),
                _ => Err(anyhow!("Unsupported operator: {}", op)),
            }
        }
        ast::Expression::FunctionCall { func, args } => {
            let mut evaluated_args = Vec::new();
            for arg in args {
                evaluated_args.push(Box::pin(evaluate_expression(arg, row, ctx.clone(), transaction_handle.clone())).await?);
            }

            match func.to_lowercase().as_str() {
                "id" => {
                    if evaluated_args.len() != 1 { return Err(anyhow!("id() expects 1 argument")); }
                    let entity = &evaluated_args[0];
                    if let Some(id) = entity.get("_id").and_then(|v| v.as_str()) {
                        Ok(json!(id))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "labels" => {
                    if evaluated_args.len() != 1 { return Err(anyhow!("labels() expects 1 argument")); }
                    let entity = &evaluated_args[0];
                    if let Some(label) = entity.get("_label").and_then(|v| v.as_str()) {
                        Ok(json!([label]))
                    } else {
                        Ok(json!([]))
                    }
                }
                "type" => {
                    if evaluated_args.len() != 1 { return Err(anyhow!("type() expects 1 argument")); }
                    let entity = &evaluated_args[0];
                    if let Some(rel_type) = entity.get("_type").and_then(|v| v.as_str()) {
                        Ok(json!(rel_type))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "properties" => {
                    if evaluated_args.len() != 1 { return Err(anyhow!("properties() expects 1 argument")); }
                    let entity = &evaluated_args[0];
                    if let Some(obj) = entity.as_object() {
                        let mut new_obj = obj.clone();
                        new_obj.retain(|k, _| !k.starts_with('_'));
                        Ok(Value::Object(new_obj))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "size" => {
                    if evaluated_args.len() != 1 { return Err(anyhow!("size() expects 1 argument")); }
                    let arg = &evaluated_args[0];
                    if let Some(s) = arg.as_str() {
                        Ok(json!(s.len() as i64))
                    } else if let Some(arr) = arg.as_array() {
                        Ok(json!(arr.len() as i64))
                    } else {
                        Ok(Value::Null)
                    }
                }
                _ => Err(anyhow!("Unsupported function: {}", func)),
            }
        }
        ast::Expression::ShortestPath(pattern) => {
            // 1. Extract start and end node variables from the pattern.
            let start_node_pattern = pattern.parts.get(0).and_then(|p| if let ast::PatternPart::Node(n) = p { Some(n) } else { None }).ok_or_else(|| anyhow!("shortestPath pattern must start with a node"))?;
            let end_node_pattern = pattern.parts.get(2).and_then(|p| if let ast::PatternPart::Node(n) = p { Some(n) } else { None }).ok_or_else(|| anyhow!("shortestPath pattern must have an end node"))?;
            let rel_pattern = pattern.parts.get(1).and_then(|p| if let ast::PatternPart::Relationship(r) = p { Some(r) } else { None }).ok_or_else(|| anyhow!("shortestPath pattern must have a relationship"))?;

            let start_var = start_node_pattern.variable.as_ref().ok_or_else(|| anyhow!("shortestPath start node must be a bound variable"))?;
            let end_var = end_node_pattern.variable.as_ref().ok_or_else(|| anyhow!("shortestPath end node must be a bound variable"))?;

            // 2. Get node IDs from the current row context.
            let start_node_obj = row.get(start_var).ok_or_else(|| anyhow!("Start node variable '{}' not found in row", start_var))?;
            let end_node_obj = row.get(end_var).ok_or_else(|| anyhow!("End node variable '{}' not found in row", end_var))?;
            let start_id = start_node_obj.get("_id").and_then(|v| v.as_str()).ok_or_else(|| anyhow!("Start node ID not found"))?.to_string();
            let end_id = end_node_obj.get("_id").and_then(|v| v.as_str()).ok_or_else(|| anyhow!("End node ID not found"))?.to_string();

            if start_id == end_id {
                return Ok(json!([start_node_obj]));
            }

            // 3. Perform BFS.
            let mut q: std::collections::VecDeque<String> = std::collections::VecDeque::new();
            q.push_back(start_id.clone());

            let mut predecessors: std::collections::HashMap<String, (String, Value)> = std::collections::HashMap::new();
            let mut visited = std::collections::HashSet::new();
            visited.insert(start_id.clone());

            let tx_guard = transaction_handle.read().await;
            let tx_opt = tx_guard.as_ref();

            let mut found = false;

            let rel_type_filter = if rel_pattern.types.is_empty() {
                "*".to_string()
            } else {
                rel_pattern.types[0].clone()
            };

            while let Some(current_node_id) = q.pop_front() {
                if current_node_id == end_id {
                    found = true;
                    break;
                }

                let out_prefix_base = format!("_edge:out:{}:", current_node_id);
                for entry in ctx.db.iter() {
                    if entry.key().starts_with(&out_prefix_base) {
                        let parts: Vec<&str> = entry.key().split(':').collect();
                        if parts.len() < 6 { continue; } // _edge:out:start_id:type:end_id:rel_id

                        let rel_type_in_db = parts[3]; // The actual relationship type in the DB

                        if rel_type_filter == "*" || rel_type_filter == rel_type_in_db {
                            // This is a match
                            let neighbor_id = parts[4].to_string();

                            if !visited.contains(&neighbor_id) {
                                visited.insert(neighbor_id.clone());
                                if let Some(DbValue::JsonB(rel_bytes)) = get_visible_db_value(entry.key(), &ctx, tx_opt).await {
                                    let mut rel_props = serde_json::from_slice::<Value>(&rel_bytes)?;
                                    if let Some(obj) = rel_props.as_object_mut() {
                                        obj.insert("_start_id".to_string(), json!(current_node_id));
                                        obj.insert("_end_id".to_string(), json!(neighbor_id));
                                    }

                                    predecessors.insert(neighbor_id.clone(), (current_node_id.clone(), rel_props));
                                    q.push_back(neighbor_id);
                                }
                            }
                        }
                    }
                }
            }

            // 4. Reconstruct path.
            if found {
                let mut path = std::collections::VecDeque::new();
                let mut current_id = end_id;

                while current_id != start_id {
                    let (predecessor_id, rel_obj) = predecessors.get(&current_id).ok_or_else(|| anyhow!("Path reconstruction failed"))?;
                    
                    // Fetch the node object for the current ID
                    let pk_key = format!("_pk_node:{}", current_id);
                    let label = String::from_utf8(get_visible_db_value(&pk_key, &ctx, tx_opt).await.and_then(|v| if let DbValue::Bytes(b) = v { Some(b) } else { None }).unwrap_or_default())?;
                    let node_key = format!("_node:{}:{}", label, current_id);
                    let node_bytes = get_visible_db_value(&node_key, &ctx, tx_opt).await.and_then(|v| if let DbValue::JsonB(b) = v { Some(b) } else { None }).unwrap_or_default();
                    let mut node_obj = serde_json::from_slice::<Value>(&node_bytes)?;
                    if let Some(obj) = node_obj.as_object_mut() {
                        obj.insert("_id".to_string(), json!(current_id));
                        obj.insert("_label".to_string(), json!(label));
                    }

                    path.push_front(node_obj);
                    path.push_front(rel_obj.clone());
                    
                    current_id = predecessor_id.clone();
                }
                path.push_front(start_node_obj.clone());
                Ok(json!(path))
            } else {
                Ok(Value::Null)
            }
        }
    }
}

fn compare_cypher_values(val_a: &Value, val_b: &Value) -> std::cmp::Ordering {
    if val_a.is_null() && val_b.is_null() { return std::cmp::Ordering::Equal; }
    if val_a.is_null() { return std::cmp::Ordering::Less; } // NULLS FIRST
    if val_b.is_null() { return std::cmp::Ordering::Greater; }

    match (val_a, val_b) {
        (Value::Number(n_a), Value::Number(n_b)) =>
            n_a.as_f64().unwrap_or(f64::NAN).partial_cmp(&n_b.as_f64().unwrap_or(f64::NAN)).unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(s_a), Value::String(s_b)) => s_a.cmp(s_b),
        (Value::Bool(b_a), Value::Bool(b_b)) => b_a.cmp(b_b),
        _ => val_a.to_string().cmp(&val_b.to_string()),
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
                if let Some(schema) = ctx.schema_cache.get(&label) {
                    if schema.source == crate::schema::SchemaSource::Native {
                        let prefix = format!("{}:", label);
                        let tx_guard = transaction_handle.read().await;
                        let tx_opt = tx_guard.as_ref();

                        let mut keys_to_process: std::collections::HashSet<String> = std::collections::HashSet::new();
                        for r in ctx.db.iter() { if r.key().starts_with(&prefix) { keys_to_process.insert(r.key().clone()); } }
                        if let Some(tx) = tx_opt {
                            for item in tx.writes.iter() { if item.key().starts_with(&prefix) { keys_to_process.insert(item.key().clone()); } }
                        }

                        for key in keys_to_process {
                            if let Some(db_val) = get_visible_db_value(&key, &ctx, tx_opt).await {
                                if let DbValue::JsonB(bytes) = db_val {
                                    if let Ok(mut props) = serde_json::from_slice::<Value>(&bytes) {
                                        let id = key.split(':').last().unwrap_or("");
                                        if let Some(obj) = props.as_object_mut() {
                                            obj.insert("_id".to_string(), json!(id));
                                            obj.insert("_label".to_string(), json!(label.clone()));
                                        }
                                        let mut row = json!({});
                                        row[variable.clone()] = props;
                                        yield row;
                                    }
                                }
                            }
                        }
                        return;
                    }
                }

                let prefix = format!("_node:{}:", label);
                let tx_guard = transaction_handle.read().await;
                let tx_opt = tx_guard.as_ref();

                let mut keys_to_process: std::collections::HashSet<String> = std::collections::HashSet::new();

                // Get keys from main DB
                for entry in ctx.db.iter() {
                    if entry.key().starts_with(&prefix) {
                        keys_to_process.insert(entry.key().clone());
                    }
                }

                // Get keys from transaction writeset
                if let Some(tx) = tx_opt {
                    for entry in tx.writes.iter() {
                        if entry.key().starts_with(&prefix) {
                            keys_to_process.insert(entry.key().clone());
                        }
                    }
                }

                for key in keys_to_process {
                    if let Some(db_val) = get_visible_db_value(&key, &ctx, tx_opt).await {
                         if let DbValue::JsonB(bytes) = db_val {
                            if let Ok(mut props) = serde_json::from_slice::<Value>(&bytes) {
                                let id = key.split(':').last().unwrap_or("");
                                if let Some(obj) = props.as_object_mut() {
                                    obj.insert("_id".to_string(), json!(id));
                                    obj.insert("_label".to_string(), json!(label.clone()));
                                }

                                let mut row = json!({});
                                row[variable.clone()] = props;
                                yield row;
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
                                            obj.insert("_label".to_string(), json!(label.clone()));
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
            PhysicalPlan::Expand { start_node_var, rel_var, end_node_var, rel_type, direction, path_variable, input, is_optional, range } => {
                let mut stream = Box::pin(execute(*input, ctx.clone(), transaction_handle.clone()));
                while let Some(start_row_result) = stream.next().await {
                    let start_row = start_row_result?;
                    let start_node_obj = start_row.get(&start_node_var).ok_or_else(|| anyhow!("Start node variable '{}' not found", start_node_var))?;

                    if start_node_obj.is_null() {
                        if is_optional {
                            let mut null_row = start_row.clone();
                            if let Some(obj) = null_row.as_object_mut() {
                                obj.insert(end_node_var.clone(), Value::Null);
                                obj.insert(rel_var.clone(), Value::Null);
                                if let Some(path_var) = &path_variable {
                                    obj.insert(path_var.clone(), Value::Null);
                                }
                            }
                            yield null_row;
                        }
                        continue;
                    }

                    let start_node_id = start_node_obj.get("_id").and_then(|v| v.as_str()).ok_or_else(|| anyhow!("Start node ID not found"))?;

                    let mut matched_once = false;

                    // Phase 1.2: Virtual Relationship Expansion via Foreign Keys
                    let start_node_label = start_node_obj.get("_label").and_then(|v| v.as_str());
                    if let (Some(label), ast::RelationshipDirection::Outgoing) = (start_node_label, &direction) {
                        if let Some(schema) = ctx.schema_cache.get(label) {
                            if schema.source == crate::schema::SchemaSource::Native {
                                for constraint in &schema.constraints {
                                    if let crate::query_engine::ast::TableConstraint::ForeignKey(fk) = constraint {
                                        // Convention: rel_type in query matches the FK column name.
                                        if fk.columns.len() == 1 && fk.columns[0] == rel_type {
                                            let fk_col_name = &fk.columns[0];
                                            let fk_val = start_node_obj.get(fk_col_name);

                                            if let Some(val) = fk_val {
                                                let pk_val_str = match val {
                                                    Value::String(s) => s.clone(),
                                                    Value::Number(n) => n.to_string(),
                                                    _ => continue,
                                                };
                                                let referenced_table = &fk.references_table;
                                                let referenced_db_key = format!("{}:{}", referenced_table, pk_val_str);

                                                let tx_guard = transaction_handle.read().await;
                                                if let Some(db_val) = get_visible_db_value(&referenced_db_key, &ctx, tx_guard.as_ref()).await {
                                                    if let DbValue::JsonB(bytes) = db_val {
                                                        let mut end_node_props: Value = serde_json::from_slice(&bytes)?;
                                                        if let Some(obj) = end_node_props.as_object_mut() {
                                                            obj.insert("_id".to_string(), json!(pk_val_str));
                                                            obj.insert("_label".to_string(), json!(referenced_table));
                                                        }

                                                        let rel_props = json!({
                                                            "_type": rel_type,
                                                            "_start_id": start_node_id,
                                                            "_end_id": pk_val_str
                                                        });

                                                        matched_once = true;
                                                        let mut new_row = start_row.clone();
                                                        if let Some(obj) = new_row.as_object_mut() {
                                                            obj.insert(end_node_var.clone(), end_node_props.clone());
                                                            obj.insert(rel_var.clone(), rel_props.clone());
                                                            if let Some(path_var) = &path_variable {
                                                                obj.insert(path_var.clone(), json!([start_node_obj.clone(), rel_props, end_node_props]));
                                                            }
                                                        }
                                                        yield new_row;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if let Some((min_raw, max_raw)) = range {
                        // Variable-length path traversal using BFS
                        let min_depth = min_raw.unwrap_or(1);
                        let max_depth = max_raw.unwrap_or(5); // Default max depth to 5 if unbounded to prevent explosions

                        let mut q: std::collections::VecDeque<(String, u32, Vec<Value>)> = std::collections::VecDeque::new();
                        q.push_back((start_node_id.to_string(), 0, vec![start_node_obj.clone()]));

                        let mut visited_nodes = std::collections::HashSet::new();
                        visited_nodes.insert(start_node_id.to_string());

                        while let Some((current_id, current_depth, current_path)) = q.pop_front() {
                            if current_depth >= max_depth {
                                continue;
                            }

                            let mut prefixes_to_scan = Vec::new();
                            match direction {
                                ast::RelationshipDirection::Outgoing => {
                                    if rel_type.is_empty() {
                                        prefixes_to_scan.push(format!("_edge:out:{}:", current_id));
                                    } else {
                                        prefixes_to_scan.push(format!("_edge:out:{}:{}:", current_id, rel_type));
                                    }
                                }
                                ast::RelationshipDirection::Incoming => {
                                    if rel_type.is_empty() {
                                        prefixes_to_scan.push(format!("_edge:in:{}:", current_id));
                                    } else {
                                        prefixes_to_scan.push(format!("_edge:in:{}:{}:", current_id, rel_type));
                                    }
                                }
                                ast::RelationshipDirection::Both => {
                                    if rel_type.is_empty() {
                                        prefixes_to_scan.push(format!("_edge:out:{}:", current_id));
                                        prefixes_to_scan.push(format!("_edge:in:{}:", current_id));
                                    } else {
                                        prefixes_to_scan.push(format!("_edge:out:{}:{}:", current_id, rel_type));
                                        prefixes_to_scan.push(format!("_edge:in:{}:{}:", current_id, rel_type));
                                    }
                                }
                            };

                            let tx_guard = transaction_handle.read().await;
                            for edge_prefix in prefixes_to_scan {
                                for entry in ctx.db.iter() {
                                    if entry.key().starts_with(&edge_prefix) {
                                        let parts: Vec<&str> = entry.key().split(':').collect();
                                        if parts.len() < 6 { continue; }
                                        let end_node_id = parts[4];

                                        if visited_nodes.contains(end_node_id) { continue; }

                                        if let Some(DbValue::JsonB(rel_bytes)) = get_visible_db_value(entry.key(), &ctx, tx_guard.as_ref()).await {
                                            let pk_key = format!("_pk_node:{}", end_node_id);
                                            if let Some(DbValue::Bytes(label_bytes)) = get_visible_db_value(&pk_key, &ctx, tx_guard.as_ref()).await {
                                                let end_node_label = String::from_utf8(label_bytes)?;
                                                let end_node_key = format!("_node:{}:{}", end_node_label, end_node_id);

                                                if let Some(DbValue::JsonB(end_node_bytes)) = get_visible_db_value(&end_node_key, &ctx, tx_guard.as_ref()).await {
                                                    let next_depth = current_depth + 1;

                                                    let mut end_node_props = serde_json::from_slice::<Value>(&end_node_bytes)?;
                                                    if let Some(obj) = end_node_props.as_object_mut() {
                                                        obj.insert("_id".to_string(), json!(end_node_id));
                                                        obj.insert("_label".to_string(), json!(end_node_label.clone()));
                                                    }
                                                    let mut rel_props = serde_json::from_slice::<Value>(&rel_bytes)?;
                                                    if let Some(obj) = rel_props.as_object_mut() {
                                                        obj.insert("_type".to_string(), json!(rel_type.clone()));
                                                        if edge_prefix.starts_with("_edge:in:") {
                                                            obj.insert("_start_id".to_string(), json!(end_node_id));
                                                            obj.insert("_end_id".to_string(), json!(current_id.clone()));
                                                        } else {
                                                            obj.insert("_start_id".to_string(), json!(current_id.clone()));
                                                            obj.insert("_end_id".to_string(), json!(end_node_id));
                                                        }
                                                    }

                                                    let mut new_path = current_path.clone();
                                                    new_path.push(rel_props.clone());
                                                    new_path.push(end_node_props.clone());

                                                    if next_depth >= min_depth {
                                                        matched_once = true;
                                                        let mut new_row = start_row.clone();
                                                        if let Some(obj) = new_row.as_object_mut() {
                                                            obj.insert(end_node_var.clone(), end_node_props.clone());
                                                            obj.insert(rel_var.clone(), rel_props.clone());
                                                            if let Some(path_var) = &path_variable {
                                                                obj.insert(path_var.clone(), json!(new_path));
                                                            }
                                                        }
                                                        yield new_row;
                                                    }

                                                    q.push_back((end_node_id.to_string(), next_depth, new_path));
                                                    visited_nodes.insert(end_node_id.to_string());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        // Single-step expansion
                        let tx_guard = transaction_handle.read().await;
                        let tx_opt = tx_guard.as_ref();

                        let mut prefixes_to_scan = Vec::new();
                        match direction {
                            ast::RelationshipDirection::Outgoing => {
                                if rel_type.is_empty() {
                                    prefixes_to_scan.push(format!("_edge:out:{}:", start_node_id));
                                } else {
                                    prefixes_to_scan.push(format!("_edge:out:{}:{}:", start_node_id, rel_type));
                                }
                            }
                            ast::RelationshipDirection::Incoming => {
                                if rel_type.is_empty() {
                                    prefixes_to_scan.push(format!("_edge:in:{}:", start_node_id));
                                } else {
                                    prefixes_to_scan.push(format!("_edge:in:{}:{}:", start_node_id, rel_type));
                                }
                            }
                            ast::RelationshipDirection::Both => {
                                let out_prefix = if rel_type.is_empty() {
                                    format!("_edge:out:{}:", start_node_id)
                                } else {
                                    format!("_edge:out:{}:{}:", start_node_id, rel_type)
                                };
                                let in_prefix = if rel_type.is_empty() {
                                    format!("_edge:in:{}:", start_node_id)
                                } else {
                                    format!("_edge:in:{}:{}:", start_node_id, rel_type)
                                };

                                let mut out_degree = 0;
                                let mut in_degree = 0;

                                let mut visible_keys = std::collections::HashSet::new();
                                for entry in ctx.db.iter() {
                                    if entry.key().starts_with(&out_prefix) || entry.key().starts_with(&in_prefix) {
                                        visible_keys.insert(entry.key().clone());
                                    }
                                }
                                if let Some(tx) = tx_opt {
                                    for entry in tx.writes.iter() {
                                        let key = entry.key();
                                        if key.starts_with(&out_prefix) || key.starts_with(&in_prefix) {
                                            if entry.value().is_some() {
                                                visible_keys.insert(key.clone());
                                            } else {
                                                visible_keys.remove(key);
                                            }
                                        }
                                    }
                                }

                                for key in &visible_keys {
                                    if key.starts_with(&out_prefix) { out_degree += 1; }
                                    if key.starts_with(&in_prefix) { in_degree += 1; }
                                }

                                if out_degree <= in_degree {
                                    prefixes_to_scan.push(out_prefix);
                                    prefixes_to_scan.push(in_prefix);
                                } else {
                                    prefixes_to_scan.push(in_prefix);
                                    prefixes_to_scan.push(out_prefix);
                                }
                            }
                        };

                        for edge_prefix in prefixes_to_scan {
                            let mut keys_to_process: std::collections::HashSet<String> = std::collections::HashSet::new();
                            for entry in ctx.db.iter() {
                                if entry.key().starts_with(&edge_prefix) {
                                    keys_to_process.insert(entry.key().clone());
                                }
                            }
                            if let Some(tx) = tx_opt {
                                for entry in tx.writes.iter() {
                                    if entry.key().starts_with(&edge_prefix) {
                                        if entry.value().is_some() {
                                            keys_to_process.insert(entry.key().clone());
                                        } else {
                                            keys_to_process.remove(entry.key());
                                        }
                                    }
                                }
                            }

                            for key in keys_to_process {
                                let parts: Vec<&str> = key.split(':').collect();
                                if parts.len() < 6 { continue; }

                                let end_node_id = parts[4];

                                // Fetch the end node
                                let pk_key = format!("_pk_node:{}", end_node_id);
                                if let Some(DbValue::Bytes(label_bytes)) = get_visible_db_value(&pk_key, &ctx, tx_opt).await {
                                    let end_node_label = String::from_utf8(label_bytes)?;
                                    let end_node_key = format!("_node:{}:{}", end_node_label, end_node_id);

                                    if let Some(DbValue::JsonB(end_node_bytes)) = get_visible_db_value(&end_node_key, &ctx, tx_opt).await {
                                        let mut end_node_props = serde_json::from_slice::<Value>(&end_node_bytes)?;
                                        if let Some(obj) = end_node_props.as_object_mut() {
                                            obj.insert("_id".to_string(), json!(end_node_id));
                                            obj.insert("_label".to_string(), json!(end_node_label.clone()));
                                        }

                                        // Fetch the relationship properties
                                        if let Some(DbValue::JsonB(rel_bytes)) = get_visible_db_value(&key, &ctx, tx_opt).await {
                                            let mut rel_props = serde_json::from_slice::<Value>(&rel_bytes)?;
                                            if let Some(obj) = rel_props.as_object_mut() {
                                                let current_rel_type = key.split(':').nth(3).unwrap_or("");
                                                obj.insert("_type".to_string(), json!(current_rel_type));
                                                if edge_prefix.starts_with("_edge:in:") {
                                                    obj.insert("_start_id".to_string(), json!(end_node_id));
                                                    obj.insert("_end_id".to_string(), json!(start_node_id));
                                                } else {
                                                    obj.insert("_start_id".to_string(), json!(start_node_id));
                                                    obj.insert("_end_id".to_string(), json!(end_node_id));
                                                }
                                            }

                                            matched_once = true;
                                            let mut new_row = start_row.clone();
                                            if let Some(obj) = new_row.as_object_mut() {
                                                obj.insert(end_node_var.clone(), end_node_props.clone());
                                                obj.insert(rel_var.clone(), rel_props.clone()); // Insert rel properties

                                                if let Some(path_var) = &path_variable {
                                                    let path_list = vec![start_node_obj.clone(), rel_props, end_node_props];
                                                    obj.insert(path_var.clone(), json!(path_list));
                                                }
                                            }
                                            yield new_row;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if !matched_once && is_optional {
                        let mut new_row = start_row.clone();
                        if let Some(obj) = new_row.as_object_mut() {
                            obj.insert(end_node_var.clone(), Value::Null);
                            obj.insert(rel_var.clone(), Value::Null);
                            if let Some(path_var) = &path_variable {
                                obj.insert(path_var.clone(), Value::Null);
                            }
                        }
                        yield new_row;
                    }
                }
            }
            PhysicalPlan::Filter { predicate, input } => {
                let mut stream = Box::pin(execute(*input, ctx.clone(), transaction_handle.clone()));
                while let Some(row_result) = stream.next().await {
                    let row = row_result?;
                    if evaluate_expression(&predicate, &row, ctx.clone(), transaction_handle.clone()).await?.as_bool().unwrap_or(false) {
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
                        let value = evaluate_expression(expr, &row, ctx.clone(), transaction_handle.clone()).await?;
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
            PhysicalPlan::Dummy => {
                // Yield a single empty row to kickstart pipelines that don't start with a scan (e.g., CREATE only).
                yield json!({});
            }
            PhysicalPlan::Values(rows) => {
                for row in rows {
                    yield row;
                }
            }
            PhysicalPlan::Create { pattern, input } => {
                let input_rows: Vec<Row> = Box::pin(execute(*input, ctx.clone(), transaction_handle.clone())).try_collect().await?;
                let storage_executor = crate::storage_executor::StorageExecutor::new(ctx.clone(), transaction_handle.clone());

                for input_row in input_rows {
                    let mut new_row = input_row.clone();
                    let mut created_vars: std::collections::HashMap<String, Value> = std::collections::HashMap::new();

                    // Pass 1: Handle all nodes in the pattern.
                    for part in &pattern.parts {
                        if let ast::PatternPart::Node(node_pattern) = part {
                            if let Some(var) = &node_pattern.variable {
                                if input_row.get(var).is_some() {
                                    continue; // Already bound from MATCH
                                }
                            }

                            // Check for _id in properties to decide whether to create or just bind
                            let mut id_to_match = None;
                            if let Some(ast::Expression::Map(props)) = &node_pattern.properties {
                                if let Some((_, id_expr)) = props.iter().find(|(k, _)| k == "_id") {
                                    if let ast::Expression::Literal(ast::LiteralValue::String(id_val)) = id_expr {
                                        id_to_match = Some(id_val.clone());
                                    }
                                }
                            }

                            if let Some(id) = id_to_match {
                                // This is a match, not a create. We need to fetch the node data.
                                let pk_key = format!("_pk_node:{}", id);
                                let tx_guard = transaction_handle.read().await;
                                if let Some(DbValue::Bytes(label_bytes)) = get_visible_db_value(&pk_key, &ctx, tx_guard.as_ref()).await {
                                    let label = String::from_utf8(label_bytes)?;
                                    let node_key = format!("_node:{}:{}", label, id);
                                    if let Some(DbValue::JsonB(props_bytes)) = get_visible_db_value(&node_key, &ctx, tx_guard.as_ref()).await {
                                        let mut node_obj: Value = serde_json::from_slice(&props_bytes)?;
                                        if let Some(obj) = node_obj.as_object_mut() {
                                            obj.insert("_id".to_string(), json!(id.clone()));
                                            obj.insert("_label".to_string(), json!(label.clone()));
                                        }
                                        if let Some(var) = &node_pattern.variable {
                                            created_vars.insert(var.clone(), node_obj.clone());
                                            if let Some(obj) = new_row.as_object_mut() {
                                                obj.insert(var.clone(), node_obj);
                                            }
                                        }
                                    }
                                }
                                continue; // Done with this node part
                            }

                            // This is a new node to create.
                            let label = node_pattern.labels.get(0).cloned().unwrap_or_default();
                            let properties_val = if let Some(props_expr) = &node_pattern.properties {
                                evaluate_expression(props_expr, &input_row, ctx.clone(), transaction_handle.clone()).await?
                            } else {
                                json!({})
                            };
                            let properties_bytes = serde_json::to_vec(&properties_val)?;

                            let response = storage_executor.graph_add_node(label.clone(), properties_bytes).await;
                            if let crate::types::Response::Bytes(id_bytes) = response {
                                let id = String::from_utf8(id_bytes).unwrap_or_default();
                                if let Some(var) = &node_pattern.variable {
                                    let mut node_obj = properties_val;
                                    if let Some(obj) = node_obj.as_object_mut() {
                                        obj.insert("_id".to_string(), json!(id.clone()));
                                        obj.insert("_label".to_string(), json!(label.clone()));
                                    }
                                    created_vars.insert(var.clone(), node_obj.clone());
                                    if let Some(obj) = new_row.as_object_mut() {
                                        obj.insert(var.clone(), node_obj);
                                    }
                                }
                            } else {
                                Err(anyhow!("Failed to create node"))?;
                            }
                        }
                    }

                    // Pass 2: Create relationships
                    for (i, part) in pattern.parts.iter().enumerate() {
                        if let ast::PatternPart::Relationship(rel_pattern) = part {
                            let start_node_pattern = if i > 0 {
                                if let Some(ast::PatternPart::Node(n)) = pattern.parts.get(i - 1) { n } else { continue; }
                            } else { continue; };

                            let end_node_pattern = if let Some(ast::PatternPart::Node(n)) = pattern.parts.get(i + 1) {
                                n
                            } else { continue; };

                            let start_var = start_node_pattern.variable.as_ref().ok_or_else(|| anyhow!("Start node in relationship must have a variable"))?;
                            let end_var = end_node_pattern.variable.as_ref().ok_or_else(|| anyhow!("End node in relationship must have a variable"))?;

                            let get_node_id = |var: &String| -> Result<String> {
                                if let Some(node) = created_vars.get(var) {
                                    if let Some(id) = node.get("_id").and_then(|v| v.as_str()) {
                                        return Ok(id.to_string());
                                    }
                                }
                                if let Some(node) = input_row.get(var) {
                                    if let Some(id) = node.get("_id").and_then(|v| v.as_str()) {
                                        return Ok(id.to_string());
                                    }
                                }
                                Err(anyhow!("Node variable '{}' not found for creating relationship", var))
                            };

                            let (start_id, end_id) = match rel_pattern.direction {
                                ast::RelationshipDirection::Outgoing => (get_node_id(start_var)?, get_node_id(end_var)?),
                                ast::RelationshipDirection::Incoming => (get_node_id(end_var)?, get_node_id(start_var)?),
                                ast::RelationshipDirection::Both => (get_node_id(start_var)?, get_node_id(end_var)?),
                            };

                            let rel_type = rel_pattern.types.get(0).cloned().unwrap_or_default();
                            let properties_val = if let Some(props_expr) = &rel_pattern.properties {
                                evaluate_expression(props_expr, &input_row, ctx.clone(), transaction_handle.clone()).await?
                            } else {
                                json!({})
                            };
                            let properties_bytes = serde_json::to_vec(&properties_val)?;

                            let response = storage_executor.graph_add_relationship(start_id.clone(), end_id.clone(), rel_type.clone(), properties_bytes).await;

                            if let (Some(rel_var), crate::types::Response::Bytes(id_bytes)) = (&rel_pattern.variable, response) {
                                let rel_id = String::from_utf8(id_bytes).unwrap_or_default();
                                let mut rel_obj = properties_val;
                                if let Some(obj) = rel_obj.as_object_mut() {
                                    obj.insert("_id".to_string(), json!(rel_id));
                                    obj.insert("_type".to_string(), json!(rel_type));
                                    obj.insert("_start_id".to_string(), json!(start_id));
                                    obj.insert("_end_id".to_string(), json!(end_id));
                                }

                                if let Some(obj) = new_row.as_object_mut() {
                                    obj.insert(rel_var.clone(), rel_obj);
                                }
                            }
                        }
                    }
                    yield new_row;
                }
            }
            PhysicalPlan::Set { items, input } => {
                let input_rows: Vec<Row> = Box::pin(execute(*input, ctx.clone(), transaction_handle.clone())).try_collect().await?;
                let storage_executor = crate::storage_executor::StorageExecutor::new(ctx.clone(), transaction_handle.clone());
                for mut row in input_rows {
                    for item in &items {
                        if let ast::Expression::Property(var_expr, prop_name) = &item.property {
                            if let ast::Expression::Variable(var_name) = &**var_expr {
                                let entity_val = row.get(var_name).ok_or_else(|| anyhow!("Variable '{}' not found for SET", var_name))?;
                                let entity_id = entity_val.get("_id").and_then(|v| v.as_str()).ok_or_else(|| anyhow!("Entity ID not found for variable '{}'", var_name))?;
                                
                                let value_to_set = evaluate_expression(&item.expression, &row, ctx.clone(), transaction_handle.clone()).await?;
                                let value_bytes = serde_json::to_vec(&value_to_set)?;

                                // Check if it's a node or relationship. Nodes have a _label.
                                let is_node = entity_val.get("_label").is_some();

                                let response = if is_node {
                                    storage_executor.graph_set_node_property(entity_id.to_string(), prop_name.clone(), value_bytes).await
                                } else {
                                    storage_executor.graph_set_relationship_property(entity_id.to_string(), prop_name.clone(), value_bytes).await
                                };

                                if let crate::types::Response::Error(e) = response {
                                    Err(anyhow!("Failed to set property: {}", e))?;
                                }

                                // Update the row being yielded with the new value
                                if let Some(obj) = row.get_mut(var_name).and_then(|v| v.as_object_mut()) {
                                    obj.insert(prop_name.clone(), value_to_set);
                                }
                            }
                        }
                    }
                    yield row;
                }
            }
            PhysicalPlan::Remove { items, input } => {
                let input_rows: Vec<Row> = Box::pin(execute(*input, ctx.clone(), transaction_handle.clone())).try_collect().await?;
                let storage_executor = crate::storage_executor::StorageExecutor::new(ctx.clone(), transaction_handle.clone());
                for mut row in input_rows {
                    for item in &items {
                        if let ast::Expression::Property(var_expr, prop_name) = item {
                            if let ast::Expression::Variable(var_name) = &**var_expr {
                                let entity_val = row.get(var_name).ok_or_else(|| anyhow!("Variable '{}' not found for REMOVE", var_name))?;
                                let entity_id = entity_val.get("_id").and_then(|v| v.as_str()).ok_or_else(|| anyhow!("Entity ID not found for variable '{}'", var_name))?;

                                let is_node = entity_val.get("_label").is_some();

                                let response = if is_node {
                                    storage_executor.graph_remove_node_property(entity_id.to_string(), prop_name.clone()).await
                                } else {
                                    storage_executor.graph_remove_relationship_property(entity_id.to_string(), prop_name.clone()).await
                                };

                                if let crate::types::Response::Error(e) = response {
                                    Err(anyhow!("Failed to remove property: {}", e))?;
                                }

                                // Update the row being yielded by removing the property
                                if let Some(obj) = row.get_mut(var_name).and_then(|v| v.as_object_mut()) {
                                    obj.remove(prop_name);
                                }
                            }
                        }
                    }
                    yield row;
                }
            }
            PhysicalPlan::Delete { expressions, detach, input } => {
                let stream = Box::pin(execute(*input, ctx.clone(), transaction_handle.clone()));
                let storage_executor = crate::storage_executor::StorageExecutor::new(ctx.clone(), transaction_handle.clone());
                let rows: Vec<Row> = stream.try_collect().await?;

                for row in &rows {
                    for expr in &expressions {
                        if let ast::Expression::Variable(var_name) = expr {
                            let node_val = row.get(var_name).ok_or_else(|| anyhow!("Variable '{}' not found for DELETE", var_name))?;
                            let node_id = node_val.get("_id").and_then(|v| v.as_str()).ok_or_else(|| anyhow!("Node ID not found for variable '{}'", var_name))?;
                            
                            if detach {
                                // Find and delete all relationships connected to this node
                                let out_prefix = format!("_edge:out:{}:", node_id);
                                let in_prefix = format!("_edge:in:{}:", node_id);

                                let mut rel_ids_to_delete = Vec::new();

                                { 
                                    let tx_guard = transaction_handle.read().await;
                                    let tx_ref = tx_guard.as_ref();

                                    let mut keys_to_check = std::collections::HashSet::new();
                                    for entry in ctx.db.iter() {
                                        if entry.key().starts_with(&out_prefix) || entry.key().starts_with(&in_prefix) {
                                            keys_to_check.insert(entry.key().clone());
                                        }
                                    }
                                    if let Some(tx) = tx_ref {
                                        for entry in tx.writes.iter() {
                                            let key = entry.key();
                                            if key.starts_with(&out_prefix) || key.starts_with(&in_prefix) {
                                                if entry.value().is_some() {
                                                    keys_to_check.insert(key.clone());
                                                } else {
                                                    keys_to_check.remove(key);
                                                }
                                            }
                                        }
                                    }

                                    for key in keys_to_check {
                                        if let Some(DbValue::JsonB(bytes)) = get_visible_db_value(&key, &ctx, tx_ref).await {
                                            if let Ok(props) = serde_json::from_slice::<Value>(&bytes) {
                                                if let Some(rel_id) = props.get("_id").and_then(|v| v.as_str()) {
                                                    rel_ids_to_delete.push(rel_id.to_string());
                                                }
                                            }
                                        }
                                    }
                                }
                                
                                for rel_id in rel_ids_to_delete {
                                    let response = storage_executor.graph_delete(rel_id).await;
                                    if let crate::types::Response::Error(e) = response {
                                        Err(anyhow!("Failed to detach relationship: {}", e))?;
                                    }
                                }
                            }

                            let response = storage_executor.graph_delete(node_id.to_string()).await;
                            if let crate::types::Response::Error(e) = response {
                                Err(anyhow!("Failed to delete: {}", e))?;
                            }
                        }
                    }
                }
            }
            PhysicalPlan::Merge { pattern, on_create, on_match, input } => {
                let matched_rows: Vec<Row> = Box::pin(execute(*input, ctx.clone(), transaction_handle.clone())).try_collect().await?;

                if !matched_rows.is_empty() {
                    // ON MATCH
                    if let Some(set_clause) = on_match {
                        let input_plan = Box::new(PhysicalPlan::Values(matched_rows));
                        let set_plan = PhysicalPlan::Set { items: set_clause.items, input: input_plan };
                        let mut stream = Box::pin(execute(set_plan, ctx.clone(), transaction_handle.clone()));
                        while let Some(row) = stream.next().await {
                            yield row?;
                        }
                    } else {
                        for row in matched_rows {
                            yield row;
                        }
                    }
                } else {
                    // ON CREATE
                    let create_plan = PhysicalPlan::Create { pattern, input: Box::new(PhysicalPlan::Dummy) };
                    if let Some(set_clause) = on_create {
                        let set_plan = PhysicalPlan::Set { items: set_clause.items, input: Box::new(create_plan) };
                        let mut stream = Box::pin(execute(set_plan, ctx.clone(), transaction_handle.clone()));
                        while let Some(row) = stream.next().await {
                            yield row?;
                        }
                    } else {
                        let mut stream = Box::pin(execute(create_plan, ctx.clone(), transaction_handle.clone()));
                        while let Some(row) = stream.next().await {
                            yield row?;
                        }
                    }
                }
            }
            PhysicalPlan::Join { left, right, condition, join_type: _ } => {
                let left_rows: Vec<Row> = Box::pin(execute(*left, ctx.clone(), transaction_handle.clone())).try_collect().await?;
                let right_rows: Vec<Row> = Box::pin(execute(*right, ctx.clone(), transaction_handle.clone())).try_collect().await?;

                for l_row in left_rows {
                    for r_row in &right_rows {
                        let mut combined = l_row.as_object().unwrap().clone();
                        combined.extend(r_row.as_object().unwrap().clone());
                        let combined_row = json!(combined);

                        if evaluate_expression(&condition, &combined_row, ctx.clone(), transaction_handle.clone()).await?.as_bool().unwrap_or(false) {
                            yield combined_row;
                        }
                    }
                }
            }
            PhysicalPlan::Sort { input, sort_expressions } => {
                let rows: Vec<Row> = Box::pin(execute(*input, ctx.clone(), transaction_handle.clone())).try_collect().await?;
                let mut sort_data = Vec::new();
                for row in rows.into_iter() {
                    let mut keys = Vec::new();
                    for (expr, _) in &sort_expressions {
                        keys.push(evaluate_expression(expr, &row, ctx.clone(), transaction_handle.clone()).await?);
                    }
                    sort_data.push((keys, row));
                }

                sort_data.sort_by(|(keys_a, _), (keys_b, _)| {
                    for (i, (_, asc)) in sort_expressions.iter().enumerate() {
                        let val_a = &keys_a[i];
                        let val_b = &keys_b[i];

                        let ord = compare_cypher_values(val_a, val_b);
                        let final_ord = if *asc { ord } else { ord.reverse() };

                        if final_ord != std::cmp::Ordering::Equal {
                            return final_ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });

                for (_, row) in sort_data {
                    yield row;
                }
            }
        }
    }
}