use crate::transaction::{Transaction, TransactionHandle};
use std::sync::atomic::Ordering;
use anyhow::{anyhow, Result};
use async_stream::try_stream;
use futures::stream::{Stream, StreamExt, TryStreamExt};
use serde_json::{json, Value};

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};


use super::ast::{AlterTableAction, TableConstraint};
use super::logical_plan::{cast_value_to_type, Expression, JoinType, LogicalOperator, LogicalPlan, Operator};
use super::physical_plan::PhysicalPlan;
use crate::schema::{ColumnDefinition, DataType, VirtualSchema, SCHEMA_PREFIX, VIEW_PREFIX};
use crate::types::{AppContext, Command, DbValue, LogEntry, LogRequest, PersistenceRequest, Response, SchemaCache, ViewDefinition};

pub const SCHEMALIST_PREFIX: &str = "_internal:schemalist:";

type Row = Value;

macro_rules! log_and_wait_qe {
    ($logger:expr, $entry:expr, $ctx:expr) => {
        async {
            let (ack_tx, ack_rx) = oneshot::channel();
            let log_req = LogRequest {
                entry: $entry,
                ack: ack_tx,
                durability: $ctx.config.durability.clone(),
            };
            if $logger.send(PersistenceRequest::Log(log_req)).await.is_err() {
                Err(anyhow!("Persistence engine is down"))
            } else {
                match ack_rx.await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(anyhow!("WAL write error: {}", e)),
                    Err(_) => Err(anyhow!("Persistence engine dropped ACK channel")),
                }
            }
        }
    };
}

/// Determines the data type of a column expression by inspecting the row and schema cache.
fn get_expression_type(
    expr: &Expression,
    row: &Value,
    schema_cache: &SchemaCache,
) -> Option<DataType> {
    match expr {
        Expression::Column(col_name) => {
            if let Some(obj) = row.as_object() {
                // Handle qualified name first: "users.age"
                if col_name.contains('.') {
                    let parts: Vec<&str> = col_name.split('.').collect();
                    let table_name = parts[0];
                    let column_name = parts[1];
                    if let Some(table_obj) = obj.get(table_name) {
                        if table_obj.get(column_name).is_some() {
                            if let Some(schema) = schema_cache.get(table_name) {
                                if let Some(col_def) = schema.columns.get(column_name) {
                                    return Some(col_def.data_type.clone());
                                }
                            }
                        }
                    }
                } else {
                    // Unqualified name: "age"
                    for (table_name, sub_obj) in obj {
                        if sub_obj.get(col_name).is_some() {
                            if let Some(schema) = schema_cache.get(table_name) {
                                if let Some(col_def) = schema.columns.get(col_name) {
                                    return Some(col_def.data_type.clone());
                                }
                            }
                            // Found it, but no type info, so stop searching and return None.
                            return None;
                        }
                    }
                }
            }
            None
        }
        _ => None,
    }
}

fn compare_typed_values(
    val_a: &Value,
    val_b: &Value,
    target_type: &Option<DataType>,
) -> std::cmp::Ordering {
    if val_a.is_null() && val_b.is_null() {
        return std::cmp::Ordering::Equal;
    }
    if val_a.is_null() {
        return std::cmp::Ordering::Less;
    } // NULLS FIRST
    if val_b.is_null() {
        return std::cmp::Ordering::Greater;
    }

    if let Some(data_type) = target_type {
        match data_type {
            DataType::Integer
            | DataType::Timestamp
            | DataType::SmallInt
            | DataType::BigInt
            | DataType::Real
            | DataType::DoublePrecision
            | DataType::Numeric { .. } => {
                let a_num = val_a
                    .as_f64()
                    .or_else(|| val_a.as_str().and_then(|s| s.parse::<f64>().ok()));
                let b_num = val_b
                    .as_f64()
                    .or_else(|| val_b.as_str().and_then(|s| s.parse::<f64>().ok()));

                if let (Some(a), Some(b)) = (a_num, b_num) {
                    return a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal);
                }
            }
            _ => {} // For Text and Boolean, default comparison is fine.
        }
    }

    // Default comparison logic
    match (val_a, val_b) {
        (Value::Number(n_a), Value::Number(n_b)) => n_a
            .as_f64()
            .unwrap()
            .partial_cmp(&n_b.as_f64().unwrap())
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(s_a), Value::String(s_b)) => s_a.cmp(s_b),
        (Value::Bool(b_a), Value::Bool(b_b)) => b_a.cmp(b_b),
        _ => val_a.to_string().cmp(&val_b.to_string()),
    }
}

async fn project_row<'a>(
    row: &'a Row,
    expressions: &'a Vec<(Expression, Option<String>)>, 
    ctx: Arc<AppContext>,
    outer_row: Option<&'a Row>,
    _working_tables: Option<&'a HashMap<String, Vec<Row>>>,
    transaction_handle: Option<TransactionHandle>,
) -> Result<Row> {
    let mut new_row = json!({});
    let mut is_star = false;

    if expressions.len() == 1 {
        if let (Expression::Column(col_name), None) = &expressions[0] {
            if col_name == "*" {
                is_star = true;
            }
        }
    }

    if is_star {
        // For DML returning, the row is not nested like in SELECT
        if let Some(obj) = row.as_object() {
            for (k, v) in obj {
                if k != "_key" {
                    new_row[k.clone()] = v.clone();
                }
            }
        }
    } else {
        for (expr, alias) in expressions {
            let val = expr.evaluate_with_context(row, outer_row, ctx.clone(), transaction_handle.clone()).await?;
            let name = alias.clone().unwrap_or_else(|| expr.to_string());
            new_row[name] = val;
        }
    }
    Ok(new_row)
}

pub fn execute<'a>(
    plan: PhysicalPlan,
    ctx: Arc<AppContext>,
    outer_row: Option<&'a Row>,
    _working_tables: Option<&'a HashMap<String, Vec<Row>>>,
    transaction_handle: Option<TransactionHandle>,
) -> Pin<Box<dyn Stream<Item = Result<Row>> + Send + 'a>> {
    Box::pin(try_stream! {        match plan {
            PhysicalPlan::TableScan { prefix } => {
                let table_name = prefix.strip_suffix(':').unwrap_or(&prefix).to_string();

                let tx_guard_read = if let Some(handle) = &transaction_handle {
                    Some(handle.read().await)
                } else {
                    None
                };
                let tx_opt = tx_guard_read.as_ref().map(|guard| guard.as_ref()).flatten();

                // In a transaction, we must consider all keys in the DB and in the write set.
                let mut keys_to_process: HashSet<String> = HashSet::new();
                if let Some(tx) = tx_opt {
                    for r in ctx.db.iter() {
                        if r.key().starts_with(&prefix) {
                            keys_to_process.insert(r.key().clone());
                        }
                    }
                    for item in tx.writes.iter() {
                        if item.key().starts_with(&prefix) {
                            keys_to_process.insert(item.key().clone());
                        }
                    }
                } else {
                    // Not in a transaction, just iterate the db.
                    for r in ctx.db.iter() {
                        if r.key().starts_with(&prefix) {
                            keys_to_process.insert(r.key().clone());
                        }
                    }
                }

                for key in keys_to_process {
                    let visible_value: Option<DbValue> =
                        if let Some(tx) = tx_opt {
                            crate::storage_executor::get_visible_db_value(&key, &ctx, Some(tx)).await
                        } else {
                            crate::storage_executor::get_visible_db_value(&key, &ctx, None).await
                        };

                    if let Some(db_value) = visible_value {
                        let mut value = match db_value {
                            DbValue::Json(v) => v.clone(),
                            DbValue::JsonB(b) => serde_json::from_slice(&b)?,
                            _ => json!({}), // Or handle error for non-json types in tables
                        };
                        if let Some(obj) = value.as_object_mut() {
                            let key_without_prefix = key.strip_prefix(&prefix).unwrap_or(&key);
                            let id_from_key =
                                key_without_prefix.strip_suffix(':').unwrap_or(key_without_prefix);
                            if !obj.contains_key("id") {
                                obj.insert("id".to_string(), json!(id_from_key));
                            }
                            obj.insert("_key".to_string(), json!(key));
                        }
                        let mut new_row = json!({});
                        new_row[table_name.clone()] = value;
                        yield new_row;
                    }
                }
            }
            PhysicalPlan::IndexScan { index_name, key } => {
                let table_name = index_name.split('|').next().unwrap_or("").strip_suffix(":*").unwrap_or("").to_string();
                let prefix = format!("{}:", table_name);
                if let Some(index) = ctx.index_manager.indexes.get(&index_name) {
                    let index_key = serde_json::to_string(&key).unwrap_or_default();
                    let index_data = index.read().await;
                    if let Some(keys) = index_data.get(&index_key) {
                        for db_key in keys {
                            let tx_guard_read = if let Some(handle) = &transaction_handle {
                                Some(handle.read().await)
                            } else {
                                None
                            };
                            let tx_opt = tx_guard_read.as_ref().map(|guard| guard.as_ref()).flatten();

                            let visible_value: Option<DbValue> =
                                if let Some(tx) = tx_opt {
                                    crate::storage_executor::get_visible_db_value(db_key, &ctx, Some(tx)).await
                                } else {
                                    crate::storage_executor::get_visible_db_value(db_key, &ctx, None).await
                                };
                            // --- End Visibility Check ---

                            if let Some(db_value) = visible_value {
                                let mut value = match db_value {
                                    DbValue::Json(v) => v.clone(),
                                    DbValue::JsonB(b) => serde_json::from_slice(&b).unwrap_or_else(|_| json!({})),
                                    _ => json!({}),
                                };
                                if let Some(obj) = value.as_object_mut() {
                                    let key_without_prefix = db_key.strip_prefix(&prefix).unwrap_or(db_key);
                                    let id_from_key = key_without_prefix.strip_suffix(':').unwrap_or(key_without_prefix);
                                    if !obj.contains_key("id") {
                                        obj.insert("id".to_string(), json!(id_from_key));
                                    }
                                    obj.insert("_key".to_string(), json!(db_key));
                                }
                                let mut new_row = json!({});
                                new_row[table_name.clone()] = value;
                                yield new_row;
                            }
                        }
                    }
                }
            }
            PhysicalPlan::Filter { input, predicate } => {
                let mut stream = execute(*input, ctx.clone(), outer_row, _working_tables, transaction_handle.clone());
                while let Some(row_result) = stream.next().await {
                    let row = row_result?;
                    if predicate.evaluate_with_context(&row, outer_row, ctx.clone(), transaction_handle.clone()).await?.as_bool().unwrap_or(false) {
                        yield row;
                    }
                }
            }
            PhysicalPlan::Projection {
                input,
                expressions,
            } => {
                let mut stream = execute(*input, ctx.clone(), outer_row, _working_tables, transaction_handle.clone());
                while let Some(row_result) = stream.next().await {
                    let row = row_result?;
                    let mut new_row = json!({});
                    let mut is_star = false;

                    // Check for SELECT *
                    if expressions.len() == 1 {
                        if let (Expression::Column(col_name), None) = &expressions[0] {
                            if col_name == "*" {
                                is_star = true;
                            }
                        }
                    }

                    if is_star {
                        // Flatten the row for SELECT *. It could be nested from a join.
                        if let Some(obj) = row.as_object() {
                            for sub_obj in obj.values() {
                                if let Some(sub_obj_map) = sub_obj.as_object() {
                                    for (k, v) in sub_obj_map {
                                        if k != "_key" {
                                            new_row[k.clone()] = v.clone();
                                        }
                                    }
                                }
                            }
                        }
                        yield new_row;
                        continue;
                    }

                    for (expr, alias) in &expressions {
                        // For aggregates, the value is already computed by the Aggregate operator.
                        // For other expressions, evaluate them against the row.
                        let val = if let Expression::AggregateFunction {..} = expr {
                            let key = format!("{}", expr);
                            row.get(&key).cloned().unwrap_or(Value::Null)
                        } else {
                            expr.evaluate_with_context(&row, outer_row, ctx.clone(), transaction_handle.clone()).await?
                        };

                        // Determine the output column name. Use alias if it exists.
                        let name = match alias {
                            Some(name) => name.clone(),
                            None => match expr {
                                Expression::Column(name) => {
                                    // Keep the full qualified name in the output if no alias is given.
                                    // This is what the tests expect.
                                    name.clone()
                                },
                                Expression::AggregateFunction { func, arg } => {
                                    if let Expression::Column(arg_name) = &**arg {
                                        format!("{}({})", func, arg_name)
                                    } else {
                                        format!("{}(*)", func)
                                    }
                                }
                                _ => expr.to_string(),
                            }
                        };
                        new_row[name] = val;
                    }
                    yield new_row;
                }
            }
            PhysicalPlan::Join { left, right, condition, join_type } => {
                let left_rows: Vec<Row> = execute(*left, ctx.clone(), outer_row, _working_tables, transaction_handle.clone()).try_collect().await?;
                let right_rows: Vec<Row> = execute(*right, ctx.clone(), outer_row, _working_tables, transaction_handle.clone()).try_collect().await?;

                if join_type == JoinType::Cross {
                    for l_row in &left_rows {
                        for r_row in &right_rows {
                             let mut combined = l_row.as_object().unwrap().clone();
                             combined.extend(r_row.as_object().unwrap().clone());
                             yield json!(combined);
                        }
                    }
                    return;
                }

                let mut right_matched = vec![false; right_rows.len()];

                for l_row in &left_rows {
                    let mut has_match = false;
                    for (i, r_row) in right_rows.iter().enumerate() {
                        // Combine the nested objects from the left and right rows.
                        let mut combined = l_row.as_object().unwrap().clone();
                        combined.extend(r_row.as_object().unwrap().clone());
                        let combined_row = json!(combined);

                        if condition.evaluate_with_context(&combined_row, outer_row, ctx.clone(), transaction_handle.clone()).await?.as_bool().unwrap_or(false) {
                            yield combined_row;
                            has_match = true;
                            if i < right_matched.len() {
                                right_matched[i] = true;
                            }
                        }
                    }
                    if !has_match && (join_type == JoinType::Left || join_type == JoinType::FullOuter) {
                        // For left/full joins, yield the left row with nulls for the right side.
                        // The structure of the right side needs to be known, which is a limitation here.
                        // A simple approach is to just yield the left row as is.
                        yield l_row.clone();
                    }
                }

                if join_type == JoinType::Right || join_type == JoinType::FullOuter {
                    for (i, matched) in right_matched.iter().enumerate() {
                        if !matched {
                            // Yield the unmatched right rows.
                            yield right_rows[i].clone();
                        }
                    }
                }
            }
            PhysicalPlan::HashAggregate { input, group_expressions, agg_expressions } => {
                let all_rows: Vec<Row> = execute(*input, ctx.clone(), outer_row, _working_tables, transaction_handle.clone()).try_collect().await?;
                let mut groups: HashMap<String, (Row, Vec<Row>)> = HashMap::new();

                if group_expressions.is_empty() {
                    if !all_rows.is_empty() {
                        let mut agg_row = json!({});
                        for agg_expr in &agg_expressions {
                            if let Expression::AggregateFunction { func, arg } = agg_expr {
                                let result = run_agg_fn(func, arg, &all_rows, outer_row, ctx.clone(), transaction_handle.clone()).await?;
                                let agg_name = format!("{}", agg_expr);
                                agg_row[agg_name] = result;
                            }
                        }
                        yield agg_row;
                    }
                    return;
                }

                for row in all_rows {
                    let mut group_key_parts = Vec::new();
                    for expr in &group_expressions {
                        group_key_parts.push(expr.evaluate_with_context(&row, outer_row, ctx.clone(), transaction_handle.clone()).await?.to_string());
                    }
                    let group_key = group_key_parts.join("-");

                    if !groups.contains_key(&group_key) {
                        let mut group_row = json!({});
                        for expr in &group_expressions {
                            // The expression already knows how to get the value from the nested row
                            let value =
                                expr.evaluate_with_context(&row, outer_row, ctx.clone(), transaction_handle.clone())
                                    .await
                                    .unwrap_or(Value::Null);
                            // We need the final name of the column for the output row
                            if let Expression::Column(name) = expr {
                                group_row[name.clone()] = value;
                            }
                        }
                        groups.insert(group_key.clone(), (group_row, Vec::new()));
                    }
                    groups.get_mut(&group_key).unwrap().1.push(row);
                }

                for (_, (mut group_row, rows)) in groups {
                    for agg_expr in &agg_expressions {
                        if let Expression::AggregateFunction { func, arg } = agg_expr {
                            let result = run_agg_fn(func, arg, &rows, outer_row, ctx.clone(), transaction_handle.clone()).await?;
                            let agg_name = format!("{}", agg_expr);
                            group_row[agg_name] = result;
                        }
                    }
                    yield group_row;
                }
            }
            PhysicalPlan::Sort { input, sort_expressions } => {
                let rows: Vec<Row> = execute(*input, ctx.clone(), outer_row, _working_tables, transaction_handle.clone()).try_collect().await?;
                let mut sort_data = Vec::new();
                for row in rows.into_iter() {
                    let mut keys = Vec::new();
                    for (expr, _) in &sort_expressions {
                        keys.push(expr.evaluate_with_context(&row, outer_row, ctx.clone(), transaction_handle.clone()).await?);
                    }
                    sort_data.push((keys, row));
                }

                sort_data.sort_by(|(keys_a, row_a), (keys_b, _row_b)| {
                    for (i, (expr, asc)) in sort_expressions.iter().enumerate() {
                        let val_a = &keys_a[i];
                        let val_b = &keys_b[i];

                        let target_type = get_expression_type(expr, row_a, &ctx.schema_cache);
                        let ord = compare_typed_values(val_a, val_b, &target_type);

                        let ord = if *asc { ord } else { ord.reverse() };
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });

                for (_, row) in sort_data {
                    yield row;
                }
            }
            PhysicalPlan::Limit { input, limit, offset } => {
                let stream = execute(*input, ctx.clone(), outer_row, _working_tables, transaction_handle.clone()).skip(offset.unwrap_or(0));
                let stream = if let Some(limit) = limit {
                    if limit == 0 { stream.take(0) } else { stream.take(limit) }
                } else {
                    stream.take(usize::MAX)
                };
                let mut stream = Box::pin(stream);
                while let Some(row) = stream.next().await {
                    yield row?;
                }
            }
            PhysicalPlan::CreateTable { table_name, columns, if_not_exists, constraints } => {
                if table_name.contains('.') {
                    let schema_name = table_name.split('.').next().unwrap();
                    let schema_list_key = format!("{}{}", SCHEMALIST_PREFIX, schema_name);
                    if !ctx.db.contains_key(&schema_list_key) {
                        Err(anyhow!("Schema '{}' does not exist", schema_name))?;
                    }
                }

                if ctx.schema_cache.contains_key(&table_name) {
                    if if_not_exists {
                        yield json!({ "status": format!("Table '{}' already exists, skipped", table_name) });
                        return;
                    } else {
                        Err(anyhow!("Table '{}' already exists", table_name))?;
                    }
                }
                let schema_key = format!("{}{}", SCHEMA_PREFIX, table_name);
                let mut cols = std::collections::BTreeMap::new();
                let mut column_order = Vec::with_capacity(columns.len());

                for c in columns {
                    column_order.push(c.name.clone());
                    let dt = DataType::from_str(&c.data_type)?;
                    let default = if let Some(d) = c.default {
                        Some(super::logical_plan::simple_expr_to_expression(d, &ctx.schema_cache, &ctx.view_cache, &ctx.function_registry, None)?)
                    } else {
                        None
                    };
                    cols.insert(c.name, ColumnDefinition { data_type: dt, nullable: c.nullable, default });
                }

                let mut primary_key_cols = Vec::new(); // To be populated from constraints
                let mut foreign_key_clauses = Vec::new(); // To be populated from constraints
                let mut check_expression: Option<Expression> = None; // To be populated from constraints

                for constraint in &constraints {
                    match constraint {
                        TableConstraint::PrimaryKey { name: _, columns } => {
                            if !primary_key_cols.is_empty() {
                                Err(anyhow!("Multiple primary keys defined for table '{}'", table_name))?;
                            }
                            primary_key_cols = columns.clone();
                            for pk_col_name in columns.iter() {
                                if let Some(c) = cols.get_mut(pk_col_name) {
                                    c.nullable = false; // Primary key implies NOT NULL
                                } else {
                                    Err(anyhow!("Column '{}' in PRIMARY KEY not found in table definition", pk_col_name))?;
                                }
                            }
                        }
                        TableConstraint::ForeignKey(fk) => {
                            foreign_key_clauses.push(fk.clone());
                        }
                        TableConstraint::Check { name: _, expression } => {
                            if check_expression.is_some() {
                                Err(anyhow!("Multiple CHECK constraints defined for table '{}'", table_name))?;
                            }
                            check_expression = Some(super::logical_plan::simple_expr_to_expression(expression.clone(), &ctx.schema_cache, &ctx.view_cache, &ctx.function_registry, None)?);
                        }
                        TableConstraint::Unique { name: constraint_name_option, columns } => {
                            // Create unique indexes for unique constraints
                            if columns.len() != 1 {
                                Err(anyhow!("Composite UNIQUE constraints are not yet supported."))?;
                            }
                            let col_name = &columns[0];
                            if !cols.contains_key(col_name) {
                                                        Err(anyhow!("Column '{}' in UNIQUE constraint not found in table definition", col_name))?
                            }
                            let index_name = constraint_name_option.clone().unwrap_or_else(|| format!("unique_{}_{}", table_name, col_name));
                            let key_prefix = format!("{}:*", table_name);
                            let json_path = col_name.clone();
                            let create_index_command = Command {
                                name: "IDX.CREATE".to_string(),
                                args: vec![
                                    b"IDX.CREATE".to_vec(),
                                    index_name.into_bytes(),
                                    key_prefix.into_bytes(),
                                    json_path.into_bytes(),
                                ],
                            };
                            if let Response::Error(e) = super::super::commands::handle_idx_create(create_index_command, ctx.clone()).await {
                                Err(anyhow!("Failed to create unique index: {}", e))?;
                            }
                        }
                    }
                }

                let schema = VirtualSchema {
                    table_name: table_name.clone(),
                    columns: cols,
                    column_order,
                    constraints, // Store all constraints
                };

                // Validate foreign keys (using the populated foreign_key_clauses)
                for fk in &foreign_key_clauses {
                    let referenced_table_name = &fk.references_table;
                    let referenced_schema = ctx.schema_cache.get(referenced_table_name)
                        .ok_or_else(|| anyhow!("Referenced table '{}' for foreign key does not exist", referenced_table_name))?;

                    // Check that referenced columns exist
                    for col_name in &fk.references_columns {
                        if !referenced_schema.columns.contains_key(col_name) {
                            Err(anyhow!("Referenced column '{}' does not exist in table '{}'", col_name, referenced_table_name))?;
                        }
                    }

                    // Check that referenced columns are unique or a primary key
                    let referenced_cols_set: std::collections::HashSet<_> = fk.references_columns.iter().cloned().collect();

                    let is_pk = referenced_schema.constraints.iter().any(|c| {
                        if let TableConstraint::PrimaryKey { name: _, columns } = c {
                            let pk_cols_set: std::collections::HashSet<_> = columns.iter().cloned().collect();
                            !pk_cols_set.is_empty() && pk_cols_set == referenced_cols_set
                        } else {
                            false
                        }
                    });

                    let is_unique_constraint = referenced_schema.constraints.iter().any(|c| {
                        if let TableConstraint::Unique { name: _, columns } = c {
                            let unique_cols_set: std::collections::HashSet<_> = columns.iter().cloned().collect();
                            !unique_cols_set.is_empty() && unique_cols_set == referenced_cols_set
                        } else {
                            false
                        }
                    });

                    if !is_pk && !is_unique_constraint {
                        Err(anyhow!("There is no unique constraint matching the referenced columns for foreign key in table '{}'", referenced_table_name))?;
                    }
                }

                let schema_bytes = serde_json::to_vec(&schema)?;

                let log_entry = LogEntry::SetBytes { key: schema_key.clone(), value: schema_bytes.clone() };
                log_and_wait_qe!(ctx.logger, log_entry, ctx).await?;

                let version = crate::types::VersionedValue { value: DbValue::Bytes(schema_bytes), creator_txid: 0, expirer_txid: 0 };
                ctx.db.insert(schema_key, Arc::new(RwLock::new(vec![version])));
                ctx.schema_cache.insert(table_name, Arc::new(schema));
                yield json!({"status": "Table created"});
            }
            PhysicalPlan::DropTable { table_name } => {
                if !ctx.schema_cache.contains_key(&table_name) {
                    Err(anyhow!("Table '{}' does not exist", table_name))?;
                }
                let schema_key = format!("{}{}", SCHEMA_PREFIX, table_name);

                // Log the deletion
                let log_entry = LogEntry::Delete { key: schema_key.clone() };
                log_and_wait_qe!(ctx.logger, log_entry, ctx).await?;

                // Execute the deletion
                ctx.schema_cache.remove(&table_name);
                ctx.db.remove(&schema_key);

                yield json!({"status": "Table dropped"});
            }
            PhysicalPlan::DropView { view_name } => {
                if !ctx.view_cache.contains_key(&view_name) {
                    Err(anyhow!("View '{}' does not exist", view_name))?;
                }
                let view_key = format!("{}{}", VIEW_PREFIX, view_name);

                let log_entry = LogEntry::Delete { key: view_key.clone() };
                log_and_wait_qe!(ctx.logger, log_entry, ctx).await?;

                ctx.view_cache.remove(&view_name);
                ctx.db.remove(&view_key);

                yield json!({"status": "View dropped"});
            }
            PhysicalPlan::Insert { table_name, columns, source, source_column_names, on_conflict, returning } => {
                let source_rows: Vec<Row> = execute(*source, ctx.clone(), outer_row, _working_tables, transaction_handle.clone()).try_collect().await?;

                let executor = crate::storage_executor::StorageExecutor::new(ctx.clone(), transaction_handle.clone().expect("Transaction handle must be present for INSERT"));
                let inserted_rows = executor.insert_rows(&table_name, &columns, source_rows.clone(), &source_column_names, &on_conflict).await?;

                if !returning.is_empty() {
                    for returned_row in inserted_rows {
                        let proj_row = project_row(&returned_row, &returning, ctx.clone(), outer_row, _working_tables, transaction_handle.clone()).await?;
                        yield proj_row;
                    }
                } else {
                    yield json!({ "rows_affected": inserted_rows.len() as u64 });
                }
            }
            PhysicalPlan::Delete { table_name, from, returning } => {
                let stream = execute(*from, ctx.clone(), outer_row, _working_tables, transaction_handle.clone());
                let rows_to_delete: Vec<Row> = stream.try_collect().await?;


                // First, apply all ON DELETE actions. If any of these fail, we abort before any actual deletion.
                for row in &rows_to_delete {
                    if let Some(obj) = row.as_object() {
                        if let Some(table_obj) = obj.get(&table_name) {
                            // table_obj is the inner JSON object, e.g., {"id": 1, "name": "Alice"}
                            apply_on_delete_actions(&table_name, table_obj, &ctx, transaction_handle.clone()).await?;
                        }
                    }
                }

                let executor = crate::storage_executor::StorageExecutor::new(ctx.clone(), transaction_handle.clone().unwrap());
                let deleted_count = executor.delete_rows(&table_name, rows_to_delete.clone()).await?;

                if !returning.is_empty() {
                    for returned_row in rows_to_delete {
                        // The row from the plan is nested, e.g. {"users": {"id":1, ...}}
                        // We need to un-nest it for projection.
                        if let Some(inner_row) = returned_row.get(&table_name) {
                            let proj_row = project_row(inner_row, &returning, ctx.clone(), outer_row, _working_tables, transaction_handle.clone()).await?;
                            yield proj_row;
                        }
                    }
                } else {
                    yield json!({ "rows_affected": deleted_count });
                }
            }
            PhysicalPlan::Update { table_name, from, set, returning } => {
                let stream = execute(*from, ctx.clone(), outer_row, _working_tables, transaction_handle.clone());
                let rows_to_update: Vec<Row> = stream.try_collect().await?;

                for row in &rows_to_update {
                    if let Some(table_part) = row.get(&table_name) {
                        let mut new_val = table_part.clone();
                        for (col, expr) in &set {
                            let val = expr.evaluate_with_context(row, None, ctx.clone(), transaction_handle.clone()).await?;
                            new_val[col] = val;
                        }
                        apply_on_update_actions(&table_name, table_part, &new_val, &ctx, transaction_handle.clone()).await?;
                    }
                }

                let executor = crate::storage_executor::StorageExecutor::new(ctx.clone(), transaction_handle.clone().unwrap());
                let updated_rows = executor.update_rows(&table_name, rows_to_update.clone(), &set).await?;

                if !returning.is_empty() {
                    for returned_row in updated_rows {
                        let wrapped_row = json!({ table_name.clone(): returned_row });
                        let proj_row = project_row(&wrapped_row, &returning, ctx.clone(), outer_row, _working_tables, transaction_handle.clone()).await?;
                        yield proj_row;
                    }
                } else {
                    yield json!({ "rows_affected": updated_rows.len() as u64 });
                }
            }
            PhysicalPlan::AlterTable { table_name, action } => {
                let schema_key = format!("{}{}", SCHEMA_PREFIX, table_name);

                let mut schema = match ctx.schema_cache.get(&table_name) {
                    Some(s) => (**s).clone(),
                    None => Err(anyhow!("Table '{}' does not exist", table_name))?,
                };

                match action {
                    AlterTableAction::AddColumn(col_def) => {
                        if schema.columns.contains_key(&col_def.name) {
                            Err(anyhow!("Column '{}' already exists in table '{}'", col_def.name, table_name))?;
                        }
                        let dt = DataType::from_str(&col_def.data_type)?;
                        let default_expr = if let Some(d) = col_def.default {
                            Some(super::logical_plan::simple_expr_to_expression(d, &ctx.schema_cache, &ctx.view_cache, &ctx.function_registry, None)?)
                        } else {
                            None
                        };

                        let new_col = ColumnDefinition { data_type: dt, nullable: col_def.nullable, default: default_expr };
                        schema.columns.insert(col_def.name.clone(), new_col);
                        schema.column_order.push(col_def.name);
                    }
                    AlterTableAction::DropColumn { column_name } => {
                        if schema.columns.remove(&column_name).is_none() {
                             Err(anyhow!("Column '{}' does not exist in table '{}'", column_name, table_name))?;
                        }
                        schema.column_order.retain(|c| c != &column_name);
                        // Note: This doesn't remove the data from existing rows. A background job or a more complex
                        // operation would be needed for that.
                    }
                    AlterTableAction::AlterColumnSetDefault { column_name, default_expr } => {
                        let col = schema.columns.get_mut(&column_name).ok_or_else(|| anyhow!("Column '{}' does not exist", column_name))?;
                        col.default = Some(super::logical_plan::simple_expr_to_expression(default_expr, &ctx.schema_cache, &ctx.view_cache, &ctx.function_registry, None)?);
                    }
                    AlterTableAction::AlterColumnDropDefault { column_name } => {
                        let col = schema.columns.get_mut(&column_name).ok_or_else(|| anyhow!("Column '{}' does not exist", column_name))?;
                        col.default = None;
                    }
                    AlterTableAction::AlterColumnSetNotNull { column_name } => {
                        // First, check if any existing data violates the new constraint
                        let table_prefix = format!("{}:", table_name);
                        let keys_to_check: Vec<String> = ctx.db.iter()
                            .filter(|e| e.key().starts_with(&table_prefix))
                            .map(|e| e.key().clone())
                            .collect();

                        for key in keys_to_check {
                            if let Some(db_value) = crate::storage_executor::get_visible_db_value(&key, &ctx, None).await {
                                let val: Value = match db_value {
                                    DbValue::Json(v) => v,
                                    DbValue::JsonB(b) => serde_json::from_slice(&b).unwrap_or_default(),
                                    _ => continue,
                                };
                                if let Some(col_val) = val.get(&column_name) {
                                    if col_val.is_null() {
                                        Err(anyhow!("Cannot add NOT NULL constraint on column '{}' because it contains null values.", column_name))?;
                                    }
                                }
                            }
                        }
                        let col = schema.columns.get_mut(&column_name).ok_or_else(|| anyhow!("Column '{}' does not exist", column_name))?;
                        col.nullable = false;
                    }
                    AlterTableAction::AlterColumnDropNotNull { column_name } => {
                        let col = schema.columns.get_mut(&column_name).ok_or_else(|| anyhow!("Column '{}' does not exist", column_name))?;
                        col.nullable = true;
                    }
                    AlterTableAction::RenameTable { new_table_name } => {
                        // 1. Check for conflicts
                        if ctx.schema_cache.contains_key(&new_table_name) {
                            Err(anyhow!("Table or view with name '{}' already exists", new_table_name))?;
                        }

                        // 2. Log the atomic operation
                        let log_entry = LogEntry::RenameTable {
                            old_name: table_name.clone(),
                            new_name: new_table_name.clone(),
                        };
                        log_and_wait_qe!(ctx.logger, log_entry, ctx).await?;

                        // 3. Perform the rename in memory. This is not perfectly atomic in-memory,
                        // but it is consistent upon WAL replay.

                        // 3a. Rename schema
                        let old_schema_key = format!("{}{}", SCHEMA_PREFIX, &table_name);
                        if let Some((_, schema_val)) = ctx.db.remove(&old_schema_key) {
                            let version_chain = schema_val.read().await;
                            if let Some(latest_version) = version_chain.last() {
                                if let DbValue::Bytes(bytes) = &latest_version.value {
                                    if let Ok(mut schema) = serde_json::from_slice::<VirtualSchema>(&bytes) {
                                        schema.table_name = new_table_name.clone();
                                        if let Ok(new_bytes) = serde_json::to_vec(&schema) {
                                            let new_schema_key = format!("{}{}", SCHEMA_PREFIX, &new_table_name);
                                            let new_version = crate::types::VersionedValue {
                                                value: DbValue::Bytes(new_bytes),
                                                creator_txid: 0, // Or a proper txid
                                                expirer_txid: 0,
                                            };
                                            ctx.db.insert(new_schema_key, Arc::new(RwLock::new(vec![new_version])));
                                            ctx.schema_cache.remove(&table_name);
                                            ctx.schema_cache.insert(new_table_name.clone(), Arc::new(schema));
                                        }
                                    }
                                }
                            }
                        }

                        // 3b. Rename data keys
                        let old_prefix = format!("{}:", table_name);
                        let new_prefix = format!("{}:", new_table_name);
                        let keys_to_rename: Vec<_> = ctx.db.iter().filter(|e| e.key().starts_with(&old_prefix)).map(|e| e.key().clone()).collect();

                        for old_key in keys_to_rename {
                            if let Some(entry) = ctx.db.remove(&old_key) {
                                let new_key = old_key.replacen(&old_prefix, &new_prefix, 1);
                                ctx.memory.forget_key(&old_key).await;
                                ctx.db.insert(new_key.clone(), entry.1);
                                ctx.memory.track_access(&new_key).await;
                            }
                        }
                        // TODO: Update all foreign key references in other tables that point to this table.
                    }
                    AlterTableAction::RenameColumn { old_column_name, new_column_name } => {
                        if !schema.columns.contains_key(&old_column_name) {
                            Err(anyhow!("Column '{}' does not exist", old_column_name))?;
                        }
                        if schema.columns.contains_key(&new_column_name) {
                            Err(anyhow!("Column '{}' already exists", new_column_name))?;
                        }

                        // 1. Update schema first
                        if let Some(col_def) = schema.columns.remove(&old_column_name) {
                            schema.columns.insert(new_column_name.clone(), col_def);
                        }
                        if let Some(pos) = schema.column_order.iter().position(|c| c == &old_column_name) {
                            schema.column_order[pos] = new_column_name.clone();
                        }
                        // TODO: Update CHECK constraints and FOREIGN KEY definitions that reference this column.

                        // 2. Rewrite data
                        let table_prefix = format!("{}:", table_name);
                        let keys_to_update: Vec<_> = ctx.db.iter().filter(|e| e.key().starts_with(&table_prefix)).map(|e| e.key().clone()).collect();
                        
                        for key in keys_to_update {
                            let (new_bytes, should_update) = {
                                let entry = match ctx.db.get(&key) { Some(e) => e, None => continue };
                                let version_chain_arc = entry.value().clone();
                                drop(entry);
                                let version_chain = version_chain_arc.read().await;
                                let mut new_bytes_res = (vec![], false);

                                if let Some(version) = version_chain.iter().rev().find(|v| {
                                    let status = ctx.tx_status_manager.get_status(v.creator_txid);
                                    status == Some(crate::types::TransactionStatus::Committed) && (v.expirer_txid == 0 || ctx.tx_status_manager.get_status(v.expirer_txid) != Some(crate::types::TransactionStatus::Committed))
                                }) {
                                    let mut val: serde_json::Value = match &version.value {
                                        DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_default(),
                                        _ => continue,
                                    };
                                    if let Some(obj) = val.as_object_mut() {
                                        if let Some(v) = obj.remove(&old_column_name) {
                                            obj.insert(new_column_name.clone(), v);
                                            new_bytes_res = (serde_json::to_vec(&val)?, true);
                                        }
                                    }
                                }
                                new_bytes_res
                            };

                            if !should_update { continue; }

                            log_and_wait_qe!(ctx.logger, LogEntry::SetJsonB { key: key.clone(), value: new_bytes.clone() }, ctx).await?;

                            let txid = ctx.tx_id_manager.new_txid();
                            ctx.tx_status_manager.begin(txid);

                            if let Some(entry) = ctx.db.get(&key) {
                                let version_chain_arc = entry.value().clone();
                                drop(entry);
                                let mut version_chain = version_chain_arc.write().await;
                                if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| v.expirer_txid == 0) {
                                    latest_version.expirer_txid = txid;
                                }
                                let new_version = crate::types::VersionedValue {
                                    value: DbValue::JsonB(new_bytes),
                                    creator_txid: txid,
                                    expirer_txid: 0,
                                };
                                version_chain.push(new_version);
                            }
                            ctx.tx_status_manager.commit(txid);
                        }
                    }
                    AlterTableAction::AddConstraint(constraint) => {
                        // Implement validation to ensure existing data meets the constraint.
                        // For now, just add the constraint. Validation should happen at a higher level.
                        if let TableConstraint::Unique { name: constraint_name_option, columns } = &constraint {
                            if columns.len() != 1 {
                                Err(anyhow!("Composite UNIQUE constraints are not yet supported for direct column flags. Use CREATE UNIQUE INDEX instead."))?;
                            }
                            let col_name = &columns[0];
                            // Check if column exists in schema
                            if !schema.columns.contains_key(col_name) {
                                Err(anyhow!("Column '{}' in UNIQUE constraint not found in table definition", col_name))?;
                            }
                            let index_name = constraint_name_option.clone().unwrap_or_else(|| format!("unique_{}_{}", table_name, col_name));
                            let key_prefix = format!("{}:*", table_name);
                            let json_path = col_name.clone();
                            let create_index_command = Command {
                                name: "IDX.CREATE".to_string(),
                                args: vec![
                                    b"IDX.CREATE".to_vec(),
                                    index_name.into_bytes(),
                                    key_prefix.into_bytes(),
                                    json_path.into_bytes(),
                                ],
                            };
                            if let Response::Error(e) = super::super::commands::handle_idx_create(create_index_command, ctx.clone()).await {
                                Err(anyhow!("Failed to create unique index: {}", e))?;
                            }
                        }
                        schema.constraints.push(constraint);
                    }
                    AlterTableAction::DropConstraint { constraint_name } => {
                        let mut constraint_to_remove = None;
                        let mut found = false;

                        if let Some(index) = schema.constraints.iter().position(|c| {
                            let name = match c {
                                TableConstraint::Unique { name, .. } => name.as_deref(),
                                TableConstraint::PrimaryKey { name, .. } => name.as_deref(),
                                TableConstraint::ForeignKey(fk) => fk.name.as_deref(),
                                TableConstraint::Check { name, .. } => name.as_deref(),
                            };
                            name == Some(constraint_name.as_str())
                        }) {
                            found = true;
                            constraint_to_remove = Some(schema.constraints.remove(index));
                        }

                        if !found {
                            Err(anyhow!("Constraint '{}' does not exist on table '{}'", constraint_name, table_name))?
                        }

                        if let Some(constraint) = constraint_to_remove {
                             match constraint {
                                TableConstraint::Unique { name, columns } => {
                                    if columns.len() == 1 { // Only single-column unique constraints are handled here
                                        let col_name = &columns[0];
                                        let index_name = name.unwrap_or_else(|| format!("unique_{}_{}", table_name, col_name));
                                        let drop_index_command = Command {
                                            name: "IDX.DROP".to_string(),
                                            args: vec![
                                                b"IDX.DROP".to_vec(),
                                                index_name.into_bytes(),
                                            ],
                                        };
                                        if let Response::Error(e) = super::super::commands::handle_idx_drop(drop_index_command, ctx.clone()).await {
                                            eprintln!("Warning: Could not drop index for unique constraint '{}': {}", constraint_name, e);
                                        }
                                    }
                                }
                                TableConstraint::PrimaryKey { name, .. } => {
                                    let index_name = name.unwrap_or_else(|| format!("pk_{}", table_name));
                                     let drop_index_command = Command {
                                        name: "IDX.DROP".to_string(),
                                        args: vec![
                                            b"IDX.DROP".to_vec(),
                                            index_name.into_bytes(),
                                        ],
                                    };
                                    if let Response::Error(e) = super::super::commands::handle_idx_drop(drop_index_command, ctx.clone()).await {
                                        eprintln!("Warning: Could not drop index for primary key constraint '{}': {}", constraint_name, e);
                                    }
                                }
                                _ => {} // No side effects for CHECK or FOREIGN KEY for now
                            }
                        }
                    }
                    AlterTableAction::AlterColumnType { column_name, new_data_type } => {
                        let new_dt = DataType::from_str(&new_data_type)?;
                        // 1. Rewrite data by casting
                        let table_prefix = format!("{}:", table_name);
                        let keys_to_update: Vec<_> = ctx.db.iter().filter(|e| e.key().starts_with(&table_prefix)).map(|e| e.key().clone()).collect();
                        for key in keys_to_update {
                            let (new_bytes, should_update) = {
                                let entry = match ctx.db.get(&key) { Some(e) => e, None => continue };
                                let version_chain_arc = entry.value().clone();
                                drop(entry);
                                let version_chain = version_chain_arc.read().await;
                                let mut new_bytes_res = (vec![], false);

                                if let Some(version) = version_chain.iter().rev().find(|v| {
                                    let status = ctx.tx_status_manager.get_status(v.creator_txid);
                                    status == Some(crate::types::TransactionStatus::Committed) && (v.expirer_txid == 0 || ctx.tx_status_manager.get_status(v.expirer_txid) != Some(crate::types::TransactionStatus::Committed))
                                }) {
                                    let mut val: serde_json::Value = match &version.value {
                                        DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_default(),
                                        _ => continue,
                                    };
                                    if let Some(obj) = val.as_object_mut() {
                                        if let Some(old_v) = obj.get_mut(&column_name) {
                                            let casted_v = cast_value_to_type(old_v.clone(), &new_dt)?;
                                            *old_v = casted_v;
                                            new_bytes_res = (serde_json::to_vec(&val)?, true);
                                        }
                                    }
                                }
                                new_bytes_res
                            };

                            if !should_update { continue; }

                            log_and_wait_qe!(ctx.logger, LogEntry::SetJsonB { key: key.clone(), value: new_bytes.clone() }, ctx).await?;

                            let txid = ctx.tx_id_manager.new_txid();
                            ctx.tx_status_manager.begin(txid);

                            if let Some(entry) = ctx.db.get(&key) {
                                let version_chain_arc = entry.value().clone();
                                drop(entry);
                                let mut version_chain = version_chain_arc.write().await;
                                if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| v.expirer_txid == 0) {
                                    latest_version.expirer_txid = txid;
                                }
                                let new_version = crate::types::VersionedValue {
                                    value: DbValue::JsonB(new_bytes),
                                    creator_txid: txid,
                                    expirer_txid: 0,
                                };
                                version_chain.push(new_version);
                            }
                            ctx.tx_status_manager.commit(txid);
                        }
                        // 2. Update schema
                        let col = schema.columns.get_mut(&column_name).ok_or_else(|| anyhow!("Column '{}' does not exist", column_name))?;
                        col.data_type = new_dt;
                    }
                }

                let schema_bytes = serde_json::to_vec(&schema)?;
                let log_entry = LogEntry::SetBytes { key: schema_key.clone(), value: schema_bytes.clone() };
                log_and_wait_qe!(ctx.logger, log_entry, ctx).await?;

                let txid = ctx.tx_id_manager.new_txid();
                ctx.tx_status_manager.begin(txid);

                let version_chain_arc = ctx.db.entry(schema_key.clone()).or_default().clone();
                let mut version_chain = version_chain_arc.write().await;

                if let Some(latest_version) = version_chain.iter_mut().rev().find(|v| {
                    v.expirer_txid == 0
                        && ctx.tx_status_manager.get_status(v.creator_txid)
                            == Some(crate::types::TransactionStatus::Committed)
                }) {
                    latest_version.expirer_txid = txid;
                }

                let new_version = crate::types::VersionedValue {
                    value: DbValue::Bytes(schema_bytes),
                    creator_txid: txid,
                    expirer_txid: 0,
                };
                version_chain.push(new_version);

                ctx.tx_status_manager.commit(txid);

                ctx.schema_cache.insert(table_name, Arc::new(schema));

                yield json!({"status": "Table altered"});
            }
            PhysicalPlan::CreateView { view_name, query } => {
                if ctx.view_cache.contains_key(&view_name) || ctx.schema_cache.contains_key(&view_name) {
                    Err(anyhow!("View or table with name '{}' already exists", view_name))?;
                }

                let view_def = ViewDefinition {
                    name: view_name.clone(),
                    columns: Vec::new(),
                    query,
                };

                let view_key = format!("{}{}", VIEW_PREFIX, view_name);
                let view_bytes = serde_json::to_vec(&view_def)?;

                let log_entry = LogEntry::SetBytes { key: view_key.clone(), value: view_bytes.clone() };
                log_and_wait_qe!(ctx.logger, log_entry, ctx).await?;

                let version = crate::types::VersionedValue { value: DbValue::Bytes(view_bytes), creator_txid: 0, expirer_txid: 0 };
                ctx.db.insert(view_key, Arc::new(RwLock::new(vec![version])));
                ctx.view_cache.insert(view_name, Arc::new(view_def));

                yield json!({"status": "View created"});
            }
            PhysicalPlan::CreateSchema { schema_name } => {
                let schema_list_key = format!("{}{}", SCHEMALIST_PREFIX, schema_name);
                if ctx.db.contains_key(&schema_list_key) {
                    Err(anyhow!("Schema '{}' already exists", schema_name))?;
                }

                let log_entry = LogEntry::SetBytes { key: schema_list_key.clone(), value: vec![] };
                log_and_wait_qe!(ctx.logger, log_entry, ctx).await?;

                let version = crate::types::VersionedValue { value: DbValue::Bytes(vec![]), creator_txid: 0, expirer_txid: 0 };
                ctx.db.insert(schema_list_key, Arc::new(RwLock::new(vec![version])));

                yield json!({"status": "Schema created"});
            }
            PhysicalPlan::Values { values } => {
                for row_values in values {
                    let mut row = json!({});
                    for (i, value_expr) in row_values.iter().enumerate() {
                        let value = value_expr.evaluate_with_context(&json!({}), None, ctx.clone(), transaction_handle.clone()).await?;
                        // Use a generic column name like "column_0", "column_1", etc.
                        row[format!("column_{}", i)] = value;
                    }
                    yield row;
                }
            }
            PhysicalPlan::DistinctOn { input, expressions } => {
                let mut seen_keys = HashSet::new();
                let mut stream = execute(*input, ctx.clone(), outer_row, _working_tables, transaction_handle.clone());
                while let Some(row_result) = stream.next().await {
                    let row = row_result?;
                    let mut key_parts = Vec::new();
                    for expr in &expressions {
                        key_parts.push(expr.evaluate_with_context(&row, outer_row, ctx.clone(), transaction_handle.clone()).await?);
                    }
                    let key = serde_json::to_string(&key_parts)?;
                    if seen_keys.insert(key) {
                        yield row;
                    }
                }
            }
            PhysicalPlan::UnionAll { left, right } => {
                let mut left_stream = execute(*left, ctx.clone(), outer_row, _working_tables, transaction_handle.clone());
                while let Some(row) = left_stream.next().await {
                    yield row?;
                }
                let mut right_stream = execute(*right, ctx.clone(), outer_row, _working_tables, transaction_handle.clone());
                while let Some(row) = right_stream.next().await {
                    yield row?;
                }
            }
            PhysicalPlan::Intersect { left, right } => {
                let left_rows: std::collections::HashSet<String> = execute(*left, ctx.clone(), outer_row, _working_tables, transaction_handle.clone())
                    .map_ok(|row| row.to_string())
                    .try_collect()
                    .await?;
                let right_rows: std::collections::HashSet<String> = execute(*right, ctx.clone(), outer_row, _working_tables, transaction_handle.clone())
                    .map_ok(|row| row.to_string())
                    .try_collect()
                    .await?;

                for row_str in left_rows.intersection(&right_rows) {
                    yield serde_json::from_str(row_str)?;
                }
            }
            PhysicalPlan::Except { left, right } => {
                let left_rows: std::collections::HashSet<String> = execute(*left, ctx.clone(), outer_row, _working_tables, transaction_handle.clone())
                    .map_ok(|row| row.to_string())
                    .try_collect()
                    .await?;
                let right_rows: std::collections::HashSet<String> = execute(*right, ctx.clone(), outer_row, _working_tables, transaction_handle.clone())
                    .map_ok(|row| row.to_string())
                    .try_collect()
                    .await?;

                for row_str in left_rows.difference(&right_rows) {
                    yield serde_json::from_str(row_str)?;
                }
            }
        PhysicalPlan::CreateIndex { statement } => {
                let index_metadata_key = format!("_internal:indexes:{}", statement.index_name);
                if ctx.db.contains_key(&index_metadata_key) {
                    Err(anyhow!("Index '{}' already exists", statement.index_name))?;
                }

                // Store index metadata
                let index_metadata_bytes = serde_json::to_vec(&statement)?;
                let log_entry = LogEntry::SetBytes { key: index_metadata_key.clone(), value: index_metadata_bytes.clone() };
                log_and_wait_qe!(ctx.logger, log_entry, ctx).await?;
                let version = crate::types::VersionedValue { value: DbValue::Bytes(index_metadata_bytes), creator_txid: 0, expirer_txid: 0 };
                ctx.db.insert(index_metadata_key, Arc::new(RwLock::new(vec![version])));

                // Create the actual index data structure in IndexManager
                let new_index = Arc::new(crate::indexing::Index::default());
                ctx.index_manager.indexes.insert(statement.index_name.clone(), new_index.clone());

                // Backfill the index
                let table_prefix = format!("{}:", statement.table_name);
                let mut backfilled_count = 0;

                for entry in ctx.db.iter() {
                    if entry.key().starts_with(&table_prefix) {
                        let version_chain_arc = entry.value().clone();
                        let key = entry.key().clone();
                        drop(entry);
                        let version_chain = version_chain_arc.read().await;
                        if let Some(latest_version) = version_chain.last() {
                             if ctx.tx_status_manager.get_status(latest_version.creator_txid) != Some(crate::types::TransactionStatus::Committed) {
                                continue;
                             }

                            let row_key = key.clone();
                            let val: Value = match &latest_version.value {
                                DbValue::Json(v) => v.clone(),
                                DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_default(),
                                _ => continue,
                            };

                            let mut index_values = Vec::new();
                            for simple_expr in &statement.columns {
                                let expr = super::logical_plan::simple_expr_to_expression(
                                    simple_expr.clone(), // Clone because simple_expr_to_expression consumes it
                                    &ctx.schema_cache,
                                    &ctx.view_cache,
                                    &ctx.function_registry,
                                    None,
                                )?;
                                let evaluated_val = expr.evaluate_with_context(&val, None, ctx.clone(), transaction_handle.clone()).await?;
                                index_values.push(evaluated_val);
                            }

                            let index_value_str = serde_json::to_string(&index_values)?;

                            let mut index_data = new_index.write().await;
                            if statement.unique && index_data.contains_key(&index_value_str) {
                                // If unique index and key already exists, error out
                                // This is a simplified check, a more robust one would check if the existing key is for the same row
                                Err(anyhow!("Unique index violation: duplicate key for index '{}'", statement.index_name))?;
                            }
                            index_data.entry(index_value_str).or_default().insert(row_key);
                            backfilled_count += 1;
                        }
                    }
                }

                yield json!({ "status": format!("Index '{}' created and backfilled {} items.", statement.index_name, backfilled_count) });
            },
            PhysicalPlan::SubqueryScan { alias, input } => {
                let mut stream = execute(*input, ctx.clone(), outer_row, _working_tables, transaction_handle.clone());
                while let Some(row_result) = stream.next().await {
                    let row = row_result?;
                    let mut new_row = json!({});

                    // If the input row is a single-key object (like from a table scan),
                    // unwrap it and re-wrap it with the alias.
                    if let Some(obj) = row.as_object() {
                        if obj.keys().len() == 1 {
                            if let Some(val) = obj.values().next() {
                                new_row[alias.clone()] = val.clone();
                                yield new_row;
                                continue;
                            }
                        }
                    }
                    
                    // Otherwise, for complex subqueries (e.g. from a projection), wrap the whole row.
                    new_row[alias.clone()] = row;
                    yield new_row;
                }
            }
            PhysicalPlan::WorkingTableScan { cte_name, alias } => {
                if let Some(tables) = _working_tables {
                    if let Some(rows) = tables.get(&cte_name) {
                        for row in rows.clone() {
                            let mut new_row = json!({});
                            new_row[alias.clone()] = row;
                            yield new_row;
                        }
                    }
                }
            }
            PhysicalPlan::RecursiveCteScan { alias, column_aliases, non_recursive, recursive, union_all } => {
                let get_projection_columns = |plan: &PhysicalPlan| -> Option<Vec<String>> {
                    let mut current_plan = plan;
                    // The projection might be under a sort or limit
                    loop {
                        match current_plan {
                            PhysicalPlan::Projection { expressions, .. } => {
                                return Some(expressions.iter().map(|(expr, alias)| {
                                    alias.clone().unwrap_or_else(|| expr.to_string())
                                }).collect());
                            }
                            PhysicalPlan::Sort { input, .. } => current_plan = input,
                            PhysicalPlan::Limit { input, .. } => current_plan = input,
                            _ => return None,
                        }
                    }
                };

                let non_recursive_cols = get_projection_columns(&*non_recursive);
                let recursive_cols = get_projection_columns(&*recursive);

                let rename_row_columns = |row: Row, aliases: &[String], original_cols: &Option<Vec<String>>| -> Result<Row> {
                    let obj = row.as_object().ok_or_else(|| anyhow!("Recursive CTE row is not an object"))?;
                    
                    if aliases.is_empty() {
                        return Ok(Value::Object(obj.clone()));
                    }

                    if let Some(original_cols) = original_cols {
                        if !aliases.is_empty() && original_cols.len() != aliases.len() {
                             return Err(anyhow!("CTE '{}' has {} column aliases but its query returned {} columns", alias, aliases.len(), original_cols.len()));
                        }

                        let mut new_row_map = serde_json::Map::new();
                        for (i, alias) in aliases.iter().enumerate() {
                            let original_col_name = &original_cols[i];
                            let val = obj.get(original_col_name).cloned().unwrap_or(Value::Null);
                            new_row_map.insert(alias.clone(), val);
                        }
                        Ok(Value::Object(new_row_map))
                    } else {
                        // Fallback to the old, order-dependent logic if we couldn't get columns
                        let values: Vec<_> = obj.values().cloned().collect();
                        if !aliases.is_empty() && values.len() != aliases.len() {
                            return Err(anyhow!("CTE '{}' has {} column aliases but its query returned {} columns", alias, aliases.len(), values.len()));
                        }
                        let mut new_row_map = serde_json::Map::new();
                        for (i, val) in values.into_iter().enumerate() {
                            let key = aliases.get(i).cloned().unwrap_or_else(|| format!("column_{}", i));
                            new_row_map.insert(key, val);
                        }
                        Ok(Value::Object(new_row_map))
                    }
                };

                let non_recursive_rows_unaliased: Vec<Row> = execute(*non_recursive, ctx.clone(), outer_row, _working_tables, transaction_handle.clone()).try_collect().await?;
                let mut non_recursive_rows = Vec::new();
                for row in non_recursive_rows_unaliased {
                    non_recursive_rows.push(rename_row_columns(row, &column_aliases, &non_recursive_cols)?);
                }

                let mut all_rows = non_recursive_rows.clone();
                let mut working_set = non_recursive_rows;
                let mut new_rows_hashes = std::collections::HashSet::new();

                if !union_all {
                    for row in &working_set {
                        new_rows_hashes.insert(row.to_string());
                    }
                }

                loop {
                    if working_set.is_empty() {
                        break;
                    }

                    let mut current_working_tables = HashMap::new();
                    current_working_tables.insert(alias.clone(), working_set);

                    let recursive_stream = execute(*recursive.clone(), ctx.clone(), outer_row, Some(&current_working_tables), transaction_handle.clone());
                    let next_working_set_unaliased: Vec<Row> = recursive_stream.try_collect().await?;

                    let mut next_working_set = Vec::new();
                    for row in next_working_set_unaliased {
                        next_working_set.push(rename_row_columns(row, &column_aliases, &recursive_cols)?);
                    }

                    working_set = Vec::new();
                    if next_working_set.is_empty() {
                        break;
                    }

                    if union_all {
                        working_set.extend(next_working_set.clone());
                        all_rows.extend(next_working_set);
                    } else {
                        let mut found_new = false;
                        for row in next_working_set {
                            if new_rows_hashes.insert(row.to_string()) {
                                working_set.push(row.clone());
                                all_rows.push(row);
                                found_new = true;
                            }
                        }
                        if !found_new {
                            break;
                        }
                    }
                }

                for row in all_rows {
                    yield row;
                }
            }
            PhysicalPlan::BeginTransaction => {
                if let Some(handle) = transaction_handle {
                    let mut tx_guard = handle.write().await;
                    if tx_guard.is_some() {
                        Err(anyhow!("Transaction already in progress"))?;
                    }
                    let new_tx = Arc::new(Transaction::new(&ctx.tx_id_manager, &ctx.tx_status_manager));
                    println!("Transaction {} ({}) started.", new_tx.id, new_tx.txid);
                    ctx.active_transactions.insert(new_tx.txid, new_tx.clone());
                    *tx_guard = Some(new_tx);
                    yield json!({"status": "Transaction started"});
                } else {
                    Err(anyhow!("Transaction handle not provided for BEGIN"))?;
                }
            }
                    PhysicalPlan::CommitTransaction => {
                        if let Some(handle) = transaction_handle {
                            let tx = {
                                let mut tx_guard = handle.write().await;
                                match tx_guard.take() {
                                    Some(t) => t,
                                    None => Err(anyhow!("No transaction in progress"))?,
                                }
                            };

                            ctx.active_transactions.remove(&tx.txid);

                            if ctx.config.isolation_level == crate::config::IsolationLevel::Serializable {
                                if tx.ssi_in_conflict.load(Ordering::Relaxed) {
                                    ctx.tx_status_manager.abort(tx.txid);
                                    println!("Transaction {} ({}) aborted due to serialization conflict.", tx.id, tx.txid);
                                    Err(anyhow!("ABORT: Serialization failure, please retry transaction"))?;
                                }
                            }

                            if ctx.config.persistence {
                                let log_entries = tx.log_entries.read().await;
                                if !log_entries.is_empty() {
                                    for (i, entry) in log_entries.iter().enumerate() {
                                        let is_last = i == log_entries.len() - 1;
                                        let durability = if is_last { ctx.config.durability.clone() } else { crate::config::DurabilityLevel::None };
                                        let (ack_tx, ack_rx) = oneshot::channel();
                                        if ctx.logger.send(PersistenceRequest::Log(LogRequest { entry: entry.clone(), ack: ack_tx, durability })).await.is_err() {
                                            ctx.tx_status_manager.abort(tx.txid);
                                            Err(anyhow!("Persistence engine is down, commit failed. Transaction rolled back."))?;
                                        }
                                        if is_last {
                                            match ack_rx.await {
                                                Ok(Ok(())) => {}
                                                Ok(Err(e)) => {
                                                    ctx.tx_status_manager.abort(tx.txid);
                                                    Err(anyhow!("WAL write error during commit: {}. Transaction rolled back.", e))?;
                                                }
                                                Err(_) => {
                                                    ctx.tx_status_manager.abort(tx.txid);
                                                    Err(anyhow!("Persistence engine dropped ACK channel during commit. Transaction rolled back."))?;
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if ctx.config.isolation_level == crate::config::IsolationLevel::Serializable {
                                for write_item in tx.writes.iter() {
                                    let written_key = write_item.key();
                                    for other_tx_entry in ctx.active_transactions.iter() {
                                        let other_tx = other_tx_entry.value();
                                        if tx.snapshot.xip.contains(&other_tx.txid) && other_tx.reads.contains_key(written_key) {
                                            other_tx.ssi_in_conflict.store(true, Ordering::Relaxed);
                                        }
                                    }
                                }
                            }

                            for write_item in tx.writes.iter() {
                                let key = write_item.key();
                                let new_value_opt = write_item.value();
                                let version_chain_arc = ctx.db.entry(key.clone()).or_default().clone();
                                let mut version_chain = version_chain_arc.write().await;
                                for version in version_chain.iter_mut().rev() {
                                    if tx.snapshot.is_visible(version, &ctx.tx_status_manager) {
                                        if version.expirer_txid == 0 {
                                            version.expirer_txid = tx.txid;
                                        }
                                        break;
                                    }
                                }
                                if let Some(new_db_value) = new_value_opt {
                                    let new_version = crate::types::VersionedValue {
                                        value: new_db_value.clone(),
                                        creator_txid: tx.txid,
                                        expirer_txid: 0,
                                    };
                                    version_chain.push(new_version);
                                }
                            }

                            ctx.tx_status_manager.commit(tx.txid);
                            println!("Transaction {} ({}) committed.", tx.id, tx.txid);
                            yield json!({ "status": "Transaction committed" });
                        } else {
                            Err(anyhow!("Transaction handle not provided for COMMIT"))?;
                        }
                    }            PhysicalPlan::RollbackTransaction => {
                if let Some(handle) = transaction_handle {
                    let mut tx_guard = handle.write().await;
                    if let Some(tx) = tx_guard.take() {
                        ctx.tx_status_manager.abort(tx.txid);
                        ctx.active_transactions.remove(&tx.txid);
                        println!("Transaction {} ({}) rolled back.", tx.id, tx.txid);
                        yield json!({"status": "Transaction rolled back"});
                    } else {
                        Err(anyhow!("No transaction in progress"))?;
                    }
                } else {
                    Err(anyhow!("Transaction handle not provided for ROLLBACK"))?;
                }
            }
        }
    })
}

async fn run_agg_fn<'a>(
    func: &str,
    arg: &Expression,
    rows: &'a [Row],
    outer_row: Option<&'a Row>,
    ctx: Arc<AppContext>,
    transaction_handle: Option<TransactionHandle>,
) -> Result<Value> {
    let result = match func.to_uppercase().as_str() {
        "COUNT" => json!(rows.len()),
        "SUM" => {
            let mut sum = 0.0;
            for row in rows {
                if let Some(n) = arg.evaluate_with_context(row, outer_row, ctx.clone(), transaction_handle.clone()).await?.as_f64() {
                    sum += n;
                }
            }
            json!(sum)
        }
        "AVG" => {
            let mut sum = 0.0;
            let mut count = 0;
            for row in rows {
                if let Some(n) = arg.evaluate_with_context(row, outer_row, ctx.clone(), transaction_handle.clone()).await?.as_f64() {
                    sum += n;
                    count += 1;
                }
            }
            if count > 0 { json!(sum / count as f64) } else { Value::Null }
        }
        "MIN" => {
            let mut min = Value::Null;
            for row in rows {
                let val = arg.evaluate_with_context(row, outer_row, ctx.clone(), transaction_handle.clone()).await?;
                if min.is_null() || (!val.is_null() && val.as_f64() < min.as_f64()) {
                    min = val;
                }
            }
            min
        }
        "MAX" => {
            let mut max = Value::Null;
            for row in rows {
                let val = arg.evaluate_with_context(row, outer_row, ctx.clone(), transaction_handle.clone()).await?;
                if max.is_null() || (!val.is_null() && val.as_f64() > max.as_f64()) {
                    max = val;
                }
            }
            max
        }
        _ => Value::Null,
    };
    Ok(result)
}





async fn apply_on_delete_actions(
    parent_table_name: &str,
    parent_row_data: &Value, // The inner object of the parent row
    ctx: &Arc<AppContext>,
    transaction_handle: Option<TransactionHandle>,
) -> Result<()> {
    for schema_entry in ctx.schema_cache.iter() {
        let child_schema = schema_entry.value();
        for constraint in &child_schema.constraints {
            if let TableConstraint::ForeignKey(fk) = constraint {
                if fk.references_table == parent_table_name {
                    let parent_key_values: Vec<Value> = fk
                        .references_columns
                        .iter()
                        .map(|col_name| parent_row_data.get(col_name).cloned().unwrap_or(Value::Null))
                        .collect();

                    if parent_key_values.iter().any(|v| v.is_null()) {
                        continue; // Cannot match on NULL
                    }

                    let mut filter_expr: Option<Expression> = None;
                    for (i, col_name) in fk.columns.iter().enumerate() {
                        let condition = Expression::BinaryOp {
                            left: Box::new(Expression::Column(col_name.clone())),
                            op: Operator::Eq,
                            right: Box::new(Expression::Literal(parent_key_values[i].clone())),
                        };
                        if let Some(existing_expr) = filter_expr {
                            filter_expr = Some(Expression::LogicalOp {
                                left: Box::new(existing_expr),
                                op: LogicalOperator::And,
                                right: Box::new(condition),
                            });
                        } else {
                            filter_expr = Some(condition);
                        }
                    }

                    if let Some(predicate) = filter_expr {
                        let scan_plan = LogicalPlan::TableScan {
                            table_name: child_schema.table_name.clone(),
                        };
                        let filter_plan = LogicalPlan::Filter {
                            input: Box::new(scan_plan),
                            predicate,
                        };
                        let physical_plan =
                            super::physical_plan::logical_to_physical_plan(filter_plan, &ctx.index_manager)?;

                        let results: Vec<Value> = execute(physical_plan, ctx.clone(), None, None, transaction_handle.clone()).try_collect().await?;

                        if !results.is_empty() {
                            let on_delete_action = fk.on_delete.as_deref().unwrap_or("NO ACTION").to_uppercase();
                            match on_delete_action.as_str() {
                                "CASCADE" => {
                                    let executor = crate::storage_executor::StorageExecutor::new(ctx.clone(), transaction_handle.clone().unwrap());
                                    executor.delete_rows(&child_schema.table_name, results).await?;
                                }
                                "SET NULL" => {
                                    let mut set_clauses = Vec::new();
                                    for col_name in &fk.columns {
                                        set_clauses.push((col_name.clone(), Expression::Literal(Value::Null)));
                                    }
                                    let executor = crate::storage_executor::StorageExecutor::new(ctx.clone(), transaction_handle.clone().unwrap());
                                    executor.update_rows(&child_schema.table_name, results, &set_clauses).await?;
                                }
                                "SET DEFAULT" => {
                                    let mut set_clauses = Vec::new();
                                    for col_name in &fk.columns {
                                        let col_def = child_schema.columns.get(col_name)
                                            .ok_or_else(|| anyhow!("Column '{}' not found in child table '{}' for SET DEFAULT", col_name, child_schema.table_name))?;

                                        if let Some(default_expr) = &col_def.default {
                                            set_clauses.push((col_name.clone(), default_expr.clone()));
                                        } else {
                                            return Err(anyhow!("Cannot SET DEFAULT because column '{}' in table '{}' has no default value", col_name, child_schema.table_name));
                                        }
                                    }
                                    let executor = crate::storage_executor::StorageExecutor::new(ctx.clone(), transaction_handle.clone().unwrap());
                                    executor.update_rows(&child_schema.table_name, results, &set_clauses).await?;
                                }
                                _ => { // RESTRICT or NO ACTION
                                    return Err(anyhow!(
                                        "Delete on table '{}' violates foreign key constraint on table '{}'",
                                        parent_table_name,
                                        child_schema.table_name,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Checks for rows in other tables that reference a given parent row, and applies ON UPDATE actions.
async fn apply_on_update_actions(
    parent_table_name: &str,
    old_parent_row_data: &Value, // The old inner object of the parent row
    new_parent_row_data: &Value, // The new inner object of the parent row
    ctx: &Arc<AppContext>,
    transaction_handle: Option<TransactionHandle>,
) -> Result<()> {
    for schema_entry in ctx.schema_cache.iter() {
        let child_schema = schema_entry.value();
        for constraint in &child_schema.constraints {
            if let TableConstraint::ForeignKey(fk) = constraint {
                if fk.references_table == parent_table_name {
                    let old_parent_key_values: Vec<Value> = fk
                        .references_columns
                        .iter()
                        .map(|col_name| old_parent_row_data.get(col_name).cloned().unwrap_or(Value::Null))
                        .collect();

                    let new_parent_key_values: Vec<Value> = fk
                        .references_columns
                        .iter()
                        .map(|col_name| new_parent_row_data.get(col_name).cloned().unwrap_or(Value::Null))
                        .collect();

                    // If the key didn't change, no action is needed.
                    if old_parent_key_values == new_parent_key_values {
                        continue;
                    }

                    if old_parent_key_values.iter().any(|v| v.is_null()) {
                        continue; // Cannot match on NULL
                    }

                    let mut filter_expr: Option<Expression> = None;
                    for (i, col_name) in fk.columns.iter().enumerate() {
                        let condition = Expression::BinaryOp {
                            left: Box::new(Expression::Column(col_name.clone())),
                            op: Operator::Eq,
                            right: Box::new(Expression::Literal(old_parent_key_values[i].clone())),
                        };
                        if let Some(existing_expr) = filter_expr {
                            filter_expr = Some(Expression::LogicalOp {
                                left: Box::new(existing_expr),
                                op: LogicalOperator::And,
                                right: Box::new(condition),
                            });
                        } else {
                            filter_expr = Some(condition);
                        }
                    }

                    if let Some(predicate) = filter_expr {
                        let scan_plan = LogicalPlan::TableScan {
                            table_name: child_schema.table_name.clone(),
                        };
                        let filter_plan = LogicalPlan::Filter {
                            input: Box::new(scan_plan),
                            predicate,
                        };
                        let physical_plan =
                            super::physical_plan::logical_to_physical_plan(filter_plan, &ctx.index_manager)?;

                        let results: Vec<Value> = execute(physical_plan, ctx.clone(), None, None, transaction_handle.clone()).try_collect().await?;

                        if !results.is_empty() {
                            let on_update_action = fk.on_update.as_deref().unwrap_or("NO ACTION").to_uppercase();
                            match on_update_action.as_str() {
                                "CASCADE" => {
                                    let mut set_clauses = Vec::new();
                                    for (i, col_name) in fk.columns.iter().enumerate() {
                                        set_clauses.push((col_name.clone(), Expression::Literal(new_parent_key_values[i].clone())));
                                    }
                                    let executor = crate::storage_executor::StorageExecutor::new(ctx.clone(), transaction_handle.clone().unwrap());
                                    executor.update_rows(&child_schema.table_name, results, &set_clauses).await?;
                                }
                                "SET NULL" => {
                                    let mut set_clauses = Vec::new();
                                    for col_name in &fk.columns {
                                        set_clauses.push((col_name.clone(), Expression::Literal(Value::Null)));
                                    }
                                    let executor = crate::storage_executor::StorageExecutor::new(ctx.clone(), transaction_handle.clone().unwrap());
                                    executor.update_rows(&child_schema.table_name, results, &set_clauses).await?;
                                }
                                "SET DEFAULT" => {
                                    let mut set_clauses = Vec::new();
                                    for col_name in &fk.columns {
                                        let col_def = child_schema.columns.get(col_name)
                                            .ok_or_else(|| anyhow!("Column '{}' not found in child table '{}' for SET DEFAULT", col_name, child_schema.table_name))?;

                                        if let Some(default_expr) = &col_def.default {
                                            set_clauses.push((col_name.clone(), default_expr.clone()));
                                        } else {
                                            return Err(anyhow!("Cannot SET DEFAULT because column '{}' in table '{}' has no default value", col_name, child_schema.table_name));
                                        }
                                    }
                                    let executor = crate::storage_executor::StorageExecutor::new(ctx.clone(), transaction_handle.clone().unwrap());
                                    executor.update_rows(&child_schema.table_name, results, &set_clauses).await?;
                                }
                                _ => { // RESTRICT or NO ACTION
                                    return Err(anyhow!(
                                        "Update on table '{}' violates foreign key constraint on table '{}'",
                                        parent_table_name,
                                        child_schema.table_name,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}