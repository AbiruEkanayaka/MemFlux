use anyhow::{anyhow, Result};
use async_stream::try_stream;
use futures::stream::{Stream, StreamExt, TryStreamExt};
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::oneshot;
use uuid::Uuid;

use super::ast::{AlterTableAction, TableConstraint};
use super::logical_plan::{cast_value_to_type, Expression, JoinType, LogicalOperator, LogicalPlan, OnConflictAction, Operator};
use super::physical_plan::PhysicalPlan;
use crate::schema::{ColumnDefinition, DataType, VirtualSchema, SCHEMA_PREFIX, VIEW_PREFIX};
use crate::types::{AppContext, Command, DbValue, LogEntry, LogRequest, Response, SchemaCache, ViewDefinition};

pub const SCHEMALIST_PREFIX: &str = "_internal:schemalist:";

type Row = Value;

macro_rules! log_and_wait_qe {
    ($logger:expr, $entry:expr) => {
        async {
            let (ack_tx, ack_rx) = oneshot::channel();
            if $logger
                .send(LogRequest {
                    entry: $entry,
                    ack: ack_tx,
                })
                .await
                .is_err()
            {
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
) -> Ordering {
    if val_a.is_null() && val_b.is_null() {
        return Ordering::Equal;
    }
    if val_a.is_null() {
        return Ordering::Less;
    } // NULLS FIRST
    if val_b.is_null() {
        return Ordering::Greater;
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
                    return a.partial_cmp(&b).unwrap_or(Ordering::Equal);
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
            .unwrap_or(Ordering::Equal),
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
            let val = expr.evaluate_with_context(row, outer_row, ctx.clone()).await?;
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
) -> Pin<Box<dyn Stream<Item = Result<Row>> + Send + 'a>> {
    Box::pin(try_stream! {        match plan {
            PhysicalPlan::TableScan { prefix } => {
                let table_name = prefix.strip_suffix(':').unwrap_or(&prefix).to_string();
                let mut keys = Vec::new();
                for r in ctx.db.iter() {
                    if r.key().starts_with(&prefix) {
                        keys.push(r.key().clone());
                    }
                }
                for key in keys {
                    if let Some(r) = ctx.db.get(&key) {
                        let mut value = match r.value() {
                            DbValue::Json(v) => v.clone(),
                            DbValue::JsonB(b) => serde_json::from_slice(b)?,
                            _ => json!({}),
                        };
                        if let Some(obj) = value.as_object_mut() {
                            let key_without_prefix = key.strip_prefix(&prefix).unwrap_or(&key);
                            let id_from_key = key_without_prefix.strip_suffix(':').unwrap_or(key_without_prefix);
                            // Only insert 'id' if it's not already present.
                            if !obj.contains_key("id") {
                                obj.insert("id".to_string(), json!(id_from_key));
                            }
                            obj.insert("_key".to_string(), json!(key));
                        }
                        // Wrap the row in an object with the table name as the key
                        // to provide context for downstream operators like joins.
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
                            if let Some(r) = ctx.db.get(db_key) {
                                let mut value = match r.value() {
                                    DbValue::Json(v) => v.clone(),
                                    DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_else(|_| json!({})),
                                    _ => json!({}),
                                };
                                if let Some(obj) = value.as_object_mut() {
                                    let key_without_prefix = db_key.strip_prefix(&prefix).unwrap_or(db_key);
                                    let id_from_key = key_without_prefix.strip_suffix(':').unwrap_or(key_without_prefix);
                                    if !obj.contains_key("id") {
                                        obj.insert("id".to_string(), json!(id_from_key));
                                    }
                                    obj.insert("_key".to_string(), json!(r.key()));
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
                let mut stream = execute(*input, ctx.clone(), outer_row, _working_tables);
                while let Some(row_result) = stream.next().await {
                    let row = row_result?;
                    if predicate.evaluate_with_context(&row, outer_row, ctx.clone()).await?.as_bool().unwrap_or(false) {
                        yield row;
                    }
                }
            }
            PhysicalPlan::Projection {
                input,
                expressions,
            } => {
                let mut stream = execute(*input, ctx.clone(), outer_row, _working_tables);
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
                            expr.evaluate_with_context(&row, outer_row, ctx.clone()).await?
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
                let left_rows: Vec<Row> = execute(*left, ctx.clone(), outer_row, _working_tables).try_collect().await?;
                let right_rows: Vec<Row> = execute(*right, ctx.clone(), outer_row, _working_tables).try_collect().await?;

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

                        if condition.evaluate_with_context(&combined_row, outer_row, ctx.clone()).await?.as_bool().unwrap_or(false) {
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
                let all_rows: Vec<Row> = execute(*input, ctx.clone(), outer_row, _working_tables).try_collect().await?;
                let mut groups: HashMap<String, (Row, Vec<Row>)> = HashMap::new();

                if group_expressions.is_empty() {
                    if !all_rows.is_empty() {
                        let mut agg_row = json!({});
                        for agg_expr in &agg_expressions {
                            if let Expression::AggregateFunction { func, arg } = agg_expr {
                                let result = run_agg_fn(func, arg, &all_rows, outer_row, ctx.clone()).await?;
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
                        group_key_parts.push(expr.evaluate_with_context(&row, outer_row, ctx.clone()).await?.to_string());
                    }
                    let group_key = group_key_parts.join("-");

                    if !groups.contains_key(&group_key) {
                        let mut group_row = json!({});
                        for expr in &group_expressions {
                            // The expression already knows how to get the value from the nested row
                            let value =
                                expr.evaluate_with_context(&row, outer_row, ctx.clone())
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
                            let result = run_agg_fn(func, arg, &rows, outer_row, ctx.clone()).await?;
                            let agg_name = format!("{}", agg_expr);
                            group_row[agg_name] = result;
                        }
                    }
                    yield group_row;
                }
            }
            PhysicalPlan::Sort { input, sort_expressions } => {
                let rows: Vec<Row> = execute(*input, ctx.clone(), outer_row, _working_tables).try_collect().await?;
                let mut sort_data = Vec::new();
                for row in rows.into_iter() {
                    let mut keys = Vec::new();
                    for (expr, _) in &sort_expressions {
                        keys.push(expr.evaluate_with_context(&row, outer_row, ctx.clone()).await?);
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
                        if ord != Ordering::Equal {
                            return ord;
                        }
                    }
                    Ordering::Equal
                });

                for (_, row) in sort_data {
                    yield row;
                }
            }
            PhysicalPlan::Limit { input, limit, offset } => {
                let stream = execute(*input, ctx.clone(), outer_row, _working_tables).skip(offset.unwrap_or(0));
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
                                    c.nullable = false; // Primary key columns are implicitly NOT NULL
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
                            if let Response::Error(e) = super::super::commands::handle_idx_create(create_index_command, &ctx).await {
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
                log_and_wait_qe!(ctx.logger, log_entry).await?;

                ctx.db.insert(schema_key, DbValue::Bytes(schema_bytes));
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
                log_and_wait_qe!(ctx.logger, log_entry).await?;

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
                log_and_wait_qe!(ctx.logger, log_entry).await?;

                ctx.view_cache.remove(&view_name);
                ctx.db.remove(&view_key);

                yield json!({"status": "View dropped"});
            }
            PhysicalPlan::Insert { table_name, columns, source, source_column_names, on_conflict, returning } => {
                let schema = ctx.schema_cache.get(&table_name);
                let mut total_rows_affected = 0;
                let mut returned_rows = Vec::new();

                // Step 1: Execute the source plan and collect all rows to prevent deadlocks.
                let source_rows: Vec<Row> = execute(*source, ctx.clone(), outer_row, _working_tables).try_collect().await?;

                // Step 2: Iterate over the collected rows to perform inserts.
                for source_row in source_rows {
                    let source_row_obj = source_row.as_object().ok_or_else(|| anyhow!("INSERT source did not produce an object"))?;

                    let insert_columns = if columns.is_empty() {
                        if let Some(s) = &schema {
                            if !s.column_order.is_empty() {
                                s.column_order.clone()
                            } else {
                                s.columns.keys().cloned().collect::<Vec<String>>()
                            }
                        } else {
                            Err(anyhow!("Cannot INSERT without column list into a table with no schema"))?
                        }
                    } else {
                        columns.clone()
                    };

                    if insert_columns.len() != source_column_names.len() {
                        Err(anyhow!("INSERT has mismatch between number of columns ({}) and values from source ({})", insert_columns.len(), source_column_names.len()))?;
                    }

                    let mut row_data = json!({});

                    for (i, target_col_name) in insert_columns.iter().enumerate() {
                        let source_col_name = &source_column_names[i];
                        let mut val = source_row_obj.get(source_col_name).cloned().unwrap_or(Value::Null);
                        if let Some(s) = &schema {
                            if let Some(col_def) = s.columns.get(target_col_name) {
                                val = cast_value_to_type(val, &col_def.data_type)?;
                            }
                        }
                        row_data[target_col_name] = val;
                    }


                    if let Some(s) = &schema {
                        for (col_name, col_def) in &s.columns {
                            if !row_data.get(col_name).is_some() {
                                if let Some(default_expr) = &col_def.default {
                                    let mut val = default_expr.evaluate_with_context(&json!({}), None, ctx.clone()).await?;
                                    val = cast_value_to_type(val, &col_def.data_type)?;
                                    row_data[col_name.clone()] = val;
                                }
                            }
                        }
                    }

                    if let Some(s) = &schema {
                        for (col_name, col_def) in &s.columns {
                            if !col_def.nullable && row_data.get(col_name).map_or(true, |v| v.is_null()) {
                                Err(anyhow!("NULL value in column '{}' violates not-null constraint", col_name))?;
                            }
                        }

                        // Check CHECK constraints before insert/update
                        for constraint in &s.constraints {
                            if let TableConstraint::Check { name: _, expression } = constraint {
                                let check_row = json!({ table_name.clone(): row_data.clone() });
                                if !super::logical_plan::simple_expr_to_expression(expression.clone(), &ctx.schema_cache, &ctx.view_cache, &ctx.function_registry, None)?.evaluate_with_context(&check_row, None, ctx.clone()).await?.as_bool().unwrap_or(false) {
                                    Err(anyhow!("CHECK constraint failed for table '{}'", table_name))?;
                                }
                            }
                        }
                    }

                    check_foreign_key_constraints(&table_name, &row_data, &ctx).await?;

                    // Check UNIQUE constraints
                    if let Some(s) = &schema {
                        for constraint in &s.constraints {
                            if let TableConstraint::Unique { name: _, columns } = constraint {
                                if columns.len() == 1 { // Only single-column unique constraints are handled here
                                    let col_name = &columns[0];
                                    if let Some(val) = row_data.get(col_name) {
                                        let key_prefix = format!("{}:*", table_name);
                                        let full_index_name = format!("{}|{}", key_prefix, col_name);
                                        if let Some(index) = ctx.index_manager.indexes.get(&full_index_name) {
                                            let index_key = serde_json::to_string(val)?;
                                            let index_data = index.read().await;
                                            if let Some(keys) = index_data.get(&index_key) {
                                                // If the key already exists in the index, it's a unique violation
                                                // unless it's the same key being updated (which is handled by ON CONFLICT)
                                                if !keys.is_empty() {
                                                    Err(anyhow!("UNIQUE constraint failed for column '{}'", col_name))?;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    let pk_col = if let Some(s) = &schema {
                        s.constraints.iter().filter_map(|c| {
                            if let TableConstraint::PrimaryKey { name: _, columns } = c {
                                columns.first().cloned()
                            } else {
                                None
                            }
                        }).next().unwrap_or_else(|| "id".to_string())
                    } else {
                        "id".to_string()
                    };

                    let pk = match row_data.get(&pk_col) {
                        Some(Value::String(s)) => s.clone(),
                        Some(Value::Number(n)) => n.to_string(),
                        _ => Uuid::new_v4().to_string(),
                    };
                    let key = format!("{}:{}", table_name, pk);

                    if ctx.db.contains_key(&key) {
                        if let Some((_target, action)) = &on_conflict {
                            match action {
                                OnConflictAction::DoNothing => {
                                    // Skip this row
                                    continue;
                                }
                                OnConflictAction::DoUpdate(set_clauses) => {
                                    // This block is now self-contained to prevent deadlocks.
                                    // It performs its own read, modify, and write cycle.
                                    let old_val = match ctx.db.get(&key) {
                                        Some(entry) => match entry.value() {
                                            DbValue::JsonB(b) => serde_json::from_slice(b)?,
                                            _ => json!({}),
                                        },
                                        None => continue, // Should not happen if we are in a conflict path
                                    };

                                    let mut new_val = old_val.clone();
                                    let excluded_row = json!({ "excluded": row_data.clone() });

                                    for (col, expr) in set_clauses {
                                        let mut val = expr.evaluate_with_context(&excluded_row, Some(&old_val), ctx.clone()).await?;
                                        if let Some(s) = &schema {
                                            if let Some(col_def) = s.columns.get(col) {
                                                if !col_def.nullable && val.is_null() {
                                                    Err(anyhow!("NULL value in column '{}' violates not-null constraint", col))?;
                                                }
                                                val = cast_value_to_type(val, &col_def.data_type)?;
                                            }
                                        }
                                        new_val[col] = val;
                                    }

                                    apply_on_update_actions(&table_name, &old_val, &new_val, &ctx).await?;

                                    if let Some(s) = &schema {
                                        for constraint in &s.constraints {
                                            if let TableConstraint::Unique { name: _, columns } = constraint {
                                                if columns.len() == 1 { // Only single-column unique constraints are handled here
                                                    let col_name = &columns[0];
                                                    if let Some(val) = new_val.get(col_name) {
                                                        let key_prefix = format!("{}:*", table_name);
                                                        let full_index_name = format!("{}|{}", key_prefix, col_name);
                                                        if let Some(index) = ctx.index_manager.indexes.get(&full_index_name) {
                                                            let index_key = serde_json::to_string(val)?;
                                                            let index_data = index.read().await;
                                                            if let Some(keys) = index_data.get(&index_key) {
                                                                if !keys.contains(&key) { // Ensure it's not the same key
                                                                    Err(anyhow!("UNIQUE constraint failed for column '{}'", col_name))?;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    if let Some(s) = &schema {
                                        for constraint in &s.constraints {
                                            if let TableConstraint::Check { name: _, expression } = constraint {
                                                let check_row = json!({ table_name.clone(): new_val.clone() });
                                                if !super::logical_plan::simple_expr_to_expression(expression.clone(), &ctx.schema_cache, &ctx.view_cache, &ctx.function_registry, None)?.evaluate_with_context(&check_row, None, ctx.clone()).await?.as_bool().unwrap_or(false)
                                                {
                                                    Err(anyhow!("CHECK constraint failed for table '{}'", table_name))?;
                                                }
                                            }
                                        }
                                    }

                                    check_foreign_key_constraints(&table_name, &new_val, &ctx).await?;

                                    let new_val_bytes = serde_json::to_vec(&new_val)?;
                                    let log_entry = LogEntry::SetJsonB { key: key.clone(), value: new_val_bytes.clone() };
                                    log_and_wait_qe!(ctx.logger, log_entry).await?;

                                    ctx.index_manager.remove_key_from_indexes(&key, &old_val).await;
                                    ctx.index_manager.add_key_to_indexes(&key, &new_val).await;

                                    ctx.db.insert(key.clone(), DbValue::JsonB(new_val_bytes));
                                    total_rows_affected += 1;
                                    if !returning.is_empty() {
                                        returned_rows.push(new_val);
                                    }
                                    continue;
                                }
                            }
                        } else {
                             Err(anyhow!("PRIMARY KEY constraint failed for table '{}'. Key '{}' already exists.", table_name, key))?;
                        }
                    }

                    // Standard insert path (no conflict)
                    let value_bytes = serde_json::to_vec(&row_data)?;
                    let log_entry = LogEntry::SetJsonB { key: key.clone(), value: value_bytes.clone() };
                    log_and_wait_qe!(ctx.logger, log_entry).await?;

                    ctx.index_manager
                        .add_key_to_indexes(&key, &row_data)
                        .await;

                    ctx.db.insert(key, DbValue::JsonB(value_bytes));
                    total_rows_affected += 1;

                    if !returning.is_empty() {
                        returned_rows.push(row_data);
                    }
                }

                if !returning.is_empty() {
                    for returned_row in returned_rows {
                        let proj_row = project_row(&returned_row, &returning, ctx.clone(), outer_row, _working_tables).await?;
                        yield proj_row;
                    }
                } else {
                    yield json!({"rows_affected": total_rows_affected});
                }
            }
            PhysicalPlan::Delete { table_name, from, returning } => {
                let stream = execute(*from, ctx.clone(), outer_row, _working_tables);
                let rows_to_delete: Vec<Row> = stream.try_collect().await?;

                // First, apply all ON DELETE actions. If any of these fail, we abort before any actual deletion.
                for row in &rows_to_delete {
                    if let Some(obj) = row.as_object() {
                        if let Some(table_obj) = obj.get(&table_name) {
                            // table_obj is the inner JSON object, e.g., {"id": 1, "name": "Alice"}
                            apply_on_delete_actions(&table_name, table_obj, &ctx).await?;
                        }
                    }
                }

                let mut deleted_count = 0;
                for row in &rows_to_delete {
                    let table_part = match row.get(&table_name) {
                        Some(part) => part,
                        None => continue,
                    };

                    let key = match table_part.get("_key").and_then(|k| k.as_str()) {
                        Some(k) => k.to_string(),
                        None => continue,
                    };

                    // Remove from indexes BEFORE removing from DB
                    ctx.index_manager
                        .remove_key_from_indexes(&key, table_part)
                        .await;

                    let log_entry = LogEntry::Delete { key: key.clone() };
                    log_and_wait_qe!(ctx.logger, log_entry).await?;
                    if ctx.db.remove(&key).is_some() {
                        deleted_count += 1;
                    }
                }

                if !returning.is_empty() {
                    for returned_row in rows_to_delete {
                        // The row from the plan is nested, e.g. {"users": {"id":1, ...}}
                        // We need to un-nest it for projection.
                        if let Some(inner_row) = returned_row.get(&table_name) {
                            let proj_row = project_row(inner_row, &returning, ctx.clone(), outer_row, _working_tables).await?;
                            yield proj_row;
                        }
                    }
                } else {
                    yield json!({"rows_affected": deleted_count});
                }
            }
            PhysicalPlan::Update { table_name, from, set, returning } => {
                let schema = ctx.schema_cache.get(&table_name);
                let stream = execute(*from, ctx.clone(), outer_row, _working_tables);
                let rows_to_update: Vec<Row> = stream.try_collect().await?;

                let mut updated_count = 0;
                let mut returned_rows = Vec::new();

                for row in rows_to_update {
                    let table_part = match row.get(&table_name) {
                        Some(part) => part,
                        None => continue, // This row from the join doesn't have the target table, skip.
                    };

                    let key = match table_part.get("_key").and_then(|k| k.as_str()) {
                        Some(k) => k.to_string(),
                        None => continue, // No key found for this row, cannot update.
                    };

                    let old_val_for_index = table_part;
                    let mut new_val = table_part.clone();

                    for (col, expr) in &set {
                        let mut val = expr.evaluate_with_context(&row, outer_row, ctx.clone()).await?;

                        if let Some(schema) = &schema {
                            if let Some(col_def) = schema.columns.get(col) {
                                if !col_def.nullable && val.is_null() {
                                    Err(anyhow!("NULL value in column '{}' violates not-null constraint", col))?;
                                }
                                val = cast_value_to_type(val, &col_def.data_type)?;
                            }
                        }
                        new_val[col] = val;
                    }

                    apply_on_update_actions(&table_name, old_val_for_index, &new_val, &ctx).await?;

                    if let Some(schema) = &schema {
                        for constraint in &schema.constraints {
                            if let TableConstraint::Unique { name: _, columns } = constraint {
                                if columns.len() == 1 { // Only single-column unique constraints are handled here
                                    let col_name = &columns[0];
                                    if let Some(val) = new_val.get(col_name) {
                                        let key_prefix = format!("{}:*", table_name);
                                        let full_index_name = format!("{}|{}", key_prefix, col_name);
                                        if let Some(index) = ctx.index_manager.indexes.get(&full_index_name) {
                                            let index_key = serde_json::to_string(val)?;
                                            let index_data = index.read().await;
                                            if let Some(keys) = index_data.get(&index_key) {
                                                if !keys.contains(&key) { // Ensure it's not the same key
                                                    Err(anyhow!("UNIQUE constraint failed for column '{}'", col_name))?;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if let Some(schema) = &schema {
                        for constraint in &schema.constraints {
                            if let TableConstraint::Check { name: _, expression } = constraint {
                                let check_row = json!({ table_name.clone(): new_val.clone() });
                                if !super::logical_plan::simple_expr_to_expression(expression.clone(), &ctx.schema_cache, &ctx.view_cache, &ctx.function_registry, None)?.evaluate_with_context(&check_row, None, ctx.clone()).await?.as_bool().unwrap_or(false)
                                {
                                    Err(anyhow!("CHECK constraint failed for table '{}'", table_name))?;
                                }
                            }
                        }
                    }

                    check_foreign_key_constraints(&table_name, &new_val, &ctx).await?;

                    let new_val_bytes = serde_json::to_vec(&new_val)?;
                    let log_entry = LogEntry::SetJsonB { key: key.clone(), value: new_val_bytes.clone() };
                    log_and_wait_qe!(ctx.logger, log_entry).await?;

                    ctx.index_manager
                        .remove_key_from_indexes(&key, old_val_for_index)
                        .await;
                    ctx.index_manager
                        .add_key_to_indexes(&key, &new_val)
                        .await;

                    ctx.db.insert(key, DbValue::JsonB(new_val_bytes));
                    updated_count += 1;

                    if !returning.is_empty() {
                        returned_rows.push(new_val);
                    }
                }

                if !returning.is_empty() {
                    for returned_row in returned_rows {
                        let proj_row = project_row(&returned_row, &returning, ctx.clone(), outer_row, _working_tables).await?;
                        yield proj_row;
                    }
                } else {
                    yield json!({"rows_affected": updated_count});
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
                        for entry in ctx.db.iter() {
                            if entry.key().starts_with(&table_prefix) {
                                let val: Value = match entry.value() {
                                    DbValue::Json(v) => v.clone(),
                                    DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_default(),
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
                        log_and_wait_qe!(ctx.logger, log_entry).await?;

                        // 3. Perform the rename in memory. This is not perfectly atomic in-memory,
                        // but it is consistent upon WAL replay.

                        // 3a. Rename schema
                        let old_schema_key = format!("{}{}", SCHEMA_PREFIX, &table_name);
                        if let Some((_, schema_val)) = ctx.db.remove(&old_schema_key) {
                             if let DbValue::Bytes(bytes) = schema_val {
                                if let Ok(mut schema) = serde_json::from_slice::<VirtualSchema>(&bytes) {
                                    schema.table_name = new_table_name.clone();
                                    if let Ok(new_bytes) = serde_json::to_vec(&schema) {
                                        let new_schema_key = format!("{}{}", SCHEMA_PREFIX, &new_table_name);
                                        ctx.db.insert(new_schema_key, DbValue::Bytes(new_bytes));
                                        ctx.schema_cache.remove(&table_name);
                                        ctx.schema_cache.insert(new_table_name.clone(), Arc::new(schema));
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

                        // 1. Update schema
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
                             if let Some(mut entry) = ctx.db.get_mut(&key) {
                                let mut val: serde_json::Value = match entry.value() {
                                    DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_default(),
                                    _ => continue,
                                };
                                if let Some(obj) = val.as_object_mut() {
                                    if let Some(v) = obj.remove(&old_column_name) {
                                        obj.insert(new_column_name.clone(), v);
                                    }
                                }
                                let new_bytes = serde_json::to_vec(&val)?;
                                log_and_wait_qe!(ctx.logger, LogEntry::SetJsonB { key: key.clone(), value: new_bytes.clone() }).await?;
                                *entry.value_mut() = DbValue::JsonB(new_bytes);
                             }
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
                            if let Response::Error(e) = super::super::commands::handle_idx_create(create_index_command, &ctx).await {
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
                                        if let Response::Error(e) = super::super::commands::handle_idx_drop(drop_index_command, &ctx).await {
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
                                    if let Response::Error(e) = super::super::commands::handle_idx_drop(drop_index_command, &ctx).await {
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
                             if let Some(mut entry) = ctx.db.get_mut(&key) {
                                let mut val: serde_json::Value = match entry.value() {
                                    DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_default(),
                                    _ => continue,
                                };
                                if let Some(obj) = val.as_object_mut() {
                                    if let Some(old_v) = obj.get_mut(&column_name) {
                                        let casted_v = cast_value_to_type(old_v.clone(), &new_dt)?;
                                        *old_v = casted_v;
                                    }
                                }
                                let new_bytes = serde_json::to_vec(&val)?;
                                log_and_wait_qe!(ctx.logger, LogEntry::SetJsonB { key: key.clone(), value: new_bytes.clone() }).await?;
                                *entry.value_mut() = DbValue::JsonB(new_bytes);
                             }
                        }
                        // 2. Update schema
                        let col = schema.columns.get_mut(&column_name).ok_or_else(|| anyhow!("Column '{}' does not exist", column_name))?;
                        col.data_type = new_dt;
                    }
                }

                let schema_bytes = serde_json::to_vec(&schema)?;
                let log_entry = LogEntry::SetBytes { key: schema_key.clone(), value: schema_bytes.clone() };
                log_and_wait_qe!(ctx.logger, log_entry).await?;

                ctx.db.insert(schema_key, DbValue::Bytes(schema_bytes));
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
                log_and_wait_qe!(ctx.logger, log_entry).await?;

                ctx.db.insert(view_key, DbValue::Bytes(view_bytes));
                ctx.view_cache.insert(view_name, Arc::new(view_def));

                yield json!({"status": "View created"});
            }
            PhysicalPlan::CreateSchema { schema_name } => {
                let schema_list_key = format!("{}{}", SCHEMALIST_PREFIX, schema_name);
                if ctx.db.contains_key(&schema_list_key) {
                    Err(anyhow!("Schema '{}' already exists", schema_name))?;
                }

                let log_entry = LogEntry::SetBytes { key: schema_list_key.clone(), value: vec![] };
                log_and_wait_qe!(ctx.logger, log_entry).await?;

                ctx.db.insert(schema_list_key, DbValue::Bytes(vec![]));

                yield json!({"status": "Schema created"});
            }
            PhysicalPlan::Values { values } => {
                for row_values in values {
                    let mut row = json!({});
                    for (i, value_expr) in row_values.iter().enumerate() {
                        let value = value_expr.evaluate_with_context(&json!({}), None, ctx.clone()).await?;
                        // Use a generic column name like "column_0", "column_1", etc.
                        row[format!("column_{}", i)] = value;
                    }
                    yield row;
                }
            }
            PhysicalPlan::DistinctOn { input, expressions } => {
                let mut seen_keys = std::collections::HashSet::new();
                let mut stream = execute(*input, ctx.clone(), outer_row, _working_tables);
                while let Some(row_result) = stream.next().await {
                    let row = row_result?;
                    let mut key_parts = Vec::new();
                    for expr in &expressions {
                        key_parts.push(expr.evaluate_with_context(&row, outer_row, ctx.clone()).await?);
                    }
                    let key = serde_json::to_string(&key_parts)?;
                    if seen_keys.insert(key) {
                        yield row;
                    }
                }
            }
            PhysicalPlan::UnionAll { left, right } => {
                let mut left_stream = execute(*left, ctx.clone(), outer_row, _working_tables);
                while let Some(row) = left_stream.next().await {
                    yield row?;
                }
                let mut right_stream = execute(*right, ctx.clone(), outer_row, _working_tables);
                while let Some(row) = right_stream.next().await {
                    yield row?;
                }
            }
            PhysicalPlan::Intersect { left, right } => {
                let left_rows: std::collections::HashSet<String> = execute(*left, ctx.clone(), outer_row, _working_tables)
                    .map_ok(|row| row.to_string())
                    .try_collect()
                    .await?;
                let right_rows: std::collections::HashSet<String> = execute(*right, ctx.clone(), outer_row, _working_tables)
                    .map_ok(|row| row.to_string())
                    .try_collect()
                    .await?;

                for row_str in left_rows.intersection(&right_rows) {
                    yield serde_json::from_str(row_str)?;
                }
            }
            PhysicalPlan::Except { left, right } => {
                let left_rows: std::collections::HashSet<String> = execute(*left, ctx.clone(), outer_row, _working_tables)
                    .map_ok(|row| row.to_string())
                    .try_collect()
                    .await?;
                let right_rows: std::collections::HashSet<String> = execute(*right, ctx.clone(), outer_row, _working_tables)
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
                log_and_wait_qe!(ctx.logger, log_entry).await?;
                ctx.db.insert(index_metadata_key, DbValue::Bytes(index_metadata_bytes));

                // Create the actual index data structure in IndexManager
                let new_index = Arc::new(crate::indexing::Index::default());
                ctx.index_manager.indexes.insert(statement.index_name.clone(), new_index.clone());

                // Backfill the index
                let table_prefix = format!("{}:", statement.table_name);
                let mut backfilled_count = 0;

                for entry in ctx.db.iter() {
                    if entry.key().starts_with(&table_prefix) {
                        let row_key = entry.key().clone();
                        let val: Value = match entry.value() {
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
                            let evaluated_val = expr.evaluate_with_context(&val, None, ctx.clone()).await?;
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

                yield json!({ "status": format!("Index '{}' created and backfilled {} items.", statement.index_name, backfilled_count) });
            },
            PhysicalPlan::SubqueryScan { alias, input } => {
                let mut stream = execute(*input, ctx.clone(), outer_row, _working_tables);
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

                let non_recursive_rows_unaliased: Vec<Row> = execute(*non_recursive, ctx.clone(), outer_row, _working_tables).try_collect().await?;
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

                    let recursive_stream = execute(*recursive.clone(), ctx.clone(), outer_row, Some(&current_working_tables));
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
        }
    })
}

async fn run_agg_fn<'a>(
    func: &str,
    arg: &Expression,
    rows: &'a [Row],
    outer_row: Option<&'a Row>,
    ctx: Arc<AppContext>,
) -> Result<Value> {
    let result = match func.to_uppercase().as_str() {
        "COUNT" => json!(rows.len()),
        "SUM" => {
            let mut sum = 0.0;
            for row in rows {
                if let Some(n) = arg.evaluate_with_context(row, outer_row, ctx.clone()).await?.as_f64() {
                    sum += n;
                }
            }
            json!(sum)
        }
        "AVG" => {
            let mut sum = 0.0;
            let mut count = 0;
            for row in rows {
                if let Some(n) = arg.evaluate_with_context(row, outer_row, ctx.clone()).await?.as_f64() {
                    sum += n;
                    count += 1;
                }
            }
            if count > 0 { json!(sum / count as f64) } else { Value::Null }
        }
        "MIN" => {
            let mut min = Value::Null;
            for row in rows {
                let val = arg.evaluate_with_context(row, outer_row, ctx.clone()).await?;
                if min.is_null() || (!val.is_null() && val.as_f64() < min.as_f64()) {
                    min = val;
                }
            }
            min
        }
        "MAX" => {
            let mut max = Value::Null;
            for row in rows {
                let val = arg.evaluate_with_context(row, outer_row, ctx.clone()).await?;
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

/// Checks if inserting or updating a row in a child table violates any foreign key constraints.
async fn check_foreign_key_constraints(
    table_name: &str,
    row_data: &Value,
    ctx: &Arc<AppContext>,
) -> Result<()> {
    if let Some(schema) = ctx.schema_cache.get(table_name) {
        for constraint in &schema.constraints {
            if let TableConstraint::ForeignKey(fk) = constraint {
                let child_key_values: Vec<Value> = fk
                    .columns
                    .iter()
                    .map(|col_name| row_data.get(col_name).cloned().unwrap_or(Value::Null))
                    .collect();

                // If any part of the foreign key is NULL, the constraint is satisfied (MATCH SIMPLE)
                if child_key_values.iter().any(|v| v.is_null()) {
                    continue;
                }

                // Build a filter expression to find the parent row
                let mut filter_expr: Option<Expression> = None;
                for (i, col_name) in fk.references_columns.iter().enumerate() {
                    let condition = Expression::BinaryOp {
                        left: Box::new(Expression::Column(col_name.clone())),
                        op: Operator::Eq,
                        right: Box::new(Expression::Literal(child_key_values[i].clone())),
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
                        table_name: fk.references_table.clone(),
                    };
                    let filter_plan = LogicalPlan::Filter {
                        input: Box::new(scan_plan),
                        predicate,
                    };
                    let limit_plan = LogicalPlan::Limit {
                        input: Box::new(filter_plan),
                        limit: Some(1),
                        offset: None,
                    };

                    let physical_plan =
                        super::physical_plan::logical_to_physical_plan(limit_plan, &ctx.index_manager)?;

                    let results: Vec<Value> = execute(physical_plan, ctx.clone(), None, None).try_collect().await?;

                    if results.is_empty() {
                        return Err(anyhow!(
                            "Insert or update on table '{}' violates foreign key constraint. A matching key was not found in table '{}'.",
                            table_name,
                            fk.references_table,
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}



async fn apply_on_delete_actions(
    parent_table_name: &str,
    parent_row_data: &Value, // The inner object of the parent row
    ctx: &Arc<AppContext>,
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

                        let results: Vec<Value> = execute(physical_plan, ctx.clone(), None, None).try_collect().await?;

                        if !results.is_empty() {
                            let on_delete_action = fk.on_delete.as_deref().unwrap_or("NO ACTION").to_uppercase();
                            match on_delete_action.as_str() {
                                "CASCADE" => {
                                    for child_row in results {
                                        let child_key = child_row.as_object()
                                            .and_then(|obj| obj.values().next())
                                            .and_then(|table_obj| table_obj.get("_key"))
                                            .and_then(|k| k.as_str())
                                            .map(|s| s.to_string())
                                            .ok_or_else(|| anyhow!("Could not get child key for CASCADE delete"))?;

                                        let log_entry = LogEntry::Delete { key: child_key.clone() };
                                        log_and_wait_qe!(ctx.logger, log_entry).await?;
                                        if let Some(entry) = ctx.db.remove(&child_key) {
                                            let size = crate::memory::estimate_db_value_size(&entry.1).await;
                                            ctx.memory.decrease_memory(size + child_key.len() as u64);
                                            ctx.memory.forget_key(&child_key).await;
                                            if let DbValue::JsonB(b) = entry.1 {
                                                if let Ok(val) = serde_json::from_slice::<Value>(&b) {
                                                    ctx.index_manager.remove_key_from_indexes(&child_key, &val).await;
                                                }
                                            }
                                        }
                                    }
                                }
                                "SET NULL" => {
                                    for child_row in results {
                                        let child_key = child_row.as_object()
                                            .and_then(|obj| obj.values().next())
                                            .and_then(|table_obj| table_obj.get("_key"))
                                            .and_then(|k| k.as_str())
                                            .map(|s| s.to_string())
                                            .ok_or_else(|| anyhow!("Could not get child key for SET NULL update"))?;

                                        let old_child_val_for_index = ctx.db.get(&child_key).map(|v| match v.value() {
                                            DbValue::Json(j) => j.clone(),
                                            DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_default(),
                                            _ => Value::Null,
                                        }).unwrap_or_default();

                                        let mut new_child_val = old_child_val_for_index.clone();
                                        for col_name in &fk.columns {
                                            new_child_val[col_name] = Value::Null;
                                        }

                                        let new_val_bytes = serde_json::to_vec(&new_child_val)?;
                                        let log_entry = LogEntry::SetJsonB { key: child_key.clone(), value: new_val_bytes.clone() };
                                        log_and_wait_qe!(ctx.logger, log_entry).await?;

                                        ctx.index_manager.remove_key_from_indexes(&child_key, &old_child_val_for_index).await;
                                        ctx.index_manager.add_key_to_indexes(&child_key, &new_child_val).await;

                                        ctx.db.insert(child_key, DbValue::JsonB(new_val_bytes));
                                    }
                                }
                                "SET DEFAULT" => {
                                    for child_row in results {
                                        let child_key = child_row.as_object()
                                            .and_then(|obj| obj.values().next())
                                            .and_then(|table_obj| table_obj.get("_key"))
                                            .and_then(|k| k.as_str())
                                            .map(|s| s.to_string())
                                            .ok_or_else(|| anyhow!("Could not get child key for SET DEFAULT update"))?;

                                        let old_child_val_for_index = ctx.db.get(&child_key).map(|v| match v.value() {
                                            DbValue::Json(j) => j.clone(),
                                            DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_default(),
                                            _ => Value::Null,
                                        }).unwrap_or_default();

                                        let mut new_child_val = old_child_val_for_index.clone();
                                        for col_name in &fk.columns {
                                            let col_def = child_schema.columns.get(col_name)
                                                .ok_or_else(|| anyhow!("Column '{}' not found in child table '{}' for SET DEFAULT", col_name, child_schema.table_name))?;

                                            if let Some(default_expr) = &col_def.default {
                                                let mut default_val = default_expr.evaluate_with_context(&json!({}), None, ctx.clone()).await?;
                                                default_val = cast_value_to_type(default_val, &col_def.data_type)?;
                                                new_child_val[col_name] = default_val;
                                            } else {
                                                return Err(anyhow!("Cannot SET DEFAULT because column '{}' in table '{}' has no default value", col_name, child_schema.table_name));
                                            }
                                        }

                                        let new_val_bytes = serde_json::to_vec(&new_child_val)?;
                                        let log_entry = LogEntry::SetJsonB { key: child_key.clone(), value: new_val_bytes.clone() };
                                        log_and_wait_qe!(ctx.logger, log_entry).await?;

                                        ctx.index_manager.remove_key_from_indexes(&child_key, &old_child_val_for_index).await;
                                        ctx.index_manager.add_key_to_indexes(&child_key, &new_child_val).await;

                                        ctx.db.insert(child_key, DbValue::JsonB(new_val_bytes));
                                    }
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

                        let results: Vec<Value> = execute(physical_plan, ctx.clone(), None, None).try_collect().await?;

                        if !results.is_empty() {
                            let on_update_action = fk.on_update.as_deref().unwrap_or("NO ACTION").to_uppercase();
                            match on_update_action.as_str() {
                                "CASCADE" => {
                                    let new_parent_pk_values: HashMap<String, Value> = fk.references_columns.iter()
                                        .zip(fk.columns.iter())
                                        .filter_map(|(ref_col, fk_col)| {
                                            new_parent_row_data.get(ref_col).map(|v| (fk_col.clone(), v.clone()))
                                        })
                                        .collect();

                                    for child_row in results {
                                        let child_key = child_row.as_object()
                                            .and_then(|obj| obj.values().next())
                                            .and_then(|table_obj| table_obj.get("_key"))
                                            .and_then(|k| k.as_str())
                                            .map(|s| s.to_string())
                                            .ok_or_else(|| anyhow!("Could not get child key for CASCADE update"))?;

                                        let old_child_val_for_index = ctx.db.get(&child_key).map(|v| match v.value() {
                                            DbValue::Json(j) => j.clone(),
                                            DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_default(),
                                            _ => Value::Null,
                                        }).unwrap_or_default();

                                        let mut new_child_val = old_child_val_for_index.clone();
                                        for (col_name, new_val) in &new_parent_pk_values {
                                            new_child_val[col_name] = new_val.clone();
                                        }

                                        let new_val_bytes = serde_json::to_vec(&new_child_val)?;
                                        let log_entry = LogEntry::SetJsonB { key: child_key.clone(), value: new_val_bytes.clone() };
                                        log_and_wait_qe!(ctx.logger, log_entry).await?;

                                        ctx.index_manager.remove_key_from_indexes(&child_key, &old_child_val_for_index).await;
                                        ctx.index_manager.add_key_to_indexes(&child_key, &new_child_val).await;

                                        ctx.db.insert(child_key, DbValue::JsonB(new_val_bytes));
                                    }
                                }
                                "SET NULL" => {
                                    for child_row in results {
                                        let child_key = child_row.as_object()
                                            .and_then(|obj| obj.values().next())
                                            .and_then(|table_obj| table_obj.get("_key"))
                                            .and_then(|k| k.as_str())
                                            .map(|s| s.to_string())
                                            .ok_or_else(|| anyhow!("Could not get child key for SET NULL update"))?;

                                        let old_child_val_for_index = ctx.db.get(&child_key).map(|v| match v.value() {
                                            DbValue::Json(j) => j.clone(),
                                            DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_default(),
                                            _ => Value::Null,
                                        }).unwrap_or_default();

                                        let mut new_child_val = old_child_val_for_index.clone();
                                        for col_name in &fk.columns {
                                            new_child_val[col_name] = Value::Null;
                                        }

                                        let new_val_bytes = serde_json::to_vec(&new_child_val)?;
                                        let log_entry = LogEntry::SetJsonB { key: child_key.clone(), value: new_val_bytes.clone() };
                                        log_and_wait_qe!(ctx.logger, log_entry).await?;

                                        ctx.index_manager.remove_key_from_indexes(&child_key, &old_child_val_for_index).await;
                                        ctx.index_manager.add_key_to_indexes(&child_key, &new_child_val).await;

                                        ctx.db.insert(child_key, DbValue::JsonB(new_val_bytes));
                                    }
                                }
                                "SET DEFAULT" => {
                                    for child_row in results {
                                        let child_key = child_row.as_object()
                                            .and_then(|obj| obj.values().next())
                                            .and_then(|table_obj| table_obj.get("_key"))
                                            .and_then(|k| k.as_str())
                                            .map(|s| s.to_string())
                                            .ok_or_else(|| anyhow!("Could not get child key for SET DEFAULT update"))?;

                                        let old_child_val_for_index = ctx.db.get(&child_key).map(|v| match v.value() {
                                            DbValue::Json(j) => j.clone(),
                                            DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_default(),
                                            _ => Value::Null,
                                        }).unwrap_or_default();

                                        let mut new_child_val = old_child_val_for_index.clone();
                                        for col_name in &fk.columns {
                                            let col_def = child_schema.columns.get(col_name)
                                                .ok_or_else(|| anyhow!("Column '{}' not found in child table '{}' for SET DEFAULT", col_name, child_schema.table_name))?;

                                            if let Some(default_expr) = &col_def.default {
                                                let mut default_val = default_expr.evaluate_with_context(&json!({}), None, ctx.clone()).await?;
                                                default_val = cast_value_to_type(default_val, &col_def.data_type)?;
                                                new_child_val[col_name] = default_val;
                                            } else {
                                                return Err(anyhow!("Cannot SET DEFAULT because column '{}' in table '{}' has no default value", col_name, child_schema.table_name));
                                            }
                                        }

                                        let new_val_bytes = serde_json::to_vec(&new_child_val)?;
                                        let log_entry = LogEntry::SetJsonB { key: child_key.clone(), value: new_val_bytes.clone() };
                                        log_and_wait_qe!(ctx.logger, log_entry).await?;

                                        ctx.index_manager.remove_key_from_indexes(&child_key, &old_child_val_for_index).await;
                                        ctx.index_manager.add_key_to_indexes(&child_key, &new_child_val).await;

                                        ctx.db.insert(child_key, DbValue::JsonB(new_val_bytes));
                                    }
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