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
use super::logical_plan::{cast_value_to_type, Expression, JoinType, LogicalOperator, LogicalPlan, Operator};
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

/// Compares two serde_json Values, respecting the virtual data type.
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
        _ => Ordering::Equal,
    }
}

pub fn execute<'a>(
    plan: PhysicalPlan,
    ctx: Arc<AppContext>,
    outer_row: Option<&'a Row>,
) -> Pin<Box<dyn Stream<Item = Result<Row>> + Send + 'a>> {
    Box::pin(try_stream! {
        match plan {
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
                            DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_else(|_| json!({})),
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
                let mut stream = execute(*input, ctx.clone(), outer_row);
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
                let mut stream = execute(*input, ctx.clone(), outer_row);
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
                                Expression::Column(name) => name.clone(),
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
                let left_rows: Vec<Row> = execute(*left, ctx.clone(), outer_row).try_collect().await?;
                let right_rows: Vec<Row> = execute(*right, ctx.clone(), outer_row).try_collect().await?;

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
                let all_rows: Vec<Row> = execute(*input, ctx.clone(), outer_row).try_collect().await?;
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
                let rows: Vec<Row> = execute(*input, ctx.clone(), outer_row).try_collect().await?;
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
                let stream = execute(*input, ctx.clone(), outer_row).skip(offset.unwrap_or(0));
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
                        Some(super::logical_plan::simple_expr_to_expression(d, &ctx.schema_cache, &ctx.view_cache, &ctx.function_registry)?)
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
                            check_expression = Some(super::logical_plan::simple_expr_to_expression(expression.clone(), &ctx.schema_cache, &ctx.view_cache, &ctx.function_registry)?);
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
            PhysicalPlan::Insert { table_name, columns, values } => {
                let schema = ctx.schema_cache.get(&table_name);

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

                let mut total_rows_affected = 0;

                for row_values in values {
                    if insert_columns.len() != row_values.len() {
                        Err(anyhow!("INSERT has mismatch between number of columns ({}) and values ({})", insert_columns.len(), row_values.len()))?;
                    }

                    let mut row_data = json!({});

                    for (i, col_name) in insert_columns.iter().enumerate() {
                        let val_expr = &row_values[i];
                        let mut val = val_expr.evaluate_with_context(&json!({}), None, ctx.clone()).await?;

                        if let Some(s) = &schema {
                            if let Some(col_def) = s.columns.get(col_name) {
                                val = cast_value_to_type(val, &col_def.data_type)?;
                            }
                        }
                        row_data[col_name] = val;
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

                        // Check UNIQUE constraints
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
                                            if index_data.contains_key(&index_key) {
                                                Err(anyhow!("UNIQUE constraint failed for column '{}'", col_name))?;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Check CHECK constraints
                        for constraint in &s.constraints {
                            if let TableConstraint::Check { name: _, expression } = constraint {
                                let check_row = json!({ table_name.clone(): row_data.clone() });
                                if !super::logical_plan::simple_expr_to_expression(expression.clone(), &ctx.schema_cache, &ctx.view_cache, &ctx.function_registry)?.evaluate_with_context(&check_row, None, ctx.clone()).await?.as_bool().unwrap_or(false) {
                                    Err(anyhow!("CHECK constraint failed for table '{}'", table_name))?;
                                }
                            }
                        }

                        
                    }

                    check_foreign_key_constraints(&table_name, &row_data, &ctx).await?;

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
                        Err(anyhow!("PRIMARY KEY constraint failed for table '{}'. Key '{}' already exists.", table_name, key))?;
                    }

                    let value_bytes = serde_json::to_vec(&row_data)?;
                    let log_entry = LogEntry::SetJsonB { key: key.clone(), value: value_bytes.clone() };
                    log_and_wait_qe!(ctx.logger, log_entry).await?;

                    ctx.index_manager
                        .add_key_to_indexes(&key, &row_data)
                        .await;

                    ctx.db.insert(key, DbValue::JsonB(value_bytes));
                    total_rows_affected += 1;
                }
                yield json!({"rows_affected": total_rows_affected});
            }
            PhysicalPlan::Delete { from } => {
                let stream = execute(*from, ctx.clone(), outer_row);
                let rows_to_delete: Vec<Row> = stream.try_collect().await?;

                for row in &rows_to_delete {
                    if let Some(obj) = row.as_object() {
                        if let Some((table_name, table_obj)) = obj.iter().next() {
                            // table_obj is the inner JSON object, e.g., {"id": 1, "name": "Alice"}
                            apply_on_delete_actions(table_name, table_obj, &ctx).await?;
                        }
                    }
                }

                let keys_to_delete: Vec<String> = rows_to_delete.iter().filter_map(|row| {
                     row.as_object()
                        .and_then(|obj| obj.values().next())
                        .and_then(|table_obj| table_obj.get("_key"))
                        .and_then(|k| k.as_str())
                        .map(|s| s.to_string())
                }).collect();

                let mut deleted_count = 0;
                for key in keys_to_delete {
                    let log_entry = LogEntry::Delete { key: key.clone() };
                    log_and_wait_qe!(ctx.logger, log_entry).await?;
                    if ctx.db.remove(&key).is_some() {
                        deleted_count += 1;
                    }
                }
                yield json!({"rows_affected": deleted_count});
            }
            PhysicalPlan::Update { table_name, from, set } => {
                let schema = ctx.schema_cache.get(&table_name);
                let stream = execute(*from, ctx.clone(), outer_row);
                let rows_to_update: Vec<Row> = stream.try_collect().await?;

                // Check if any referenced key is being updated, which would require cascading checks.
                // This check is now handled within apply_on_update_actions.

                let mut updated_count = 0;
                for row in rows_to_update {
                    // The row from the 'from' plan is nested, e.g., {"user": {"name": "Alice", ...}}
                    // We need to extract the _key from the nested object.
                    let key = row.as_object()
                        .and_then(|obj| obj.values().next())
                        .and_then(|table_obj| table_obj.get("_key"))
                        .and_then(|k| k.as_str())
                        .map(|s| s.to_string());

                    if let Some(key) = key {
                        let old_val_for_index = ctx.db.get(&key).map(|v| match v.value() {
                            DbValue::Json(j) => j.clone(),
                            DbValue::JsonB(b) => serde_json::from_slice(b).unwrap_or_default(),
                            _ => Value::Null,
                        }).unwrap_or_default();

                        let mut new_val = old_val_for_index.clone();

                        for (col, expr) in &set {
                            // Evaluate the SET expression against the full row context
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

                        // Apply ON UPDATE actions for foreign keys
                        apply_on_update_actions(&table_name, &old_val_for_index, &new_val, &ctx).await?;

                        // Check UNIQUE constraints
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

                        // Check table-level CHECK constraint
                        if let Some(schema) = &schema {
                            for constraint in &schema.constraints {
                                if let TableConstraint::Check { name: _, expression } = constraint {
                                    // Wrap the new row value in a table-namespaced object
                                    let check_row = json!({ table_name.clone(): new_val.clone() });
                                    if !super::logical_plan::simple_expr_to_expression(expression.clone(), &ctx.schema_cache, &ctx.view_cache, &ctx.function_registry)?.evaluate_with_context(&check_row, None, ctx.clone()).await?.as_bool().unwrap_or(false)
                                    {
                                        Err(anyhow!("CHECK constraint failed for table '{}'", table_name))?;
                                    }
                                }
                            }
                        }

                        // Check foreign key constraints (for child table's FKs)
                        check_foreign_key_constraints(&table_name, &new_val, &ctx).await?;

                        let new_val_bytes = serde_json::to_vec(&new_val)?;
                        let log_entry = LogEntry::SetJsonB { key: key.clone(), value: new_val_bytes.clone() };
                        log_and_wait_qe!(ctx.logger, log_entry).await?;

                        ctx.index_manager
                            .remove_key_from_indexes(&key, &old_val_for_index)
                            .await;
                        ctx.index_manager
                            .add_key_to_indexes(&key, &new_val)
                            .await;

                        ctx.db.insert(key, DbValue::JsonB(new_val_bytes));
                        updated_count += 1;
                    }
                }
                yield json!({"rows_affected": updated_count});
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
                            Some(super::logical_plan::simple_expr_to_expression(d, &ctx.schema_cache, &ctx.view_cache, &ctx.function_registry)?)
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
                        col.default = Some(super::logical_plan::simple_expr_to_expression(default_expr, &ctx.schema_cache, &ctx.view_cache, &ctx.function_registry)?);
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
            PhysicalPlan::Union { left, right, all } => {
                if all {
                    // UNION ALL
                    let mut left_stream = execute(*left, ctx.clone(), outer_row);
                    while let Some(row) = left_stream.next().await {
                        yield row?;
                    }
                    let mut right_stream = execute(*right, ctx.clone(), outer_row);
                    while let Some(row) = right_stream.next().await {
                        yield row?;
                    }
                } else {
                    // UNION (DISTINCT)
                    let mut seen = std::collections::HashSet::new();
                    let mut left_stream = execute(*left, ctx.clone(), outer_row);
                    while let Some(row_result) = left_stream.next().await {
                        let row = row_result?;
                        let row_str = row.to_string();
                        if seen.insert(row_str) {
                            yield row;
                        }
                    }
                    let mut right_stream = execute(*right, ctx.clone(), outer_row);
                    while let Some(row_result) = right_stream.next().await {
                        let row = row_result?;
                        let row_str = row.to_string();
                        if seen.insert(row_str) {
                            yield row;
                        }
                    }
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

                    let results: Vec<Value> = execute(physical_plan, ctx.clone(), None).try_collect().await?;

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

                        let results: Vec<Value> = execute(physical_plan, ctx.clone(), None).try_collect().await?;

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

                        let results: Vec<Value> = execute(physical_plan, ctx.clone(), None).try_collect().await?;

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











