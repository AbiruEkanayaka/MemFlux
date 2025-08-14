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

use super::ast::AlterTableAction;
use super::logical_plan::{cast_value_to_type, Expression, JoinType};
use super::physical_plan::PhysicalPlan;
use crate::schema::{ColumnDefinition, DataType, VirtualSchema, SCHEMA_PREFIX};
use crate::types::{AppContext, DbValue, LogEntry, LogRequest, SchemaCache};

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
            DataType::Integer | DataType::Timestamp => {
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
                if let Some(index) = ctx.index_manager.indexes.get(&index_name) {
                    let index_key = serde_json::to_string(&key).unwrap_or_default();
                    let index_data = index.read().await;
                    if let Some(keys) = index_data.get(&index_key) {
                        for db_key in keys {
                            if let Some(r) = ctx.db.get(db_key) {
                                let mut value = match r.value() {
                                    DbValue::Json(v) => v.clone(),
                                    _ => json!({}),
                                };
                                if let Some(obj) = value.as_object_mut() {
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
                        // If it wasn't nested, the loop above does nothing.
                        // This handles the case of a single-table SELECT *.
                        if new_row.as_object().map_or(true, |m| m.is_empty()) {
                             if let Some(obj) = row.as_object() {
                                for (k, v) in obj {
                                    if k != "_key" {
                                        new_row[k.clone()] = v.clone();
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
            PhysicalPlan::CreateTable { table_name, columns } => {
                if ctx.schema_cache.contains_key(&table_name) {
                    Err(anyhow!("Table '{}' already exists", table_name))?;
                }
                let schema_key = format!("{}{}", SCHEMA_PREFIX, table_name);
                let mut cols = std::collections::BTreeMap::new();
                for c in columns {
                    let dt = match c.data_type.to_uppercase().as_str() {
                        "INTEGER" => DataType::Integer,
                        "TEXT" => DataType::Text,
                        "BOOLEAN" => DataType::Boolean,
                        "TIMESTAMP" => DataType::Timestamp,
                        _ => {
                            Err(anyhow!("Unsupported data type: {}", c.data_type))?;
                            unreachable!();
                        }
                    };
                    cols.insert(c.name, ColumnDefinition { data_type: dt, nullable: true, unique: false });
                }
                let schema = VirtualSchema { table_name: table_name.clone(), columns: cols, primary_key: None };
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
            PhysicalPlan::Insert { table_name, columns, values } => {
                let schema = ctx.schema_cache.get(&table_name);
                let mut row_data = json!({});
                for (i, col_name) in columns.iter().enumerate() {
                    let val_expr = &values[i];
                    let mut val = val_expr.evaluate_with_context(&json!({}), None, ctx.clone()).await?;

                    if let Some(schema) = &schema {
                        if let Some(col_def) = schema.columns.get(col_name) {
                            val = cast_value_to_type(val, &col_def.data_type)?;
                        }
                    }

                    row_data[col_name] = val;
                }

                let pk_col = schema.as_ref().and_then(|s| s.primary_key.clone()).unwrap_or_else(|| "id".to_string());
                let pk = row_data.get(&pk_col)
                    .and_then(|v| v.as_str().map(|s| s.to_string())
                    .or_else(|| v.as_i64().map(|i| i.to_string()))
                    .or_else(|| v.as_f64().map(|f| f.to_string())))
                    .unwrap_or_else(|| Uuid::new_v4().to_string());
                let key = format!("{}:{}", table_name, pk);

                let log_entry = LogEntry::JsonSet { path: key.clone(), value: row_data.to_string() };
                log_and_wait_qe!(ctx.logger, log_entry).await?;

                ctx.db.insert(key, DbValue::Json(row_data));
                yield json!({"rows_affected": 1});
            }
            PhysicalPlan::Delete { from } => {
                let stream = execute(*from, ctx.clone(), outer_row);
                let keys_to_delete: Vec<String> = stream.try_filter_map(|row| async move {
                     let key = row.as_object()
                        .and_then(|obj| obj.values().next())
                        .and_then(|table_obj| table_obj.get("_key"))
                        .and_then(|k| k.as_str())
                        .map(|s| s.to_string());
                    Ok(key)
                }).try_collect().await?;

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
                        let mut new_val = ctx.db.get(&key).map(|v| match v.value() {
                            DbValue::Json(j) => j.clone(),
                            _ => json!({})
                        }).unwrap_or_else(|| json!({}));

                        for (col, expr) in &set {
                            // Evaluate the SET expression against the full row context
                            let mut val = expr.evaluate_with_context(&row, outer_row, ctx.clone()).await?;

                            if let Some(schema) = &schema {
                                if let Some(col_def) = schema.columns.get(col) {
                                    val = cast_value_to_type(val, &col_def.data_type)?;
                                }
                            }
                            new_val[col] = val;
                        }

                        let log_entry = LogEntry::JsonSet { path: key.clone(), value: new_val.to_string() };
                        log_and_wait_qe!(ctx.logger, log_entry).await?;

                        ctx.db.insert(key, DbValue::Json(new_val));
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
                        let dt = match col_def.data_type.to_uppercase().as_str() {
                            "INTEGER" => DataType::Integer,
                            "TEXT" => DataType::Text,
                            "BOOLEAN" => DataType::Boolean,
                            "TIMESTAMP" => DataType::Timestamp,
                            _ => Err(anyhow!("Unsupported data type: {}", col_def.data_type))?,
                        };
                        let new_col = ColumnDefinition { data_type: dt, nullable: true, unique: false };
                        schema.columns.insert(col_def.name, new_col);
                    }
                    AlterTableAction::DropColumn { column_name } => {
                        if schema.columns.remove(&column_name).is_none() {
                             Err(anyhow!("Column '{}' does not exist in table '{}'", column_name, table_name))?;
                        }
                    }
                }

                let schema_bytes = serde_json::to_vec(&schema)?;
                let log_entry = LogEntry::SetBytes { key: schema_key.clone(), value: schema_bytes.clone() };
                log_and_wait_qe!(ctx.logger, log_entry).await?;

                ctx.db.insert(schema_key, DbValue::Bytes(schema_bytes));
                ctx.schema_cache.insert(table_name, Arc::new(schema));

                yield json!({"status": "Table altered"});
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











