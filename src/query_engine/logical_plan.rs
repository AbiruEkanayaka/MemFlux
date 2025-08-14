use anyhow::{anyhow, Result};
use regex;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

use super::ast::*;
use crate::schema::{DataType, VirtualSchema};
use crate::types::{AppContext, FunctionRegistry, SchemaCache};

pub(crate) fn cast_value_to_type(value: Value, target_type: &DataType) -> Result<Value> {
    // Allow nulls to pass through without casting. Nullability is a separate check.
    if value.is_null() {
        return Ok(Value::Null);
    }

    match target_type {
        DataType::Integer => match value {
            Value::Number(n) => {
                if n.is_f64() {
                    Ok(serde_json::json!(n.as_f64().unwrap() as i64))
                } else {
                    Ok(Value::Number(n))
                }
            }
            Value::String(s) => {
                if let Ok(i) = s.parse::<i64>() {
                    Ok(serde_json::json!(i))
                } else if let Ok(f) = s.parse::<f64>() {
                    Ok(serde_json::json!(f as i64))
                } else {
                    Err(anyhow!("Cannot cast string '{}' to INTEGER", s))
                }
            }
            Value::Bool(b) => Ok(serde_json::json!(if b { 1 } else { 0 })),
            _ => Err(anyhow!("Cannot cast {:?} to INTEGER", value)),
        },
        DataType::Text => match value {
            Value::String(s) => Ok(Value::String(s)),
            other => Ok(Value::String(other.to_string())),
        },
        DataType::Boolean => match value {
            Value::Bool(b) => Ok(Value::Bool(b)),
            Value::String(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "t" | "y" | "yes" => Ok(Value::Bool(true)),
                "false" | "0" | "f" | "n" | "no" => Ok(Value::Bool(false)),
                _ => Err(anyhow!("Cannot cast string '{}' to BOOLEAN", s)),
            },
            Value::Number(n) => {
                if n.as_i64() == Some(1) {
                    Ok(Value::Bool(true))
                } else if n.as_i64() == Some(0) {
                    Ok(Value::Bool(false))
                } else {
                    Err(anyhow!("Cannot cast number {} to BOOLEAN", n))
                }
            }
            _ => Err(anyhow!("Cannot cast {:?} to BOOLEAN", value)),
        },
        DataType::Timestamp => {
            // For now, we'll just expect a string and store it as text.
            // A more robust implementation would parse it with chrono.
            match value {
                Value::String(s) => Ok(Value::String(s)),
                _ => Err(anyhow!(
                    "Cannot cast {:?} to TIMESTAMP, expected string",
                    value
                )),
            }
        }
    }
}


use super::execution::execute;
use futures::stream::TryStreamExt;

impl Expression {
    pub fn evaluate_with_context<'a>(
        &'a self,
        row: &'a Value,
        outer_row: Option<&'a Value>,
        ctx: Arc<AppContext>,
    ) -> Pin<Box<dyn futures::Future<Output = Result<Value>> + Send + 'a>> {
        Box::pin(async move {
            match self {
                Expression::Literal(v) => Ok(v.clone()),
                Expression::Column(col_name) => {
                    // Case 1: Qualified name like "user.name" on a nested row from a join
                    if col_name.contains('.') {
                        let pointer = format!("/{}", col_name.replace('.', "/"));
                        if let Some(val) = row.pointer(&pointer) {
                            return Ok(val.clone());
                        }
                        // Check outer row
                        if let Some(outer) = outer_row {
                            if let Some(val) = outer.pointer(&pointer) {
                                return Ok(val.clone());
                            }
                        }
                    }

                    // Case 2: Unqualified name on a nested row (from join)
                    if let Some(obj) = row.as_object() {
                        for sub_obj in obj.values() {
                            if let Some(val) = sub_obj.get(col_name) {
                                return Ok(val.clone());
                            }
                        }
                    }
                    // Check outer row
                    if let Some(outer) = outer_row {
                        if let Some(obj) = outer.as_object() {
                            for sub_obj in obj.values() {
                                if let Some(val) = sub_obj.get(col_name) {
                                    return Ok(val.clone());
                                }
                            }
                        }
                    }

                    // Case 3: Name on a flat row (after projection or from single table scan)
                    if let Some(val) = row.get(col_name) {
                        return Ok(val.clone());
                    }
                    if let Some(outer) = outer_row {
                        if let Some(val) = outer.get(col_name) {
                            return Ok(val.clone());
                        }
                    }

                    Ok(Value::Null)
                }
                Expression::BinaryOp { left, op, right } => {
                    let left_val = left
                        .evaluate_with_context(row, outer_row, ctx.clone())
                        .await?;

                    if *op == Operator::In {
                        if let Expression::Subquery(subquery_plan) = &**right {
                            let physical_plan = super::physical_plan::logical_to_physical_plan(
                                subquery_plan.as_ref().clone(),
                                &ctx.index_manager,
                            )?;
                            let results: Vec<Value> =
                                execute(physical_plan, ctx, Some(row)).try_collect().await?;

                            let subquery_values: Result<Vec<Value>> = results.iter().map(|result_row| {
                                let obj = result_row
                                    .as_object()
                                    .ok_or_else(|| anyhow!("Subquery result is not a JSON object"))?;
                                if obj.len() != 1 {
                                    Err(anyhow!("Subquery for IN operator must return exactly one column"))
                                } else {
                                    Ok(obj.values().next().unwrap().clone())
                                }
                            }).collect();
                            
                            let subquery_values = subquery_values?;

                            return Ok(Value::Bool(subquery_values.contains(&left_val)));
                        }
                    }

                    let right_val = right
                        .evaluate_with_context(row, outer_row, ctx.clone())
                        .await?;

                    if *op == Operator::Like || *op == Operator::ILike {
                        let left_str = match left_val.as_str() {
                            Some(s) => s,
                            None => return Ok(Value::Bool(false)), // LIKE on non-string is false
                        };
                        let right_str = match right_val.as_str() {
                            Some(s) => s,
                            None => return Err(anyhow!("LIKE pattern must be a string")),
                        };

                        let regex_pattern = like_to_regex(right_str);
                        let final_pattern = if *op == Operator::ILike {
                            format!("(?i){}", regex_pattern)
                        } else {
                            regex_pattern
                        };

                        let re = regex::Regex::new(&final_pattern)?;
                        return Ok(Value::Bool(re.is_match(left_str)));
                    }

                    let op_clone = op.clone();

                    let compare_as_f64 = |l: f64, r: f64| match op_clone {
                        Operator::Eq => (l - r).abs() < f64::EPSILON,
                        Operator::NotEq => (l - r).abs() > f64::EPSILON,
                        Operator::Lt => l < r,
                        Operator::LtEq => l <= r,
                        Operator::Gt => l > r,
                        Operator::GtEq => l >= r,
                        _ => false, // Should not be reached for Like/ILike
                    };

                    let result = match (left_val, right_val) {
                        (Value::Number(l), Value::Number(r)) => {
                            compare_as_f64(l.as_f64().unwrap(), r.as_f64().unwrap())
                        }
                        (Value::Number(l), Value::String(r_str)) => r_str
                            .parse::<f64>()
                            .map(|r| compare_as_f64(l.as_f64().unwrap(), r))
                            .unwrap_or(false),
                        (Value::String(l_str), Value::Number(r)) => l_str
                            .parse::<f64>()
                            .map(|l| compare_as_f64(l, r.as_f64().unwrap()))
                            .unwrap_or(false),
                        (Value::String(l), Value::String(r)) => match op_clone {
                            Operator::Eq => {
                                if l == r {
                                    true
                                } else if let (Ok(l_f64), Ok(r_f64)) =
                                    (l.parse::<f64>(), r.parse::<f64>())
                                {
                                    compare_as_f64(l_f64, r_f64)
                                } else {
                                    false
                                }
                            }
                            Operator::NotEq => {
                                if l != r {
                                    true
                                } else if let (Ok(l_f64), Ok(r_f64)) =
                                    (l.parse::<f64>(), r.parse::<f64>())
                                {
                                    !compare_as_f64(l_f64, r_f64)
                                } else {
                                    true
                                }
                            }
                            _ => {
                                if let (Ok(l_f64), Ok(r_f64)) =
                                    (l.parse::<f64>(), r.parse::<f64>())
                                {
                                    compare_as_f64(l_f64, r_f64)
                                } else {
                                    false
                                }
                            }
                        },
                        (l, r) => match op_clone {
                            Operator::Eq => l == r,
                            Operator::NotEq => l != r,
                            _ => false, // Unsupported comparison for other types
                        },
                    };
                    Ok(Value::Bool(result))
                }
                Expression::LogicalOp { left, op, right } => {
                    let left_val = left
                        .evaluate_with_context(row, outer_row, ctx.clone())
                        .await?;
                    match op {
                        LogicalOperator::And => {
                            if left_val.as_bool().unwrap_or(false) {
                                right.evaluate_with_context(row, outer_row, ctx).await
                            } else {
                                Ok(Value::Bool(false))
                            }
                        }
                        LogicalOperator::Or => {
                            if left_val.as_bool().unwrap_or(false) {
                                Ok(Value::Bool(true))
                            } else {
                                right.evaluate_with_context(row, outer_row, ctx).await
                            }
                        }
                    }
                }
                Expression::AggregateFunction { .. } => {
                    Err(anyhow!("Aggregate functions cannot be evaluated in this context"))
                }
                Expression::FunctionCall { func, args } => {
                    let function = ctx
                        .function_registry
                        .get(func)
                        .ok_or_else(|| anyhow!("Function '{}' not found", func))?;

                    let mut evaluated_args = Vec::new();
                    for arg in args {
                        evaluated_args.push(
                            arg.evaluate_with_context(row, outer_row, ctx.clone())
                                .await?,
                        );
                    }

                    function(evaluated_args)
                }
                Expression::Case {
                    when_then_pairs,
                    else_expression,
                } => {
                    for (when_expr, then_expr) in when_then_pairs {
                        if when_expr
                            .evaluate_with_context(row, outer_row, ctx.clone())
                            .await?
                            .as_bool()
                            .unwrap_or(false)
                        {
                            return then_expr
                                .evaluate_with_context(row, outer_row, ctx)
                                .await;
                        }
                    }
                    if let Some(else_expr) = else_expression {
                        return else_expr
                            .evaluate_with_context(row, outer_row, ctx)
                            .await;
                    }
                    Ok(Value::Null)
                }
                Expression::Cast { expr, data_type } => {
                    let value = expr.evaluate_with_context(row, outer_row, ctx).await?;
                    cast_value_to_type(value, data_type)
                }
                Expression::Subquery(subquery_plan) => {
                    // The `row` here is the outer row. We pass it as the `outer_row` for the subquery execution.
                    let physical_plan = super::physical_plan::logical_to_physical_plan(
                        subquery_plan.as_ref().clone(),
                        &ctx.index_manager,
                    )?;

                    let results: Vec<Value> =
                        execute(physical_plan, ctx, Some(row)).try_collect().await?;

                    if results.len() > 1 {
                        return Err(anyhow!("Subquery for '=' operator must return exactly one row"));
                    }
                    if results.is_empty() {
                        return Ok(Value::Null);
                    }

                    let result_row = &results[0];
                    let obj = result_row
                        .as_object()
                        .ok_or_else(|| anyhow!("Subquery result is not a JSON object"))?;

                    if obj.len() != 1 {
                        return Err(anyhow!("Scalar subquery returned more than one column"));
                    }

                    Ok(obj.values().next().unwrap().clone())
                }
            }
        })
    }
}

/// Converts a SQL LIKE pattern to a regex pattern.
fn like_to_regex(pattern: &str) -> String {
    let mut regex = String::with_capacity(pattern.len() * 2);
    regex.push('^');
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            // FIX 1: Use '\\' to match a literal backslash character.
            '\\' => {
                if let Some(escaped_char) = chars.next() {
                    if ".+*?()|[]{}|^$\\".contains(escaped_char) {
                        // FIX 2: Push an escaped backslash.
                        regex.push('\\');
                    }
                    regex.push(escaped_char);
                } else {
                    // This correctly pushes two backslashes to the regex string.
                    regex.push_str("\\\\");
                }
            }
            // Escape other special regex characters
            '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' => {
                // FIX 3: Push an escaped backslash.
                regex.push('\\');
                regex.push(c);
            }
            _ => regex.push(c),
        }
    }
    regex.push('$');
    regex
}

#[derive(Debug, Clone)]
pub enum Expression {
    Column(String),
    Literal(Value),
    BinaryOp {
        left: Box<Expression>,
        op: Operator,
        right: Box<Expression>,
    },
    LogicalOp {
        left: Box<Expression>,
        op: LogicalOperator,
        right: Box<Expression>,
    },
    AggregateFunction {
        func: String,
        arg: Box<Expression>,
    },
    FunctionCall {
        func: String,
        args: Vec<Expression>,
    },
    Case {
        when_then_pairs: Vec<(Box<Expression>, Box<Expression>)>,
        else_expression: Option<Box<Expression>>,
    },
    Cast {
        expr: Box<Expression>,
        data_type: DataType,
    },
    Subquery(Box<LogicalPlan>),
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Column(name) => write!(f, "{}", name),
            Expression::Literal(v) => write!(f, "{}", v),
            Expression::BinaryOp { left, op, right } => write!(f, "{} {} {}", left, op, right),
            Expression::LogicalOp { left, op, right } => write!(f, "{} {} {}", left, op, right),
            Expression::AggregateFunction { func, arg } => {
                if let Expression::Literal(Value::String(s)) = &**arg {
                    if s == "*" {
                        return write!(f, "{}(*)", func);
                    }
                }
                write!(f, "{}({})", func, arg)
            }
            Expression::FunctionCall { func, args } => {
                let args_str = args
                    .iter()
                    .map(|arg| format!("{}", arg))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "{}({})", func, args_str)
            }
            Expression::Case {
                when_then_pairs,
                else_expression,
            } => {
                write!(f, "CASE")?;
                for (when, then) in when_then_pairs {
                    write!(f, " WHEN {} THEN {}", when, then)?;
                }
                if let Some(else_expr) = else_expression {
                    write!(f, " ELSE {}", else_expr)?;
                }
                write!(f, " END")
            }
            Expression::Cast { expr, data_type } => {
                write!(f, "CAST({} AS {})", expr, data_type)
            }
            Expression::Subquery(_) => write!(f, "(SELECT ...subquery...)"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalOperator {
    And,
    Or,
}

impl fmt::Display for LogicalOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalOperator::And => write!(f, "AND"),
            LogicalOperator::Or => write!(f, "OR"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Like,
    ILike,
    In,
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operator::Eq => write!(f, "="),
            Operator::NotEq => write!(f, "!="),
            Operator::Lt => write!(f, "<"),
            Operator::LtEq => write!(f, "<="),
            Operator::Gt => write!(f, ">"),
            Operator::GtEq => write!(f, ">="),
            Operator::Like => write!(f, "LIKE"),
            Operator::ILike => write!(f, "ILIKE"),
            Operator::In => write!(f, "IN"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    TableScan {
        table_name: String,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        condition: Expression,
        join_type: JoinType,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expression,
    },
    Projection {
        input: Box<LogicalPlan>,
        expressions: Vec<(Expression, Option<String>)>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group_expressions: Vec<Expression>,
        agg_expressions: Vec<Expression>,
    },
    Sort {
        input: Box<LogicalPlan>,
        sort_expressions: Vec<(Expression, bool)>, // bool for asc/desc
    },
    Limit {
        input: Box<LogicalPlan>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDef>,
    },
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<Expression>,
    },
    Delete {
        from: Box<LogicalPlan>,
    },
    Update {
        table_name: String,
        from: Box<LogicalPlan>,
        set: Vec<(String, Expression)>,
    },
    DropTable {
        table_name: String,
    },
    AlterTable {
        table_name: String,
        action: AlterTableAction,
    },
    Union {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        all: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    FullOuter,
    Cross,
}

fn simple_expr_to_expression(
    expr: SimpleExpression,
    schema_cache: &SchemaCache,
    function_registry: &FunctionRegistry,
) -> Result<Expression> {
    match expr {
        SimpleExpression::Column(name) => Ok(Expression::Column(name)),
        SimpleExpression::Literal(val) => {
            let json_val = match val {
                SimpleValue::String(s) => Value::String(s),
                SimpleValue::Number(n) => {
                    serde_json::from_str(&n).map_err(|e| anyhow!("Invalid number literal: {}", e))?
                }
                SimpleValue::Boolean(b) => Value::Bool(b),
                SimpleValue::Null => Value::Null,
            };
            Ok(Expression::Literal(json_val))
        }
        SimpleExpression::BinaryOp { left, op, right } => {
            let op = match op.as_str() {
                "=" => Operator::Eq,
                "!=" => Operator::NotEq,
                "<" => Operator::Lt,
                "<=" => Operator::LtEq,
                ">" => Operator::Gt,
                ">=" => Operator::GtEq,
                "LIKE" => Operator::Like,
                "ILIKE" => Operator::ILike,
                "IN" => Operator::In,
                _ => return Err(anyhow!("Unsupported binary operator: {}", op)),
            };
            Ok(Expression::BinaryOp {
                left: Box::new(simple_expr_to_expression(
                    *left,
                    schema_cache,
                    function_registry,
                )?),
                op,
                right: Box::new(simple_expr_to_expression(
                    *right,
                    schema_cache,
                    function_registry,
                )?),
            })
        }
        SimpleExpression::LogicalOp { left, op, right } => {
            let op = match op.as_str() {
                "AND" => LogicalOperator::And,
                "OR" => LogicalOperator::Or,
                _ => return Err(anyhow!("Unsupported logical operator: {}", op)),
            };
            Ok(Expression::LogicalOp {
                left: Box::new(simple_expr_to_expression(
                    *left,
                    schema_cache,
                    function_registry,
                )?),
                op,
                right: Box::new(simple_expr_to_expression(
                    *right,
                    schema_cache,
                    function_registry,
                )?),
            })
        }
        SimpleExpression::AggregateFunction { func, arg } => {
            let arg_expr = if arg == "*" {
                Expression::Literal(Value::String("*".to_string()))
            } else {
                Expression::Column(arg)
            };
            Ok(Expression::AggregateFunction {
                func,
                arg: Box::new(arg_expr),
            })
        }
        SimpleExpression::FunctionCall { func, args } => {
            let args_expr = args
                .into_iter()
                .map(|e| simple_expr_to_expression(e, schema_cache, function_registry))
                .collect::<Result<_>>()?;
            Ok(Expression::FunctionCall {
                func,
                args: args_expr,
            })
        }
        SimpleExpression::Case(case_expr) => {
            let when_then_pairs = case_expr
                .when_then_pairs
                .into_iter()
                .map(|(w, t)| {
                    Ok((
                        Box::new(simple_expr_to_expression(
                            w,
                            schema_cache,
                            function_registry,
                        )?),
                        Box::new(simple_expr_to_expression(
                            t,
                            schema_cache,
                            function_registry,
                        )?),
                    ))
                })
                .collect::<Result<_>>()?;

            let else_expression = if let Some(else_expr) = case_expr.else_expression {
                Some(Box::new(simple_expr_to_expression(
                    *else_expr,
                    schema_cache,
                    function_registry,
                )?))
            } else {
                None
            };

            Ok(Expression::Case {
                when_then_pairs,
                else_expression,
            })
        }
        SimpleExpression::Cast { expr, data_type } => {
            let dt = match data_type.to_uppercase().as_str() {
                "INTEGER" => DataType::Integer,
                "TEXT" => DataType::Text,
                "BOOLEAN" => DataType::Boolean,
                "TIMESTAMP" => DataType::Timestamp,
                _ => return Err(anyhow!("Unsupported data type for CAST: {}", data_type)),
            };
            Ok(Expression::Cast {
                expr: Box::new(simple_expr_to_expression(
                    *expr,
                    schema_cache,
                    function_registry,
                )?),
                data_type: dt,
            })
        }
        SimpleExpression::Subquery(select_statement) => {
            let logical_plan =
                ast_to_logical_plan(AstStatement::Select(*select_statement), schema_cache, function_registry)?;
            Ok(Expression::Subquery(Box::new(logical_plan)))
        }
    }
}

/// Validates that columns used in an expression exist in the provided schemas.
fn validate_expression(
    expr: &SimpleExpression,
    schemas: &HashMap<String, Arc<VirtualSchema>>,
    function_registry: &FunctionRegistry,
) -> Result<()> {
    if schemas.is_empty() {
        // No schemas to validate against, but we can still check functions
        if let SimpleExpression::FunctionCall { func, args } = expr {
            if function_registry.get(func).is_none() {
                return Err(anyhow!("Function '{}' not found", func));
            }
            for arg in args {
                validate_expression(arg, schemas, function_registry)?;
            }
        }
        return Ok(());
    }

    match expr {
        SimpleExpression::Column(name) => {
            if name == "*" {
                return Ok(());
            }
            if name.contains('.') {
                let parts: Vec<&str> = name.split('.').collect();
                if parts.len() != 2 {
                    return Err(anyhow!("Invalid column format: {}. Expected table.column", name));
                }
                let table_name = parts[0];
                let column_name = parts[1];
                if let Some(schema) = schemas.get(table_name) {
                    if !schema.columns.contains_key(column_name) {
                        return Err(anyhow!("Column '{}' does not exist in table '{}'", column_name, table_name));
                    }
                }
            // If table not in schema map, we allow it (schemaless table)
            } else {
                // Unqualified column name. Check if it exists in any of the schemas.
                let mut found_in = Vec::new();
                for (table_name, schema) in schemas.iter() {
                    if schema.columns.contains_key(name) {
                        found_in.push(table_name.clone());
                    }
                }

                if found_in.is_empty() {
                    return Err(anyhow!("Column '{}' not found in any of the tables with schemas: {:?}", name, schemas.keys().collect::<Vec<_>>()));
                }

                if found_in.len() > 1 {
                    return Err(anyhow!("Column '{}' is ambiguous and exists in tables: {:?}", name, found_in));
                }
            }
            Ok(())
        }
        SimpleExpression::BinaryOp { left, right, .. } => {
            validate_expression(left, schemas, function_registry)?;
            validate_expression(right, schemas, function_registry)
        }
        SimpleExpression::LogicalOp { left, right, .. } => {
            validate_expression(left, schemas, function_registry)?;
            validate_expression(right, schemas, function_registry)
        }
        SimpleExpression::AggregateFunction { arg, .. } => {
            if arg != "*" {
                validate_expression(&SimpleExpression::Column(arg.clone()), schemas, function_registry)?;
            }
            Ok(())
        }
        SimpleExpression::FunctionCall { func, args } => {
            if function_registry.get(func).is_none() {
                return Err(anyhow!("Function '{}' not found", func));
            }
            for arg in args {
                validate_expression(arg, schemas, function_registry)?;
            }
            Ok(())
        }
        SimpleExpression::Literal(_) => Ok(()),
        SimpleExpression::Case(case_expr) => {
            for (when_expr, then_expr) in &case_expr.when_then_pairs {
                validate_expression(when_expr, schemas, function_registry)?;
                validate_expression(then_expr, schemas, function_registry)?;
            }
            if let Some(else_expr) = &case_expr.else_expression {
                validate_expression(else_expr, schemas, function_registry)?;
            }
            Ok(())
        }
        SimpleExpression::Cast { expr, .. } => {
            validate_expression(expr, schemas, function_registry)
        }
        SimpleExpression::Subquery(select_statement) => {
            // We need to validate the subquery in its own context.
            let mut sub_schemas = HashMap::new();
            if let Some(schema) = schemas.get(&select_statement.from_table) {
                sub_schemas.insert(select_statement.from_table.clone(), schema.clone());
            }
            for join in &select_statement.joins {
                if let Some(schema) = schemas.get(&join.table) {
                    sub_schemas.insert(join.table.clone(), schema.clone());
                }
            }

            for col in &select_statement.columns {
                validate_expression(&col.expr, &sub_schemas, function_registry)?;
            }
            if let Some(where_clause) = &select_statement.where_clause {
                validate_expression(where_clause, &sub_schemas, function_registry)?;
            }
            // TODO: Validate other parts of the subquery (GROUP BY, ORDER BY etc)
            Ok(())
        }
    }
}

pub fn ast_to_logical_plan(
    statement: AstStatement,
    schema_cache: &SchemaCache,
    function_registry: &FunctionRegistry,
) -> Result<LogicalPlan> {
    match statement {
        AstStatement::CreateTable(statement) => Ok(LogicalPlan::CreateTable {
            table_name: statement.table_name,
            columns: statement.columns,
        }),
        AstStatement::DropTable(statement) => Ok(LogicalPlan::DropTable {
            table_name: statement.table_name,
        }),
        AstStatement::Select(mut statement) => {
            let union_clause = statement.union.take();

            // --- VALIDATION START ---
            let mut schemas_in_scope = HashMap::new();
            if let Some(schema) = schema_cache.get(&statement.from_table) {
                schemas_in_scope.insert(statement.from_table.clone(), schema.clone());
            }
            for join in &statement.joins {
                if let Some(schema) = schema_cache.get(&join.table) {
                    schemas_in_scope.insert(join.table.clone(), schema.clone());
                }
            }

            // We can validate expressions even with no schemas, for function calls
            for col in &statement.columns {
                validate_expression(&col.expr, &schemas_in_scope, function_registry)?;
            }
            if let Some(where_clause) = &statement.where_clause {
                validate_expression(where_clause, &schemas_in_scope, function_registry)?;
            }
            for group_expr in &statement.group_by {
                validate_expression(group_expr, &schemas_in_scope, function_registry)?;
            }
            for order_expr in &statement.order_by {
                validate_expression(&order_expr.expression, &schemas_in_scope, function_registry)?;
            }
            for join in &statement.joins {
                validate_expression(&join.on_condition, &schemas_in_scope, function_registry)?;
            }
            // --- VALIDATION END ---

            let mut plan = LogicalPlan::TableScan {
                table_name: statement.from_table,
            };

            for join in statement.joins {
                let join_type = match join.join_type.to_uppercase().as_str() {
                    "INNER JOIN" => JoinType::Inner,
                    "LEFT JOIN" | "LEFT OUTER JOIN" => JoinType::Left,
                    "RIGHT JOIN" | "RIGHT OUTER JOIN" => JoinType::Right,
                    "FULL JOIN" | "FULL OUTER JOIN" => JoinType::FullOuter,
                    "CROSS JOIN" => JoinType::Cross,
                    _ => return Err(anyhow!("Unsupported join type: {}", join.join_type)),
                };
                plan = LogicalPlan::Join {
                    left: Box::new(plan),
                    right: Box::new(LogicalPlan::TableScan {
                        table_name: join.table,
                    }),
                    condition: simple_expr_to_expression(
                        join.on_condition,
                        schema_cache,
                        function_registry,
                    )?,
                    join_type,
                };
            }

            if let Some(selection) = statement.where_clause {
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: simple_expr_to_expression(
                        selection,
                        schema_cache,
                        function_registry,
                    )?,
                };
            }

            let select_expressions: Vec<(Expression, Option<String>)> = statement
                .columns
                .into_iter()
                .map(|c| {
                    simple_expr_to_expression(c.expr, schema_cache, function_registry)
                        .map(|expr| (expr, c.alias))
                })
                .collect::<Result<_>>()?;

            let mut agg_expressions = Vec::new();
            let mut non_agg_expressions = Vec::new();
            let mut has_aggregate = false;

            for (expr, _alias) in &select_expressions {
                if let Expression::AggregateFunction { .. } = expr {
                    has_aggregate = true;
                    agg_expressions.push(expr.clone());
                } else {
                    non_agg_expressions.push(expr.clone());
                }
            }

            if has_aggregate || !statement.group_by.is_empty() {
                let group_expressions: Vec<Expression> = statement
                    .group_by
                    .clone()
                    .into_iter()
                    .map(|e| simple_expr_to_expression(e, schema_cache, function_registry))
                    .collect::<Result<_>>()?;

                for proj_expr in &non_agg_expressions {
                    if !group_expressions.iter().any(|g_expr| {
                        match (proj_expr, g_expr) {
                            (Expression::Column(p), Expression::Column(g)) => p == g,
                            _ => false,
                        }
                    }) {
                        if let Expression::Column(name) = proj_expr {
                            if name != "*" {
                                return Err(anyhow!(
                        "Column '{}' must appear in the GROUP BY clause or be used in an aggregate function",
                        name
                    ));
                            }
                        }
                    }
                }

                plan = LogicalPlan::Aggregate {
                    input: Box::new(plan),
                    group_expressions,
                    agg_expressions,
                };
            }

            if !statement.order_by.is_empty() {
                plan = LogicalPlan::Sort {
                    input: Box::new(plan),
                    sort_expressions: statement
                        .order_by
                        .into_iter()
                        .map(|o| {
                            simple_expr_to_expression(o.expression, schema_cache, function_registry)
                                .map(|e| (e, o.asc))
                        })
                        .collect::<Result<_>>()?,
                };
            }

            if statement.limit.is_some() || statement.offset.is_some() {
                plan = LogicalPlan::Limit {
                    input: Box::new(plan),
                    limit: statement.limit,
                    offset: statement.offset,
                };
            }

            plan = LogicalPlan::Projection {
                input: Box::new(plan),
                expressions: select_expressions,
            };

            if let Some(union_clause) = union_clause {
                let right_plan = ast_to_logical_plan(
                    AstStatement::Select(*union_clause.select),
                    schema_cache,
                    function_registry,
                )?;
                plan = LogicalPlan::Union {
                    left: Box::new(plan),
                    right: Box::new(right_plan),
                    all: union_clause.all,
                };
            }

            Ok(plan)
        }
        AstStatement::Insert(statement) => {
            if let Some(schema) = schema_cache.get(&statement.table) {
                for col in &statement.columns {
                    if !schema.columns.contains_key(col) {
                        return Err(anyhow!("Column '{}' does not exist in table '{}'", col, statement.table));
                    }
                }
            }

            let values = statement
                .values
                .into_iter()
                .map(|e| simple_expr_to_expression(e, schema_cache, function_registry))
                .collect::<Result<Vec<_>>>()?;
            Ok(LogicalPlan::Insert {
                table_name: statement.table,
                columns: statement.columns,
                values,
            })
        }
        AstStatement::Delete(statement) => {
            let mut schemas_in_scope = HashMap::new();
            if let Some(schema) = schema_cache.get(&statement.from_table) {
                schemas_in_scope.insert(statement.from_table.clone(), schema.clone());
            }

            if let Some(where_clause) = &statement.where_clause {
                validate_expression(where_clause, &schemas_in_scope, function_registry)?;
            }

            let mut plan = LogicalPlan::TableScan {
                table_name: statement.from_table.clone(),
            };
            if let Some(selection) = statement.where_clause {
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: simple_expr_to_expression(selection, schema_cache, function_registry)?,
                };
            }
            Ok(LogicalPlan::Delete {
                from: Box::new(plan),
            })
        }
        AstStatement::Update(statement) => {
            let mut schemas_in_scope = HashMap::new();
            if let Some(schema) = schema_cache.get(&statement.table) {
                schemas_in_scope.insert(statement.table.clone(), schema.clone());
            }

            for (col, expr) in &statement.set {
                if let Some(schema) = schemas_in_scope.get(&statement.table) {
                    if !schema.columns.contains_key(col) {
                        return Err(anyhow!("Column '{}' does not exist in table '{}'", col, statement.table));
                    }
                }
                validate_expression(expr, &schemas_in_scope, function_registry)?;
            }
            if let Some(where_clause) = &statement.where_clause {
                validate_expression(where_clause, &schemas_in_scope, function_registry)?;
            }


            let mut plan = LogicalPlan::TableScan {
                table_name: statement.table.clone(),
            };
            if let Some(selection) = statement.where_clause {
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: simple_expr_to_expression(selection, schema_cache, function_registry)?,
                };
            }
            let set = statement
                .set
                .into_iter()
                .map(|(col, expr)| simple_expr_to_expression(expr, schema_cache, function_registry).map(|e| (col, e)))
                .collect::<Result<Vec<_>>>()?;

            Ok(LogicalPlan::Update {
                table_name: statement.table.clone(),
                from: Box::new(plan),
                set,
            })
        }
        AstStatement::AlterTable(statement) => Ok(LogicalPlan::AlterTable {
            table_name: statement.table_name,
            action: statement.action,
        }),
        AstStatement::TruncateTable(statement) => {
            let plan = LogicalPlan::TableScan {
                table_name: statement.table_name,
            };
            Ok(LogicalPlan::Delete {
                from: Box::new(plan),
            })
        }
    }
}


