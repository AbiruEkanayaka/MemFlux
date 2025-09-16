use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDate, NaiveTime};
use regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;
use hex;

use super::ast::*;
use crate::schema::{DataType, VirtualSchema};
use crate::types::{AppContext, FunctionRegistry, SchemaCache, ViewCache};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OnConflictAction {
    DoNothing,
    DoUpdate(Vec<(String, Expression)>),
}

pub(crate) fn cast_value_to_type(value: Value, target_type: &DataType) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }

    match target_type {
        DataType::SmallInt => match value {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    if i > i16::MAX as i64 || i < i16::MIN as i64 {
                        Err(anyhow!("Value {} out of range for SMALLINT", i))
                    } else {
                        Ok(serde_json::json!(i as i16))
                    }
                } else if let Some(f) = n.as_f64() {
                    if f.is_nan() || f > i16::MAX as f64 || f < i16::MIN as f64 {
                        Err(anyhow!("Value {} out of range for SMALLINT", f))
                    } else {
                        Ok(serde_json::json!(f.trunc() as i16))
                    }
                } else {
                    Err(anyhow!("Invalid numeric value for SMALLINT"))
                }
            },
            Value::String(s) => Ok(serde_json::json!(s.parse::<i16>()?)),
            Value::Bool(b) => Ok(serde_json::json!(if b { 1 } else { 0 })),
            _ => Err(anyhow!("Cannot cast {:?} to SMALLINT", value)),
        },
        DataType::Integer => match value {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    if i > i32::MAX as i64 || i < i32::MIN as i64 {
                        Err(anyhow!("Value {} out of range for INTEGER", i))
                    } else {
                        Ok(serde_json::json!(i as i32))
                    }
                } else if let Some(f) = n.as_f64() {
                    if f.is_nan() || f > i32::MAX as f64 || f < i32::MIN as f64 {
                        Err(anyhow!("Value {} out of range for INTEGER", f))
                    } else {
                        Ok(serde_json::json!(f.trunc() as i32))
                    }
                } else {
                    Err(anyhow!("Invalid numeric value for INTEGER"))
                }
            }
            Value::String(s) => Ok(serde_json::json!(s.parse::<i32>()?)),
            Value::Bool(b) => Ok(serde_json::json!(if b { 1 } else { 0 })),
            _ => Err(anyhow!("Cannot cast {:?} to INTEGER", value)),
        },
        DataType::BigInt => match value {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(serde_json::json!(i))
                } else if let Some(f) = n.as_f64() {
                    if f.is_nan() {
                        Err(anyhow!("NaN value cannot be cast to BIGINT"))
                    } else {
                        Ok(serde_json::json!(f.trunc() as i64))
                    }
                } else {
                    Err(anyhow!("Invalid numeric value for BIGINT"))
                }
            }
            Value::String(s) => Ok(serde_json::json!(s.parse::<i64>()?)),
            Value::Bool(b) => Ok(serde_json::json!(if b { 1 } else { 0 })),
            _ => Err(anyhow!("Cannot cast {:?} to BIGINT", value)),
        },
        DataType::Real => match value {
            Value::Number(n) => Ok(serde_json::json!(n.as_f64().unwrap_or_default() as f32)),
            Value::String(s) => Ok(serde_json::json!(s.parse::<f32>()?)),
            _ => Err(anyhow!("Cannot cast {:?} to REAL", value)),
        },
        DataType::DoublePrecision => match value {
            Value::Number(n) => Ok(serde_json::json!(n.as_f64().unwrap_or_default())),
            Value::String(s) => Ok(serde_json::json!(s.parse::<f64>()?)),
            _ => Err(anyhow!("Cannot cast {:?} to DOUBLE PRECISION", value)),
        },
        DataType::Numeric { scale, .. } => {
            let val = match value {
                Value::Number(n) => n.as_f64().ok_or_else(|| anyhow!("Invalid numeric value"))?,
                Value::String(s) => s.parse::<f64>()?,
                _ => return Err(anyhow!("Cannot cast {:?} to NUMERIC", value)),
            };

            if let Some(s) = scale {
                let multiplier = 10f64.powi(*s as i32);
                Ok(serde_json::json!((val * multiplier).round() / multiplier))
            } else {
                Ok(serde_json::json!(val))
            }
        }
        DataType::Text => match value {
            Value::String(s) => Ok(Value::String(s)),
            other => Ok(Value::String(other.to_string())),
        },
        DataType::Varchar(n) => match value {
            Value::String(s) => {
                if s.len() > *n as usize {
                    Err(anyhow!("Value too long for type VARCHAR({}): len={} > max={}", n, s.len(), n))
                } else {
                    Ok(Value::String(s))
                }
            }
            other => {
                let s = other.to_string();
                if s.len() > *n as usize {
                    Err(anyhow!("Value too long for type VARCHAR({}): len={} > max={}", n, s.len(), n))
                } else {
                    Ok(Value::String(s))
                }
            }
        },
        DataType::Char(n) => match value {
            Value::String(s) => {
                let n = *n as usize;
                if s.len() > n {
                    Ok(Value::String(s.chars().take(n).collect()))
                } else {
                    Ok(Value::String(format!("{:width$}", s, width = n)))
                }
            }
            other => {
                let s = other.to_string();
                let n = *n as usize;
                if s.len() > n {
                    Ok(Value::String(s.chars().take(n).collect()))
                } else {
                    Ok(Value::String(format!("{:width$}", s, width = n)))
                }
            }
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
        DataType::Timestamp => match value {
            Value::String(s) => {
                if DateTime::parse_from_rfc3339(&s).is_ok() {
                    Ok(Value::String(s))
                } else if chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S").is_ok() {
                    Ok(Value::String(s))
                } else {
                    Err(anyhow!("Invalid TIMESTAMP format for string: {}", s))
                }
            }
            _ => Err(anyhow!("Cannot cast {:?} to TIMESTAMP, expected string", value)),
        },
        DataType::TimestampTz => match value {
            Value::String(s) => {
                if DateTime::parse_from_rfc3339(&s).is_ok() {
                    Ok(Value::String(s))
                } else {
                    Err(anyhow!("Invalid TIMESTAMPTZ format for string: {}", s))
                }
            }
            _ => Err(anyhow!("Cannot cast {:?} to TIMESTAMPTZ, expected string", value)),
        },
        DataType::Date => match value {
            Value::String(s) => {
                if NaiveDate::parse_from_str(&s, "%Y-%m-%d").is_ok() {
                    Ok(Value::String(s))
                } else {
                    Err(anyhow!("Invalid DATE format for string: {}. Expected YYYY-MM-DD.", s))
                }
            }
            _ => Err(anyhow!("Cannot cast {:?} to DATE, expected string", value)),
        },
        DataType::Time => match value {
            Value::String(s) => {
                if NaiveTime::parse_from_str(&s, "%H:%M:%S").is_ok() {
                    Ok(Value::String(s))
                } else if NaiveTime::parse_from_str(&s, "%H:%M:%S%.f").is_ok() {
                    Ok(Value::String(s))
                } else {
                    Err(anyhow!("Invalid TIME format for string: {}. Expected HH:MM:SS[.FFF].", s))
                }
            }
            _ => Err(anyhow!("Cannot cast {:?} to TIME, expected string", value)),
        },
        DataType::Bytea => match value {
            Value::String(s) => {
                let s = s.strip_prefix("\\x").unwrap_or(&s);
                match hex::decode(s) {
                    Ok(bytes) => Ok(Value::String(format!("\\x{}", hex::encode(bytes)))),
                    Err(_) => Err(anyhow!("Invalid hex string for BYTEA")),
                }
            }
            _ => Err(anyhow!("Cannot cast {:?} to BYTEA, expected hex string", value)),
        },
        DataType::Uuid => match value {
            Value::String(s) => match Uuid::parse_str(&s) {
                Ok(uuid) => Ok(Value::String(uuid.to_string())),
                Err(_) => Err(anyhow!("Invalid UUID format for string: {}", s)),
            },
            _ => Err(anyhow!("Cannot cast {:?} to UUID, expected string", value)),
        },
        DataType::Array(inner_type) => match value {
            Value::String(s) => {
                let s = s.trim();
                if !s.starts_with('{') || !s.ends_with('}') {
                    return Err(anyhow!("Invalid array literal: {}", s));
                }
                let s = &s[1..s.len() - 1];
                let parts = s.split(',');
                let mut result = Vec::new();
                for part in parts {
                    let part = part.trim();
                    let val = cast_value_to_type(Value::String(part.to_string()), inner_type)?;
                    result.push(val);
                }
                Ok(Value::Array(result))
            }
            Value::Array(arr) => {
                let mut result = Vec::new();
                for val in arr {
                    result.push(cast_value_to_type(val, inner_type)?);
                }
                Ok(Value::Array(result))
            }
            _ => Err(anyhow!("Cannot cast {:?} to ARRAY", value)),
        },
        DataType::JsonB => match value {
            Value::String(s) => {
                let json_val: Value = serde_json::from_str(&s)?;
                Ok(json_val)
            }
            _ => Ok(value),
        },
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
                    // Try to get the column directly from the current row
                    if let Some(val) = row.get(col_name) {
                        return Ok(val.clone());
                    }

                    // If not found directly, and it's a qualified name (e.g., "table.column")
                    if col_name.contains('.') {
                        let parts: Vec<&str> = col_name.split('.').collect();
                        let table_alias = parts[0];
                        let column_name = parts[1];

                        if let Some(table_obj) = row.get(table_alias) {
                            if let Some(val) = table_obj.get(column_name) {
                                return Ok(val.clone());
                            }
                        }
                        if let Some(outer) = outer_row {
                            if let Some(table_obj) = outer.get(table_alias) {
                                if let Some(val) = table_obj.get(column_name) {
                                    return Ok(val.clone());
                                }
                            }
                        }
                    }
                    // If not found as a direct column or qualified name, check nested objects
                    if let Some(obj) = row.as_object() {
                        for sub_obj in obj.values() {
                            if let Some(val) = sub_obj.get(col_name) {
                                return Ok(val.clone());
                            }
                        }
                    }
                    if let Some(outer) = outer_row {
                        if let Some(obj) = outer.as_object() {
                            for sub_obj in obj.values() {
                                if let Some(val) = sub_obj.get(col_name) {
                                    return Ok(val.clone());
                                }
                            }
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
                            let results: Vec<Value> = execute(physical_plan, ctx, Some(row), None).try_collect().await?;

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
                            
                            return Ok(Value::Bool(subquery_values?.contains(&left_val)));
                        } else if let Expression::List(list) = &**right {
                            let mut values = Vec::new();
                            for expr in list {
                                values.push(expr.evaluate_with_context(row, outer_row, ctx.clone()).await?);
                            }
                            return Ok(Value::Bool(values.contains(&left_val)));
                        }
                    }

                    let right_val = right
                        .evaluate_with_context(row, outer_row, ctx.clone())
                        .await?;

                    if matches!(*op, Operator::Plus | Operator::Minus | Operator::Multiply | Operator::Divide) {
                        let l_num = left_val.as_f64();
                        let r_num = right_val.as_f64();

                        if let (Some(l), Some(r)) = (l_num, r_num) {
                            let result = match *op {
                                Operator::Plus => l + r,
                                Operator::Minus => l - r,
                                Operator::Multiply => l * r,
                                Operator::Divide => {
                                    if r == 0.0 {
                                        return Err(anyhow!("Division by zero"));
                                    }
                                    l / r
                                }
                                _ => unreachable!(),
                            };
                            return Ok(serde_json::json!(result));
                        } else {
                            // One of the operands is not a number.
                            return Ok(Value::Null);
                        }
                    }

                    if *op == Operator::Like || *op == Operator::ILike {
                        let left_str = match left_val.as_str() {
                            Some(s) => s,
                            None => return Ok(Value::Bool(false)),
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
                        _ => false,
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
                                if l == r { true } else if let (Ok(l_f64), Ok(r_f64)) = (l.parse::<f64>(), r.parse::<f64>()) { compare_as_f64(l_f64, r_f64) } else { false } }
                            Operator::NotEq => {
                                if l != r { true } else if let (Ok(l_f64), Ok(r_f64)) = (l.parse::<f64>(), r.parse::<f64>()) { !compare_as_f64(l_f64, r_f64) } else { true } }
                            _ => {
                                if let (Ok(l_f64), Ok(r_f64)) = (l.parse::<f64>(), r.parse::<f64>()) { compare_as_f64(l_f64, r_f64) } else { false } }
                        },
                        (l, r) => match op_clone {
                            Operator::Eq => l == r,
                            Operator::NotEq => l != r,
                            _ => false,
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
                    let key = self.to_string();
                    if let Some(val) = row.get(&key) {
                        return Ok(val.clone());
                    }
                    Err(anyhow!("Aggregate function {} used in a context where it was not computed (e.g., HAVING without GROUP BY)", key))
                }
                Expression::FunctionCall { func, args } => {
                    let function = ctx
                        .function_registry
                        .get(func)
                        .ok_or_else(|| anyhow!("Function '{}' not found", func))?;

                    let mut evaluated_args = Vec::new();
                    for arg in args {
                        if let Expression::Subquery(subquery_plan) = arg {
                             let physical_plan = super::physical_plan::logical_to_physical_plan(
                                subquery_plan.as_ref().clone(),
                                &ctx.index_manager,
                            )?;
                            let results: Vec<Value> = execute(physical_plan, ctx.clone(), Some(row), None).try_collect().await?;
                            evaluated_args.push(Value::Array(results));
                        } else {
                            evaluated_args.push(
                                arg.evaluate_with_context(row, outer_row, ctx.clone())
                                    .await?,
                            );
                        }
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
                    let physical_plan = super::physical_plan::logical_to_physical_plan(
                        subquery_plan.as_ref().clone(),
                        &ctx.index_manager,
                    )?;

                    let results: Vec<Value> = execute(physical_plan, ctx, Some(row), None).try_collect().await?;

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
                Expression::List(list) => {
                    let mut evaluated_list = Vec::new();
                    for expr in list {
                        evaluated_list.push(expr.evaluate_with_context(row, outer_row, ctx.clone()).await?);
                    }
                    Ok(Value::Array(evaluated_list))
                }
            }
        })
    }
}

fn like_to_regex(pattern: &str) -> String {
    let mut regex = String::with_capacity(pattern.len() * 2);
    regex.push('^');
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '\\' => {
                if let Some(escaped_char) = chars.next() {
                    if ".+*?()|[]{}|^$\\".contains(escaped_char) {  
                        regex.push('\\');
                    } else if escaped_char == '\\' {
                        regex.push('\\');
                    }
                    regex.push(escaped_char);
                } else {
                    // Trailing backslash, treat as literal
                    regex.push_str("\\\\");
                }
            }
            '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' => {
                regex.push('\\');
                regex.push(c);
            }
            _ => regex.push(c),
        }
    }
    regex.push('$');
    regex
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expression {
    Column(String),
    Literal(Value),
    BinaryOp { left: Box<Expression>, op: Operator, right: Box<Expression> },
    LogicalOp { left: Box<Expression>, op: LogicalOperator, right: Box<Expression> },
    AggregateFunction { func: String, arg: Box<Expression> },
    FunctionCall { func: String, args: Vec<Expression> },
    Case { when_then_pairs: Vec<(Box<Expression>, Box<Expression>)>, else_expression: Option<Box<Expression>> },
    Cast { expr: Box<Expression>, data_type: DataType },
    Subquery(Box<LogicalPlan>),
    List(Vec<Expression>),
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
                let args_str = args.iter().map(|arg| format!("{}", arg)).collect::<Vec<String>>().join(", ");
                write!(f, "{}({})", func, args_str)
            }
            Expression::Case { when_then_pairs, else_expression, } => {
                write!(f, "CASE")?;
                for (when, then) in when_then_pairs {
                    write!(f, " WHEN {} THEN {}", when, then)?;
                }
                if let Some(else_expr) = else_expression {
                    write!(f, " ELSE {}", else_expr)?;
                }
                write!(f, " END")
            }
            Expression::Cast { expr, data_type } => write!(f, "CAST({} AS {})", expr, data_type),
            Expression::Subquery(_) => write!(f, "(SELECT ...subquery...)"),
            Expression::List(list) => {
                let items = list.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", ");
                write!(f, "({})", items)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalOperator { And, Or, }

impl fmt::Display for LogicalOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalOperator::And => write!(f, "AND"),
            LogicalOperator::Or => write!(f, "OR"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operator { Eq, NotEq, Lt, LtEq, Gt, GtEq, Like, ILike, In, Plus, Minus, Multiply, Divide, }

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
            Operator::Plus => write!(f, "+"),
            Operator::Minus => write!(f, "-"),
            Operator::Multiply => write!(f, "*"),
            Operator::Divide => write!(f, "/"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
    TableScan { table_name: String },
    SubqueryScan { alias: String, input: Box<LogicalPlan> },
    RecursiveCteScan {
        alias: String,
        column_aliases: Vec<String>,
        non_recursive: Box<LogicalPlan>,
        recursive: Box<LogicalPlan>,
        union_all: bool,
    },
    WorkingTableScan {
        cte_name: String,
        alias: String,
    },
    Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, condition: Expression, join_type: JoinType },
    Filter { input: Box<LogicalPlan>, predicate: Expression },
    Projection { input: Box<LogicalPlan>, expressions: Vec<(Expression, Option<String>)> },
    Aggregate { input: Box<LogicalPlan>, group_expressions: Vec<Expression>, agg_expressions: Vec<Expression> },
    Sort { input: Box<LogicalPlan>, sort_expressions: Vec<(Expression, bool)> },
    Limit { input: Box<LogicalPlan>, limit: Option<usize>, offset: Option<usize> },
    DistinctOn { input: Box<LogicalPlan>, expressions: Vec<Expression> },
    CreateTable { table_name: String, columns: Vec<ColumnDef>, if_not_exists: bool, constraints: Vec<TableConstraint> },
    CreateSchema { schema_name: String },
    CreateView { view_name: String, query: SelectStatement },
    Insert { table_name: String, columns: Vec<String>, source: Box<LogicalPlan>, on_conflict: Option<(Vec<String>, OnConflictAction)>, returning: Vec<(Expression, Option<String>)> },
    Delete { table_name: String, from: Box<LogicalPlan>, returning: Vec<(Expression, Option<String>)> },
    Update { table_name: String, from: Box<LogicalPlan>, set: Vec<(String, Expression)>, returning: Vec<(Expression, Option<String>)> },
    DropTable { table_name: String },
    DropView { view_name: String },
    AlterTable { table_name: String, action: AlterTableAction },
    UnionAll { left: Box<LogicalPlan>, right: Box<LogicalPlan> },
    Intersect { left: Box<LogicalPlan>, right: Box<LogicalPlan> },
    Except { left: Box<LogicalPlan>, right: Box<LogicalPlan> },
    CreateIndex { statement: CreateIndexStatement },
    Values { values: Vec<Vec<Expression>> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType { Inner, Left, Right, FullOuter, Cross, }

pub(crate) fn simple_expr_to_expression(
    expr: SimpleExpression,
    schema_cache: &SchemaCache,
    view_cache: &ViewCache,
    function_registry: &FunctionRegistry,
    active_recursive_alias: Option<&str>,
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
                "+" => Operator::Plus,
                "-" => Operator::Minus,
                "*" => Operator::Multiply,
                "/" => Operator::Divide,
                _ => return Err(anyhow!("Unsupported binary operator: {}", op)),
            };
            Ok(Expression::BinaryOp {
                left: Box::new(simple_expr_to_expression(
                    *left, schema_cache, view_cache, function_registry, active_recursive_alias
                )?),
                op,
                right: Box::new(simple_expr_to_expression(
                    *right, schema_cache, view_cache, function_registry, active_recursive_alias
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
                    *left, schema_cache, view_cache, function_registry, active_recursive_alias
                )?),
                op,
                right: Box::new(simple_expr_to_expression(
                    *right, schema_cache, view_cache, function_registry, active_recursive_alias
                )?),
            })
        }
        SimpleExpression::AggregateFunction { func, arg } => {
            let arg_expr = if arg == "*" {
                Expression::Literal(Value::String("* ".to_string()))
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
                .map(|e| simple_expr_to_expression(e, schema_cache, view_cache, function_registry, active_recursive_alias))
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
                            w, schema_cache, view_cache, function_registry, active_recursive_alias
                        )?),
                        Box::new(simple_expr_to_expression(
                            t, schema_cache, view_cache, function_registry, active_recursive_alias
                        )?),
                    ))
                })
                .collect::<Result<_>>()?;

            let else_expression = if let Some(else_expr) = case_expr.else_expression {
                Some(Box::new(simple_expr_to_expression(
                    *else_expr, schema_cache, view_cache, function_registry, active_recursive_alias
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
            let dt = DataType::from_str(&data_type)?;
            Ok(Expression::Cast {
                expr: Box::new(simple_expr_to_expression(
                    *expr, schema_cache, view_cache, function_registry, active_recursive_alias
                )?),
                data_type: dt,
            })
        }
        SimpleExpression::Subquery(select_statement) => {
            let logical_plan =
                ast_to_logical_plan_inner(AstStatement::Select(*select_statement), schema_cache, view_cache, function_registry, active_recursive_alias)?;
            Ok(Expression::Subquery(Box::new(logical_plan)))
        }
        SimpleExpression::List(list) => {
            let expressions = list
                .into_iter()
                .map(|e| simple_expr_to_expression(e, schema_cache, view_cache, function_registry, active_recursive_alias))
                .collect::<Result<_>>()?;
            Ok(Expression::List(expressions))
        }
    }
}

/// Validates that columns used in an expression exist in the provided schemas.
fn validate_expression(
    expr: &SimpleExpression,
    schemas: &HashMap<String, Arc<VirtualSchema>>,
    view_cache: &ViewCache,
    function_registry: &FunctionRegistry,
) -> Result<()> {
    if schemas.is_empty() {
        // No schemas to validate against, but we can still check functions
        if let SimpleExpression::FunctionCall { func, args } = expr {
            if function_registry.get(func).is_none() {
                return Err(anyhow!("Function '{}' not found", func));
            }
            for arg in args {
                validate_expression(arg, schemas, view_cache, function_registry)?;
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
            validate_expression(left, schemas, view_cache, function_registry)?;
            validate_expression(right, schemas, view_cache, function_registry)
        }
        SimpleExpression::LogicalOp { left, right, .. } => {
            validate_expression(left, schemas, view_cache, function_registry)?;
            validate_expression(right, schemas, view_cache, function_registry)
        }
        SimpleExpression::AggregateFunction { arg, .. } => {
            if arg != "*" {
                validate_expression(&SimpleExpression::Column(arg.clone()), schemas, view_cache, function_registry)?;
            }
            Ok(())
        }
        SimpleExpression::FunctionCall { func, args } => {
            if function_registry.get(func).is_none() {
                return Err(anyhow!("Function '{}' not found", func));
            }
            for arg in args {
                validate_expression(arg, schemas, view_cache, function_registry)?;
            }
            Ok(())
        }
        SimpleExpression::Literal(_) => Ok(()),
        SimpleExpression::Case(case_expr) => {
            for (when_expr, then_expr) in &case_expr.when_then_pairs {
                validate_expression(when_expr, schemas, view_cache, function_registry)?;
                validate_expression(then_expr, schemas, view_cache, function_registry)?;
            }
            if let Some(else_expr) = &case_expr.else_expression {
                validate_expression(else_expr, schemas, view_cache, function_registry)?;
            }
            Ok(())
        }
        SimpleExpression::Cast { expr, .. } => {
            validate_expression(expr, schemas, view_cache, function_registry)
        }
        SimpleExpression::Subquery(select_statement) => {
            // We need to validate the subquery in its own context.
            let mut sub_schemas = HashMap::new();
            if let TableReference::Table { name, alias } = &select_statement.from {
                if let Some(schema) = schemas.get(name) {
                    sub_schemas.insert(alias.clone().unwrap_or_else(|| name.clone()), schema.clone());
                }
            }
            for join in &select_statement.joins {
                if let TableReference::Table { name, alias } = &join.table {
                    if let Some(schema) = schemas.get(name) {
                        sub_schemas.insert(alias.clone().unwrap_or_else(|| name.clone()), schema.clone());
                    }
                }
            }

            for col in &select_statement.columns {
                validate_expression(&col.expr, &sub_schemas, view_cache, function_registry)?;
            }
            if let Some(where_clause) = &select_statement.where_clause {
                validate_expression(where_clause, &sub_schemas, view_cache, function_registry)?;
            }
            // TODO: Validate other parts of the subquery (GROUP BY, ORDER BY etc)
            Ok(())
        }
        SimpleExpression::List(list) => {
            for expr in list {
                validate_expression(expr, schemas, view_cache, function_registry)?;
            }
            Ok(())
        }
    }
}

pub fn ast_to_logical_plan(
    statement: AstStatement,
    schema_cache: &SchemaCache,
    view_cache: &ViewCache,
    function_registry: &FunctionRegistry,
) -> Result<LogicalPlan> {
    ast_to_logical_plan_inner(statement, schema_cache, view_cache, function_registry, None)
}

fn ast_to_logical_plan_inner(
    statement: AstStatement,
    schema_cache: &SchemaCache,
    view_cache: &ViewCache,
    function_registry: &FunctionRegistry,
    active_recursive_alias: Option<&str>,
) -> Result<LogicalPlan> {
    match statement {
        AstStatement::CreateSchema(statement) => Ok(LogicalPlan::CreateSchema {
            schema_name: statement.schema_name,
        }),
        AstStatement::CreateView(statement) => Ok(LogicalPlan::CreateView {
            view_name: statement.view_name,
            query: statement.query,
        }),
        AstStatement::CreateTable(statement) => {
            Ok(LogicalPlan::CreateTable {
                table_name: statement.table_name,
                columns: statement.columns,
                if_not_exists: statement.if_not_exists,
                constraints: statement.constraints,
            })
        }
        AstStatement::DropTable(statement) => Ok(LogicalPlan::DropTable {
            table_name: statement.table_name,
        }),
        AstStatement::DropView(statement) => Ok(LogicalPlan::DropView {
            view_name: statement.view_name,
        }),
        AstStatement::Select(mut statement) => {
            let temp_view_cache = view_cache.clone();
            let mut planned_ctes: HashMap<String, LogicalPlan> = HashMap::new();

            if let Some(with_clause) = statement.with.take() {
                if with_clause.recursive {
                    if with_clause.ctes.len() > 1 {
                        return Err(anyhow!("Only one CTE is supported in a RECURSIVE WITH clause for now."));
                    }
                    let cte = &with_clause.ctes[0];
                    let cte_alias = cte.alias.clone();

                    let (non_recursive_query, recursive_query, union_all) = if let Some(set_op) = &cte.query.set_operator {
                        if set_op.operator != SetOperatorType::Union {
                            return Err(anyhow!("Recursive CTE must use UNION or UNION ALL"));
                        }
                        let mut non_recursive_query = cte.query.clone();
                        non_recursive_query.set_operator = None;
                        (non_recursive_query, *set_op.select.clone(), set_op.all)
                    } else {
                        return Err(anyhow!("Recursive CTE query must be a UNION or UNION ALL"));
                    };

                    let non_recursive_plan = ast_to_logical_plan_inner(AstStatement::Select(non_recursive_query), schema_cache, &temp_view_cache, function_registry, None)?;
                    
                    let recursive_plan = ast_to_logical_plan_inner(AstStatement::Select(recursive_query), schema_cache, &temp_view_cache, function_registry, Some(&cte_alias))?;

                    let recursive_cte_plan = LogicalPlan::RecursiveCteScan {
                        alias: cte_alias.clone(),
                        column_aliases: cte.column_names.clone(),
                        non_recursive: Box::new(non_recursive_plan),
                        recursive: Box::new(recursive_plan),
                        union_all,
                    };
                    planned_ctes.insert(cte_alias, recursive_cte_plan);
                } else {
                    for cte in with_clause.ctes {
                        if view_cache.contains_key(&cte.alias) || planned_ctes.contains_key(&cte.alias) {
                            return Err(anyhow!("CTE name '{}' conflicts with an existing view or another CTE.", cte.alias));
                        }
                        if temp_view_cache.contains_key(&cte.alias) {
                            return Err(anyhow!("Duplicate CTE name '{}' in WITH clause.", cte.alias));
                        }
                        let view_def = crate::types::ViewDefinition {
                            name: cte.alias.clone(),
                            columns: cte.column_names,
                            query: cte.query,
                        };
                        temp_view_cache.insert(cte.alias.clone(), Arc::new(view_def));
                    }
                }
            }

            let view_cache = &temp_view_cache;

            let set_operator_clause = statement.set_operator.take();

            // --- VALIDATION START ---
            let mut schemas_in_scope = HashMap::new();
            match &statement.from {
                TableReference::Table { name, alias } => {
                    if let Some(schema) = schema_cache.get(name) {
                        schemas_in_scope.insert(alias.clone().unwrap_or_else(|| name.clone()), schema.clone());
                    }
                }
                TableReference::Subquery(_, _alias) => {
                    // Subquery validation is complex; we'd need to infer its output schema.
                    // For now, we skip validation for columns coming from a subquery.
                }
            }

            for join in &statement.joins {
                if let TableReference::Table { name, alias } = &join.table {
                    if let Some(schema) = schema_cache.get(name) {
                        schemas_in_scope.insert(alias.clone().unwrap_or_else(|| name.clone()), schema.clone());
                    }
                }
            }

            // We can validate expressions even with no schemas, for function calls
            for col in &statement.columns {
                validate_expression(&col.expr, &schemas_in_scope, view_cache, function_registry)?;
            }
            if let Some(where_clause) = &statement.where_clause {
                validate_expression(where_clause, &schemas_in_scope, view_cache, function_registry)?;
            }
            for group_expr in &statement.group_by {
                validate_expression(group_expr, &schemas_in_scope, view_cache, function_registry)?;
            }
            for order_expr in &statement.order_by {
                validate_expression(&order_expr.expression, &schemas_in_scope, view_cache, function_registry)?;
            }
            for join in &statement.joins {
                validate_expression(&join.on_condition, &schemas_in_scope, view_cache, function_registry)?;
            }
            // --- VALIDATION END ---

            let mut plan = match statement.from {
                TableReference::Table { name, alias } => {
                    if let Some(cte_plan) = planned_ctes.get(&name) {
                        let base_plan = cte_plan.clone();
                        if let Some(alias_name) = alias {
                            LogicalPlan::SubqueryScan { alias: alias_name, input: Box::new(base_plan) }
                        } else {
                            base_plan
                        }
                    } else if Some(name.as_str()) == active_recursive_alias {
                        LogicalPlan::WorkingTableScan {
                            cte_name: name.clone(),
                            alias: alias.unwrap_or(name),
                        }
                    } else {
                        let base_plan = if let Some(view_def) = view_cache.get(&name) {
                            ast_to_logical_plan_inner(AstStatement::Select(view_def.query.clone()), schema_cache, view_cache, function_registry, active_recursive_alias)?
                        } else {
                            LogicalPlan::TableScan { table_name: name }
                        };

                        if let Some(alias_name) = alias {
                            LogicalPlan::SubqueryScan { alias: alias_name, input: Box::new(base_plan) }
                        } else {
                            base_plan
                        }
                    }
                }
                TableReference::Subquery(subquery_select, alias) => {
                    let sub_plan = ast_to_logical_plan_inner(AstStatement::Select(*subquery_select), schema_cache, view_cache, function_registry, active_recursive_alias)?;
                    LogicalPlan::SubqueryScan {
                        alias,
                        input: Box::new(sub_plan),
                    }
                }
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

                let right_plan = match join.table {
                    TableReference::Table { name, alias } => {
                        if Some(name.as_str()) == active_recursive_alias {
                            LogicalPlan::WorkingTableScan {
                                cte_name: name.clone(),
                                alias: alias.unwrap_or(name),
                            }
                        } else {
                            let base_plan = if let Some(view_def) = view_cache.get(&name) {
                                ast_to_logical_plan_inner(AstStatement::Select(view_def.query.clone()), schema_cache, view_cache, function_registry, active_recursive_alias)?
                            } else {
                                LogicalPlan::TableScan { table_name: name }
                            };
                            if let Some(alias_name) = alias {
                                LogicalPlan::SubqueryScan { alias: alias_name, input: Box::new(base_plan) }
                            } else {
                                base_plan
                            }
                        }
                    }
                    TableReference::Subquery(subquery_select, alias) => {
                        let sub_plan = ast_to_logical_plan_inner(AstStatement::Select(*subquery_select), schema_cache, view_cache, function_registry, active_recursive_alias)?;
                        LogicalPlan::SubqueryScan {
                            alias,
                            input: Box::new(sub_plan),
                        }
                    }
                };

                plan = LogicalPlan::Join {
                    left: Box::new(plan),
                    right: Box::new(right_plan),
                    condition: simple_expr_to_expression(
                        join.on_condition, 
                        schema_cache, view_cache, function_registry, active_recursive_alias
                    )?,
                    join_type,
                };
            }

            if let Some(selection) = statement.where_clause {
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: simple_expr_to_expression(
                        selection,
                        schema_cache, view_cache, function_registry, active_recursive_alias
                    )?,
                };
            }

            let select_expressions: Vec<(Expression, Option<String>)> = statement
                .columns
                .into_iter()
                .map(|c| {
                    simple_expr_to_expression(c.expr, schema_cache, view_cache, function_registry, active_recursive_alias)
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
                    .map(|e| simple_expr_to_expression(e, schema_cache, view_cache, function_registry, active_recursive_alias))
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

                if let Some(having_clause) = statement.having_clause {
                    plan = LogicalPlan::Filter {
                        input: Box::new(plan),
                        predicate: simple_expr_to_expression(
                            having_clause,
                            schema_cache, view_cache, function_registry, active_recursive_alias
                        )?,
                    };
                }
            }

            if let Some(set_operator_clause) = set_operator_clause {
                // Arity validation
                let op_str = match set_operator_clause.operator {
                    SetOperatorType::Union => "UNION",
                    SetOperatorType::Intersect => "INTERSECT",
                    SetOperatorType::Except => "EXCEPT",
                };
                if select_expressions.len() != set_operator_clause.select.columns.len() {
                    return Err(anyhow!(
                        "SELECTs to the left and right of {} have different number of columns",
                        op_str
                    ));
                }

                // With a set operator, we project the left side first.
                plan = LogicalPlan::Projection {
                    input: Box::new(plan),
                    expressions: select_expressions.clone(),
                };

                // Get the output column names from the left-hand side projection.
                let left_output_names: Vec<String> = select_expressions
                    .iter()
                    .map(|(expr, alias)| alias.clone().unwrap_or_else(|| expr.to_string()))
                    .collect();

                // Build the right-hand side plan.
                let right_plan_unprojected = ast_to_logical_plan_inner(
                    AstStatement::Select(*set_operator_clause.select),
                    schema_cache, view_cache, function_registry, active_recursive_alias
                )?;

                // The right plan will have a projection on top. We need to deconstruct it
                // and rebuild it with the left side's aliases.
                let (right_input, right_expressions) =
                    if let LogicalPlan::Projection { input, expressions } = right_plan_unprojected {
                        (input, expressions)
                    } else {
                        // This should not be reached if the right side is a valid SELECT query
                        return Err(anyhow!("Right side of set operator did not produce a projection plan"));
                    };

                // Create a new projection for the right side, using its original expressions
                // but with the aliases from the left side.
                let right_reprojection_exprs: Vec<(Expression, Option<String>)> = right_expressions
                    .into_iter()
                    .map(|(expr, _)| expr) // take the expression, discard old alias
                    .zip(left_output_names.iter())
                    .map(|(expr, alias)| (expr, Some(alias.clone())))
                    .collect();

                let right_plan_reprojected = LogicalPlan::Projection {
                    input: right_input,
                    expressions: right_reprojection_exprs,
                };

                plan = match set_operator_clause.operator {
                    SetOperatorType::Union => {
                        if set_operator_clause.all {
                            LogicalPlan::UnionAll { left: Box::new(plan), right: Box::new(right_plan_reprojected) }
                        } else {
                            // UNION (DISTINCT) is UnionAll followed by DistinctOn
                            let union_all_plan = LogicalPlan::UnionAll { left: Box::new(plan), right: Box::new(right_plan_reprojected) };
                            let distinct_expressions = left_output_names
                                .into_iter()
                                .map(Expression::Column)
                                .collect();
                            LogicalPlan::DistinctOn {
                                input: Box::new(union_all_plan),
                                expressions: distinct_expressions,
                            }
                        }
                    },
                    SetOperatorType::Intersect => LogicalPlan::Intersect { left: Box::new(plan), right: Box::new(right_plan_reprojected) },
                    SetOperatorType::Except => LogicalPlan::Except { left: Box::new(plan), right: Box::new(right_plan_reprojected) },
                };

                // Now apply Sort, DistinctOn, and Limit to the combined plan.
                if !statement.order_by.is_empty() {
                    plan = LogicalPlan::Sort {
                        input: Box::new(plan),
                        sort_expressions: statement
                            .order_by
                            .into_iter()
                            .map(|o| {
                                simple_expr_to_expression(o.expression, schema_cache, view_cache, function_registry, active_recursive_alias)
                                    .map(|e| (e, o.asc))
                            })
                            .collect::<Result<_>>()?,
                    };
                }

                if !statement.distinct_on.is_empty() {
                    let distinct_expressions: Vec<Expression> = statement
                        .distinct_on
                        .into_iter()
                        .map(|e| simple_expr_to_expression(e, schema_cache, view_cache, function_registry, active_recursive_alias))
                        .collect::<Result<_>>()?;
                    plan = LogicalPlan::DistinctOn {
                        input: Box::new(plan),
                        expressions: distinct_expressions,
                    };
                }

                if statement.limit.is_some() || statement.offset.is_some() {
                    plan = LogicalPlan::Limit {
                        input: Box::new(plan),
                        limit: statement.limit,
                        offset: statement.offset,
                    };
                }
            } else {
                // No set operator, original logic: Sort -> Project -> Limit
                if !statement.order_by.is_empty() {
                    plan = LogicalPlan::Sort {
                        input: Box::new(plan),
                        sort_expressions: statement
                            .order_by
                            .into_iter()
                            .map(|o| {
                                simple_expr_to_expression(o.expression, schema_cache, view_cache, function_registry, active_recursive_alias)
                                    .map(|e| (e, o.asc))
                            })
                            .collect::<Result<_>>()?,
                    };
                }

                plan = LogicalPlan::Projection {
                    input: Box::new(plan),
                    expressions: select_expressions.clone(),
                };

                if !statement.distinct_on.is_empty() {
                    let distinct_expressions: Vec<Expression> = statement
                        .distinct_on
                        .into_iter()
                        .map(|e| simple_expr_to_expression(e, schema_cache, view_cache, function_registry, active_recursive_alias))
                        .collect::<Result<_>>()?;
                    plan = LogicalPlan::DistinctOn {
                        input: Box::new(plan),
                        expressions: distinct_expressions,
                    };
                }

                if statement.limit.is_some() || statement.offset.is_some() {
                    plan = LogicalPlan::Limit {
                        input: Box::new(plan),
                        limit: statement.limit,
                        offset: statement.offset,
                    };
                }
            }

            Ok(plan)
        }
        AstStatement::Insert(statement) => {
            if !statement.columns.is_empty() {
                if let Some(schema) = schema_cache.get(&statement.table) {
                    for col in &statement.columns {
                        if !schema.columns.contains_key(col) {
                            return Err(anyhow!("Column '{}' does not exist in table '{}'", col, statement.table));
                        }
                    }
                }
            }

            let source_plan = match statement.source {
                InsertSource::Values(values) => {
                    let expressions = values
                        .into_iter()
                        .map(|row_values| {
                            row_values
                                .into_iter()
                                .map(|e| {
                                    simple_expr_to_expression(e, schema_cache, view_cache, function_registry, active_recursive_alias)
                                })
                                .collect::<Result<Vec<_>>>()
                        })
                        .collect::<Result<Vec<Vec<_>>>>()?;
                    // Validate that all rows have the same number of values
                    if !expressions.is_empty() {
                        let expected_len = expressions[0].len();
                        for (idx, row) in expressions.iter().enumerate().skip(1) {
                            if row.len() != expected_len {
                                return Err(anyhow!("VALUES row {} has {} values, expected {}", idx + 1, row.len(), expected_len));
                            }
                        }
                    }
                    LogicalPlan::Values { values: expressions }
                }
                InsertSource::Select(select_stmt) => {
                    ast_to_logical_plan_inner(
                        AstStatement::Select(*select_stmt),
                        schema_cache, view_cache, function_registry, active_recursive_alias
                    )?
                }
            };

            let returning = statement
                .returning
                .into_iter()
                .map(|c| {
                    simple_expr_to_expression(c.expr, schema_cache, view_cache, function_registry, active_recursive_alias)
                        .map(|expr| (expr, c.alias))
                })
                .collect::<Result<_>>()?;

            let on_conflict_action = if let Some((target, action)) = statement.on_conflict {
                let new_action = match action {
                    OnConflict::DoNothing => OnConflictAction::DoNothing,
                    OnConflict::DoUpdate(set) => {
                        let new_set = set
                            .into_iter()
                            .map(|(col, expr)| {
                                simple_expr_to_expression(expr, schema_cache, view_cache, function_registry, active_recursive_alias).map(|e| (col, e))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        OnConflictAction::DoUpdate(new_set)
                    }
                };
                Some((target, new_action))
            } else {
                None
            };

            Ok(LogicalPlan::Insert {
                table_name: statement.table,
                columns: statement.columns,
                source: Box::new(source_plan),
                on_conflict: on_conflict_action,
                returning,
            })
        }
        AstStatement::Delete(statement) => {
            let mut schemas_in_scope = HashMap::new();
            if let Some(schema) = schema_cache.get(&statement.from_table) {
                schemas_in_scope.insert(statement.from_table.clone(), schema.clone());
            }
            for using_table in &statement.using_list {
                if let Some(schema) = schema_cache.get(using_table) {
                    schemas_in_scope.insert(using_table.clone(), schema.clone());
                }
            }

            if let Some(where_clause) = &statement.where_clause {
                validate_expression(where_clause, &schemas_in_scope, view_cache, function_registry)?;
            }

            let mut plan = LogicalPlan::TableScan {
                table_name: statement.from_table.clone(),
            };

            if !statement.using_list.is_empty() {
                let mut from_plan = plan;
                for using_table in statement.using_list {
                    let right_plan = LogicalPlan::TableScan { table_name: using_table };
                    from_plan = LogicalPlan::Join {
                        left: Box::new(from_plan),
                        right: Box::new(right_plan),
                        condition: Expression::Literal(Value::Bool(true)), // Cross join, filter in WHERE
                        join_type: JoinType::Cross,
                    };
                }
                plan = from_plan;
            }

            if let Some(selection) = statement.where_clause {
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: simple_expr_to_expression(selection, schema_cache, view_cache, function_registry, active_recursive_alias)?,
                };
            }

            let returning = statement
                .returning
                .into_iter()
                .map(|c| {
                    simple_expr_to_expression(c.expr, schema_cache, view_cache, function_registry, active_recursive_alias)
                        .map(|expr| (expr, c.alias))
                })
                .collect::<Result<_>>()?;

            Ok(LogicalPlan::Delete {
                table_name: statement.from_table,
                from: Box::new(plan),
                returning,
            })
        }
        AstStatement::Update(statement) => {
            let mut schemas_in_scope = HashMap::new();
            if let Some(schema) = schema_cache.get(&statement.table) {
                schemas_in_scope.insert(statement.table.clone(), schema.clone());
            }
            for from_table in &statement.from_list {
                if let Some(schema) = schema_cache.get(from_table) {
                    schemas_in_scope.insert(from_table.clone(), schema.clone());
                }
            }

            for (col, expr) in &statement.set {
                if let Some(schema) = schemas_in_scope.get(&statement.table) {
                    if !schema.columns.contains_key(col) {
                        return Err(anyhow!("Column '{}' does not exist in table '{}'", col, statement.table));
                    }
                }
                validate_expression(expr, &schemas_in_scope, view_cache, function_registry)?;
            }
            if let Some(where_clause) = &statement.where_clause {
                validate_expression(where_clause, &schemas_in_scope, view_cache, function_registry)?;
            }

            let mut plan = LogicalPlan::TableScan {
                table_name: statement.table.clone(),
            };

            if !statement.from_list.is_empty() {
                let mut from_plan = plan;
                for from_table in statement.from_list {
                    let right_plan = LogicalPlan::TableScan { table_name: from_table };
                    from_plan = LogicalPlan::Join {
                        left: Box::new(from_plan),
                        right: Box::new(right_plan),
                        condition: Expression::Literal(Value::Bool(true)), // Cross join, filter in WHERE
                        join_type: JoinType::Cross,
                    };
                }
                plan = from_plan;
            }

            if let Some(selection) = statement.where_clause {
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: simple_expr_to_expression(selection, schema_cache, view_cache, function_registry, active_recursive_alias)?,
                };
            }
            let set = statement
                .set
                .into_iter()
                .map(|(col, expr)| simple_expr_to_expression(expr, schema_cache, view_cache, function_registry, active_recursive_alias).map(|e| (col, e)))
                .collect::<Result<Vec<_>>>()?;

            let returning = statement
                .returning
                .into_iter()
                .map(|c| {
                    simple_expr_to_expression(c.expr, schema_cache, view_cache, function_registry, active_recursive_alias)
                        .map(|expr| (expr, c.alias))
                })
                .collect::<Result<_>>()?;

            Ok(LogicalPlan::Update {
                table_name: statement.table.clone(),
                from: Box::new(plan),
                set,
                returning,
            })
        }
        AstStatement::AlterTable(statement) => Ok(LogicalPlan::AlterTable {
            table_name: statement.table_name,
            action: statement.action,
        }),
        AstStatement::TruncateTable(statement) => {
            let plan = LogicalPlan::TableScan {
                table_name: statement.table_name.clone(),
            };
            Ok(LogicalPlan::Delete {
                table_name: statement.table_name,
                from: Box::new(plan),
                returning: Vec::new(),
            }) 
        },
        AstStatement::CreateIndex(statement) => Ok(LogicalPlan::CreateIndex { statement }),
    }
}