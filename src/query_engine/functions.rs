use anyhow::{anyhow, Result};
use chrono::{DateTime, Datelike, Local, Timelike};
use serde_json::{json, Value};

pub fn register_string_functions(registry: &mut crate::types::FunctionRegistry) {
    registry.register("LOWER", Box::new(lower));
    registry.register("UPPER", Box::new(upper));
    registry.register("LENGTH", Box::new(length));
    registry.register("TRIM", Box::new(trim));
    registry.register("SUBSTRING", Box::new(substring));
    registry.register("IS_NULL", Box::new(is_null));
    registry.register("IS_NOT_NULL", Box::new(is_not_null));
    registry.register("EXISTS", Box::new(exists));
    registry.register("=_ANY", Box::new(eq_any));
    registry.register("!=_ANY", Box::new(ne_any));
    registry.register(">_ANY", Box::new(gt_any));
    registry.register("<_ANY", Box::new(lt_any));
    registry.register(">=_ANY", Box::new(ge_any));
    registry.register("<=_ANY", Box::new(le_any));
    registry.register("=_ALL", Box::new(eq_all));
    registry.register("!=_ALL", Box::new(ne_all));
    registry.register(">_ALL", Box::new(gt_all));
    registry.register("<_ALL", Box::new(lt_all));
    registry.register(">=_ALL", Box::new(ge_all));
    registry.register("<=_ALL", Box::new(le_all));
}

fn compare_values(op: &str, left: &Value, right: &Value) -> Result<bool> {
    match op {
        "=" => Ok(left == right),
        "!=" => Ok(left != right),
        ">" | "<" | ">=" | "<=" => {
            let l_num = left.as_f64();
            let r_num = right.as_f64();

            if let (Some(l), Some(r)) = (l_num, r_num) {
                match op {
                    ">" => Ok(l > r),
                    "<" => Ok(l < r),
                    ">=" => Ok(l >= r),
                    "<=" => Ok(l <= r),
                    _ => unreachable!(), // Should not happen due to outer match
                }
            } else {
                // If either is not a number, numeric comparison is false
                Ok(false)
            }
        }
        _ => Err(anyhow!("Unsupported comparison operator: {}", op)),
    }
}

fn handle_any_all(op: &str, quantifier: &str, args: Vec<Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("{} {} expects 2 arguments", op, quantifier));
    }
    let left_val = &args[0];
    let subquery_result = match &args[1] {
        Value::Array(arr) => arr,
        _ => return Err(anyhow!("Second argument to {} {} must be an array (subquery result)", op, quantifier)),
    };

    if subquery_result.is_empty() {
        return Ok(json!(quantifier == "ALL")); // ALL on empty set is true, ANY on empty set is false
    }

    let mut any_true_comparison = false;
    let mut any_false_comparison = false;
    let mut any_null_outcome = false;

    for sub_val_wrapper in subquery_result {
        let sub_val = match sub_val_wrapper.as_object().and_then(|obj| obj.values().next()) {
            Some(table_obj) => {
                if table_obj.is_object() && table_obj.as_object().unwrap().len() == 1 {
                    table_obj.as_object().unwrap().values().next().unwrap_or(&serde_json::Value::Null)
                } else {
                    table_obj
                }
            },
            None => {
                any_null_outcome = true;
                continue;
            }
        };

        if sub_val.is_null() || left_val.is_null() {
            any_null_outcome = true;
            continue;
        }

        let comparison_result = compare_values(op, left_val, sub_val)?;
        if comparison_result {
            any_true_comparison = true;
        } else {
            any_false_comparison = true;
        }
    }

    match quantifier {
        "ANY" => {
            if any_true_comparison {
                Ok(json!(true))
            } else if any_null_outcome {
                Ok(Value::Null)
            } else {
                Ok(json!(false))
            }
        }
        "ALL" => {
            if any_false_comparison {
                Ok(json!(false))
            } else if any_null_outcome {
                Ok(Value::Null)
            } else {
                Ok(json!(true))
            }
        }
        _ => unreachable!(),
    }
}

fn eq_any(args: Vec<Value>) -> Result<Value> { handle_any_all("=", "ANY", args) }
fn ne_any(args: Vec<Value>) -> Result<Value> { handle_any_all("!=", "ANY", args) }
fn gt_any(args: Vec<Value>) -> Result<Value> { handle_any_all(">", "ANY", args) }
fn lt_any(args: Vec<Value>) -> Result<Value> { handle_any_all("<", "ANY", args) }
fn ge_any(args: Vec<Value>) -> Result<Value> { handle_any_all(">=", "ANY", args) }
fn le_any(args: Vec<Value>) -> Result<Value> { handle_any_all("<=", "ANY", args) }

fn eq_all(args: Vec<Value>) -> Result<Value> { handle_any_all("=", "ALL", args) }
fn ne_all(args: Vec<Value>) -> Result<Value> { handle_any_all("!=", "ALL", args) }
fn gt_all(args: Vec<Value>) -> Result<Value> { handle_any_all(">", "ALL", args) }
fn lt_all(args: Vec<Value>) -> Result<Value> { handle_any_all("<", "ALL", args) }
fn ge_all(args: Vec<Value>) -> Result<Value> { handle_any_all(">=", "ALL", args) }
fn le_all(args: Vec<Value>) -> Result<Value> { handle_any_all("<=", "ALL", args) }

// This function will be evaluated in `Expression::FunctionCall`
fn exists(args: Vec<Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("EXISTS expects 1 argument (a subquery result)"));
    }
    match &args[0] {
        Value::Array(arr) => Ok(json!(!arr.is_empty())),
        _ => Err(anyhow!("EXISTS expects an array result from subquery")), // Should not happen if subquery evaluation is correct
    }
}

fn is_null(args: Vec<Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("IS_NULL expects 1 argument"));
    }
    Ok(json!(args[0].is_null()))
}

fn is_not_null(args: Vec<Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("IS_NOT_NULL expects 1 argument"));
    }
    Ok(json!(!args[0].is_null()))
}

fn lower(args: Vec<Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("LOWER expects 1 argument"));
    }
    match &args[0] {
        Value::String(s) => Ok(Value::String(s.to_lowercase())),
        _ => Ok(Value::Null), // Or error? Redis-like behavior is often to return nil.
    }
}

fn upper(args: Vec<Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("UPPER expects 1 argument"));
    }
    match &args[0] {
        Value::String(s) => Ok(Value::String(s.to_uppercase())),
        _ => Ok(Value::Null),
    }
}

fn length(args: Vec<Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("LENGTH expects 1 argument"));
    }
    match &args[0] {
        Value::String(s) => Ok(json!(s.len() as i64)),
        _ => Ok(Value::Null),
    }
}

fn trim(args: Vec<Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("TRIM expects 1 argument"));
    }
    match &args[0] {
        Value::String(s) => Ok(Value::String(s.trim().to_string())),
        _ => Ok(Value::Null),
    }
}

fn substring(args: Vec<Value>) -> Result<Value> {
    if args.len() != 2 && args.len() != 3 {
        return Err(anyhow!("SUBSTRING expects 2 or 3 arguments"));
    }
    let s = match &args[0] {
        Value::String(s) => s,
        _ => return Ok(Value::Null),
    };
    let start = match &args[1] {
        Value::Number(n) => n.as_i64().unwrap_or(0) as usize,
        _ => return Err(anyhow!("SUBSTRING start position must be a number")),
    };
    let len = if args.len() == 3 {
        match &args[2] {
            Value::Number(n) => Some(n.as_i64().unwrap_or(0) as usize),
            _ => return Err(anyhow!("SUBSTRING length must be a number")),
        }
    } else {
        None
    };

    let start_idx = if start > 0 { start - 1 } else { 0 };

    let result: String = if let Some(l) = len {
        s.chars().skip(start_idx).take(l).collect()
    } else {
        s.chars().skip(start_idx).collect()
    };

    Ok(Value::String(result.to_string()))
}

pub fn register_numeric_functions(registry: &mut crate::types::FunctionRegistry) {
    registry.register("ABS", Box::new(abs));
    registry.register("ROUND", Box::new(round));
    registry.register("CEIL", Box::new(ceil));
    registry.register("FLOOR", Box::new(floor));
}

pub fn register_datetime_functions(registry: &mut crate::types::FunctionRegistry) {
    registry.register("NOW", Box::new(now));
    registry.register("DATE_PART", Box::new(date_part));
}

// --- Numeric Functions ---

fn abs(args: Vec<Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("ABS expects 1 argument"));
    }
    match &args[0] {
        Value::Number(n) => Ok(json!(n.as_f64().unwrap_or(0.0).abs())),
        _ => Ok(Value::Null),
    }
}

fn round(args: Vec<Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("ROUND expects 1 argument"));
    }
    match &args[0] {
        Value::Number(n) => Ok(json!(n.as_f64().unwrap_or(0.0).round())),
        _ => Ok(Value::Null),
    }
}

fn ceil(args: Vec<Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("CEIL expects 1 argument"));
    }
    match &args[0] {
        Value::Number(n) => Ok(json!(n.as_f64().unwrap_or(0.0).ceil())),
        _ => Ok(Value::Null),
    }
}

fn floor(args: Vec<Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(anyhow!("FLOOR expects 1 argument"));
    }
    match &args[0] {
        Value::Number(n) => Ok(json!(n.as_f64().unwrap_or(0.0).floor())),
        _ => Ok(Value::Null),
    }
}

// --- Date/Time Functions ---

fn now(_args: Vec<Value>) -> Result<Value> {
    Ok(json!(Local::now().to_rfc3339()))
}

fn date_part(args: Vec<Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(anyhow!("DATE_PART expects 2 arguments"));
    }
    let part = match &args[0] {
        Value::String(s) => s.to_lowercase(),
        _ => return Err(anyhow!("DATE_PART expects a string for the part")),
    };
    let dt = match &args[1] {
        Value::String(s) => match DateTime::parse_from_rfc3339(s) {
            Ok(dt) => dt,
            Err(_) => return Ok(Value::Null),
        },
        _ => return Ok(Value::Null),
    };

    let result = match part.as_str() {
        "year" => dt.year() as i64,
        "month" => dt.month() as i64,
        "day" => dt.day() as i64,
        "hour" => dt.hour() as i64,
        "minute" => dt.minute() as i64,
        "second" => dt.second() as i64,
        "dow" => dt.weekday().number_from_sunday() as i64, // Sunday=1, Saturday=7
        "doy" => dt.ordinal() as i64,
        _ => return Err(anyhow!("Unsupported part for DATE_PART: {}", part)),
    };

    Ok(json!(result))
}


