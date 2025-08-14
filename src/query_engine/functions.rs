use anyhow::{anyhow, Result};
use chrono::{DateTime, Datelike, Local, Timelike};
use serde_json::{json, Value};

pub fn register_string_functions(registry: &mut crate::types::FunctionRegistry) {
    registry.register("LOWER", Box::new(lower));
    registry.register("UPPER", Box::new(upper));
    registry.register("LENGTH", Box::new(length));
    registry.register("TRIM", Box::new(trim));
    registry.register("SUBSTRING", Box::new(substring));
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


