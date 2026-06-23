//! Value representation, type coercion, comparison, and scalar-function
//! evaluation — the data primitives shared across the planner and executor.

use serde::{Deserialize, Serialize};

/// A column definition parsed from `CREATE TABLE`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub unique: bool,
    pub primary: bool,
}

/// A typed cell value. Rows are stored as `Vec<Value>` in table-column order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Array(Vec<Value>),
    Jsonb(serde_json::Value),
    Null,
}

/// Logical column type derived from a SQL type name.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ColumnType {
    Int,
    Float,
    Bool,
    Text,
}

pub(crate) fn column_type(data_type: &str) -> ColumnType {
    let t = data_type.to_uppercase();
    if t.contains("INT") || t.contains("SERIAL") {
        ColumnType::Int
    } else if t.contains("FLOAT")
        || t.contains("DOUBLE")
        || t.contains("REAL")
        || t.contains("NUMERIC")
        || t.contains("DECIMAL")
    {
        ColumnType::Float
    } else if t.contains("BOOL") {
        ColumnType::Bool
    } else {
        ColumnType::Text
    }
}

/// Coerces a literal string into a typed value for the given column type.
/// Empty strings and unparseable numerics become `Null`.
pub(crate) fn coerce(raw: &str, ty: ColumnType) -> Value {
    if raw.is_empty() {
        return Value::Null;
    }
    match ty {
        ColumnType::Int => raw.parse::<i64>().map(Value::Int).unwrap_or(Value::Null),
        ColumnType::Float => raw.parse::<f64>().map(Value::Float).unwrap_or(Value::Null),
        ColumnType::Bool => raw.parse::<bool>().map(Value::Bool).unwrap_or(Value::Null),
        ColumnType::Text => Value::Text(raw.to_string()),
    }
}

pub(crate) fn render(value: &Value) -> String {
    match value {
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Bool(b) => {
            if *b {
                "t".to_string()
            } else {
                "f".to_string()
            }
        }
        Value::Array(a) => {
            let rendered: Vec<String> = a.iter().map(render).collect();
            format!("{{{}}}", rendered.join(","))
        }
        Value::Jsonb(j) => j.to_string(),
        Value::Null => String::new(),
    }
}

/// Encodes a literal projection-function argument back into the string form the
/// planner stores: `'text'` for strings, plain digits for numbers/bools. (The
/// projection model stores args as strings; [`resolve_scalar_arg`] parses them
/// back at evaluation time.)
pub(crate) fn literal_arg(value: &Value) -> String {
    match value {
        Value::Text(s) => format!("'{s}'"),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

/// Resolves one scalar-function argument to a value for a given row: a quoted
/// `'…'` literal, a numeric literal, or otherwise a column reference.
pub(crate) fn resolve_scalar_arg(arg: &str, row: &[Value], col_names: &[String]) -> Value {
    if arg.len() >= 2 && arg.starts_with('\'') && arg.ends_with('\'') {
        Value::Text(arg[1..arg.len() - 1].to_string())
    } else if let Ok(i) = arg.parse::<i64>() {
        Value::Int(i)
    } else if let Ok(f) = arg.parse::<f64>() {
        Value::Float(f)
    } else {
        col_names
            .iter()
            .position(|tc| tc == arg || tc.ends_with(&format!(".{arg}")))
            .and_then(|i| row.get(i))
            .cloned()
            .unwrap_or(Value::Null)
    }
}

/// Evaluates a scalar SQL function over already-resolved argument values.
/// Unknown functions yield `Null` (the prior behaviour). NULL propagation
/// follows SQL: most functions return NULL on a NULL primary argument.
pub(crate) fn eval_scalar_function(name: &str, args: &[Value]) -> Value {
    let as_text = |v: &Value| -> Option<String> {
        match v {
            Value::Null => None,
            Value::Text(s) => Some(s.clone()),
            other => Some(render(other)),
        }
    };
    let as_num = |v: &Value| -> Option<f64> {
        match v {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Text(s) => s.parse().ok(),
            _ => None,
        }
    };
    match name {
        "CONCAT" => Value::Text(
            args.iter()
                .filter(|v| **v != Value::Null)
                .map(render)
                .collect(),
        ),
        "UPPER" => args
            .first()
            .and_then(&as_text)
            .map_or(Value::Null, |s| Value::Text(s.to_uppercase())),
        "LOWER" => args
            .first()
            .and_then(&as_text)
            .map_or(Value::Null, |s| Value::Text(s.to_lowercase())),
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => args
            .first()
            .and_then(&as_text)
            .map_or(Value::Null, |s| Value::Int(s.chars().count() as i64)),
        "TRIM" => args
            .first()
            .and_then(&as_text)
            .map_or(Value::Null, |s| Value::Text(s.trim().to_string())),
        "LTRIM" => args
            .first()
            .and_then(&as_text)
            .map_or(Value::Null, |s| Value::Text(s.trim_start().to_string())),
        "RTRIM" => args
            .first()
            .and_then(&as_text)
            .map_or(Value::Null, |s| Value::Text(s.trim_end().to_string())),
        "COALESCE" => args
            .iter()
            .find(|v| **v != Value::Null)
            .cloned()
            .unwrap_or(Value::Null),
        "NULLIF" => {
            if args.len() == 2 && args[0] == args[1] {
                Value::Null
            } else {
                args.first().cloned().unwrap_or(Value::Null)
            }
        }
        "ABS" => match args.first() {
            Some(Value::Int(i)) => Value::Int(i.abs()),
            Some(Value::Float(f)) => Value::Float(f.abs()),
            _ => Value::Null,
        },
        "ROUND" => match args.first().and_then(&as_num) {
            Some(x) => {
                let digits = args.get(1).and_then(&as_num).unwrap_or(0.0) as i32;
                let factor = 10f64.powi(digits);
                Value::Float((x * factor).round() / factor)
            }
            None => Value::Null,
        },
        "REPLACE" => {
            if let (Some(s), Some(from), Some(to)) = (
                args.first().and_then(&as_text),
                args.get(1).and_then(&as_text),
                args.get(2).and_then(&as_text),
            ) {
                Value::Text(s.replace(&from, &to))
            } else {
                Value::Null
            }
        }
        "SUBSTR" | "SUBSTRING" => {
            let Some(s) = args.first().and_then(&as_text) else {
                return Value::Null;
            };
            let chars: Vec<char> = s.chars().collect();
            let start = args.get(1).and_then(&as_num).unwrap_or(1.0) as i64; // 1-based
            let start_idx = (start.max(1) - 1) as usize;
            let out: String = match args.get(2).and_then(&as_num) {
                Some(len) => chars
                    .iter()
                    .skip(start_idx)
                    .take(len.max(0.0) as usize)
                    .collect(),
                None => chars.iter().skip(start_idx).collect(),
            };
            Value::Text(out)
        }
        _ => Value::Null,
    }
}

pub(crate) fn compare(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        _ => render(a).cmp(&render(b)),
    }
}
