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
            if args.len() == 2 && values_equal(&args[0], &args[1]) {
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

/// A fixed ordering rank per value category, so cross-category comparisons are
/// total and deterministic instead of rendering to text. `Int`/`Float` share a
/// rank because they compare numerically.
fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) | Value::Float(_) => 2,
        Value::Text(_) => 3,
        Value::Array(_) => 4,
        Value::Jsonb(_) => 5,
    }
}

/// Total ordering over values, used for ORDER BY, DISTINCT, MIN/MAX, and (via
/// [`values_equal`]) SQL equality so they all agree.
///
/// Numbers compare by magnitude (`Int`/`Float` interchangeably); within a
/// category the natural order applies. Values of *different* categories are
/// never compared by their rendered text — that made `Int(5)` and `Text("5")`
/// compare *equal* while `=` treated them as distinct, silently corrupting
/// `WHERE`/`JOIN`/`ORDER BY`/aggregates on any column holding mixed types.
/// Cross-category pairs order by [`type_rank`], keeping the relation total and
/// consistent with equality.
pub(crate) fn compare(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Int(x), Value::Float(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Float(x), Value::Int(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Array(x), Value::Array(y)) => {
            for (xe, ye) in x.iter().zip(y.iter()) {
                let ord = compare(xe, ye);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            x.len().cmp(&y.len())
        }
        (Value::Jsonb(x), Value::Jsonb(y)) => x.to_string().cmp(&y.to_string()),
        // Different categories: order by rank, never by rendered text.
        _ => type_rank(a).cmp(&type_rank(b)),
    }
}

/// SQL value equality, defined as `compare(a, b) == Equal` so ordering and
/// equality never disagree. Numerically-equal `Int`/`Float` are equal; values of
/// different categories (e.g. `Int(5)` vs `Text("5")`) are not. NULL handling
/// (three-valued logic) is the caller's responsibility — callers that need it
/// short-circuit on NULL before calling this.
pub(crate) fn values_equal(a: &Value, b: &Value) -> bool {
    compare(a, b) == std::cmp::Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn cross_type_orders_by_rank_not_rendered_text() {
        // The core soundness bug: `Int(5)` and `Text("5")` rendered to "5" and
        // compared *equal*. They are different categories and must not be equal.
        assert_eq!(
            compare(&Value::Int(5), &Value::Text("5".into())),
            Ordering::Less
        );
        assert!(!values_equal(&Value::Int(5), &Value::Text("5".into())));
        // And ordering is by category rank, not lexical text ("10" < "9").
        assert_eq!(
            compare(&Value::Int(10), &Value::Text("9".into())),
            Ordering::Less
        );
        // Bool vs Text, Null vs anything: total, deterministic, never equal.
        assert_eq!(
            compare(&Value::Bool(true), &Value::Text("t".into())),
            Ordering::Less
        );
        assert!(!values_equal(&Value::Bool(true), &Value::Text("t".into())));
        assert_eq!(compare(&Value::Null, &Value::Int(0)), Ordering::Less);
    }

    #[test]
    fn equality_agrees_with_ordering() {
        // For every pair, `values_equal` is exactly `compare == Equal`.
        let vals = [
            Value::Null,
            Value::Bool(false),
            Value::Bool(true),
            Value::Int(5),
            Value::Float(5.0),
            Value::Float(5.5),
            Value::Text("5".into()),
            Value::Text("abc".into()),
        ];
        for a in &vals {
            for b in &vals {
                assert_eq!(
                    values_equal(a, b),
                    compare(a, b) == Ordering::Equal,
                    "inconsistent for {a:?} vs {b:?}"
                );
            }
        }
        // Numerically-equal Int/Float are equal; same value across categories is not.
        assert!(values_equal(&Value::Int(5), &Value::Float(5.0)));
        assert!(!values_equal(&Value::Int(5), &Value::Bool(true)));
        assert!(values_equal(&Value::Null, &Value::Null));
    }

    #[test]
    fn mixed_int_float_compares_numerically_not_lexically() {
        // The lexical bug: "9" > "10". Numerically 9 < 10.
        assert_eq!(compare(&Value::Int(9), &Value::Float(10.0)), Ordering::Less);
        assert_eq!(
            compare(&Value::Float(10.0), &Value::Int(9)),
            Ordering::Greater
        );
        assert_eq!(compare(&Value::Int(2), &Value::Float(2.0)), Ordering::Equal);
        assert_eq!(
            compare(&Value::Float(2.5), &Value::Int(2)),
            Ordering::Greater
        );
        // Same-type paths are unaffected.
        assert_eq!(compare(&Value::Int(9), &Value::Int(10)), Ordering::Less);
    }
}
