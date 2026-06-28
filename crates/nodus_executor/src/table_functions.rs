//! Set-returning table functions used in `FROM` (`unnest`, `generate_series`,
//! `jsonb_array_elements[_text]`, `regexp_split_to_table`), including multi-arg
//! `unnest` and `WITH ORDINALITY`. See [`MemExecutor::eval_table_function`],
//! which both the standalone path (materialized like a CTE) and the lateral
//! join path (evaluated per driving row) call.

use crate::{MemExecutor, QueryOutput, Row, TableFnSpec, Value};
use anyhow::Result;

impl MemExecutor {
    /// Evaluates a table function against a (possibly lateral) driving `row`,
    /// returning its output column names, their declared types, and the produced
    /// rows. Argument column references resolve against `row`/`col_names`
    /// (lateral); literal arguments ignore them.
    pub(crate) fn eval_table_function(
        &self,
        spec: &TableFnSpec,
        row: &[Value],
        col_names: &[String],
    ) -> Result<(Vec<String>, Vec<String>, Vec<Vec<Value>>)> {
        let args: Vec<Value> = spec
            .args
            .iter()
            .map(|op| self.eval_operand(row, col_names, &[], op, "TEXT"))
            .collect();

        // Each function returns its value-column types and rows (a row may carry
        // several values, e.g. multi-argument `unnest`).
        let (mut types, mut rows) = match spec.name.as_str() {
            "unnest" => unnest_rows(&args),
            "generate_series" => generate_series_rows(&args),
            "jsonb_array_elements" | "json_array_elements" => {
                json_array_elements_rows(&args, false)
            }
            "jsonb_array_elements_text" | "json_array_elements_text" => {
                json_array_elements_rows(&args, true)
            }
            "regexp_split_to_table" => regexp_split_rows(&args),
            other => anyhow::bail!("Unsupported table function: {other}()"),
        };

        // Value-column names: explicit `AS f(c1, ..)` wins (per column), else the
        // relation alias for the first column, else the function name.
        let mut names: Vec<String> = (0..types.len())
            .map(|i| {
                spec.column_aliases
                    .get(i)
                    .cloned()
                    .or_else(|| (i == 0).then(|| spec.alias.clone()).flatten())
                    .unwrap_or_else(|| spec.name.clone())
            })
            .collect();

        if spec.with_ordinality {
            names.push(
                spec.column_aliases
                    .get(types.len())
                    .cloned()
                    .unwrap_or_else(|| "ordinality".to_string()),
            );
            types.push("INTEGER".to_string());
            for (i, r) in rows.iter_mut().enumerate() {
                r.push(Value::Int((i + 1) as i64));
            }
        }
        Ok((names, types, rows))
    }

    /// Executes a standalone (non-lateral) table function into a [`QueryOutput`]
    /// — the `SELECT * FROM generate_series(...)` form, materialized like a CTE.
    pub(crate) fn exec_table_function(&self, spec: TableFnSpec) -> Result<QueryOutput> {
        let (columns, types, rows) = self.eval_table_function(&spec, &[], &[])?;
        let count = rows.len();
        Ok(QueryOutput {
            columns,
            types,
            rows: rows.into_iter().map(|values| Row { values }).collect(),
            tag: format!("SELECT {count}"),
        })
    }
}

/// Coerces an argument to a list of elements: array/JSON-array contents, or
/// empty for null/non-array (lenient — PostgreSQL would require an array, but
/// introspection unnests possibly-absent array columns).
fn as_elements(v: Option<&Value>) -> Vec<Value> {
    match v {
        Some(Value::Array(items)) => items.clone(),
        Some(Value::Jsonb(serde_json::Value::Array(items))) => {
            items.iter().map(json_to_value).collect()
        }
        _ => Vec::new(),
    }
}

/// `unnest(a[, b, ...])`: one column per array argument, one row per element
/// position; shorter arrays are padded with NULL (PostgreSQL semantics).
fn unnest_rows(args: &[Value]) -> (Vec<String>, Vec<Vec<Value>>) {
    let columns: Vec<Vec<Value>> = args.iter().map(|a| as_elements(Some(a))).collect();
    if columns.is_empty() {
        return (vec!["VARCHAR".to_string()], Vec::new());
    }
    let types: Vec<String> = columns
        .iter()
        .map(|c| {
            c.first()
                .map(value_type_name)
                .unwrap_or("VARCHAR")
                .to_string()
        })
        .collect();
    let height = columns.iter().map(|c| c.len()).max().unwrap_or(0);
    let rows = (0..height)
        .map(|i| {
            columns
                .iter()
                .map(|c| c.get(i).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect();
    (types, rows)
}

/// `generate_series(start, stop[, step])` over integers (step defaults to 1).
fn generate_series_rows(args: &[Value]) -> (Vec<String>, Vec<Vec<Value>>) {
    let start = args.first().and_then(value_as_i64);
    let stop = args.get(1).and_then(value_as_i64);
    let step = args.get(2).and_then(value_as_i64).unwrap_or(1);
    let mut rows = Vec::new();
    if let (Some(start), Some(stop)) = (start, stop)
        && step != 0
    {
        let mut n = start;
        while (step > 0 && n <= stop) || (step < 0 && n >= stop) {
            rows.push(vec![Value::Int(n)]);
            n += step;
        }
    }
    (vec!["INTEGER".to_string()], rows)
}

/// `jsonb_array_elements(arr)` / `jsonb_array_elements_text(arr)`: one row per
/// element, as JSONB or as text.
fn json_array_elements_rows(args: &[Value], as_text: bool) -> (Vec<String>, Vec<Vec<Value>>) {
    let elems = as_elements(args.first());
    let (ty, rows) = if as_text {
        (
            "VARCHAR",
            elems
                .into_iter()
                .map(|v| vec![Value::Text(crate::render(&v))])
                .collect(),
        )
    } else {
        ("JSONB", elems.into_iter().map(|v| vec![v]).collect())
    };
    (vec![ty.to_string()], rows)
}

/// `regexp_split_to_table(string, pattern)`: one text row per split piece. An
/// invalid pattern yields the whole string as a single row.
fn regexp_split_rows(args: &[Value]) -> (Vec<String>, Vec<Vec<Value>>) {
    let text = args.first().map(crate::render).unwrap_or_default();
    let pattern = args.get(1).map(crate::render).unwrap_or_default();
    let pieces: Vec<String> = match regex::Regex::new(&pattern) {
        Ok(re) => re.split(&text).map(|s| s.to_string()).collect(),
        Err(_) => vec![text],
    };
    let rows = pieces.into_iter().map(|s| vec![Value::Text(s)]).collect();
    (vec!["VARCHAR".to_string()], rows)
}

fn value_as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Int(i) => Some(*i),
        Value::Float(f) => Some(*f as i64),
        Value::Text(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Int(_) => "INTEGER",
        Value::Float(_) => "DOUBLE",
        Value::Bool(_) => "BOOLEAN",
        Value::Jsonb(_) => "JSONB",
        _ => "VARCHAR",
    }
}

fn json_to_value(j: &serde_json::Value) -> Value {
    match j {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::Text(s.clone()),
        other => Value::Jsonb(other.clone()),
    }
}
