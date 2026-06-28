//! Set-returning table functions used in `FROM` (`unnest`, `generate_series`),
//! including `WITH ORDINALITY`. See [`MemExecutor::eval_table_function`], which
//! both the standalone path (materialized like a CTE) and the lateral join path
//! (evaluated per driving row) call.

use crate::{MemExecutor, QueryOutput, Row, TableFnSpec, Value};
use anyhow::Result;

impl MemExecutor {
    /// Evaluates a table function against a (possibly lateral) driving `row`,
    /// returning its bare output column names, their declared types, and the
    /// produced rows. Argument column references resolve against `row`/`col_names`
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

        let (value_type, mut rows) = match spec.name.as_str() {
            "unnest" => unnest_rows(&args),
            "generate_series" => generate_series_rows(&args),
            other => anyhow::bail!("Unsupported table function: {other}()"),
        };

        // The value column takes its name from `AS f(col, ..)`, else the relation
        // alias, else the function name.
        let mut names = vec![
            spec.column_aliases
                .first()
                .cloned()
                .or_else(|| spec.alias.clone())
                .unwrap_or_else(|| spec.name.clone()),
        ];
        let mut types = vec![value_type];

        if spec.with_ordinality {
            names.push(
                spec.column_aliases
                    .get(1)
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

/// Expands an array argument into one single-column row per element. A
/// null/non-array argument yields no rows — lenient where PostgreSQL would
/// require an array, since introspection unnests possibly-absent array columns.
fn unnest_rows(args: &[Value]) -> (String, Vec<Vec<Value>>) {
    let elems: Vec<Value> = match args.first() {
        Some(Value::Array(items)) => items.clone(),
        Some(Value::Jsonb(serde_json::Value::Array(items))) => {
            items.iter().map(json_to_value).collect()
        }
        _ => Vec::new(),
    };
    let ty = elems
        .first()
        .map(value_type_name)
        .unwrap_or("VARCHAR")
        .to_string();
    (ty, elems.into_iter().map(|v| vec![v]).collect())
}

/// `generate_series(start, stop[, step])` over integers (step defaults to 1).
fn generate_series_rows(args: &[Value]) -> (String, Vec<Vec<Value>>) {
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
    ("INTEGER".to_string(), rows)
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
