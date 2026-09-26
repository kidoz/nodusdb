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
        if !spec.rows_from.is_empty() {
            return self.eval_rows_from(spec, row, col_names);
        }
        let args: Vec<Value> = if spec.arg_exprs.is_empty() {
            spec.args
                .iter()
                .map(|op| self.eval_operand(row, col_names, &[], op, "TEXT"))
                .collect()
        } else {
            spec.arg_exprs
                .iter()
                .map(|e| crate::eval_scalar_expr(e, row, col_names))
                .collect()
        };

        // Each function returns its value-column types and rows (a row may carry
        // several values, e.g. multi-argument `unnest`).
        let (mut types, mut rows) = match spec.name.as_str() {
            "unnest" => unnest_rows(&args),
            "generate_series" => generate_series_rows(&args),
            "jsonb_array_elements" => json_array_elements_rows(&args, false),
            "jsonb_array_elements_text" => json_array_elements_rows(&args, true),
            "json_array_elements" => json_text_elements_rows(&args, false),
            "json_array_elements_text" => json_text_elements_rows(&args, true),
            "regexp_split_to_table" => regexp_split_rows(&args),
            // No table is a partition, so none has ancestors.
            "pg_partition_ancestors" => (vec!["REGCLASS".to_string()], Vec::new()),
            // The function behind the `pg_available_extensions` view.
            "pg_available_extensions" => (
                vec!["NAME".to_string(), "TEXT".to_string(), "TEXT".to_string()],
                vec![vec![
                    Value::Text("plpgsql".into()),
                    Value::Text("1.0".into()),
                    Value::Text("PL/pgSQL procedural language".into()),
                ]],
            ),
            other => anyhow::bail!("Unsupported table function: {other}()"),
        };

        // Value-column names: explicit `AS f(c1, ..)` wins (per column), else
        // the name the function gives its column, else the relation alias for
        // the first column, else the function name.
        let named = match spec.name.as_str() {
            "jsonb_array_elements"
            | "jsonb_array_elements_text"
            | "json_array_elements"
            | "json_array_elements_text" => Some("value"),
            _ => None,
        };
        let mut names: Vec<String> = (0..types.len())
            .map(|i| {
                spec.column_aliases
                    .get(i)
                    .cloned()
                    .or_else(|| named.filter(|_| i == 0).map(str::to_string))
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

    /// `ROWS FROM (f(), g())`: each function's rows, paired up in order;
    /// the shorter's missing values are NULL.
    fn eval_rows_from(
        &self,
        spec: &TableFnSpec,
        row: &[Value],
        col_names: &[String],
    ) -> Result<(Vec<String>, Vec<String>, Vec<Vec<Value>>)> {
        let mut names = Vec::new();
        let mut outputs = Vec::new();
        for member in &spec.rows_from {
            let (member_names, member_types, member_rows) =
                self.eval_table_function(member, row, col_names)?;
            names.extend(member_names);
            outputs.push((member_types, member_rows));
        }
        let height = outputs
            .iter()
            .map(|(_, rows)| rows.len())
            .max()
            .unwrap_or(0);
        let mut rows = vec![Vec::new(); height];
        for (member_types, member_rows) in &outputs {
            for (i, out) in rows.iter_mut().enumerate() {
                match member_rows.get(i) {
                    Some(values) => out.extend(values.iter().cloned()),
                    None => out.extend(std::iter::repeat_n(Value::Null, member_types.len())),
                }
            }
        }
        let names = names
            .into_iter()
            .enumerate()
            .map(|(i, name)| spec.column_aliases.get(i).cloned().unwrap_or(name))
            .collect();
        let types = outputs.into_iter().flat_map(|(t, _)| t).collect();
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
    // The array as a document: parsed from text, as an untyped literal is.
    let items = match args.first() {
        Some(Value::Jsonb(serde_json::Value::Array(items))) => items.clone(),
        Some(Value::Text(t)) => match crate::json_text::parse(t) {
            Ok(serde_json::Value::Array(items)) => items,
            _ => Vec::new(),
        },
        _ => Vec::new(),
    };
    let (ty, rows) = if as_text {
        (
            "TEXT",
            items
                .into_iter()
                .map(|item| {
                    vec![match item {
                        serde_json::Value::Null => Value::Null,
                        serde_json::Value::String(s) => Value::Text(s),
                        other => Value::Text(crate::json_text::jsonb_text(&other)),
                    }]
                })
                .collect(),
        )
    } else {
        (
            "JSONB",
            items
                .into_iter()
                .map(|item| vec![Value::Jsonb(item)])
                .collect(),
        )
    };
    (vec![ty.to_string()], rows)
}

/// `json_array_elements(arr)` / `json_array_elements_text(arr)`: one row per
/// element, as written or as text.
fn json_text_elements_rows(args: &[Value], as_text: bool) -> (Vec<String>, Vec<Vec<Value>>) {
    let text = match args.first() {
        Some(Value::Json(t) | Value::Text(t)) => t.clone(),
        Some(Value::Jsonb(j)) => crate::json_text::jsonb_text(j),
        _ => String::new(),
    };
    let elements = crate::json_text::array_elements(&text).unwrap_or_default();
    let rows = elements
        .into_iter()
        .map(|element| {
            vec![if as_text {
                crate::json_text::json_member_text(element)
            } else {
                Value::Json(element.to_string())
            }]
        })
        .collect();
    let ty = if as_text { "TEXT" } else { "JSON" };
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
