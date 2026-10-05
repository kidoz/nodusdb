//! Set-returning table functions used in `FROM` (`unnest`, `generate_series`,
//! `jsonb_array_elements[_text]`, `regexp_split_to_table`), including multi-arg
//! `unnest` and `WITH ORDINALITY`. See [`MemExecutor::eval_table_function`],
//! which both the standalone path (materialized like a CTE) and the lateral
//! join path (evaluated per driving row) call.

use crate::plan_types::{JsonTableColumn, JsonTableColumnKind, JsonTableSpec};
use crate::sqljson::{JsonTablePlan, JsonTablePlanColumn};
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
        if let Some(table) = &spec.json_table {
            return self.eval_json_table(spec, table, row, col_names);
        }
        if let Some(table) = &spec.xml_table {
            return self.eval_xml_table(spec, table, row, col_names);
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

        // The text-search table functions are strict: a NULL argument
        // yields no rows at all.
        if matches!(
            spec.name.as_str(),
            "ts_parse" | "ts_token_type" | "ts_debug"
        ) && args.iter().any(|a| matches!(a, Value::Null))
        {
            let (names, types): (Vec<String>, Vec<String>) = match spec.name.as_str() {
                "ts_parse" => (
                    vec!["tokid".to_string(), "token".to_string()],
                    vec!["INTEGER".to_string(), "TEXT".to_string()],
                ),
                "ts_token_type" => (
                    vec![
                        "tokid".to_string(),
                        "alias".to_string(),
                        "description".to_string(),
                    ],
                    vec![
                        "INTEGER".to_string(),
                        "TEXT".to_string(),
                        "TEXT".to_string(),
                    ],
                ),
                _ => (
                    [
                        "alias",
                        "description",
                        "token",
                        "dictionaries",
                        "dictionary",
                        "lexemes",
                    ]
                    .iter()
                    .map(|n| n.to_string())
                    .collect(),
                    [
                        "TEXT",
                        "TEXT",
                        "TEXT",
                        "REGDICTIONARY[]",
                        "REGDICTIONARY",
                        "TEXT[]",
                    ]
                    .iter()
                    .map(|t| t.to_string())
                    .collect(),
                ),
            };
            return Ok((names, types, Vec::new()));
        }

        // Each function returns its value-column types and rows (a row may carry
        // several values, e.g. multi-argument `unnest`).
        let (mut types, mut rows) = match spec.name.as_str() {
            // A multirange unnests into its ranges; the declared type names
            // the element type for the rows.
            "unnest" => {
                let declared = spec
                    .arg_exprs
                    .first()
                    .and_then(crate::result_types::constant_expr_type);
                match declared.and_then(|t| crate::multiranges::kind_of(&t)) {
                    Some(kind) => {
                        let (_, rows) = multirange_unnest_rows(&args);
                        (vec![kind.name().to_string()], rows)
                    }
                    None => unnest_rows(&args),
                }
            }
            // Timestamps (or dates) stepped by an interval.
            "generate_series"
                if args.len() == 3
                    && matches!(&args[0], Value::Text(t) if crate::value::parse_temporal(t).is_some()) =>
            {
                let (ty, values) = crate::datetime::series(&args[0], &args[1], &args[2])
                    .map_err(|e| anyhow::anyhow!(e))?;
                (vec![ty], values.into_iter().map(|v| vec![v]).collect())
            }
            "generate_series" => generate_series_rows(&args),
            "jsonb_array_elements" => json_array_elements_rows(&args, false),
            "jsonb_path_query" | "jsonb_path_query_tz" => {
                let rows = crate::functions::jsonpath_query_rows(&args)?;
                (
                    vec!["JSONB".to_string()],
                    rows.into_iter().map(|v| vec![v]).collect(),
                )
            }
            "jsonb_array_elements_text" => json_array_elements_rows(&args, true),
            "json_array_elements" => json_text_elements_rows(&args, false),
            "json_array_elements_text" => json_text_elements_rows(&args, true),
            "regexp_split_to_table" => regexp_split_rows(&args)?,
            "regexp_matches" => regexp_matches_rows(&args)?,
            "string_to_table" => string_to_table_rows(&args),
            "generate_subscripts" => generate_subscripts_rows(&args),
            "jsonb_each" | "jsonb_each_text" | "json_each" | "json_each_text" => json_each_rows(
                &args,
                spec.name.starts_with("jsonb"),
                spec.name.ends_with("_text"),
            ),
            "jsonb_object_keys" | "json_object_keys" => {
                let rows = json_each_rows(&args, spec.name.starts_with("jsonb"), true)
                    .1
                    .into_iter()
                    .map(|mut r| vec![r.swap_remove(0)])
                    .collect();
                (vec!["TEXT".to_string()], rows)
            }
            "jsonb_to_record" | "json_to_record" | "jsonb_to_recordset" | "json_to_recordset" => {
                json_to_record_rows(&args, spec, spec.name.ends_with("set"))?
            }
            // `ts_parse(parser, document)`: the parser's tokens.
            "ts_parse" if args.len() == 2 => {
                crate::ts_parse::check_parser_arg(&args[0]).map_err(|e| anyhow::anyhow!(e))?;
                let rows = crate::ts_parse::parse(&value_text(&args[1]))
                    .into_iter()
                    .map(|token| vec![Value::Int(i64::from(token.ty)), Value::Text(token.text)])
                    .collect();
                (vec!["INTEGER".to_string(), "TEXT".to_string()], rows)
            }
            // `ts_token_type(parser)`: the parser's token types.
            "ts_token_type" if args.len() == 1 => {
                crate::ts_parse::check_parser_arg(&args[0]).map_err(|e| anyhow::anyhow!(e))?;
                let rows = crate::ts_parse::token_types()
                    .into_iter()
                    .map(|(ty, alias, description)| {
                        vec![
                            Value::Int(i64::from(ty)),
                            Value::Text(alias.to_string()),
                            Value::Text(description.to_string()),
                        ]
                    })
                    .collect();
                (
                    vec![
                        "INTEGER".to_string(),
                        "TEXT".to_string(),
                        "TEXT".to_string(),
                    ],
                    rows,
                )
            }
            // `ts_debug([config,] document)`: each token with the
            // configuration's dictionaries and their lexemes.
            "ts_debug" if matches!(args.len(), 1 | 2) => {
                let (config, document) = if args.len() == 2 {
                    let config =
                        crate::ts_dict::config_value(&args[0]).map_err(|e| anyhow::anyhow!(e))?;
                    (config, &args[1])
                } else {
                    (crate::functions::default_ts_config(), &args[0])
                };
                let rows = crate::ts_parse::parse(&value_text(document))
                    .into_iter()
                    .map(|token| {
                        let dictionaries = crate::ts_dict::token_dictionaries(config, token.ty);
                        let dictionary = dictionaries.first().copied();
                        let lexemes = dictionary.map(|dictionary| {
                            crate::ts_dict::lexize_token(dictionary, &token.text)
                        });
                        vec![
                            Value::Text(crate::ts_parse::token_alias(token.ty).to_string()),
                            Value::Text(crate::ts_parse::token_desc(token.ty).to_string()),
                            Value::Text(token.text),
                            Value::Array(
                                dictionaries
                                    .iter()
                                    .map(|d| Value::Text(d.dictionary_name().to_string()))
                                    .collect(),
                            ),
                            match dictionary {
                                Some(d) => Value::Text(d.dictionary_name().to_string()),
                                None => Value::Null,
                            },
                            match lexemes {
                                Some(lexemes) => {
                                    Value::Array(lexemes.into_iter().map(Value::Text).collect())
                                }
                                None => Value::Null,
                            },
                        ]
                    })
                    .collect();
                (
                    vec![
                        "TEXT".to_string(),
                        "TEXT".to_string(),
                        "TEXT".to_string(),
                        "REGDICTIONARY[]".to_string(),
                        "REGDICTIONARY".to_string(),
                        "TEXT[]".to_string(),
                    ],
                    rows,
                )
            }
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
        let default_columns: &[&str] = match spec.name.as_str() {
            "ts_parse" => &["tokid", "token"],
            "ts_token_type" => &["tokid", "alias", "description"],
            "ts_debug" => &[
                "alias",
                "description",
                "token",
                "dictionaries",
                "dictionary",
                "lexemes",
            ],
            _ => &[],
        };
        let named = match spec.name.as_str() {
            "jsonb_array_elements"
            | "jsonb_array_elements_text"
            | "json_array_elements"
            | "json_array_elements_text" => Some("value"),
            _ => None,
        };
        let pair = matches!(
            spec.name.as_str(),
            "jsonb_each" | "jsonb_each_text" | "json_each" | "json_each_text"
        );
        let mut names: Vec<String> = (0..types.len())
            .map(|i| {
                spec.column_aliases
                    .get(i)
                    .cloned()
                    .or_else(|| pair.then(|| ["key", "value"][i.min(1)].to_string()))
                    .or_else(|| default_columns.get(i).map(|c| c.to_string()))
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

/// `unnest(multirange)`: one row per element range.
fn multirange_unnest_rows(args: &[Value]) -> (Vec<String>, Vec<Vec<Value>>) {
    let text = args.first().map(crate::render).unwrap_or_default();
    let rows = crate::multiranges::unnest(&text)
        .unwrap_or_default()
        .into_iter()
        .map(|value| vec![value])
        .collect();
    (vec!["VARCHAR".to_string()], rows)
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

/// `regexp_split_to_table(string, pattern [, flags])`: one text row per
/// piece between the matches.
fn regexp_split_rows(args: &[Value]) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    if args.iter().any(|a| matches!(a, Value::Null)) {
        return Ok((vec!["TEXT".to_string()], Vec::new()));
    }
    let text = args.first().map(crate::render).unwrap_or_default();
    let pattern = args.get(1).map(crate::render).unwrap_or_default();
    let flags = args.get(2).map(crate::render).unwrap_or_default();
    let pieces = crate::pg_regex::split(&text, &pattern, &flags).map_err(|e| anyhow::anyhow!(e))?;
    let rows = pieces.into_iter().map(|s| vec![Value::Text(s)]).collect();
    Ok((vec!["TEXT".to_string()], rows))
}

/// `regexp_matches(string, pattern [, flags])`: a `text[]` row per match
/// (every match with the `g` flag, else the first).
fn regexp_matches_rows(args: &[Value]) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let ty = vec!["TEXT[]".to_string()];
    if args.iter().any(|a| matches!(a, Value::Null)) {
        return Ok((ty, Vec::new()));
    }
    let text = args.first().map(crate::render).unwrap_or_default();
    let pattern = args.get(1).map(crate::render).unwrap_or_default();
    let flags = args.get(2).map(crate::render).unwrap_or_default();
    let found =
        crate::pg_regex::matches(&text, &pattern, &flags).map_err(|e| anyhow::anyhow!(e))?;
    Ok((ty, found.into_iter().map(|m| vec![m]).collect()))
}

/// `jsonb_each` / `json_each` (and their `_text` forms): a key and value
/// row per member of the object; `json` keeps its members' order and
/// duplicates, `jsonb` its keys' order.
fn json_each_rows(args: &[Value], jsonb: bool, as_text: bool) -> (Vec<String>, Vec<Vec<Value>>) {
    let value_type = match (as_text, jsonb) {
        (true, _) => "TEXT",
        (false, true) => "JSONB",
        (false, false) => "JSON",
    };
    let types = vec!["TEXT".to_string(), value_type.to_string()];
    let member_text = |value: serde_json::Value| match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::String(s) => Value::Text(s),
        other => Value::Text(crate::json_text::jsonb_text(&other)),
    };
    let rows = if jsonb {
        let doc = match args.first() {
            Some(Value::Jsonb(j)) => Some(j.clone()),
            Some(Value::Text(t) | Value::Json(t)) => crate::json_text::parse(t).ok(),
            _ => None,
        };
        match doc {
            Some(serde_json::Value::Object(map)) => {
                let mut entries: Vec<_> = map.into_iter().collect();
                entries.sort_by(|a, b| crate::json_text::key_order(&a.0, &b.0));
                entries
            }
            .into_iter()
            .map(|(k, v)| {
                vec![
                    Value::Text(k),
                    if as_text {
                        member_text(v)
                    } else {
                        Value::Jsonb(v)
                    },
                ]
            })
            .collect(),
            _ => Vec::new(),
        }
    } else {
        let text = match args.first() {
            Some(Value::Json(t) | Value::Text(t)) => t.clone(),
            Some(Value::Jsonb(j)) => crate::json_text::jsonb_text(j),
            _ => String::new(),
        };
        crate::json_text::object_members(&text)
            .unwrap_or_default()
            .into_iter()
            .map(|(k, member)| {
                vec![
                    Value::Text(k),
                    if as_text {
                        crate::json_text::json_member_text(member)
                    } else {
                        Value::Json(member.trim().to_string())
                    },
                ]
            })
            .collect()
    };
    (types, rows)
}

/// `jsonb_to_record(doc) AS r(a int, ...)` (or `..._recordset` over an
/// array of objects): each column the member of its name, as its type.
fn json_to_record_rows(
    args: &[Value],
    spec: &TableFnSpec,
    set: bool,
) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    if spec.column_types.is_empty() || spec.column_types.iter().any(Option::is_none) {
        anyhow::bail!("a column definition list is required for functions returning \"record\"");
    }
    let types: Vec<String> = spec.column_types.iter().flatten().cloned().collect();
    let doc = match args.first() {
        Some(Value::Jsonb(j)) => Some(j.clone()),
        Some(Value::Text(t) | Value::Json(t)) => crate::json_text::parse(t).ok(),
        _ => None,
    };
    let objects = match (doc, set) {
        (Some(serde_json::Value::Array(items)), true) => items,
        (Some(obj @ serde_json::Value::Object(_)), false) => vec![obj],
        (None, _) => Vec::new(),
        (Some(_), true) => anyhow::bail!("cannot call json_to_recordset on a non-array"),
        (Some(_), false) => anyhow::bail!("cannot call json_to_record on a non-object"),
    };
    let mut rows = Vec::new();
    for object in objects {
        let serde_json::Value::Object(map) = object else {
            anyhow::bail!("argument of json_to_recordset must be an array of objects");
        };
        let mut row = Vec::with_capacity(types.len());
        for (name, ty) in spec.column_aliases.iter().zip(&types) {
            let value = match map.get(name) {
                None | Some(serde_json::Value::Null) => Value::Null,
                Some(v) if crate::value::is_json_type(ty) => {
                    crate::planner::cast_value(Value::Text(v.to_string()), ty)
                }
                Some(serde_json::Value::String(s)) => {
                    crate::planner::cast_value(Value::Text(s.clone()), ty)
                }
                Some(v) => {
                    crate::planner::cast_value(Value::Text(crate::json_text::jsonb_text(v)), ty)
                }
            };
            row.push(value);
        }
        rows.push(row);
    }
    Ok((types, rows))
}

impl MemExecutor {
    /// Evaluates a `JSON_TABLE`: the context document and the `PASSING`
    /// variables are the call's arguments, and the paths and default
    /// expressions of the columns are evaluated against the driving row.
    fn eval_json_table(
        &self,
        spec: &crate::TableFnSpec,
        table: &JsonTableSpec,
        row: &[Value],
        col_names: &[String],
    ) -> Result<(Vec<String>, Vec<String>, Vec<Vec<Value>>)> {
        if let Some(refuse) = &table.refuse {
            anyhow::bail!(refuse.clone());
        }
        // A column's type must be one, as PostgreSQL checks when it parses
        // the call; the session's catalog is the one that says so.
        if let Some(ty) = json_table_type_error(&table.columns) {
            anyhow::bail!(crate::user_types::missing_type(&ty));
        }
        let eval = |e: &crate::ScalarExpr| crate::eval_scalar_expr(e, row, col_names);
        let plan = JsonTablePlan {
            path: value_text(&eval(&table.path)),
            on_error: table.on_error.clone(),
            columns: table
                .columns
                .iter()
                .map(|c| table_column(c, &eval))
                .collect(),
        };
        // The columns are the same for every row; `AS jt(b, ...)` renames
        // them positionally.
        let (names, types) = crate::sqljson::json_table_names(&plan.columns);
        let names = names
            .into_iter()
            .enumerate()
            .map(|(i, name)| spec.column_aliases.get(i).cloned().unwrap_or(name))
            .collect();
        let doc = spec.arg_exprs.first().map_or(Value::Null, |e| eval(e));
        if matches!(doc, Value::Null) {
            return Ok((names, types, Vec::new()));
        }
        let vars = spec.arg_exprs.get(1).map_or(Value::Null, |e| eval(e));
        let vars = (!matches!(vars, Value::Null)).then_some(vars);
        let rows = crate::sqljson::json_table_rows(&doc, vars.as_ref(), &plan)
            .map_err(|error| anyhow::anyhow!(error))?;
        Ok((names, types, rows))
    }
}

impl MemExecutor {
    /// Evaluates an `XMLTABLE`: the document is the call's argument, and
    /// the row path, the namespaces, and the columns' paths and defaults
    /// are evaluated against the driving row. The order is PostgreSQL's:
    /// the document, the namespaces, the row filter, the column filters,
    /// and only then the rows themselves.
    fn eval_xml_table(
        &self,
        spec: &crate::TableFnSpec,
        table: &crate::plan_types::XmlTableSpec,
        row: &[Value],
        col_names: &[String],
    ) -> Result<(Vec<String>, Vec<String>, Vec<Vec<Value>>)> {
        use crate::plan_types::XmlTableColumnKind;
        if let Some(refuse) = &table.refuse {
            anyhow::bail!(refuse.clone());
        }
        // Every column's type must be one, as PostgreSQL checks when it
        // parses the call; the session's catalog is the one that says so.
        for column in &table.columns {
            if let XmlTableColumnKind::Value { column_type, .. } = &column.kind
                && !crate::user_types::is_known_type(column_type)
            {
                anyhow::bail!(crate::user_types::missing_type(column_type));
            }
        }
        let eval = |e: &crate::ScalarExpr| crate::eval_scalar_expr(e, row, col_names);
        // The columns are the same for every row; `AS x(a, ...)` renames
        // them positionally.
        let names: Vec<String> = table
            .columns
            .iter()
            .enumerate()
            .map(|(at, column)| {
                spec.column_aliases
                    .get(at)
                    .cloned()
                    .unwrap_or_else(|| column.name.clone())
            })
            .collect();
        let types: Vec<String> = table
            .columns
            .iter()
            .map(|column| match &column.kind {
                XmlTableColumnKind::Ordinality => "INTEGER".to_string(),
                XmlTableColumnKind::Value { column_type, .. } => column_type.clone(),
            })
            .collect();
        let doc = spec.arg_exprs.first().map_or(Value::Null, |e| eval(e));
        if matches!(doc, Value::Null) {
            return Ok((names, types, Vec::new()));
        }
        // The document parses first: an XML value that is not one stops
        // here, before any namespace or path is read.
        let document = crate::xml::parse(
            &crate::xml::value_text(&doc),
            crate::xml::Mode::Document,
            true,
        )
        .map_err(|_| {
            anyhow::anyhow!(
                crate::error_fields::DbError::new("could not parse XML document")
                    .code("2200M")
                    .into_text()
            )
        })?;
        let mut namespaces = Vec::with_capacity(table.namespaces.len());
        for namespace in &table.namespaces {
            let uri = eval(&namespace.uri);
            if matches!(uri, Value::Null) {
                anyhow::bail!(
                    crate::error_fields::DbError::new("namespace URI must not be null")
                        .code("22004")
                        .into_text()
                );
            }
            let Some(prefix) = &namespace.prefix else {
                anyhow::bail!(
                    crate::error_fields::DbError::new("DEFAULT namespace is not supported")
                        .code("0A000")
                        .into_text()
                );
            };
            namespaces.push((prefix.clone(), value_text(&uri)));
        }
        let doc = crate::xpath::XpDoc::new(&document, &namespaces);
        let table_error = crate::xmltable::table_error;
        // The row filter, then the column filters, compile before any row.
        let row_path = value_text(&eval(&table.path));
        if row_path.is_empty() {
            anyhow::bail!(
                crate::error_fields::DbError::new("row path filter must not be empty string")
                    .code("2200S")
                    .into_text()
            );
        }
        let row_expr = doc
            .compile(&row_path)
            .map_err(|error| anyhow::anyhow!(table_error(error)))?;
        let mut filters = Vec::with_capacity(table.columns.len());
        for column in &table.columns {
            let XmlTableColumnKind::Value { path, .. } = &column.kind else {
                filters.push(None);
                continue;
            };
            let path = match path {
                Some(expr) => value_text(&eval(expr)),
                None => column.name.clone(),
            };
            if path.is_empty() {
                anyhow::bail!(
                    crate::error_fields::DbError::new(
                        "column path filter must not be empty string"
                    )
                    .code("2200S")
                    .into_text()
                );
            }
            let filter = doc
                .compile(&path)
                .map_err(|error| anyhow::anyhow!(table_error(error)))?;
            filters.push(Some(filter));
        }
        // The rows, then each column of each row.
        let items = doc
            .row_items(&row_expr)
            .map_err(|error| anyhow::anyhow!(table_error(error)))?;
        let mut rows = Vec::with_capacity(items.len());
        for (index, &node) in items.iter().enumerate() {
            let mut out = Vec::with_capacity(table.columns.len());
            for (at, column) in table.columns.iter().enumerate() {
                let XmlTableColumnKind::Value {
                    column_type,
                    default,
                    not_null,
                    ..
                } = &column.kind
                else {
                    out.push(Value::Int(index as i64 + 1));
                    continue;
                };
                let filter = filters[at].as_ref().expect("a value column's filter");
                let text = crate::xmltable::column_text(&doc, filter, node, column_type)
                    .map_err(|error| anyhow::anyhow!(error))?;
                let mut value = match text {
                    Some(text) => Some(
                        crate::xmltable::convert_text(&text, column_type)
                            .map_err(|error| anyhow::anyhow!(error))?,
                    ),
                    None => None,
                };
                // A null value takes the default, which is already of the
                // column's type.
                if value.is_none()
                    && let Some(default) = default
                {
                    value = Some(match eval(default) {
                        Value::Null => Value::Null,
                        Value::Text(text) => crate::xmltable::convert_text(&text, column_type)
                            .map_err(|error| anyhow::anyhow!(error))?,
                        other => crate::planner::try_cast(other, column_type)
                            .map_err(|error| anyhow::anyhow!(error))?,
                    });
                }
                if value.is_none() && *not_null {
                    anyhow::bail!(
                        crate::error_fields::DbError::new(format!(
                            "null is not allowed in column \"{}\"",
                            column.name
                        ))
                        .code("22004")
                        .into_text()
                    );
                }
                out.push(value.unwrap_or(Value::Null));
            }
            rows.push(out);
        }
        Ok((names, types, rows))
    }
}

/// The first column type of a `JSON_TABLE` spec that is not a type at all.
fn json_table_type_error(columns: &[JsonTableColumn]) -> Option<String> {
    for column in columns {
        let ty = match &column.kind {
            JsonTableColumnKind::Scalar { column_type, .. }
            | JsonTableColumnKind::Exists { column_type, .. } => column_type,
            JsonTableColumnKind::Nested { columns, .. } => {
                if let Some(ty) = json_table_type_error(columns) {
                    return Some(ty);
                }
                continue;
            }
            JsonTableColumnKind::Ordinality => continue,
        };
        if !crate::user_types::is_known_type(ty) {
            return Some(ty.clone());
        }
    }
    None
}

/// One `JSON_TABLE` column, with its path and defaults evaluated.
fn table_column(
    column: &JsonTableColumn,
    eval: &impl Fn(&crate::ScalarExpr) -> Value,
) -> JsonTablePlanColumn {
    use JsonTableColumnKind as Kind;
    let text = |e: &crate::ScalarExpr| value_text(&eval(e));
    let default = |e: &Option<crate::ScalarExpr>, behavior: &str| match (behavior, e) {
        ("default", Some(e)) => eval(e),
        _ => Value::Null,
    };
    match &column.kind {
        Kind::Ordinality => JsonTablePlanColumn::Ordinality {
            name: column.name.clone(),
        },
        Kind::Exists {
            column_type,
            path,
            on_error,
        } => JsonTablePlanColumn::Exists {
            name: column.name.clone(),
            column_type: column_type.clone(),
            path: text(path),
            on_error: on_error.clone(),
        },
        Kind::Scalar {
            column_type,
            format,
            path,
            wrapper,
            quotes,
            on_empty,
            on_empty_default,
            on_error,
            on_error_default,
            ..
        } => JsonTablePlanColumn::Scalar {
            name: column.name.clone(),
            column_type: column_type.clone(),
            format: *format,
            path: text(path),
            wrapper: wrapper.clone(),
            quotes: quotes.clone(),
            on_empty: on_empty.clone(),
            on_empty_default: default(on_empty_default, on_empty),
            on_error: on_error.clone(),
            on_error_default: default(on_error_default, on_error),
        },
        Kind::Nested { path, columns } => JsonTablePlanColumn::Nested {
            path: text(path),
            columns: columns.iter().map(|c| table_column(c, eval)).collect(),
        },
    }
}

/// `generate_subscripts(array, dim [, reverse])`: the subscripts of the
/// array's `dim`th dimension, in order or reversed.
fn generate_subscripts_rows(args: &[Value]) -> (Vec<String>, Vec<Vec<Value>>) {
    let ty = vec!["INTEGER".to_string()];
    let Some(Value::Array(items)) = args.first() else {
        return (ty, Vec::new());
    };
    let dim = args.get(1).and_then(value_as_i64).unwrap_or(1);
    let mut level = items.clone();
    for _ in 1..dim {
        level = match level.into_iter().next() {
            Some(Value::Array(inner)) => inner,
            _ => return (ty, Vec::new()),
        };
    }
    if dim < 1 {
        return (ty, Vec::new());
    }
    let mut subscripts: Vec<Vec<Value>> = (1..=level.len() as i64)
        .map(|i| vec![Value::Int(i)])
        .collect();
    if matches!(args.get(2), Some(Value::Bool(true))) {
        subscripts.reverse();
    }
    (ty, subscripts)
}

/// `string_to_table(string, delimiter [, null_string])`: a text row per
/// piece, as `string_to_array` splits.
fn string_to_table_rows(args: &[Value]) -> (Vec<String>, Vec<Vec<Value>>) {
    let ty = vec!["TEXT".to_string()];
    let pieces = match crate::functions::call("STRING_TO_ARRAY", args) {
        Value::Array(items) => items,
        _ => Vec::new(),
    };
    (ty, pieces.into_iter().map(|v| vec![v]).collect())
}

fn value_as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Int(i) => Some(*i),
        Value::Float(f) => Some(*f as i64),
        Value::Text(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

/// A text argument's value, as the text-search table functions read it.
fn value_text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        other => crate::render(other),
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
