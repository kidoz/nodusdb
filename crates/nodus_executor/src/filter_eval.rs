//! Predicate and filter-expression evaluation against in-memory rows: operand
//! resolution, `WHERE`/`ON` conjunction evaluation (including `IN (subquery)`,
//! `LIKE`, null checks, and JSONB containment), and the `row_matches` gate.

use crate::{
    CompareOp, ExecutionContext, FilterExpr, MemExecutor, Operand, Predicate, QueryOutput, Value,
    coerce, column_type, compare, render,
};
use anyhow::Result;
use nodus_catalog::ColumnDescriptor;

impl MemExecutor {
    /// Evaluates predicates against a joined or single row.
    pub(crate) fn eval_operand(
        &self,
        row: &[Value],
        col_names: &[String],
        _columns: &[ColumnDescriptor],
        op: &Operand,
        expected_type: &str,
    ) -> Value {
        match op {
            Operand::Literal(val) => {
                match val {
                    Value::Text(s) => coerce(s, column_type(expected_type)),
                    _ => val.clone(), // already typed correctly if it was binary bound
                }
            }
            Operand::Ident(col) => {
                let idx = col_names
                    .iter()
                    .position(|c| c == col || c.ends_with(&format!(".{}", col)));
                idx.and_then(|i| row.get(i))
                    .cloned()
                    .unwrap_or(crate::Value::Null)
            }
        }
    }

    /// Evaluates a FilterExpr against a joined or single row.
    pub(crate) fn eval_filter(
        &self,
        ctx: &ExecutionContext,
        row: &[Value],
        col_names: &[String],
        columns: &[ColumnDescriptor],
        filter: Option<&FilterExpr>,
    ) -> Option<bool> {
        let Some(expr) = filter else {
            return Some(true);
        };
        match expr {
            FilterExpr::And(left, right) => {
                let l = self.eval_filter(ctx, row, col_names, columns, Some(left));
                let r = self.eval_filter(ctx, row, col_names, columns, Some(right));
                match (l, r) {
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                }
            }
            FilterExpr::Or(left, right) => {
                let l = self.eval_filter(ctx, row, col_names, columns, Some(left));
                let r = self.eval_filter(ctx, row, col_names, columns, Some(right));
                match (l, r) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                }
            }
            FilterExpr::Not(inner) => self
                .eval_filter(ctx, row, col_names, columns, Some(inner))
                .map(|b| !b),
            FilterExpr::IsNull(col) => {
                let idx = col_names
                    .iter()
                    .position(|c| c == col || c.ends_with(&format!(".{}", col)));
                if let Some(i) = idx {
                    Some(row.get(i).unwrap_or(&Value::Null) == &Value::Null)
                } else {
                    Some(false)
                }
            }
            FilterExpr::IsNotNull(col) => {
                let idx = col_names
                    .iter()
                    .position(|c| c == col || c.ends_with(&format!(".{}", col)));
                if let Some(i) = idx {
                    Some(row.get(i).unwrap_or(&Value::Null) != &Value::Null)
                } else {
                    Some(false)
                }
            }
            FilterExpr::Predicate(p) => {
                let left_idx = col_names
                    .iter()
                    .position(|c| c == &p.left || c.ends_with(&format!(".{}", p.left)));
                let Some(idx) = left_idx else {
                    return Some(false);
                };
                let left_cell = row.get(idx).unwrap_or(&Value::Null);
                let right_cell =
                    self.eval_operand(row, col_names, columns, &p.right, &columns[idx].data_type);

                if left_cell == &Value::Null || right_cell == Value::Null {
                    return None;
                }

                let ord = compare(left_cell, &right_cell);
                Some(match p.op {
                    CompareOp::Eq => *left_cell == right_cell,
                    CompareOp::Ne => *left_cell != right_cell,
                    CompareOp::Lt => ord == std::cmp::Ordering::Less,
                    CompareOp::Le => ord != std::cmp::Ordering::Greater,
                    CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                    CompareOp::Ge => ord != std::cmp::Ordering::Less,
                    // `@>` left contains right; `<@` left contained by right.
                    CompareOp::Contains => value_contains(left_cell, &right_cell),
                    CompareOp::ContainedBy => value_contains(&right_cell, left_cell),
                })
            }
            FilterExpr::Like {
                left,
                right,
                negated,
            } => {
                let left_idx = col_names
                    .iter()
                    .position(|c| c == left || c.ends_with(&format!(".{}", left)));
                let Some(idx) = left_idx else {
                    return Some(false);
                };
                let left_cell = row.get(idx).unwrap_or(&Value::Null);
                let right_cell =
                    self.eval_operand(row, col_names, columns, right, &columns[idx].data_type);

                if left_cell == &Value::Null || right_cell == Value::Null {
                    return None;
                }

                if let (Value::Text(l), Value::Text(r)) = (left_cell, right_cell) {
                    let regex_str = format!("^{}$", r.replace('%', ".*").replace('_', "."));
                    let is_match = regex::Regex::new(&regex_str)
                        .map(|re| re.is_match(l))
                        .unwrap_or(false);
                    Some(if *negated { !is_match } else { is_match })
                } else {
                    Some(false)
                }
            }
            FilterExpr::InList {
                left,
                list,
                negated,
            } => {
                let left_idx = col_names
                    .iter()
                    .position(|c| c == left || c.ends_with(&format!(".{}", left)));
                let Some(idx) = left_idx else {
                    return Some(false);
                };
                let left_cell = row.get(idx).unwrap_or(&Value::Null);

                if left_cell == &Value::Null {
                    return None;
                }

                let mut is_match = false;
                let mut found_null = false;
                for op in list {
                    let right_cell =
                        self.eval_operand(row, col_names, columns, op, &columns[idx].data_type);
                    if right_cell == Value::Null {
                        found_null = true;
                    } else if *left_cell == right_cell {
                        is_match = true;
                        break;
                    }
                }

                if *negated {
                    if is_match {
                        Some(false)
                    } else if found_null {
                        None
                    } else {
                        Some(true)
                    }
                } else {
                    if is_match {
                        Some(true)
                    } else if found_null {
                        None
                    } else {
                        Some(false)
                    }
                }
            }
            FilterExpr::InSubquery {
                left,
                subquery,
                negated,
            } => {
                let left_idx = col_names
                    .iter()
                    .position(|c| c == left || c.ends_with(&format!(".{}", left)));
                let Some(idx) = left_idx else {
                    return Some(false);
                };
                let left_cell = row.get(idx).unwrap_or(&Value::Null);

                if left_cell == &Value::Null {
                    return None;
                }

                // Blocking execution
                let exec_res = self.execute_logical_inner(ctx, *subquery.clone());
                let out = exec_res.unwrap_or(QueryOutput {
                    columns: vec![],
                    types: vec![],
                    rows: vec![],
                    tag: String::new(),
                });

                let mut matches = false;
                let mut found_null = false;
                for r in out.rows {
                    if let Some(c) = r.values.first() {
                        let right_cell = coerce(&render(c), column_type(&columns[idx].data_type));
                        if right_cell == Value::Null {
                            found_null = true;
                        } else if *left_cell == right_cell {
                            matches = true;
                            break;
                        }
                    }
                }

                if *negated {
                    if matches {
                        Some(false)
                    } else if found_null {
                        None
                    } else {
                        Some(true)
                    }
                } else {
                    if matches {
                        Some(true)
                    } else if found_null {
                        None
                    } else {
                        Some(false)
                    }
                }
            }
        }
    }

    pub(crate) fn row_matches(
        &self,
        ctx: &ExecutionContext,
        row: &[Value],
        columns: &[ColumnDescriptor],
        filter: Option<&FilterExpr>,
    ) -> bool {
        let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        self.eval_filter(ctx, row, &col_names, columns, filter)
            .unwrap_or(false)
    }
}

/// Recursive JSON containment matching PostgreSQL's `@>` semantics: objects
/// contain a subset of keys (with contained values), arrays contain every
/// right element somewhere on the left, and scalars must be equal.
fn json_contains(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    use serde_json::Value as J;
    match (a, b) {
        (J::Object(am), J::Object(bm)) => bm
            .iter()
            .all(|(k, bv)| am.get(k).is_some_and(|av| json_contains(av, bv))),
        (J::Array(av), J::Array(bv)) => bv
            .iter()
            .all(|be| av.iter().any(|ae| json_contains(ae, be))),
        (J::Array(av), be) => av.iter().any(|ae| json_contains(ae, be)),
        _ => a == b,
    }
}

/// Coerces a cell value into a JSON document for containment checks. Text that
/// parses as JSON is treated as JSON; a PostgreSQL array text literal (`{a,b}`)
/// becomes a JSON string array; other text becomes a JSON string scalar.
fn value_to_json(v: &Value) -> Option<serde_json::Value> {
    use serde_json::Value as J;
    match v {
        Value::Jsonb(j) => Some(j.clone()),
        Value::Int(i) => Some(J::from(*i)),
        Value::Float(f) => serde_json::Number::from_f64(*f).map(J::Number),
        Value::Bool(b) => Some(J::Bool(*b)),
        Value::Null => Some(J::Null),
        Value::Array(items) => items
            .iter()
            .map(value_to_json)
            .collect::<Option<Vec<_>>>()
            .map(J::Array),
        Value::Text(s) => {
            let t = s.trim();
            if t.starts_with('{') || t.starts_with('[') {
                if let Ok(j) = serde_json::from_str::<J>(t) {
                    return Some(j);
                }
                // PostgreSQL array text literal, e.g. `{login,signup}`.
                if t.starts_with('{') && t.ends_with('}') {
                    let inner = &t[1..t.len() - 1];
                    let arr = if inner.is_empty() {
                        vec![]
                    } else {
                        inner
                            .split(',')
                            .map(|x| J::String(x.trim().trim_matches('"').to_string()))
                            .collect()
                    };
                    return Some(J::Array(arr));
                }
                None
            } else {
                Some(J::String(s.clone()))
            }
        }
    }
}

/// True if `left` contains `right` under JSONB/array containment semantics.
fn value_contains(left: &Value, right: &Value) -> bool {
    match (value_to_json(left), value_to_json(right)) {
        (Some(l), Some(r)) => json_contains(&l, &r),
        _ => false,
    }
}

#[cfg(test)]
mod containment_tests {
    use super::value_contains;
    use crate::Value;
    use serde_json::json;

    #[test]
    fn jsonb_object_containment() {
        let big = Value::Jsonb(json!({"a": 1, "b": {"c": 2}}));
        assert!(value_contains(&big, &Value::Jsonb(json!({"a": 1}))));
        assert!(value_contains(&big, &Value::Jsonb(json!({"b": {"c": 2}}))));
        // not contained: nested value differs / extra keys
        assert!(!value_contains(
            &Value::Jsonb(json!({"a": 1})),
            &Value::Jsonb(json!({"a": 1, "b": 2})),
        ));
        assert!(!value_contains(&big, &Value::Jsonb(json!({"b": {"c": 3}}))));
    }

    #[test]
    fn array_containment_and_membership() {
        let arr = Value::Array(vec![
            Value::Text("login".into()),
            Value::Text("signup".into()),
        ]);
        assert!(value_contains(
            &arr,
            &Value::Array(vec![Value::Text("login".into())])
        ));
        // PostgreSQL array @> scalar is element membership.
        assert!(value_contains(&arr, &Value::Text("login".into())));
        assert!(!value_contains(&arr, &Value::Text("logout".into())));
        // PostgreSQL array text literal parses as a JSON string array.
        assert!(value_contains(
            &Value::Text("{login,signup}".into()),
            &Value::Text("login".into()),
        ));
    }

    #[test]
    fn contained_by_relation() {
        // `<@` evaluates as value_contains(right, left).
        let small = Value::Jsonb(json!({"a": 1}));
        let big = Value::Jsonb(json!({"a": 1, "b": 2}));
        assert!(value_contains(&big, &small)); // small <@ big
        assert!(!value_contains(&small, &big)); // big not <@ small
    }
}
