//! Predicate and filter-expression evaluation against in-memory rows: operand
//! resolution, `WHERE`/`ON` conjunction evaluation (including `IN (subquery)`,
//! `LIKE`, null checks, and JSONB containment), and the `row_matches` gate.

use crate::{
    CompareOp, ExecutionContext, FilterExpr, LogicalPlan, MemExecutor, Operand, Predicate,
    QueryOutput, Value, coerce, column_type, compare, render, values_equal,
};
use anyhow::Result;
use nodus_catalog::ColumnDescriptor;

/// Resolves a column reference against a row's column names: exact match,
/// `<qual>.<name>` suffix match, then — for a qualified reference against bare
/// columns (e.g. `t.a` on a single-table scan) — the bare tail of the name.
pub(crate) fn col_pos(col_names: &[String], name: &str) -> Option<usize> {
    if let Some(i) = col_names
        .iter()
        .position(|c| c == name || c.ends_with(&format!(".{name}")))
    {
        return Some(i);
    }
    let tail = name.rsplit('.').next()?;
    if tail == name {
        return None;
    }
    col_names
        .iter()
        .position(|c| c == tail || c.ends_with(&format!(".{tail}")))
}

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
                let idx = col_pos(col_names, col);
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
                let idx = col_pos(col_names, col);
                if let Some(i) = idx {
                    Some(row.get(i).unwrap_or(&Value::Null) == &Value::Null)
                } else {
                    Some(false)
                }
            }
            FilterExpr::IsNotNull(col) => {
                let idx = col_pos(col_names, col);
                if let Some(i) = idx {
                    Some(row.get(i).unwrap_or(&Value::Null) != &Value::Null)
                } else {
                    Some(false)
                }
            }
            FilterExpr::Predicate(p) => {
                let left_idx = col_pos(col_names, &p.left);
                // The left side is normally a column; it can also be a JSON
                // access (`col ->> 'k'` / `col -> 'k'`) that the planner encoded
                // as a synthetic name, which we compute per row here.
                let (left_cell, expected_type): (Value, String) = if let Some(idx) = left_idx {
                    (
                        row.get(idx).cloned().unwrap_or(Value::Null),
                        columns[idx].data_type.clone(),
                    )
                } else if let Some((base, op, key)) = parse_json_ref(&p.left) {
                    let Some(i) = col_names
                        .iter()
                        .position(|c| c == &base || c.ends_with(&format!(".{base}")))
                    else {
                        return Some(false);
                    };
                    (
                        json_extract(row.get(i).unwrap_or(&Value::Null), &op, &key),
                        "TEXT".to_string(),
                    )
                } else {
                    return Some(false);
                };

                let right_cell = self.eval_operand(row, col_names, columns, &p.right, &expected_type);

                if left_cell == Value::Null || right_cell == Value::Null {
                    return None;
                }

                // Interval-aware: two interval texts compare by magnitude
                // (`10 days` > `2 days`); otherwise this is the generic `compare`,
                // which agrees with `values_equal` (`5 = 5.0` true, `5 = '5'` false).
                let ord = crate::planner::interval_aware_compare(&left_cell, &right_cell);
                Some(match p.op {
                    CompareOp::Eq => ord == std::cmp::Ordering::Equal,
                    CompareOp::Ne => ord != std::cmp::Ordering::Equal,
                    CompareOp::Lt => ord == std::cmp::Ordering::Less,
                    CompareOp::Le => ord != std::cmp::Ordering::Greater,
                    CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                    CompareOp::Ge => ord != std::cmp::Ordering::Less,
                    // `@>` left contains right; `<@` left contained by right.
                    CompareOp::Contains => value_contains(&left_cell, &right_cell),
                    CompareOp::ContainedBy => value_contains(&right_cell, &left_cell),
                })
            }
            FilterExpr::CompareSubquery {
                left,
                op,
                subquery,
            } => {
                let Some(idx) = col_pos(col_names, left) else {
                    return Some(false);
                };
                let left_cell = row.get(idx).unwrap_or(&Value::Null);
                if left_cell == &Value::Null {
                    return None;
                }
                // The subquery yields a single scalar; coerce it to the left
                // column's type so comparison agrees (e.g. text "40" vs int 40).
                // Outer references are substituted first (correlation).
                let correlated = self.correlate_subplan(subquery, row, col_names);
                let out = self.execute_logical_inner(ctx, correlated).ok()?;
                let right_cell = out
                    .rows
                    .first()
                    .and_then(|r| r.values.first())
                    .map(|v| coerce(&render(v), column_type(&columns[idx].data_type)))
                    .unwrap_or(Value::Null);
                if right_cell == Value::Null {
                    return None;
                }
                let ord = compare(left_cell, &right_cell);
                Some(match op {
                    CompareOp::Eq => values_equal(left_cell, &right_cell),
                    CompareOp::Ne => !values_equal(left_cell, &right_cell),
                    CompareOp::Lt => ord == std::cmp::Ordering::Less,
                    CompareOp::Le => ord != std::cmp::Ordering::Greater,
                    CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                    CompareOp::Ge => ord != std::cmp::Ordering::Less,
                    _ => false,
                })
            }
            FilterExpr::Like {
                left,
                right,
                negated,
            } => {
                let left_idx = col_pos(col_names, left);
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
                let left_idx = col_pos(col_names, left);
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
                    } else if values_equal(left_cell, &right_cell) {
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
                left_value,
            } => {
                // The left side is a column, or a literal (`1 IN (SELECT ..)`).
                let (left_cell, coerce_type): (Value, Option<&str>) = match left_value {
                    Some(v) => (v.clone(), None),
                    None => {
                        let Some(idx) = col_pos(col_names, left) else {
                            return Some(false);
                        };
                        (
                            row.get(idx).cloned().unwrap_or(Value::Null),
                            Some(columns[idx].data_type.as_str()),
                        )
                    }
                };

                if left_cell == Value::Null {
                    return None;
                }

                // Blocking execution; outer references in the subquery are
                // substituted with this row's values first (correlation).
                let correlated = self.correlate_subplan(subquery, row, col_names);
                let exec_res = self.execute_logical_inner(ctx, correlated);
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
                        let right_cell = match coerce_type {
                            Some(ty) => coerce(&render(c), column_type(ty)),
                            None => c.clone(),
                        };
                        if right_cell == Value::Null {
                            found_null = true;
                        } else if values_equal(&left_cell, &right_cell) {
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
            FilterExpr::ExprCmp { left, op, right } => {
                let l = crate::eval_scalar_expr(left, row, col_names);
                let r = crate::eval_scalar_expr(right, row, col_names);
                if l == Value::Null || r == Value::Null {
                    return None;
                }
                let ord = crate::planner::interval_aware_compare(&l, &r);
                Some(match op {
                    CompareOp::Eq => ord == std::cmp::Ordering::Equal,
                    CompareOp::Ne => ord != std::cmp::Ordering::Equal,
                    CompareOp::Lt => ord == std::cmp::Ordering::Less,
                    CompareOp::Le => ord != std::cmp::Ordering::Greater,
                    CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                    CompareOp::Ge => ord != std::cmp::Ordering::Less,
                    CompareOp::Contains => value_contains(&l, &r),
                    CompareOp::ContainedBy => value_contains(&r, &l),
                })
            }
            FilterExpr::Exists { subquery, negated } => {
                // Substitute any outer-column references in the subquery with
                // this row's values (correlation), then execute it. The result
                // is true iff the subquery yields at least one row.
                let correlated = self.correlate_subplan(subquery, row, col_names);
                let out = self
                    .execute_logical_inner(ctx, correlated)
                    .unwrap_or(QueryOutput {
                        columns: vec![],
                        types: vec![],
                        rows: vec![],
                        tag: String::new(),
                    });
                let exists = !out.rows.is_empty();
                Some(if *negated { !exists } else { exists })
            }
        }
    }

    /// Clones a subquery plan and rewrites its top-level `WHERE` so that any
    /// reference qualified by an alias that isn't the subquery's own table
    /// (i.e. an outer/correlated reference) is replaced with the outer row's
    /// value. Non-`Select` plans and uncorrelated subqueries pass through
    /// unchanged.
    fn correlate_subplan(
        &self,
        plan: &LogicalPlan,
        outer_row: &[Value],
        outer_cols: &[String],
    ) -> LogicalPlan {
        let mut p = plan.clone();
        if let LogicalPlan::Select {
            table_name,
            table_alias,
            joins,
            filter,
            projection,
            ..
        } = &mut p
        {
            let mut quals: Vec<String> = Vec::new();
            if let Some(a) = table_alias.as_ref() {
                quals.push(a.to_lowercase());
            }
            quals.push(table_name.to_lowercase());
            if let Some(last) = table_name.rsplit('.').next() {
                quals.push(last.to_lowercase());
            }
            // Joined tables (and their aliases) are inner too.
            for j in joins.iter() {
                if let Some(a) = j.table_alias.as_ref() {
                    quals.push(a.to_lowercase());
                }
                quals.push(j.table_name.to_lowercase());
                if let Some(last) = j.table_name.rsplit('.').next() {
                    quals.push(last.to_lowercase());
                }
            }
            if let Some(f) = filter.as_ref() {
                *filter = Some(correlate_filter(f, outer_row, outer_cols, &quals));
            }
            // Outer references can also appear in the projection
            // (e.g. `SELECT upper.f1 + f2 FROM t WHERE ...`).
            for item in projection.iter_mut() {
                if let crate::ProjectionItem::Expr { expr, .. } = item {
                    *expr = correlate_scalar(expr, outer_row, outer_cols, &quals);
                }
            }
        }
        p
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

/// Flips a comparison operator so `a <op> b` becomes `b <flip(op)> a` — used
/// when a correlated predicate has the outer reference on its left side.
fn flip_op(op: &CompareOp) -> CompareOp {
    match op {
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::Le => CompareOp::Ge,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::Ge => CompareOp::Le,
        other => other.clone(),
    }
}

/// True if `name` is qualified by an alias that is NOT the subquery's own
/// table/alias — i.e. it references an outer (correlated) column. Bare names
/// are treated as inner references.
fn is_outer_ref(name: &str, inner_quals: &[String]) -> bool {
    match name.split_once('.') {
        Some((qual, _)) => !inner_quals.iter().any(|q| q == &qual.to_lowercase()),
        None => false,
    }
}

/// Resolves a (possibly qualified) outer column reference to its value in the
/// outer row, matching on the bare column name or a `table.col` suffix.
fn outer_lookup(name: &str, outer_cols: &[String], outer_row: &[Value]) -> Option<Value> {
    let bare = name.rsplit('.').next().unwrap_or(name);
    let idx = outer_cols
        .iter()
        .position(|c| c == bare || c.ends_with(&format!(".{bare}")))?;
    outer_row.get(idx).cloned()
}

/// Rewrites a scalar expression, replacing correlated outer column references
/// with the outer row's literal values.
fn correlate_scalar(
    expr: &crate::ScalarExpr,
    outer_row: &[Value],
    outer_cols: &[String],
    inner_quals: &[String],
) -> crate::ScalarExpr {
    use crate::ScalarExpr as S;
    let recur = |e: &S| Box::new(correlate_scalar(e, outer_row, outer_cols, inner_quals));
    match expr {
        S::Column(n) if is_outer_ref(n, inner_quals) => {
            match outer_lookup(n, outer_cols, outer_row) {
                Some(v) => S::Literal(v),
                None => expr.clone(),
            }
        }
        S::Unary { op, expr } => S::Unary {
            op: *op,
            expr: recur(expr),
        },
        S::Binary { op, left, right } => S::Binary {
            op: *op,
            left: recur(left),
            right: recur(right),
        },
        S::Cast { expr, target } => S::Cast {
            expr: recur(expr),
            target: target.clone(),
        },
        S::Function { name, args } => S::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| correlate_scalar(a, outer_row, outer_cols, inner_quals))
                .collect(),
        },
        S::Case {
            operand,
            branches,
            else_result,
        } => S::Case {
            operand: operand.as_ref().map(|o| recur(o)),
            branches: branches
                .iter()
                .map(|(c, r)| {
                    (
                        correlate_scalar(c, outer_row, outer_cols, inner_quals),
                        correlate_scalar(r, outer_row, outer_cols, inner_quals),
                    )
                })
                .collect(),
            else_result: else_result.as_ref().map(|e| recur(e)),
        },
        other => other.clone(),
    }
}

/// Rewrites a subquery filter, replacing correlated outer references with the
/// outer row's literal values so the subquery can execute standalone.
fn correlate_filter(
    f: &FilterExpr,
    outer_row: &[Value],
    outer_cols: &[String],
    inner_quals: &[String],
) -> FilterExpr {
    let recur = |g: &FilterExpr| correlate_filter(g, outer_row, outer_cols, inner_quals);
    match f {
        FilterExpr::And(a, b) => FilterExpr::And(Box::new(recur(a)), Box::new(recur(b))),
        FilterExpr::Or(a, b) => FilterExpr::Or(Box::new(recur(a)), Box::new(recur(b))),
        FilterExpr::Not(a) => FilterExpr::Not(Box::new(recur(a))),
        FilterExpr::Predicate(p) => {
            // Outer reference on the right: `inner.col <op> outer.col`.
            if let Operand::Ident(rn) = &p.right {
                if is_outer_ref(rn, inner_quals) {
                    if let Some(v) = outer_lookup(rn, outer_cols, outer_row) {
                        return FilterExpr::Predicate(Predicate {
                            left: p.left.clone(),
                            op: p.op.clone(),
                            right: Operand::Literal(v),
                        });
                    }
                }
            }
            // Outer reference on the left with an inner column on the right:
            // swap sides so the inner column stays comparable, flipping the op.
            if is_outer_ref(&p.left, inner_quals) {
                if let (Some(v), Operand::Ident(rn)) =
                    (outer_lookup(&p.left, outer_cols, outer_row), &p.right)
                {
                    if !is_outer_ref(rn, inner_quals) {
                        return FilterExpr::Predicate(Predicate {
                            left: rn.clone(),
                            op: flip_op(&p.op),
                            right: Operand::Literal(v),
                        });
                    }
                }
            }
            f.clone()
        }
        FilterExpr::InList {
            left,
            list,
            negated,
        } => {
            let new_list = list
                .iter()
                .map(|op| match op {
                    Operand::Ident(n) if is_outer_ref(n, inner_quals) => {
                        outer_lookup(n, outer_cols, outer_row)
                            .map(Operand::Literal)
                            .unwrap_or_else(|| op.clone())
                    }
                    _ => op.clone(),
                })
                .collect();
            FilterExpr::InList {
                left: left.clone(),
                list: new_list,
                negated: *negated,
            }
        }
        FilterExpr::Like {
            left,
            right,
            negated,
        } => {
            let new_right = match right {
                Operand::Ident(n) if is_outer_ref(n, inner_quals) => {
                    outer_lookup(n, outer_cols, outer_row)
                        .map(Operand::Literal)
                        .unwrap_or_else(|| right.clone())
                }
                _ => right.clone(),
            };
            FilterExpr::Like {
                left: left.clone(),
                right: new_right,
                negated: *negated,
            }
        }
        FilterExpr::ExprCmp { left, op, right } => FilterExpr::ExprCmp {
            left: correlate_scalar(left, outer_row, outer_cols, inner_quals),
            op: op.clone(),
            right: correlate_scalar(right, outer_row, outer_cols, inner_quals),
        },
        // Nested subquery predicates and null checks pass through: nested
        // correlation is not resolved here.
        other => other.clone(),
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

/// Splits a synthetic JSON-access name like `data->>'name'` into
/// `(base_column, operator, key)`. `->>` is checked before `->`.
pub(crate) fn parse_json_ref(s: &str) -> Option<(String, String, String)> {
    for op in ["->>", "->"] {
        if let Some(pos) = s.find(op) {
            let base = s[..pos].trim().to_string();
            let key = s[pos + op.len()..].trim().trim_matches('\'').to_string();
            if !base.is_empty() && !key.is_empty() {
                return Some((base, op.to_string(), key));
            }
        }
    }
    None
}

/// Evaluates `value -> key` / `value ->> key` on a JSON(B) or JSON-text value.
/// `->>` returns text; `->` returns the sub-document. Missing keys yield NULL.
pub(crate) fn json_extract(value: &Value, op: &str, key: &str) -> Value {
    let json: serde_json::Value = match value {
        Value::Jsonb(j) => j.clone(),
        Value::Text(s) => match serde_json::from_str(s) {
            Ok(j) => j,
            Err(_) => return Value::Null,
        },
        _ => return Value::Null,
    };
    let Some(sub) = json.get(key) else {
        return Value::Null;
    };
    match op {
        "->>" => match sub {
            serde_json::Value::String(s) => Value::Text(s.clone()),
            serde_json::Value::Null => Value::Null,
            other => Value::Text(other.to_string()),
        },
        _ => Value::Jsonb(sub.clone()),
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
