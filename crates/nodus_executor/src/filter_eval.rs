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
                    CompareOp::Contains => {
                        println!("Contains: left={:?}, right={:?}", left_cell, right_cell);
                        match (left_cell, &right_cell) {
                            (Value::Array(l), Value::Array(r)) => {
                                r.iter().all(|r_item| l.contains(r_item))
                            }
                            (Value::Text(l), Value::Text(r))
                                if l.starts_with('{') || l.starts_with('[') =>
                            {
                                // Simplified JSONB @> eval for MVP text-encoded JSON
                                let l_json: Result<serde_json::Value, _> = serde_json::from_str(l);
                                let r_json: Result<serde_json::Value, _> = serde_json::from_str(r);
                                if let (Ok(l_obj), Ok(r_obj)) = (l_json, r_json) {
                                    if let (Some(l_map), Some(r_map)) =
                                        (l_obj.as_object(), r_obj.as_object())
                                    {
                                        let matched = r_map.iter().all(|(k, v)| {
                                            if let Some(lv) = l_map.get(k) {
                                                lv == v
                                            } else {
                                                false
                                            }
                                        });
                                        println!(
                                            "JSONB @> l='{}', r='{}', matched={}",
                                            l, r, matched
                                        );
                                        matched
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                }
                            }
                            (Value::Text(l), Value::Array(r)) if l.starts_with('{') => {
                                // Array check against text representing array: "{login,signup}"
                                let mut l_str = l.clone();
                                if l_str.starts_with('{') && l_str.ends_with('}') {
                                    l_str = l_str[1..l_str.len() - 1].to_string();
                                }
                                let l_items: Vec<&str> = l_str.split(',').collect();
                                r.iter().all(|r_item| {
                                    let r_str = render(r_item);
                                    l_items
                                        .iter()
                                        .any(|&s| s == r_str || s == format!("'{}'", r_str))
                                })
                            }
                            (Value::Array(l), Value::Text(r)) => {
                                // Right might be a text parsing failure for ARRAY[] in some ASTs?
                                // Actually, `right_cell` should be evaluated correctly if it's an ARRAY[] literal.
                                false
                            }
                            (Value::Jsonb(l), Value::Jsonb(r)) => {
                                if let (Some(l_map), Some(r_map)) = (l.as_object(), r.as_object()) {
                                    r_map.iter().all(|(k, v)| l_map.get(k) == Some(v))
                                } else {
                                    false
                                }
                            }
                            _ => false,
                        }
                    }
                    CompareOp::ContainedBy => {
                        // <@
                        false // Simplified MVP
                    }
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
