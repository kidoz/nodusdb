//! Window functions: each call computed over the rows of a query (or over
//! its groups), partitioned, ordered, and framed as PostgreSQL does. The
//! calls are taken out of the select list first and their values read back
//! as hidden columns.

use crate::plan_types::{FrameBound, FrameExclude, WindowCall, WindowFrameUnits};
use crate::{ProjectionItem, ScalarExpr, SortTarget, Value};
use anyhow::{Result, bail};
use std::cmp::Ordering;

/// The hidden column a window call's values are read from.
pub(crate) fn hidden_column(k: usize) -> String {
    format!("\u{1}window{k}")
}

/// Which window call's values a hidden column holds.
pub(crate) fn hidden_index(column: &str) -> Option<usize> {
    column.strip_prefix("\u{1}window")?.parse().ok()
}

/// Replaces every window call in the select list and the sort keys with a
/// reference to its hidden column, and returns the calls (identical calls
/// once).
pub(crate) fn extract(
    projection: Vec<ProjectionItem>,
    keys: Vec<SortTarget>,
) -> (Vec<ProjectionItem>, Vec<SortTarget>, Vec<WindowCall>) {
    fn replace(e: &ScalarExpr, calls: &mut Vec<WindowCall>) -> ScalarExpr {
        if let ScalarExpr::Window(call) = e {
            let k = match calls.iter().position(|c| c == &**call) {
                Some(k) => k,
                None => {
                    calls.push((**call).clone());
                    calls.len() - 1
                }
            };
            return ScalarExpr::Column(hidden_column(k));
        }
        e.map_children(&mut |c| replace(c, calls))
    }
    let mut calls = Vec::new();
    let projection = projection
        .into_iter()
        .map(|item| match item {
            ProjectionItem::Expr { expr, alias } => ProjectionItem::Expr {
                expr: replace(&expr, &mut calls),
                alias,
            },
            other => other,
        })
        .collect();
    let keys = keys
        .into_iter()
        .map(|key| match key {
            SortTarget::Expr(e) => SortTarget::Expr(replace(&e, &mut calls)),
            other => other,
        })
        .collect();
    (projection, keys, calls)
}

/// Each call's value for each of `n` input rows, given how to evaluate an
/// expression for a row: `[call][row]`.
pub(crate) fn compute(
    calls: &[WindowCall],
    n: usize,
    eval: &dyn Fn(&ScalarExpr, usize) -> Value,
) -> Result<Vec<Vec<Value>>> {
    calls
        .iter()
        .map(|call| compute_call(call, n, eval))
        .collect()
}

fn keys_equal(a: &[Value], b: &[Value]) -> bool {
    a.iter()
        .zip(b)
        .all(|(x, y)| crate::compare(x, y) == Ordering::Equal)
}

fn compute_call(
    call: &WindowCall,
    n: usize,
    eval: &dyn Fn(&ScalarExpr, usize) -> Value,
) -> Result<Vec<Value>> {
    let partition: Vec<Vec<Value>> = (0..n)
        .map(|i| call.partition_by.iter().map(|e| eval(e, i)).collect())
        .collect();
    let order: Vec<Vec<Value>> = (0..n)
        .map(|i| call.order_by.iter().map(|(e, _, _)| eval(e, i)).collect())
        .collect();
    let mut sorted: Vec<usize> = (0..n).collect();
    sorted.sort_by(|&a, &b| {
        for (x, y) in partition[a].iter().zip(&partition[b]) {
            let ord = crate::compare(x, y);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        for (k, (_, asc, nulls_first)) in call.order_by.iter().enumerate() {
            let ord = crate::select::order_cmp(&order[a][k], &order[b][k], *asc, *nulls_first);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });
    let mut results = vec![Value::Null; n];
    let mut start = 0;
    while start < sorted.len() {
        let mut end = start + 1;
        while end < sorted.len() && keys_equal(&partition[sorted[start]], &partition[sorted[end]]) {
            end += 1;
        }
        Partition::new(call, &sorted[start..end], &order, eval).compute(&mut results)?;
        start = end;
    }
    Ok(results)
}

/// One partition's rows in window order, and their peer groups (rows with
/// equal ordering keys).
struct Partition<'a> {
    call: &'a WindowCall,
    rows: &'a [usize],
    order: &'a [Vec<Value>],
    eval: &'a dyn Fn(&ScalarExpr, usize) -> Value,
    group_of: Vec<usize>,
    group_start: Vec<usize>,
    group_end: Vec<usize>,
}

impl<'a> Partition<'a> {
    fn new(
        call: &'a WindowCall,
        rows: &'a [usize],
        order: &'a [Vec<Value>],
        eval: &'a dyn Fn(&ScalarExpr, usize) -> Value,
    ) -> Self {
        let mut group_of = Vec::with_capacity(rows.len());
        let (mut group_start, mut group_end) = (Vec::new(), Vec::new());
        for pos in 0..rows.len() {
            if pos == 0 || !keys_equal(&order[rows[pos]], &order[rows[pos - 1]]) {
                if pos > 0 {
                    group_end.push(pos - 1);
                }
                group_start.push(pos);
            }
            group_of.push(group_start.len() - 1);
        }
        if !rows.is_empty() {
            group_end.push(rows.len() - 1);
        }
        Partition {
            call,
            rows,
            order,
            eval,
            group_of,
            group_start,
            group_end,
        }
    }

    fn arg(&self, i: usize, pos: usize) -> Value {
        self.call
            .args
            .get(i)
            .map_or(Value::Null, |e| (self.eval)(e, self.rows[pos]))
    }

    fn compute(&self, results: &mut [Value]) -> Result<()> {
        let len = self.rows.len();
        let name = self.call.func.as_str();
        for pos in 0..len {
            let g = self.group_of[pos];
            let value = match name {
                "ROW_NUMBER" => Value::Int(pos as i64 + 1),
                "RANK" => Value::Int(self.group_start[g] as i64 + 1),
                "DENSE_RANK" => Value::Int(g as i64 + 1),
                "PERCENT_RANK" => Value::Float(if len > 1 {
                    self.group_start[g] as f64 / (len - 1) as f64
                } else {
                    0.0
                }),
                "CUME_DIST" => Value::Float((self.group_end[g] + 1) as f64 / len as f64),
                "NTILE" => match integer(&self.arg(0, 0)) {
                    None => Value::Null,
                    Some(buckets) if buckets <= 0 => {
                        bail!("argument of ntile must be greater than zero")
                    }
                    Some(buckets) => Value::Int(ntile(pos, len, buckets as usize)),
                },
                "LAG" | "LEAD" => {
                    let offset = match self.call.args.get(1) {
                        Some(_) => integer(&self.arg(1, pos)),
                        None => Some(1),
                    };
                    match offset {
                        None => Value::Null,
                        Some(offset) => {
                            let target = if name == "LEAD" {
                                pos as i64 + offset
                            } else {
                                pos as i64 - offset
                            };
                            if (0..len as i64).contains(&target) {
                                self.arg(0, target as usize)
                            } else {
                                self.arg(2, pos)
                            }
                        }
                    }
                }
                "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => {
                    let frame = self.frame(pos)?;
                    let pick = match name {
                        "FIRST_VALUE" => frame.first().copied(),
                        "LAST_VALUE" => frame.last().copied(),
                        _ => match integer(&self.arg(1, pos)) {
                            None => None,
                            Some(n) if n <= 0 => {
                                bail!("argument of nth_value must be greater than zero")
                            }
                            Some(n) => frame.get(n as usize - 1).copied(),
                        },
                    };
                    pick.map_or(Value::Null, |p| self.arg(0, p))
                }
                _ => match crate::planner::aggregate_op(name) {
                    Some(op) => {
                        let inputs: Vec<(Value, Vec<Value>)> = self
                            .frame(pos)?
                            .into_iter()
                            .filter(|&p| {
                                self.call.filter.as_ref().is_none_or(|f| {
                                    (self.eval)(f, self.rows[p]) == Value::Bool(true)
                                })
                            })
                            .map(|p| {
                                if self.call.args.is_empty() {
                                    // `count(*)`: every row counts.
                                    (Value::Int(1), Vec::new())
                                } else {
                                    let extra =
                                        (1..self.call.args.len()).map(|i| self.arg(i, p)).collect();
                                    (self.arg(0, p), extra)
                                }
                            })
                            .collect();
                        crate::aggregates::aggregate_inputs(&op, &inputs)
                    }
                    None => bail!("function {}() does not exist", name.to_ascii_lowercase()),
                },
            };
            results[self.rows[pos]] = value;
        }
        crate::eval_error::check()?;
        Ok(())
    }

    /// The positions of the row at `pos`'s frame, in window order.
    fn frame(&self, pos: usize) -> Result<Vec<usize>> {
        let len = self.rows.len() as i64;
        let g = self.group_of[pos];
        let Some(frame) = &self.call.frame else {
            // Without a frame: the whole partition, or with an ORDER BY up
            // to the current row's last peer.
            let end = if self.call.order_by.is_empty() {
                len - 1
            } else {
                self.group_end[g] as i64
            };
            return Ok((0..=end as usize).collect());
        };
        let groups = self.group_start.len() as i64;
        let p = pos as i64;
        let start = match (&frame.units, &frame.start) {
            (_, FrameBound::UnboundedPreceding) => 0,
            (_, FrameBound::UnboundedFollowing) => len,
            (WindowFrameUnits::Rows, FrameBound::CurrentRow) => p,
            (_, FrameBound::CurrentRow) => self.group_start[g] as i64,
            (WindowFrameUnits::Rows, FrameBound::Preceding(e)) => {
                p - self.offset(e, pos, "starting")?
            }
            (WindowFrameUnits::Rows, FrameBound::Following(e)) => {
                p + self.offset(e, pos, "starting")?
            }
            (WindowFrameUnits::Groups, FrameBound::Preceding(e)) => {
                let target = g as i64 - self.offset(e, pos, "starting")?;
                if target < 0 {
                    0
                } else {
                    self.group_start[target as usize] as i64
                }
            }
            (WindowFrameUnits::Groups, FrameBound::Following(e)) => {
                let target = g as i64 + self.offset(e, pos, "starting")?;
                if target >= groups {
                    len
                } else {
                    self.group_start[target as usize] as i64
                }
            }
            (WindowFrameUnits::Range, FrameBound::Preceding(e)) => {
                self.range_bound(e, pos, true, true)?
            }
            (WindowFrameUnits::Range, FrameBound::Following(e)) => {
                self.range_bound(e, pos, false, true)?
            }
        };
        let end = match (&frame.units, &frame.end) {
            (_, FrameBound::UnboundedPreceding) => -1,
            (_, FrameBound::UnboundedFollowing) => len - 1,
            (WindowFrameUnits::Rows, FrameBound::CurrentRow) => p,
            (_, FrameBound::CurrentRow) => self.group_end[g] as i64,
            (WindowFrameUnits::Rows, FrameBound::Preceding(e)) => {
                p - self.offset(e, pos, "ending")?
            }
            (WindowFrameUnits::Rows, FrameBound::Following(e)) => {
                p + self.offset(e, pos, "ending")?
            }
            (WindowFrameUnits::Groups, FrameBound::Preceding(e)) => {
                let target = g as i64 - self.offset(e, pos, "ending")?;
                if target < 0 {
                    -1
                } else {
                    self.group_end[target as usize] as i64
                }
            }
            (WindowFrameUnits::Groups, FrameBound::Following(e)) => {
                let target = (g as i64 + self.offset(e, pos, "ending")?).min(groups - 1);
                self.group_end[target as usize] as i64
            }
            (WindowFrameUnits::Range, FrameBound::Preceding(e)) => {
                self.range_bound(e, pos, true, false)?
            }
            (WindowFrameUnits::Range, FrameBound::Following(e)) => {
                self.range_bound(e, pos, false, false)?
            }
        };
        let (lo, hi) = (start.max(0), end.min(len - 1));
        if lo > hi {
            return Ok(Vec::new());
        }
        let peers = self.group_start[g]..=self.group_end[g];
        Ok((lo as usize..=hi as usize)
            .filter(|&q| match frame.exclude {
                FrameExclude::NoOthers => true,
                FrameExclude::CurrentRow => q != pos,
                FrameExclude::Group => !peers.contains(&q),
                FrameExclude::Ties => q == pos || !peers.contains(&q),
            })
            .collect())
    }

    /// A `ROWS`/`GROUPS` offset: a count that must not be null or negative.
    fn offset(&self, e: &ScalarExpr, pos: usize, which: &str) -> Result<i64> {
        match (self.eval)(e, self.rows[pos]) {
            Value::Null => bail!("frame {which} offset must not be null"),
            value => match integer(&value) {
                Some(n) if n >= 0 => Ok(n),
                Some(_) => bail!("frame {which} offset must not be negative"),
                None => bail!("argument of ROWS must be type bigint"),
            },
        }
    }

    /// A `RANGE` bound: the first (for a start) or last (for an end)
    /// position whose ordering key lies within `offset` of the current row's,
    /// before it (`preceding`) or after it in window order.
    fn range_bound(
        &self,
        e: &ScalarExpr,
        pos: usize,
        preceding: bool,
        is_start: bool,
    ) -> Result<i64> {
        let [(_, asc, nulls_first)] = self.call.order_by.as_slice() else {
            bail!("RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column");
        };
        let nulls_first = nulls_first.unwrap_or(!asc);
        let key = &self.order[self.rows[pos]][0];
        let g = self.group_of[pos];
        let offset = (self.eval)(e, self.rows[pos]);
        if matches!(key, Value::Null) {
            // A NULL key's frame is its peers.
            return Ok(if is_start {
                self.group_start[g]
            } else {
                self.group_end[g]
            } as i64);
        }
        let target = shifted(key, &offset, preceding == *asc)?;
        let position = |q: usize| -> Ordering {
            // Where row `q`'s key lies relative to the target, in window order.
            match range_key(&self.order[self.rows[q]][0]) {
                None if nulls_first => Ordering::Less,
                None => Ordering::Greater,
                Some(k) => {
                    let ord = k.cmp(&target);
                    if *asc { ord } else { ord.reverse() }
                }
            }
        };
        let len = self.rows.len();
        Ok(if is_start {
            (0..len)
                .find(|&q| position(q) != Ordering::Less)
                .map_or(len as i64, |q| q as i64)
        } else {
            (0..len)
                .rev()
                .find(|&q| position(q) != Ordering::Greater)
                .map_or(-1, |q| q as i64)
        })
    }
}

/// A value as an integer, when it is one.
fn integer(value: &Value) -> Option<i64> {
    match value {
        Value::Int(i) => Some(*i),
        Value::Numeric(d) if d.is_integral() => d.to_i64(),
        Value::Float(f) if f.fract() == 0.0 => Some(*f as i64),
        Value::Text(t) => t.trim().parse().ok(),
        _ => None,
    }
}

/// The bucket (1-based) of position `pos` among `len` rows split into
/// `buckets` as evenly as possible, the first ones one larger.
fn ntile(pos: usize, len: usize, buckets: usize) -> i64 {
    let (base, extra) = (len / buckets, len % buckets);
    let large = extra * (base + 1);
    let bucket = if pos < large {
        pos / (base + 1)
    } else {
        extra + (pos - large) / base.max(1)
    };
    bucket as i64 + 1
}

/// An ordering key in the terms a `RANGE` offset is measured in.
#[derive(Debug, Clone, PartialEq)]
enum RangeKey {
    Number(crate::numeric::Numeric),
    Float(f64),
    /// A moment (or time of day, or interval) in microseconds.
    Micros(i128),
}

impl RangeKey {
    fn cmp(&self, other: &RangeKey) -> Ordering {
        let float = |k: &RangeKey| match k {
            RangeKey::Number(d) => d.to_f64(),
            RangeKey::Float(f) => *f,
            RangeKey::Micros(m) => *m as f64,
        };
        match (self, other) {
            (RangeKey::Number(a), RangeKey::Number(b)) => a.cmp(b),
            (RangeKey::Micros(a), RangeKey::Micros(b)) => a.cmp(b),
            (a, b) => crate::value::float_cmp(float(a), float(b)),
        }
    }
}

fn range_key(value: &Value) -> Option<RangeKey> {
    use crate::datetime::Temporal;
    Some(match value {
        Value::Int(i) => RangeKey::Number((*i).into()),
        Value::Numeric(d) => RangeKey::Number(d.clone()),
        Value::Float(f) => RangeKey::Float(*f),
        Value::Text(_) => match Temporal::read(value, None)? {
            Temporal::Time(micros) => RangeKey::Micros(micros as i128),
            Temporal::Interval(iv) => RangeKey::Micros(iv.span()),
            other => RangeKey::Micros(moment(&other)?),
        },
        _ => return None,
    })
}

fn moment(temporal: &crate::datetime::Temporal) -> Option<i128> {
    use crate::datetime::Temporal;
    let ts = match temporal {
        Temporal::Date(d) => d.and_hms_opt(0, 0, 0)?,
        Temporal::Timestamp(ts) | Temporal::TimestampTz(ts) => *ts,
        _ => return None,
    };
    Some(ts.and_utc().timestamp_micros() as i128)
}

/// `key - offset` (with `subtract`) or `key + offset`, as a range key.
fn shifted(key: &Value, offset: &Value, subtract: bool) -> Result<RangeKey> {
    use crate::datetime::{Interval, Temporal};
    let invalid = || anyhow::anyhow!("invalid preceding or following size in window function");
    let unsupported = || match range_key(key) {
        None => anyhow::anyhow!(
            "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {}",
            crate::value::value_type_name(key)
        ),
        Some(_) => anyhow::anyhow!(
            "RANGE with offset PRECEDING/FOLLOWING is not supported for column type {} and offset type {}",
            crate::value::value_type_name(key),
            crate::value::value_type_name(offset)
        ),
    };
    if matches!(offset, Value::Null) {
        return Err(invalid());
    }
    let number = match offset {
        Value::Int(i) => Some(crate::numeric::Numeric::from(*i)),
        Value::Numeric(d) => Some(d.clone()),
        Value::Float(f) if f.is_nan() => Some(crate::numeric::Numeric::NaN),
        Value::Float(f) => crate::numeric::Numeric::from_f64_exact(*f),
        _ => None,
    };
    if let Some(n) = number {
        if n.is_sign_negative() || n.is_nan() {
            return Err(invalid());
        }
        return Ok(match range_key(key) {
            Some(RangeKey::Number(k)) => RangeKey::Number(if subtract { &k - &n } else { &k + &n }),
            Some(RangeKey::Float(k)) => {
                let n = n.to_f64();
                RangeKey::Float(if subtract { k - n } else { k + n })
            }
            _ => return Err(unsupported()),
        });
    }
    let Some(interval) = Interval::parse(&crate::render(offset)) else {
        return Err(unsupported());
    };
    if interval.span() < 0 {
        return Err(invalid());
    }
    let interval = if subtract {
        interval.negate()
    } else {
        interval
    };
    match Temporal::read(key, None) {
        Some(Temporal::Time(micros)) => {
            Ok(RangeKey::Micros(micros as i128 + interval.micros as i128))
        }
        Some(Temporal::Interval(iv)) => Ok(RangeKey::Micros(iv.add(interval).span())),
        Some(
            temporal @ (Temporal::Date(_) | Temporal::Timestamp(_) | Temporal::TimestampTz(_)),
        ) => {
            let base = match temporal {
                Temporal::Date(d) => d.and_hms_opt(0, 0, 0),
                Temporal::Timestamp(ts) | Temporal::TimestampTz(ts) => Some(ts),
                _ => None,
            };
            base.and_then(|ts| crate::datetime::add_interval(ts, interval))
                .map(|ts| RangeKey::Micros(ts.and_utc().timestamp_micros() as i128))
                .ok_or_else(|| anyhow::anyhow!("timestamp out of range"))
        }
        _ => Err(unsupported()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_types::FrameSpec;

    fn call(
        func: &str,
        args: Vec<ScalarExpr>,
        order: bool,
        frame: Option<FrameSpec>,
    ) -> WindowCall {
        WindowCall {
            func: func.to_string(),
            args,
            filter: None,
            partition_by: vec![],
            order_by: if order {
                vec![(ScalarExpr::Column("x".into()), true, None)]
            } else {
                vec![]
            },
            frame,
        }
    }

    fn run(call: &WindowCall, xs: &[Value]) -> Vec<Value> {
        let eval = |e: &ScalarExpr, row: usize| match e {
            ScalarExpr::Column(_) => xs[row].clone(),
            ScalarExpr::Literal(v) => v.clone(),
            _ => Value::Null,
        };
        compute(std::slice::from_ref(call), xs.len(), &eval)
            .unwrap()
            .remove(0)
    }

    fn ints(values: &[i64]) -> Vec<Value> {
        values.iter().map(|v| Value::Int(*v)).collect()
    }

    #[test]
    fn ranking_functions_follow_peers() {
        let xs = ints(&[10, 5, 7, 5]);
        assert_eq!(
            run(&call("RANK", vec![], true, None), &xs),
            ints(&[4, 1, 3, 1])
        );
        assert_eq!(
            run(&call("DENSE_RANK", vec![], true, None), &xs),
            ints(&[3, 1, 2, 1])
        );
        assert_eq!(
            run(&call("CUME_DIST", vec![], true, None), &xs),
            vec![
                Value::Float(1.0),
                Value::Float(0.5),
                Value::Float(0.75),
                Value::Float(0.5)
            ]
        );
        assert_eq!(ntile(0, 7, 3), 1);
        assert_eq!(ntile(3, 7, 3), 2);
        assert_eq!(ntile(6, 7, 3), 3);
    }

    #[test]
    fn frames_measure_rows_groups_and_ranges() {
        let x = || ScalarExpr::Column("x".into());
        let lit = |v: i64| ScalarExpr::Literal(Value::Int(v));
        let frame = |units, start, end, exclude| {
            Some(FrameSpec {
                units,
                start,
                end,
                exclude,
            })
        };
        let xs = ints(&[1, 2, 2, 5]);
        // RANGE 1 PRECEDING .. 1 FOLLOWING: values within 1 of the row's.
        let range = call(
            "SUM",
            vec![x()],
            true,
            frame(
                WindowFrameUnits::Range,
                FrameBound::Preceding(lit(1)),
                FrameBound::Following(lit(1)),
                FrameExclude::NoOthers,
            ),
        );
        assert_eq!(run(&range, &xs), ints(&[5, 5, 5, 5]));
        // GROUPS 1 PRECEDING .. CURRENT ROW: the previous peer group too.
        let groups = call(
            "COUNT",
            vec![],
            true,
            frame(
                WindowFrameUnits::Groups,
                FrameBound::Preceding(lit(1)),
                FrameBound::CurrentRow,
                FrameExclude::NoOthers,
            ),
        );
        assert_eq!(run(&groups, &xs), ints(&[1, 3, 3, 3]));
        // EXCLUDE TIES keeps the row but not its peers.
        let ties = call(
            "COUNT",
            vec![],
            true,
            frame(
                WindowFrameUnits::Rows,
                FrameBound::UnboundedPreceding,
                FrameBound::UnboundedFollowing,
                FrameExclude::Ties,
            ),
        );
        assert_eq!(run(&ties, &xs), ints(&[4, 3, 3, 4]));
    }

    #[test]
    fn value_functions_read_offsets_and_defaults() {
        let x = || ScalarExpr::Column("x".into());
        let lit = |v: i64| ScalarExpr::Literal(Value::Int(v));
        let xs = ints(&[1, 2, 3]);
        assert_eq!(
            run(&call("LAG", vec![x(), lit(1), lit(-1)], true, None), &xs),
            ints(&[-1, 1, 2])
        );
        assert_eq!(
            run(&call("NTH_VALUE", vec![x(), lit(2)], true, None), &xs),
            vec![Value::Null, Value::Int(2), Value::Int(2)]
        );
    }
}
