//! Aggregate computation (COUNT/SUM/MIN/MAX) and HAVING predicate evaluation,
//! shared by the SELECT executor and the GROUP BY path.

use crate::*;

/// Computes one aggregate over a group's rows. `inner` is the aggregated column
/// name (or `*` for `COUNT(*)`). Shared by SELECT projection and HAVING.
pub(crate) fn compute_aggregate(
    op: &AggregateOp,
    inner: &str,
    group_rows: &[Vec<Value>],
    col_names: &[String],
) -> Value {
    let mut idx = col_names
        .iter()
        .position(|tc| tc == inner || tc.ends_with(&format!(".{inner}")));
    if inner == "*" {
        idx = Some(0);
    }
    match op {
        AggregateOp::Count => {
            let count = if inner == "*" {
                group_rows.len() as i64
            } else {
                group_rows
                    .iter()
                    .filter(|r| {
                        idx.and_then(|i| r.get(i))
                            .is_some_and(|v| !matches!(v, Value::Null))
                    })
                    .count() as i64
            };
            Value::Int(count)
        }
        // Every other aggregate skips NULLs, and is NULL over no values.
        _ => {
            let values: Vec<Value> = group_rows
                .iter()
                .map(|r| idx.and_then(|i| r.get(i)).cloned().unwrap_or(Value::Null))
                .collect();
            aggregate_values(op, &values)
        }
    }
}

/// A value as a `double precision`, for the statistics aggregates.
fn float_of(value: &Value) -> Option<f64> {
    match value {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Numeric(d) => Some(crate::value::decimal_to_f64(d)),
        _ => None,
    }
}

fn no_such_function(op: &AggregateOp, values: &[&Value]) -> Value {
    crate::eval_error::raise(format!(
        "function {}({}) does not exist",
        op.sql_name(),
        values
            .iter()
            .map(|v| crate::value::value_type_name(v))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

/// `stddev`/`variance` and their population forms. Integers and numerics
/// are exact, with PostgreSQL's result scale; floats accumulate as
/// PostgreSQL's do (Youngs-Cramer), so the last digits agree.
fn spread(op: &AggregateOp, values: Vec<&Value>) -> Value {
    use crate::numeric::Numeric;
    let sample = matches!(op, AggregateOp::StddevSamp | AggregateOp::VarSamp);
    let root = matches!(op, AggregateOp::StddevSamp | AggregateOp::StddevPop);
    let n = values.len();
    if n == 0 || (sample && n < 2) {
        return Value::Null;
    }
    if values.iter().any(|v| matches!(v, Value::Float(_))) {
        let (mut count, mut sum, mut squares) = (0f64, 0f64, 0f64);
        for value in &values {
            let Some(x) = float_of(value) else {
                return no_such_function(op, &[value]);
            };
            count += 1.0;
            sum += x;
            if count > 1.0 {
                let t = x * count - sum;
                squares += t * t / (count * (count - 1.0));
            }
        }
        let variance = squares / if sample { count - 1.0 } else { count };
        return Value::Float(if root { variance.sqrt() } else { variance });
    }
    let (mut sum, mut squares) = (Numeric::zero(), Numeric::zero());
    for value in &values {
        let x = match value {
            Value::Int(i) => Numeric::from(*i),
            Value::Numeric(d) => d.clone(),
            other => return no_such_function(op, &[other]),
        };
        squares += &(&x * &x);
        sum += &x;
    }
    if !sum.is_finite() || !squares.is_finite() {
        return Value::Numeric(Numeric::NaN);
    }
    let count = Numeric::from(n);
    // No spread at all (or rounding below it) is an exact zero.
    let numerator = &(&count * &squares) - &(&sum * &sum);
    if numerator.signum() <= 0 {
        return Value::Numeric(Numeric::zero());
    }
    let denominator = &count
        * &if sample {
            &count - &Numeric::one()
        } else {
            count.clone()
        };
    match numerator.checked_div(&denominator) {
        Ok(variance) if root => {
            let scale = i64::from(variance.scale());
            Value::Numeric(variance.sqrt_to(scale))
        }
        Ok(variance) => Value::Numeric(variance),
        Err(e) => crate::eval_error::raise(e),
    }
}

/// `percentile_cont`, `percentile_disc`, and `mode` over the inputs, which
/// arrive sorted by the `WITHIN GROUP` order; the direct argument (the
/// fraction, or an array of them) is the first extra argument.
fn ordered_set(op: &AggregateOp, inputs: &[(Value, Vec<Value>)]) -> Value {
    let values: Vec<&Value> = inputs
        .iter()
        .map(|(v, _)| v)
        .filter(|v| !matches!(v, Value::Null))
        .collect();
    if values.is_empty() {
        return Value::Null;
    }
    if *op == AggregateOp::Mode {
        // The most frequent value; of equally frequent ones, the first.
        let (mut best, mut best_count, mut run_start) = (0, 0, 0);
        for i in 1..=values.len() {
            if i == values.len() || !crate::values_equal(values[i], values[run_start]) {
                if i - run_start > best_count {
                    (best, best_count) = (run_start, i - run_start);
                }
                run_start = i;
            }
        }
        return values[best].clone();
    }
    let fraction = inputs
        .first()
        .and_then(|(_, extra)| extra.first())
        .cloned()
        .unwrap_or(Value::Null);
    let one = |fraction: &Value| -> Value {
        let Some(p) = float_of(fraction) else {
            return Value::Null;
        };
        if !(0.0..=1.0).contains(&p) {
            return crate::eval_error::raise(format!(
                "percentile value {} is not between 0 and 1",
                crate::render(fraction)
            ));
        }
        let n = values.len();
        if *op == AggregateOp::PercentileDisc {
            let row = ((p * n as f64).ceil() as usize).clamp(1, n);
            return values[row - 1].clone();
        }
        let position = p * (n - 1) as f64;
        let (lo, hi) = (position.floor() as usize, position.ceil() as usize);
        let proportion = position - lo as f64;
        // Intervals interpolate as intervals; numbers as doubles.
        if let (Value::Text(a), Value::Text(b)) = (values[lo], values[hi])
            && let (Some(a), Some(b)) = (
                crate::datetime::Interval::parse(a),
                crate::datetime::Interval::parse(b),
            )
        {
            if lo == hi {
                return Value::Text(a.format());
            }
            return match b.add(a.negate()).mul(proportion) {
                Ok(step) => Value::Text(a.add(step).format()),
                Err(e) => crate::eval_error::raise(e),
            };
        }
        match (float_of(values[lo]), float_of(values[hi])) {
            (Some(a), _) if lo == hi => Value::Float(a),
            (Some(a), Some(b)) => Value::Float(a + (b - a) * proportion),
            _ => crate::eval_error::raise(format!(
                "function percentile_cont({}, {}) does not exist",
                if matches!(fraction, Value::Float(_)) {
                    "double precision"
                } else {
                    "numeric"
                },
                crate::datetime::Temporal::read(values[lo], None)
                    .map_or(crate::value::value_type_name(values[lo]), |t| t
                        .kind()
                        .sql_name())
            )),
        }
    };
    match &fraction {
        Value::Null => Value::Null,
        Value::Array(fractions) => Value::Array(fractions.iter().map(one).collect()),
        f => one(f),
    }
}

/// `rank(h) WITHIN GROUP (ORDER BY x)` and the other hypothetical-set
/// aggregates: where `h` would fall among the inputs. The extra arguments
/// are `h`, whether the order is ascending, and its `NULLS FIRST`.
fn hypothetical(op: &AggregateOp, inputs: &[(Value, Vec<Value>)]) -> Value {
    let extra = inputs.first().map(|(_, e)| e.as_slice()).unwrap_or(&[]);
    let hypothetical = extra.first().cloned().unwrap_or(Value::Null);
    let ascending = !matches!(extra.get(1), Some(Value::Bool(false)));
    let nulls_first = match extra.get(2) {
        Some(Value::Bool(b)) => Some(*b),
        _ => None,
    };
    let n = inputs.len();
    let (mut before, mut peers) = (0usize, 0usize);
    let mut distinct_before: Vec<&Value> = Vec::new();
    for (value, _) in inputs {
        match crate::select::order_cmp(value, &hypothetical, ascending, nulls_first) {
            std::cmp::Ordering::Less => {
                before += 1;
                if !distinct_before
                    .iter()
                    .any(|v| crate::values_equal(v, value))
                {
                    distinct_before.push(value);
                }
            }
            std::cmp::Ordering::Equal => peers += 1,
            std::cmp::Ordering::Greater => {}
        }
    }
    match op {
        AggregateOp::HypotheticalRank => Value::Int(before as i64 + 1),
        AggregateOp::HypotheticalDenseRank => Value::Int(distinct_before.len() as i64 + 1),
        AggregateOp::HypotheticalPercentRank => Value::Float(if n == 0 {
            0.0
        } else {
            before as f64 / n as f64
        }),
        _ => Value::Float((before + peers + 1) as f64 / (n + 1) as f64),
    }
}

/// `corr`, `covar_*`, and `regr_*` over (y, x) pairs, accumulated as
/// PostgreSQL does (Youngs-Cramer), so the results agree to the last digit.
fn regression(op: &AggregateOp, inputs: &[(Value, Vec<Value>)]) -> Value {
    let (mut n, mut sx, mut sy, mut sxx, mut syy, mut sxy) = (0f64, 0f64, 0f64, 0f64, 0f64, 0f64);
    for (y, extra) in inputs {
        let x = extra.first().unwrap_or(&Value::Null);
        if matches!(y, Value::Null) || matches!(x, Value::Null) {
            continue;
        }
        let (Some(y), Some(x)) = (float_of(y), float_of(x)) else {
            return no_such_function(op, &[y, x]);
        };
        n += 1.0;
        sx += x;
        sy += y;
        if n > 1.0 {
            let (tx, ty) = (x * n - sx, y * n - sy);
            let scale = 1.0 / (n * (n - 1.0));
            sxx += tx * tx * scale;
            syy += ty * ty * scale;
            sxy += tx * ty * scale;
        }
    }
    if *op == AggregateOp::RegrCount {
        return Value::Int(n as i64);
    }
    if n < 1.0 {
        return Value::Null;
    }
    let float = |v: f64| Value::Float(v);
    match op {
        AggregateOp::RegrSxx => float(sxx),
        AggregateOp::RegrSyy => float(syy),
        AggregateOp::RegrSxy => float(sxy),
        AggregateOp::RegrAvgX => float(sx / n),
        AggregateOp::RegrAvgY => float(sy / n),
        AggregateOp::CovarPop => float(sxy / n),
        AggregateOp::CovarSamp if n < 2.0 => Value::Null,
        AggregateOp::CovarSamp => float(sxy / (n - 1.0)),
        AggregateOp::Corr if sxx == 0.0 || syy == 0.0 => Value::Null,
        AggregateOp::Corr => float(sxy / (sxx * syy).sqrt()),
        AggregateOp::RegrR2 if sxx == 0.0 => Value::Null,
        AggregateOp::RegrR2 if syy == 0.0 => float(1.0),
        AggregateOp::RegrR2 => float((sxy * sxy) / (sxx * syy)),
        AggregateOp::RegrSlope if sxx == 0.0 => Value::Null,
        AggregateOp::RegrSlope => float(sxy / sxx),
        AggregateOp::RegrIntercept if sxx == 0.0 => Value::Null,
        AggregateOp::RegrIntercept => float((sy - sx * sxy / sxx) / n),
        _ => Value::Null,
    }
}

/// Parses a HAVING predicate left-hand side: an aggregate key like `SUM(amount)`
/// or `COUNT(*)`, otherwise `None` (a plain group column).
pub(crate) fn parse_aggregate_key(key: &str) -> Option<(AggregateOp, String)> {
    let open = key.find('(')?;
    if !key.ends_with(')') {
        return None;
    }
    let func = key[..open].to_ascii_uppercase();
    let arg = key[open + 1..key.len() - 1].to_string();
    let op = match func.as_str() {
        "COUNT" => AggregateOp::Count,
        "SUM" => AggregateOp::Sum,
        "MIN" => AggregateOp::Min,
        "MAX" => AggregateOp::Max,
        _ => return None,
    };
    Some((op, arg))
}

/// Aggregates a list of already-computed per-row values (used for aggregates
/// over expressions, e.g. `sum(a + 1)`). NULLs are skipped, matching SQL
/// aggregate semantics; an all-NULL/empty input yields NULL (0 for COUNT).
pub(crate) fn aggregate_values(op: &AggregateOp, vals: &[Value]) -> Value {
    let inputs: Vec<(Value, Vec<Value>)> = vals.iter().map(|v| (v.clone(), Vec::new())).collect();
    aggregate_inputs(op, &inputs)
}

/// Aggregates per-row inputs, in order: each row's argument value and its
/// further arguments (a delimiter, an object value). Every aggregate but
/// `array_agg` and the JSON aggregates skips NULL values; over no values each
/// is NULL except `count`, which is 0.
pub(crate) fn aggregate_inputs(op: &AggregateOp, inputs: &[(Value, Vec<Value>)]) -> Value {
    let values = || inputs.iter().map(|(v, _)| v);
    let non_null = || values().filter(|v| **v != Value::Null);
    match op {
        AggregateOp::Count
        | AggregateOp::Sum
        | AggregateOp::Avg
        | AggregateOp::Min
        | AggregateOp::Max => {
            let vals: Vec<Value> = values().cloned().collect();
            aggregate_numeric(op, &vals)
        }
        AggregateOp::StringAgg => {
            let mut out: Option<String> = None;
            for (value, extra) in inputs {
                if *value == Value::Null {
                    continue;
                }
                let text = crate::render(value);
                out = Some(match out {
                    // Each value after the first is preceded by its own row's
                    // delimiter; a NULL delimiter adds nothing.
                    Some(mut acc) => {
                        if let Some(d) = extra.first().filter(|d| **d != Value::Null) {
                            acc.push_str(&crate::render(d));
                        }
                        acc.push_str(&text);
                        acc
                    }
                    None => text,
                });
            }
            out.map_or(Value::Null, Value::Text)
        }
        AggregateOp::ArrayAgg => {
            if inputs.is_empty() {
                Value::Null
            } else {
                Value::Array(values().cloned().collect())
            }
        }
        AggregateOp::BoolAnd | AggregateOp::BoolOr => {
            let mut result: Option<bool> = None;
            for value in non_null() {
                let Value::Bool(b) = value else {
                    return crate::eval_error::raise(format!(
                        "function {}({}) does not exist",
                        op.sql_name(),
                        crate::value::value_type_name(value)
                    ));
                };
                result = Some(match (op, result) {
                    (AggregateOp::BoolAnd, Some(acc)) => acc && *b,
                    (_, Some(acc)) => acc || *b,
                    (_, None) => *b,
                });
            }
            result.map_or(Value::Null, Value::Bool)
        }
        AggregateOp::JsonAgg => {
            if inputs.is_empty() {
                return Value::Null;
            }
            // Arrays and rows each start a line after the first.
            let structured = values().any(|v| matches!(v, Value::Array(_) | Value::Record(_)));
            let mut out = String::from("[");
            for (i, value) in values().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                    if structured && *value != Value::Null {
                        out.push_str("\n ");
                    }
                }
                crate::json_text::value_json(value, false, &mut out);
            }
            out.push(']');
            Value::Json(out)
        }
        AggregateOp::JsonbAgg => {
            if inputs.is_empty() {
                Value::Null
            } else {
                Value::Jsonb(serde_json::Value::Array(
                    values().map(crate::functions::to_json).collect(),
                ))
            }
        }
        AggregateOp::JsonObjectAgg => {
            if inputs.is_empty() {
                return Value::Null;
            }
            let mut out = String::from("{ ");
            for (i, (key, extra)) in inputs.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                if let Err(e) = crate::json_text::key_json(key, &mut out) {
                    return crate::eval_error::raise(e);
                }
                out.push_str(" : ");
                crate::json_text::value_json(
                    extra.first().unwrap_or(&Value::Null),
                    false,
                    &mut out,
                );
            }
            out.push_str(" }");
            Value::Json(out)
        }
        AggregateOp::JsonbObjectAgg => {
            if inputs.is_empty() {
                return Value::Null;
            }
            let mut object = serde_json::Map::new();
            for (key, extra) in inputs {
                if *key == Value::Null {
                    return crate::eval_error::raise("null value not allowed for object key");
                }
                let value = extra
                    .first()
                    .map_or(serde_json::Value::Null, crate::functions::to_json);
                object.insert(crate::render(key), value);
            }
            Value::Jsonb(serde_json::Value::Object(object))
        }
        AggregateOp::StddevSamp
        | AggregateOp::StddevPop
        | AggregateOp::VarSamp
        | AggregateOp::VarPop => spread(op, non_null().collect()),
        AggregateOp::PercentileCont | AggregateOp::PercentileDisc | AggregateOp::Mode => {
            ordered_set(op, inputs)
        }
        AggregateOp::HypotheticalRank
        | AggregateOp::HypotheticalDenseRank
        | AggregateOp::HypotheticalPercentRank
        | AggregateOp::HypotheticalCumeDist => hypothetical(op, inputs),
        AggregateOp::Corr
        | AggregateOp::CovarPop
        | AggregateOp::CovarSamp
        | AggregateOp::RegrSlope
        | AggregateOp::RegrIntercept
        | AggregateOp::RegrCount
        | AggregateOp::RegrR2
        | AggregateOp::RegrAvgX
        | AggregateOp::RegrAvgY
        | AggregateOp::RegrSxx
        | AggregateOp::RegrSyy
        | AggregateOp::RegrSxy => regression(op, inputs),
        AggregateOp::AnyValue => non_null().next().cloned().unwrap_or(Value::Null),
        AggregateOp::BitAnd | AggregateOp::BitOr | AggregateOp::BitXor => {
            let mut result: Option<i64> = None;
            for value in non_null() {
                let Value::Int(i) = value else {
                    return crate::eval_error::raise(format!(
                        "function {}({}) does not exist",
                        op.sql_name(),
                        crate::value::value_type_name(value)
                    ));
                };
                result = Some(match (op, result) {
                    (AggregateOp::BitAnd, Some(acc)) => acc & i,
                    (AggregateOp::BitXor, Some(acc)) => acc ^ i,
                    (_, Some(acc)) => acc | i,
                    (_, None) => *i,
                });
            }
            result.map_or(Value::Null, Value::Int)
        }
    }
}

/// The values as intervals, when every one is interval text as PostgreSQL
/// shows it (`1 day 02:00:00`, `-03:00:00`).
fn interval_values(values: &[&Value]) -> Option<Vec<crate::datetime::Interval>> {
    if values.is_empty() {
        return None;
    }
    values
        .iter()
        .map(|value| match value {
            Value::Text(text) if crate::value::parse_temporal(text).is_none() => {
                crate::datetime::Interval::parse(text).filter(|iv| iv.format() == *text)
            }
            _ => None,
        })
        .collect()
}

/// `sum`, `avg`, `min`, and `max` of intervals, ordered by their length.
fn aggregate_intervals(
    op: &AggregateOp,
    values: &[&Value],
    intervals: &[crate::datetime::Interval],
) -> Value {
    let sum = || {
        intervals
            .iter()
            .fold(crate::datetime::Interval::default(), |acc, iv| acc.add(*iv))
    };
    match op {
        AggregateOp::Count => Value::Int(values.len() as i64),
        AggregateOp::Sum => Value::Text(sum().format()),
        AggregateOp::Avg => match sum().div(intervals.len() as f64) {
            Ok(avg) => Value::Text(avg.format()),
            Err(error) => crate::eval_error::raise(error),
        },
        // Of equal intervals (`1 day`, `24 hours`), the last one wins.
        _ => {
            let mut best = 0;
            for (i, iv) in intervals.iter().enumerate().skip(1) {
                let ord = iv.span().cmp(&intervals[best].span());
                if ord == std::cmp::Ordering::Equal
                    || (ord == std::cmp::Ordering::Less) == (*op == AggregateOp::Min)
                {
                    best = i;
                }
            }
            values[best].clone()
        }
    }
}

/// `count`, `sum`, `avg`, `min`, and `max` over values.
fn aggregate_numeric(op: &AggregateOp, vals: &[Value]) -> Value {
    let non_null: Vec<&Value> = vals.iter().filter(|v| **v != Value::Null).collect();
    if let Some(intervals) = interval_values(&non_null) {
        return aggregate_intervals(op, &non_null, &intervals);
    }
    match op {
        AggregateOp::Count => Value::Int(non_null.len() as i64),
        AggregateOp::Sum | AggregateOp::Avg => {
            if non_null.is_empty() {
                return Value::Null;
            }
            // Integers and numerics sum exactly; `avg` of them is a numeric
            // with PostgreSQL's division scale. A float makes both floats.
            if !non_null.iter().any(|v| matches!(v, Value::Float(_))) {
                let mut sum = crate::numeric::Numeric::zero();
                let mut any_numeric = false;
                for v in &non_null {
                    match v {
                        Value::Int(i) => sum += &crate::numeric::Numeric::from(*i),
                        Value::Numeric(d) => {
                            any_numeric = true;
                            sum += d;
                        }
                        _ => return Value::Null,
                    }
                }
                return if *op == AggregateOp::Avg {
                    crate::planner::numeric_div(sum, crate::numeric::Numeric::from(non_null.len()))
                } else if any_numeric {
                    Value::Numeric(sum)
                } else {
                    // An integer sum too large for bigint is a numeric.
                    sum.to_i64().map_or(Value::Numeric(sum), Value::Int)
                };
            }
            let mut int_sum = 0i64;
            let mut float_sum = 0f64;
            let mut is_float = false;
            for v in &non_null {
                match v {
                    Value::Int(i) => {
                        int_sum = match int_sum.checked_add(*i) {
                            Some(sum) => sum,
                            None => return crate::eval_error::raise("bigint out of range"),
                        };
                        float_sum += *i as f64;
                    }
                    Value::Float(f) => {
                        is_float = true;
                        float_sum += f;
                    }
                    Value::Numeric(d) => {
                        is_float = true;
                        float_sum += crate::value::decimal_to_f64(d);
                    }
                    _ => return Value::Null,
                }
            }
            if *op == AggregateOp::Avg {
                Value::Float(float_sum / non_null.len() as f64)
            } else if is_float {
                Value::Float(float_sum)
            } else {
                Value::Int(int_sum)
            }
        }
        AggregateOp::Min | AggregateOp::Max => {
            let mut best: Option<&Value> = None;
            for v in non_null {
                best = Some(match best {
                    None => v,
                    Some(b) => {
                        let ord = crate::compare(v, b);
                        let take = if *op == AggregateOp::Min {
                            ord == std::cmp::Ordering::Less
                        } else {
                            ord == std::cmp::Ordering::Greater
                        };
                        if take { v } else { b }
                    }
                });
            }
            best.cloned().unwrap_or(Value::Null)
        }
        // Routed by `aggregate_inputs`.
        _ => Value::Null,
    }
}

/// Evaluates a scalar expression over an aggregated group: `Aggregate` nodes
/// compute over `group_rows`; a `Column` reads the group's representative
/// (first) row. Enables `sum(a) + 1`, `count(*) * 2`, etc.
pub(crate) fn eval_scalar_expr_grouped(
    expr: &ScalarExpr,
    group_rows: &[Vec<Value>],
    col_names: &[String],
) -> Value {
    crate::planner::eval_scalar_in(
        expr,
        &GroupScope {
            group_rows,
            col_names,
        },
    )
}

struct GroupScope<'a> {
    group_rows: &'a [Vec<Value>],
    col_names: &'a [String],
}

impl crate::planner::ScalarScope for GroupScope<'_> {
    fn column(&self, name: &str) -> Value {
        crate::filter_eval::col_pos(self.col_names, name)
            .and_then(|i| self.group_rows.first().and_then(|r| r.get(i)))
            .cloned()
            .unwrap_or(Value::Null)
    }

    fn aggregate(&self, expr: &ScalarExpr) -> Value {
        let ScalarExpr::Aggregate {
            op,
            arg,
            arg_expr,
            distinct,
            extra_args,
            filter,
            order_by,
        } = expr
        else {
            return Value::Null;
        };
        let (group_rows, col_names) = (self.group_rows, self.col_names);
        let eval = |e: &ScalarExpr, r: &[Value]| crate::planner::eval_scalar_expr(e, r, col_names);
        // Only rows where FILTER is true are aggregated.
        let rows: Vec<&Vec<Value>> = group_rows
            .iter()
            .filter(|r| {
                filter
                    .as_ref()
                    .is_none_or(|f| eval(f, r) == Value::Bool(true))
            })
            .collect();
        // `count(*)` counts rows rather than values.
        if arg == "*" && arg_expr.is_none() {
            return Value::Int(rows.len() as i64);
        }
        let column = crate::filter_eval::col_pos(col_names, arg);
        let mut inputs: Vec<(Vec<Value>, (Value, Vec<Value>))> = rows
            .iter()
            .map(|r| {
                let value = match arg_expr {
                    Some(e) => eval(e, r),
                    None => column
                        .and_then(|i| r.get(i))
                        .cloned()
                        .unwrap_or(Value::Null),
                };
                let extra = extra_args.iter().map(|e| eval(e, r)).collect();
                let keys = order_by.iter().map(|(e, _, _)| eval(e, r)).collect();
                (keys, (value, extra))
            })
            .collect();
        if !order_by.is_empty() {
            inputs.sort_by(|(a, _), (b, _)| {
                for (i, (_, ascending, nulls_first)) in order_by.iter().enumerate() {
                    let ord = crate::select::order_cmp(&a[i], &b[i], *ascending, *nulls_first);
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        let mut inputs: Vec<(Value, Vec<Value>)> = inputs.into_iter().map(|(_, i)| i).collect();
        if *distinct {
            // `agg(DISTINCT x)`: the first of each distinct value, in order.
            let mut kept: Vec<(Value, Vec<Value>)> = Vec::with_capacity(inputs.len());
            for input in inputs {
                if !kept.iter().any(|(v, _)| crate::values_equal(v, &input.0)) {
                    kept.push(input);
                }
            }
            inputs = kept;
        }
        aggregate_inputs(op, &inputs)
    }
}

/// Resolves a HAVING reference (aggregate key or group column) to a value.
pub(crate) fn having_value(
    name: &str,
    group_rows: &[Vec<Value>],
    col_names: &[String],
) -> Option<Value> {
    if let Some((op, arg)) = parse_aggregate_key(name) {
        return Some(compute_aggregate(&op, &arg, group_rows, col_names));
    }
    let idx = col_names
        .iter()
        .position(|tc| tc == name || tc.ends_with(&format!(".{name}")))?;
    group_rows.first().and_then(|r| r.get(idx)).cloned()
}

/// Coerces a numeric-looking text operand so comparisons are numeric, not lexical.
pub(crate) fn having_operand(
    op: &Operand,
    group_rows: &[Vec<Value>],
    col_names: &[String],
) -> Option<Value> {
    match op {
        Operand::Literal(Value::Text(s)) => {
            if let Ok(i) = s.parse::<i64>() {
                Some(Value::Int(i))
            } else if let Ok(f) = s.parse::<f64>() {
                Some(Value::Float(f))
            } else {
                Some(Value::Text(s.clone()))
            }
        }
        Operand::Literal(v) => Some(v.clone()),
        Operand::Ident(name) => having_value(name, group_rows, col_names),
    }
}

/// Evaluates a HAVING predicate against one aggregated group.
pub(crate) fn eval_having(
    expr: &FilterExpr,
    group_rows: &[Vec<Value>],
    col_names: &[String],
) -> bool {
    match expr {
        FilterExpr::And(l, r) => {
            eval_having(l, group_rows, col_names) && eval_having(r, group_rows, col_names)
        }
        FilterExpr::Or(l, r) => {
            eval_having(l, group_rows, col_names) || eval_having(r, group_rows, col_names)
        }
        FilterExpr::Not(inner) => !eval_having(inner, group_rows, col_names),
        FilterExpr::Predicate(p) => {
            let (Some(left), Some(right)) = (
                having_value(&p.left, group_rows, col_names),
                having_operand(&p.right, group_rows, col_names),
            ) else {
                return false;
            };
            let ord = compare(&left, &right);
            match p.op {
                CompareOp::Eq => left == right,
                CompareOp::Ne => left != right,
                CompareOp::Lt => ord == std::cmp::Ordering::Less,
                CompareOp::Le => ord != std::cmp::Ordering::Greater,
                CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                CompareOp::Ge => ord != std::cmp::Ordering::Less,
                _ => false,
            }
        }
        // A computed comparison, e.g. `HAVING min(a) = max(a)`: both sides
        // evaluate over the group (aggregates included).
        FilterExpr::ExprCmp { left, op, right } => {
            let l = eval_scalar_expr_grouped(left, group_rows, col_names);
            let r = eval_scalar_expr_grouped(right, group_rows, col_names);
            if l == Value::Null || r == Value::Null {
                return false;
            }
            let ord = compare(&l, &r);
            match op {
                CompareOp::Eq => crate::values_equal(&l, &r),
                CompareOp::Ne => !crate::values_equal(&l, &r),
                CompareOp::Lt => ord == std::cmp::Ordering::Less,
                CompareOp::Le => ord != std::cmp::Ordering::Greater,
                CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                CompareOp::Ge => ord != std::cmp::Ordering::Less,
                _ => false,
            }
        }
        // The planner lowers HAVING to one scalar condition over the group.
        FilterExpr::Scalar(e) => {
            eval_scalar_expr_grouped(e, group_rows, col_names) == Value::Bool(true)
        }
        // Shapes the planner never produces for HAVING must not admit groups.
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs(values: &[Value], extra: &[Value]) -> Vec<(Value, Vec<Value>)> {
        values.iter().map(|v| (v.clone(), extra.to_vec())).collect()
    }

    fn ints(values: &[i64]) -> Vec<Value> {
        values.iter().map(|v| Value::Int(*v)).collect()
    }

    fn numeric(text: &str) -> Value {
        Value::Numeric(text.parse().unwrap())
    }

    #[test]
    fn spread_is_exact_for_integers_and_numerics() {
        let qty = ints(&[10, 5, 7, 3, 7, 1, 2]);
        let refs: Vec<&Value> = qty.iter().collect();
        assert_eq!(
            spread(&AggregateOp::VarPop, refs.clone()),
            numeric("8.8571428571428571")
        );
        assert_eq!(
            spread(&AggregateOp::StddevSamp, refs),
            numeric("3.2145502536643183")
        );
        // A single value has no spread: an exact zero, or none for a sample.
        let one = [Value::Int(4)];
        assert_eq!(
            spread(&AggregateOp::VarPop, one.iter().collect()),
            numeric("0")
        );
        assert_eq!(
            spread(&AggregateOp::VarSamp, one.iter().collect()),
            Value::Null
        );
    }

    #[test]
    fn ordered_sets_read_their_sorted_input() {
        let sorted = ints(&[1, 2, 3, 5, 7, 7, 10]);
        let half = [numeric("0.5")];
        assert_eq!(
            ordered_set(&AggregateOp::PercentileCont, &inputs(&sorted, &half)),
            Value::Float(5.0)
        );
        assert_eq!(
            ordered_set(&AggregateOp::PercentileDisc, &inputs(&sorted, &half)),
            Value::Int(5)
        );
        assert_eq!(
            ordered_set(&AggregateOp::Mode, &inputs(&sorted, &[])),
            Value::Int(7)
        );
        let quartile = [numeric("0.25")];
        assert_eq!(
            ordered_set(&AggregateOp::PercentileCont, &inputs(&sorted, &quartile)),
            Value::Float(2.5)
        );
        let many = [Value::Array(vec![numeric("0.1"), numeric("0.9")])];
        assert_eq!(
            ordered_set(&AggregateOp::PercentileDisc, &inputs(&sorted, &many)),
            Value::Array(ints(&[1, 10]))
        );
    }

    #[test]
    fn hypothetical_rows_rank_among_the_inputs() {
        let values = ints(&[10, 5, 7, 3, 7, 1, 2]);
        let seven = [Value::Int(7), Value::Bool(true), Value::Null];
        assert_eq!(
            hypothetical(&AggregateOp::HypotheticalRank, &inputs(&values, &seven)),
            Value::Int(5)
        );
        assert_eq!(
            hypothetical(
                &AggregateOp::HypotheticalDenseRank,
                &inputs(&values, &seven)
            ),
            Value::Int(5)
        );
        assert_eq!(
            hypothetical(&AggregateOp::HypotheticalCumeDist, &inputs(&values, &seven)),
            Value::Float(0.875)
        );
    }

    #[test]
    fn regressions_follow_youngs_cramer() {
        let pairs: Vec<(Value, Vec<Value>)> = [(1, 2), (2, 4), (3, 6)]
            .iter()
            .map(|(y, x)| (Value::Int(*y), vec![Value::Int(*x)]))
            .collect();
        assert_eq!(
            regression(&AggregateOp::RegrSlope, &pairs),
            Value::Float(0.5)
        );
        assert_eq!(regression(&AggregateOp::Corr, &pairs), Value::Float(1.0));
        assert_eq!(regression(&AggregateOp::RegrCount, &pairs), Value::Int(3));
        assert_eq!(
            regression(&AggregateOp::CovarSamp, &pairs[..1]),
            Value::Null
        );
    }
}
