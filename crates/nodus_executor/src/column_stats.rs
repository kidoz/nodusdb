//! What `ANALYZE` finds in a column: PostgreSQL's `pg_statistic` (and so
//! `pg_stats`). The algorithms are PostgreSQL 18's `compute_scalar_stats`,
//! `compute_array_stats`, `compute_range_stats` and the trivial case,
//! transcribed: the same sample, the same most-common-value cut, the same
//! histogram bounds, and the same correlation, so the numbers match a
//! PostgreSQL of the same version.

use crate::*;
use nodus_catalog::{ColumnStatistics, StatSlot};

/// PostgreSQL's `default_statistics_target`: the MCV and histogram size.
/// PostgreSQL's `default_statistics_target`.
const DEFAULT_TARGET: usize = 100;
/// PostgreSQL samples `300 * target` rows; a table up to that size is its
/// own sample.

/// `pg_statistic`'s statistic kinds.
const KIND_MCV: i16 = 1;
const KIND_HISTOGRAM: i16 = 2;
const KIND_CORRELATION: i16 = 3;
const KIND_MCELEM: i16 = 4;
const KIND_DECHIST: i16 = 5;
const KIND_RANGE_LENGTH_HISTOGRAM: i16 = 6;
const KIND_BOUNDS_HISTOGRAM: i16 = 7;

/// The float8 `<` operator, which the range length histogram is keyed by.
const FLOAT8_LT: i64 = 672;
/// The anyarray `=` and `<` operators: an array column's scalar statistics.
const ARRAY_EQ: i64 = 1070;
const ARRAY_LT: i64 = 1072;
/// The generic anyenum `=` and `<` operators: an enum column's.
const ENUM_EQ: i64 = 3516;
const ENUM_LT: i64 = 3518;

/// The columns' statistics, in column order: one entry per column whose
/// analysis found something (PostgreSQL stores no `pg_statistic` row for a
/// zero-row table's columns). A table with descendants also gets, after its
/// own rows, a pass over its whole inheritance tree's rows, recorded with
/// `stainherit` as PostgreSQL records it.
pub(crate) fn analyze_table(
    executor: &MemExecutor,
    ctx: &ExecutionContext,
    tbl: &nodus_catalog::TableDescriptor,
) -> Result<Vec<ColumnStatistics>> {
    let rows = executor.scan_rows(tbl.id, &ctx.session_id)?;
    let mut all = column_statistics(tbl, &rows, false);
    crate::statistics::analyze_objects(executor, tbl, &rows, false)?;
    let descendants = executor.descendants("default", tbl.id)?;
    if !descendants.is_empty() {
        // The parent's rows, then each descendant's, as PostgreSQL's
        // inherited sample reads them.
        let mut inherited = rows;
        for child in &descendants {
            inherited.extend(executor.scan_rows(child.id, &ctx.session_id)?);
        }
        all.extend(column_statistics(tbl, &inherited, true));
        crate::statistics::analyze_objects(executor, tbl, &inherited, true)?;
    }
    Ok(all)
}

/// One pass over `rows`: each of `tbl`'s columns analyzed, in column order.
fn column_statistics(
    tbl: &nodus_catalog::TableDescriptor,
    rows: &[Vec<Value>],
    inherited: bool,
) -> Vec<ColumnStatistics> {
    // PostgreSQL draws a sample of 300 * statistics target rows at random;
    // taking the first rows of the scan keeps ANALYZE deterministic. The
    // sample covers the widest target among the table's columns.
    let widest = tbl
        .columns
        .iter()
        .map(|column| column_target(column))
        .max()
        .unwrap_or(0);
    let sample_rows = 300 * widest;
    // PostgreSQL's `totalrows` is the table's own row count, not the
    // sample's: the distinct estimate and the MCV list's cutoff are scaled
    // by it.
    let totalrows = rows.len() as f64;
    let sampled: &[Vec<Value>] = if rows.len() > sample_rows {
        &rows[..sample_rows]
    } else {
        rows
    };
    let mut all = Vec::new();
    for (index, column) in tbl.columns.iter().enumerate() {
        // A column whose statistics target is zero is not analyzed, and
        // keeps whatever statistics it had, as PostgreSQL leaves them.
        let target = column_target(column);
        if target == 0 {
            if let Some(previous) = tbl
                .statistics
                .iter()
                .find(|stats| stats.attnum == index as i32 + 1 && stats.inherited == inherited)
            {
                all.push(previous.clone());
            }
            continue;
        }
        let fill = Value::Null;
        let cells: Vec<&Value> = sampled
            .iter()
            .map(|row| row.get(index).unwrap_or(&fill))
            .collect();
        let attnum = index as i32 + 1;
        if let Some(mut statistics) =
            analyze_column(attnum, &column.data_type, &cells, totalrows, target)
        {
            statistics.inherited = inherited;
            all.push(statistics);
        }
    }
    all
}

/// A column's statistics target: its own (`ALTER COLUMN ... SET STATISTICS`),
/// or the session's `default_statistics_target`.
pub(crate) fn column_target(column: &nodus_catalog::ColumnDescriptor) -> usize {
    if column.statistics_target >= 0 {
        return column.statistics_target as usize;
    }
    crate::session_env::setting("default_statistics_target")
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_TARGET)
}

/// One expression's statistics, as a column's would be: what extended
/// statistics record for an expression key (`pg_stats_ext_exprs`).
pub(crate) fn analyze_values(
    attnum: i32,
    data_type: &str,
    cells: &[&Value],
    totalrows: f64,
    target: usize,
) -> Option<ColumnStatistics> {
    analyze_column(attnum, data_type, cells, totalrows, target)
}

/// One column's statistics, or `None` when the sample had no rows at all.
fn analyze_column(
    attnum: i32,
    data_type: &str,
    cells: &[&Value],
    totalrows: f64,
    target: usize,
) -> Option<ColumnStatistics> {
    let samplerows = cells.len();
    if samplerows == 0 {
        return None;
    }
    let (declared, is_array) = split_array_type(data_type);
    // A domain's statistics are its base type's.
    let base = crate::user_types::base_type(&declared).to_ascii_lowercase();
    // An enum's statistics are keyed by the generic enum operators, and its
    // values are four bytes wide.
    let enum_labels = crate::user_types::enum_type(&base).is_some();
    let element_facts = if enum_labels {
        TypeFacts {
            typlen: 4,
            ordered: true,
            eq_opr: ENUM_EQ,
            lt_opr: ENUM_LT,
            collation: 0,
            name: "enum",
            align: 4,
        }
    } else {
        type_facts(&base)
    };
    // An array column is its own type: a varlena with the anyarray operators
    // and the element's collation.
    let facts = if is_array {
        TypeFacts {
            typlen: -1,
            ordered: true,
            eq_opr: ARRAY_EQ,
            lt_opr: ARRAY_LT,
            collation: element_facts.collation,
            name: "array",
            align: element_facts.align,
        }
    } else {
        element_facts.clone()
    };

    let range_kind = crate::ranges::Kind::of(&base);
    let mut null_cnt = 0usize;
    let mut nonnull: Vec<&Value> = Vec::new();
    let mut total_width = 0f64;
    for cell in cells {
        match cell {
            Value::Null => null_cnt += 1,
            value => {
                nonnull.push(value);
                total_width += value_width(value, &facts, &element_facts, is_array, range_kind);
            }
        }
    }
    let null_frac = (null_cnt as f64 / samplerows as f64) as f32;
    let avg_width = width_of(&facts, total_width, nonnull.len());

    // A range column has its own history: the bounds and length histograms.
    if let Some(kind) = range_kind {
        let mut slots = Vec::new();
        report_range_stats(target, kind, &nonnull, samplerows - null_cnt, &mut slots);
        // Non-null ranges are assumed distinct ("unknown" would be 0; PG
        // estimates them unique).
        return Some(ColumnStatistics {
            attnum,
            data_type: data_type.to_string(),
            inherited: false,
            null_frac,
            avg_width,
            n_distinct: -(1.0 - null_frac as f64) as f32,
            slots,
        });
    }

    if nonnull.is_empty() {
        // Only nulls: PostgreSQL assumes the column is entirely null.
        return Some(ColumnStatistics {
            attnum,
            data_type: data_type.to_string(),
            inherited: false,
            null_frac,
            avg_width,
            n_distinct: 0.0,
            slots: Vec::new(),
        });
    }

    if !facts.ordered {
        // No default btree order: the distinct count is "unknown" to
        // PostgreSQL, which stores zero.
        return Some(ColumnStatistics {
            attnum,
            data_type: data_type.to_string(),
            inherited: false,
            null_frac,
            avg_width,
            n_distinct: 0.0,
            slots: Vec::new(),
        });
    }

    let mut slots = Vec::new();
    // A tsvector has no order statistics: its lexemes are its statistics
    // (PostgreSQL's `ts_typanalyze`).
    if base == "tsvector" {
        tsvector_stats(&nonnull, &mut slots, target);
        return Some(ColumnStatistics {
            attnum,
            data_type: data_type.to_string(),
            inherited: false,
            null_frac,
            avg_width,
            n_distinct: -(1.0 - null_frac as f64) as f32,
            slots,
        });
    }
    let n_distinct = scalar_stats(
        target,
        &base,
        &facts,
        &nonnull,
        null_cnt,
        samplerows,
        totalrows,
        null_frac as f64,
        &mut slots,
    );
    if is_array {
        array_stats(target, &nonnull, &base, &element_facts, &mut slots);
    }
    Some(ColumnStatistics {
        attnum,
        data_type: data_type.to_string(),
        inherited: false,
        null_frac,
        avg_width,
        n_distinct: n_distinct as f32,
        slots,
    })
}

/// A value's order for a column: numeric and textual types compare by value
/// (text by bytes — the engine's own collation), arrays element-wise, and the
/// network and money types as PostgreSQL orders them.
fn cmp_values(base: &str, a: &Value, b: &Value) -> std::cmp::Ordering {
    // An enum's labels compare in their declared order; an array of them
    // element by element.
    if crate::user_types::enum_type(base).is_some() {
        if let (Value::Array(x), Value::Array(y)) = (a, b) {
            for (xe, ye) in x.iter().zip(y.iter()) {
                let ord = cmp_values(base, xe, ye);
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            return x.len().cmp(&y.len());
        }
        return match (
            crate::user_types::enum_sort(a, base),
            crate::user_types::enum_sort(b, base),
        ) {
            (Value::Float(x), Value::Float(y)) => {
                x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal)
            }
            (x, y) => crate::value::compare(&x, &y),
        };
    }
    match (base, a, b) {
        ("inet" | "cidr", Value::Text(x), Value::Text(y)) => crate::net::cmp_inet(x, y),
        ("macaddr", Value::Text(x), Value::Text(y)) => crate::net::cmp_mac(x, y, false),
        ("macaddr8", Value::Text(x), Value::Text(y)) => crate::net::cmp_mac(x, y, true),
        ("money", Value::Text(x), Value::Text(y)) => crate::net::cmp_money(x, y),
        _ => crate::value::compare(a, b),
    }
}

/// A statistic value in the executor's serialized form: the catalog stores
/// statistic values as opaque text, as it stores default expressions.
fn serialized(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

/// A statistic value as PostgreSQL renders it inside `pg_statistic`'s or
/// `pg_stats`' array column: an array column's values are themselves arrays,
/// and their text — braces and all — is quoted, as an array of that type
/// stores it. Other values pass through.
pub(crate) fn statistic_value(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Text(array_literal(&items)),
        other => other,
    }
}

/// An array literal, its elements quoted as PostgreSQL quotes them (the same
/// rule the wire's array text uses: empty, `NULL`, or a brace, comma, quote,
/// backslash or space forces quotes).
fn array_literal(items: &[Value]) -> String {
    let rendered: Vec<String> = items
        .iter()
        .map(|item| match item {
            Value::Null => "NULL".to_string(),
            Value::Array(nested) => quote_element(&array_literal(nested)),
            other => quote_element(&crate::render(other)),
        })
        .collect();
    format!("{{{}}}", rendered.join(","))
}

/// `text` quoted as an array element when it needs it.
fn quote_element(text: &str) -> String {
    if text.is_empty()
        || text.eq_ignore_ascii_case("NULL")
        || text
            .bytes()
            .any(|b| matches!(b, b'"' | b'\\' | b',' | b'{' | b'}') || b.is_ascii_whitespace())
    {
        format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        text.to_owned()
    }
}

/// PostgreSQL's `compute_scalar_stats`: most common values, histogram,
/// correlation, and the distinct estimate. Returns `stadistinct`.
fn scalar_stats(
    target: usize,
    base: &str,
    facts: &TypeFacts,
    nonnull: &[&Value],
    null_cnt: usize,
    samplerows: usize,
    totalrows: f64,
    null_frac: f64,
    slots: &mut Vec<StatSlot>,
) -> f64 {
    // The values with their position in the sample order (their tupno),
    // sorted by value; ties keep their sample order, as PostgreSQL's sort
    // orders equal items by tupno.
    let mut order: Vec<usize> = (0..nonnull.len()).collect();
    order.sort_by(|&x, &y| cmp_values(base, nonnull[x], nonnull[y]).then(x.cmp(&y)));

    let values_cnt = nonnull.len();
    let mut corr_xysum = 0f64;
    let mut ndistinct = 0usize;
    let mut nmultiple = 0usize;
    // The MCV track: up to the target's (count, first sorted index) pairs,
    // ordered by count descending (ties keep the smaller value first).
    let mut track: Vec<(usize, usize)> = Vec::new();
    let mut dups_cnt = 0usize;
    for i in 0..values_cnt {
        let tupno = order[i];
        corr_xysum += (i as f64) * (tupno as f64);
        dups_cnt += 1;
        let last = i + 1 == values_cnt
            || cmp_values(base, nonnull[order[i]], nonnull[order[i + 1]])
                != std::cmp::Ordering::Equal;
        if last {
            ndistinct += 1;
            if dups_cnt > 1 {
                nmultiple += 1;
                if track.len() < target || dups_cnt > track[track.len() - 1].0 {
                    if track.len() < target {
                        track.push((0, 0));
                    }
                    let mut j = track.len() - 1;
                    while j > 0 {
                        if dups_cnt <= track[j - 1].0 {
                            break;
                        }
                        track[j] = track[j - 1];
                        j -= 1;
                    }
                    track[j] = (dups_cnt, i + 1 - dups_cnt);
                }
            }
            dups_cnt = 0;
        }
    }

    // The distinct estimate, as PostgreSQL's three cases.
    let mut n_distinct = if nmultiple == 0 {
        // No repeated value: assume a unique column, discounted for nulls.
        -1.0 * (1.0 - null_frac)
    } else if nmultiple == ndistinct {
        // Every sampled value appeared more than once: assume these are all.
        ndistinct as f64
    } else {
        // The Haas and Stokes Duj1 estimator.
        let f1 = ndistinct - nmultiple;
        let d = f1 + nmultiple;
        let n = (samplerows - null_cnt) as f64;
        let big_n = totalrows * (1.0 - null_frac);
        let estimate = if big_n > 0.0 {
            (n * d as f64) / ((n - f1 as f64) + f1 as f64 * n / big_n)
        } else {
            0.0
        };
        let estimate = estimate.max(d as f64).min(big_n);
        (estimate + 0.5).floor()
    };
    if n_distinct > 0.1 * totalrows {
        n_distinct = -(n_distinct / totalrows);
    }

    // How many MCVs to keep.
    let num_mcv = if track.len() == ndistinct && n_distinct > 0.0 && track.len() <= target {
        // The whole sample's values fit, and they are believed to be all
        // there is: keep the complete list.
        track.len()
    } else {
        let mut num_mcv = track.len();
        if num_mcv > 0 {
            let counts: Vec<i64> = track.iter().map(|&(count, _)| count as i64).collect();
            num_mcv = analyze_mcv_list(&counts, n_distinct, null_frac, samplerows, totalrows);
        }
        num_mcv
    };

    if num_mcv > 0 {
        let values: Vec<String> = track[..num_mcv]
            .iter()
            .map(|&(_, first)| serialized(nonnull[order[first]]))
            .collect();
        let numbers: Vec<f32> = track[..num_mcv]
            .iter()
            .map(|&(count, _)| (count as f64 / samplerows as f64) as f32)
            .collect();
        slots.push(StatSlot {
            kind: KIND_MCV,
            op: facts.eq_opr,
            coll: facts.collation,
            numbers: Some(numbers),
            values: Some(values),
        });
    }

    // The histogram: the non-MCV values, evenly spaced.
    let mut num_hist = ndistinct - num_mcv;
    if num_hist > target {
        num_hist = target + 1;
    }
    if num_hist >= 2 {
        // Collapse the MCV items out of the sorted values, in MCV position
        // order.
        let mut mcv_sorted: Vec<usize> = (0..num_mcv).collect();
        mcv_sorted.sort_by_key(|&j| track[j].1);
        let mut keep: Vec<usize> = Vec::with_capacity(values_cnt);
        let mut src = 0usize;
        let mut j = 0usize;
        while src < values_cnt {
            if j < num_mcv {
                let first = track[mcv_sorted[j]].1;
                if src >= first {
                    src = first + track[mcv_sorted[j]].0;
                    j += 1;
                    continue;
                }
                keep.extend(src..first);
                src = first;
            } else {
                keep.extend(src..values_cnt);
                src = values_cnt;
            }
        }
        let nvals = keep.len();
        let delta = (nvals - 1) / (num_hist - 1);
        let deltafrac = (nvals - 1) % (num_hist - 1);
        let mut pos = 0usize;
        let mut posfrac = 0usize;
        let mut bounds = Vec::with_capacity(num_hist);
        for _ in 0..num_hist {
            bounds.push(serialized(nonnull[order[keep[pos]]]));
            pos += delta;
            posfrac += deltafrac;
            if posfrac >= num_hist - 1 {
                pos += 1;
                posfrac -= num_hist - 1;
            }
        }
        slots.push(StatSlot {
            kind: KIND_HISTOGRAM,
            op: facts.lt_opr,
            coll: facts.collation,
            numbers: None,
            values: Some(bounds),
        });
    }

    // The correlation of sorted order to sample order.
    if values_cnt > 1 {
        let n = values_cnt as f64;
        let corr_xsum = (n - 1.0) * n / 2.0;
        let corr_x2sum = (n - 1.0) * n * (2.0 * n - 1.0) / 6.0;
        let corr =
            (n * corr_xysum - corr_xsum * corr_xsum) / (n * corr_x2sum - corr_xsum * corr_xsum);
        slots.push(StatSlot {
            kind: KIND_CORRELATION,
            op: facts.lt_opr,
            coll: facts.collation,
            numbers: Some(vec![corr as f32]),
            values: None,
        });
    }

    n_distinct
}

/// PostgreSQL's `analyze_mcv_list`: drop the least common values that are not
/// significantly more common than the non-MCV values are.
fn analyze_mcv_list(
    counts: &[i64],
    n_distinct: f64,
    null_frac: f64,
    samplerows: usize,
    totalrows: f64,
) -> usize {
    // A full-table sample keeps the whole list.
    if samplerows as f64 >= totalrows || totalrows <= 1.0 {
        return counts.len();
    }
    let mut ndistinct_table = n_distinct;
    if ndistinct_table < 0.0 {
        ndistinct_table = -ndistinct_table * totalrows;
    }
    let mut num_mcv = counts.len();
    let mut sumcount: f64 = counts[..num_mcv.saturating_sub(1)]
        .iter()
        .map(|&count| count as f64)
        .sum();
    while num_mcv > 0 {
        let mut selec = 1.0 - sumcount / samplerows as f64 - null_frac;
        selec = selec.clamp(0.0, 1.0);
        let otherdistinct = ndistinct_table - (num_mcv as f64 - 1.0);
        if otherdistinct > 1.0 {
            selec /= otherdistinct;
        }
        let n = samplerows as f64;
        let big_n = totalrows;
        let k = big_n * counts[num_mcv - 1] as f64 / n;
        let variance = n * k * (big_n - k) * (big_n - n) / (big_n * big_n * (big_n - 1.0));
        let stddev = variance.max(0.0).sqrt();
        if counts[num_mcv - 1] as f64 > selec * n + 2.0 * stddev + 0.5 {
            break;
        }
        num_mcv -= 1;
        if num_mcv == 0 {
            break;
        }
        sumcount -= counts[num_mcv - 1] as f64;
    }
    num_mcv
}

/// PostgreSQL's `ts_typanalyze`: the most common lexemes of a tsvector
/// column, with their frequencies. The lexemes are stored by length, then
/// bytes, and the last two numbers are the smallest and the largest frequency.
fn tsvector_stats(nonnull: &[&Value], slots: &mut Vec<StatSlot>, target: usize) {
    let mut lexemes: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut nonnull_cnt = 0usize;
    for value in nonnull {
        let Value::Text(text) = value else {
            continue;
        };
        let Ok(vector) = crate::textsearch::parse_tsvector(text) else {
            continue;
        };
        nonnull_cnt += 1;
        for entry in &vector.entries {
            *lexemes.entry(entry.lexeme.clone()).or_default() += 1;
        }
    }
    if nonnull_cnt == 0 || lexemes.is_empty() {
        return;
    }
    let mut kept: Vec<(String, usize)> = lexemes.into_iter().collect();
    kept.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    if kept.len() > target * 10 {
        kept.truncate(target * 10);
    }
    let minfreq = kept.iter().map(|(_, count)| *count).min().unwrap_or(0);
    let maxfreq = kept.iter().map(|(_, count)| *count).max().unwrap_or(0);
    // By length, then bytes, as PostgreSQL sorts them for binary search.
    kept.sort_by(|a, b| a.0.len().cmp(&b.0.len()).then_with(|| a.0.cmp(&b.0)));
    let mut numbers: Vec<f32> = kept
        .iter()
        .map(|(_, count)| (*count as f64 / nonnull_cnt as f64) as f32)
        .collect();
    numbers.push((minfreq as f64 / nonnull_cnt as f64) as f32);
    numbers.push((maxfreq as f64 / nonnull_cnt as f64) as f32);
    let values: Vec<String> = kept
        .iter()
        .map(|(lexeme, _)| serialized(&Value::Text(lexeme.clone())))
        .collect();
    slots.push(StatSlot {
        kind: KIND_MCELEM,
        op: 98,
        coll: DEFAULT_COLLATION,
        numbers: Some(numbers),
        values: Some(values),
    });
}

/// PostgreSQL's `compute_array_stats`: the most common elements with their
/// frequencies, and a histogram of the distinct element counts.
fn array_stats(
    target: usize,
    nonnull: &[&Value],
    element_base: &str,
    element: &TypeFacts,
    slots: &mut Vec<StatSlot>,
) {
    let mut elements: std::collections::HashMap<String, (Value, usize)> =
        std::collections::HashMap::new();
    let mut counts: Vec<usize> = Vec::new();
    let mut element_no = 0usize;
    let mut null_elem_cnt = 0usize;
    let mut nonnull_cnt = 0usize;
    for value in nonnull {
        let Value::Array(items) = value else {
            continue;
        };
        nonnull_cnt += 1;
        // An element counts once per array, however often it repeats in it.
        let mut seen: Vec<String> = Vec::new();
        let mut distinct_elements = 0usize;
        for item in items {
            if matches!(item, Value::Null) {
                null_elem_cnt += 1;
                continue;
            }
            let key = serialized(item);
            if !seen.contains(&key) {
                seen.push(key.clone());
                distinct_elements += 1;
                // PostgreSQL counts each distinct element once per array.
                element_no += 1;
                elements.entry(key).or_insert_with(|| (item.clone(), 0)).1 += 1;
            }
        }
        counts.push(distinct_elements);
    }
    if nonnull_cnt == 0 {
        return;
    }
    // More than ten times the target elements: keep the most frequent ones.
    let mut kept: Vec<(Value, usize)> = elements.into_values().collect();
    kept.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| cmp_values(element_base, &a.0, &b.0))
    });
    if kept.len() > target * 10 {
        kept.truncate(target * 10);
    }
    let minfreq = kept.iter().map(|(_, count)| *count).min().unwrap_or(0);
    let maxfreq = kept.iter().map(|(_, count)| *count).max().unwrap_or(0);
    // The values are stored sorted by the element's own order; the last
    // three numbers are the minimum frequency, the maximum frequency and the
    // NULL element fraction.
    kept.sort_by(|a, b| cmp_values(element_base, &a.0, &b.0));
    if !kept.is_empty() {
        let mut numbers: Vec<f32> = kept
            .iter()
            .map(|(_, count)| (*count as f64 / nonnull_cnt as f64) as f32)
            .collect();
        numbers.push((minfreq as f64 / nonnull_cnt as f64) as f32);
        numbers.push((maxfreq as f64 / nonnull_cnt as f64) as f32);
        numbers.push((null_elem_cnt as f64 / nonnull_cnt as f64) as f32);
        let values: Vec<String> = kept.iter().map(|(value, _)| serialized(value)).collect();
        slots.push(StatSlot {
            kind: KIND_MCELEM,
            op: element.eq_opr,
            coll: element.collation,
            numbers: Some(numbers),
            values: Some(values),
        });
    }
    if !counts.is_empty() {
        // The histogram of distinct element counts: the counts' runs,
        // evenly spaced over the analyzed rows.
        let mut runs: Vec<(usize, usize)> = Vec::new();
        let mut sorted = counts.clone();
        sorted.sort_unstable();
        for count in sorted {
            match runs.last_mut() {
                Some((value, frequency)) if *value == count => *frequency += 1,
                _ => runs.push((count, 1)),
            }
        }
        let analyzed_rows = counts.len();
        let num_hist = target.max(2);
        let delta = analyzed_rows as i64 - 1;
        let mut hist: Vec<f32> = Vec::with_capacity(num_hist + 1);
        let mut j = 0usize;
        let mut frac = runs[0].1 as i64 * (num_hist as i64 - 1);
        for _ in 0..num_hist {
            while frac <= 0 {
                j += 1;
                frac += runs[j].1 as i64 * (num_hist as i64 - 1);
            }
            hist.push(runs[j].0 as f32);
            frac -= delta;
        }
        // The last number is the average element count per non-null row.
        hist.push((element_no as f64 / nonnull_cnt as f64) as f32);
        slots.push(StatSlot {
            kind: KIND_DECHIST,
            op: element.eq_opr,
            coll: element.collation,
            numbers: Some(hist),
            values: None,
        });
    }
}

/// PostgreSQL's `compute_range_stats`: the bounds histogram and the length
/// histogram.
fn report_range_stats(
    target: usize,
    kind: crate::ranges::Kind,
    nonnull: &[&Value],
    non_null_cnt: usize,
    slots: &mut Vec<StatSlot>,
) {
    let mut lowers: Vec<RangeEnd> = Vec::new();
    let mut uppers: Vec<RangeEnd> = Vec::new();
    let mut lengths: Vec<f64> = Vec::new();
    let mut empty_cnt = 0usize;
    for value in nonnull {
        let Value::Text(text) = value else {
            continue;
        };
        match crate::ranges::parse(text) {
            Ok(crate::ranges::Range::Bounds { lower, upper }) => {
                lengths.push(length_between(kind, &lower, &upper));
                lowers.push(RangeEnd::of(&lower));
                uppers.push(RangeEnd::of(&upper));
            }
            Ok(crate::ranges::Range::Empty) => empty_cnt += 1,
            Err(_) => {}
        }
    }
    let non_empty = lowers.len();
    if non_empty >= 2 {
        lowers.sort_by(|a, b| a.cmp_end(kind, b));
        uppers.sort_by(|a, b| a.cmp_end(kind, b));
        let mut num_hist = non_empty;
        if num_hist > target {
            num_hist = target + 1;
        }
        let delta = (non_empty - 1) / (num_hist - 1);
        let deltafrac = (non_empty - 1) % (num_hist - 1);
        let mut pos = 0usize;
        let mut posfrac = 0usize;
        let mut bounds = Vec::with_capacity(num_hist);
        for _ in 0..num_hist {
            bounds.push(serialized(&Value::Text(range_text(
                &lowers[pos],
                &uppers[pos],
            ))));
            pos += delta;
            posfrac += deltafrac;
            if posfrac >= num_hist - 1 {
                pos += 1;
                posfrac -= num_hist - 1;
            }
        }
        slots.push(StatSlot {
            kind: KIND_BOUNDS_HISTOGRAM,
            op: 0,
            coll: 0,
            numbers: None,
            values: Some(bounds),
        });
    }
    // The length histogram: an empty array when there are fewer than two
    // non-empty ranges. PostgreSQL stores the empty fraction either way.
    let mut length_values = Vec::new();
    if non_empty >= 2 {
        lengths.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mut num_hist = non_empty;
        if num_hist > target {
            num_hist = target + 1;
        }
        let delta = (non_empty - 1) / (num_hist - 1);
        let deltafrac = (non_empty - 1) % (num_hist - 1);
        let mut pos = 0usize;
        let mut posfrac = 0usize;
        for _ in 0..num_hist {
            length_values.push(serialized(&Value::Float(lengths[pos])));
            pos += delta;
            posfrac += deltafrac;
            if posfrac >= num_hist - 1 {
                pos += 1;
                posfrac -= num_hist - 1;
            }
        }
    }
    let empty_frac = if non_null_cnt > 0 {
        (empty_cnt as f64 / non_null_cnt as f64) as f32
    } else {
        0.0
    };
    slots.push(StatSlot {
        kind: KIND_RANGE_LENGTH_HISTOGRAM,
        op: FLOAT8_LT,
        coll: 0,
        numbers: Some(vec![empty_frac]),
        values: Some(length_values),
    });
}

/// A range's end in the form the sorts and histograms use: its value text, or
/// none when unbounded, and whether the endpoint is included.
#[derive(Clone)]
struct RangeEnd {
    value: Option<String>,
    inclusive: bool,
}

impl RangeEnd {
    fn of(bound: &crate::ranges::Bound) -> RangeEnd {
        RangeEnd {
            value: bound.value.clone(),
            inclusive: bound.inclusive,
        }
    }

    /// Two ends of one kind of range in PostgreSQL's bound order: by value,
    /// then with an inclusive bound above an exclusive one.
    fn cmp_end(&self, kind: crate::ranges::Kind, other: &RangeEnd) -> std::cmp::Ordering {
        match (&self.value, &other.value) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(a), Some(b)) => {
                crate::ranges::cmp_value(kind, a, b).then(self.inclusive.cmp(&other.inclusive))
            }
        }
    }
}

/// The length (`subdiff`) between a range's bounds, as the range's subtype
/// defines it: the difference of the two values, infinite when either end is.
fn length_between(
    kind: crate::ranges::Kind,
    lower: &crate::ranges::Bound,
    upper: &crate::ranges::Bound,
) -> f64 {
    let (Some(lower), Some(upper)) = (&lower.value, &upper.value) else {
        return f64::INFINITY;
    };
    match kind {
        crate::ranges::Kind::Int4 | crate::ranges::Kind::Int8 => {
            match (lower.parse::<i64>(), upper.parse::<i64>()) {
                (Ok(l), Ok(u)) => (u - l) as f64,
                _ => f64::INFINITY,
            }
        }
        crate::ranges::Kind::Numeric => {
            match (
                crate::value::parse_decimal(lower),
                crate::value::parse_decimal(upper),
            ) {
                (Some(l), Some(u)) => u.to_f64() - l.to_f64(),
                _ => f64::INFINITY,
            }
        }
        crate::ranges::Kind::Date => {
            match (
                chrono::NaiveDate::parse_from_str(lower, "%Y-%m-%d"),
                chrono::NaiveDate::parse_from_str(upper, "%Y-%m-%d"),
            ) {
                (Ok(l), Ok(u)) => (u - l).num_days() as f64,
                _ => f64::INFINITY,
            }
        }
        crate::ranges::Kind::Timestamp | crate::ranges::Kind::TimestampTz => {
            // The difference in seconds, as `tsrange_subdiff` returns it.
            match (timestamp_micros(lower), timestamp_micros(upper)) {
                (Some(l), Some(u)) => (u - l) as f64 / 1_000_000.0,
                _ => f64::INFINITY,
            }
        }
    }
}

/// A timestamp's microseconds since the epoch, for the length difference.
fn timestamp_micros(text: &str) -> Option<i64> {
    match crate::datetime::Temporal::parse_as(text, crate::datetime::Kind::Timestamp)
        .or_else(|| crate::datetime::Temporal::parse_as(text, crate::datetime::Kind::TimestampTz))
    {
        Some(crate::datetime::Temporal::Timestamp(ts)) => Some(ts.and_utc().timestamp_micros()),
        Some(crate::datetime::Temporal::TimestampTz(ts)) => Some(ts.timestamp_micros()),
        _ => None,
    }
}

/// A range in PostgreSQL's bracket notation from its two sorted ends.
fn range_text(lower: &RangeEnd, upper: &RangeEnd) -> String {
    crate::ranges::format_range(&crate::ranges::Range::Bounds {
        lower: crate::ranges::Bound {
            value: lower.value.clone(),
            inclusive: lower.inclusive,
        },
        upper: crate::ranges::Bound {
            value: upper.value.clone(),
            inclusive: upper.inclusive,
        },
    })
}

/// The statistics facts of a type, from PostgreSQL's catalogs: its fixed
/// width (negative: a varlena type), whether the type has a default btree
/// operator class with an `=` and a `<`, the two operators, and the collation
/// the statistics are computed with (the database's default for the
/// collatable types, none otherwise).
#[derive(Clone)]
struct TypeFacts {
    typlen: i32,
    ordered: bool,
    eq_opr: i64,
    lt_opr: i64,
    collation: i64,
    name: &'static str,
    /// The type's alignment (`pg_type.typalign`) in bytes.
    align: i32,
}

/// PostgreSQL's default collation (`DEFAULT_COLLATION_OID`), which its
/// statistics record for the collatable types.
const DEFAULT_COLLATION: i64 = 100;

fn type_facts(base: &str) -> TypeFacts {
    let (name, typlen, ordered, eq_opr, lt_opr, collation, align): (
        &str,
        i32,
        bool,
        i64,
        i64,
        i64,
        i32,
    ) = match base {
        "bool" | "boolean" => ("bool", 1, true, 91, 58, 0, 1),
        "bytea" => ("bytea", -1, true, 1955, 1957, 0, 4),
        "char" | "pg_char" => ("char", 1, true, 92, 631, 0, 1),
        "name" => ("name", 64, true, 254, 660, 950, 1),
        "int8" | "bigint" => ("int8", 8, true, 410, 412, 0, 8),
        "int2" | "smallint" => ("int2", 2, true, 94, 95, 0, 2),
        "int4" | "int" | "integer" | "serial" => ("int4", 4, true, 96, 97, 0, 4),
        "text" => ("text", -1, true, 98, 664, DEFAULT_COLLATION, 4),
        "oid" => ("oid", 4, true, 607, 609, 0, 4),
        "float4" | "real" => ("float4", 4, true, 620, 622, 0, 4),
        "float8" | "float" | "double" => ("float8", 8, true, 670, 672, 0, 8),
        "bpchar" | "character" => ("bpchar", -1, true, 1054, 1058, DEFAULT_COLLATION, 4),
        "varchar" => ("varchar", -1, true, 98, 664, DEFAULT_COLLATION, 4),
        "bit" => ("bit", -1, true, 1784, 1786, 0, 4),
        "varbit" => ("varbit", -1, true, 1804, 1806, 0, 4),
        "date" => ("date", 4, true, 1093, 1095, 0, 4),
        "time" => ("time", 8, true, 1108, 1110, 0, 8),
        "timetz" => ("timetz", 12, true, 1550, 1552, 0, 8),
        "timestamp" => ("timestamp", 8, true, 2060, 2062, 0, 8),
        "timestamptz" => ("timestamptz", 8, true, 1320, 1322, 0, 8),
        "interval" => ("interval", 16, true, 1330, 1332, 0, 8),
        "numeric" | "decimal" => ("numeric", -1, true, 1752, 1754, 0, 4),
        "uuid" => ("uuid", 16, true, 2972, 2974, 0, 1),
        "jsonb" => ("jsonb", -1, true, 3240, 3242, 0, 4),
        "tsvector" => ("tsvector", -1, true, 3629, 3627, 0, 4),
        "tsquery" => ("tsquery", -1, true, 3676, 3674, 0, 4),
        "inet" => ("inet", -1, true, 1201, 1203, 0, 4),
        // cidr has no default btree operator class: trivial statistics.
        "cidr" => ("cidr", -1, false, 0, 0, 0, 4),
        "macaddr" => ("macaddr", 6, true, 1220, 1222, 0, 4),
        "macaddr8" => ("macaddr8", 8, true, 3362, 3364, 0, 4),
        "money" => ("money", 8, true, 900, 902, 0, 8),
        // The geometric types: fixed or variable width, and no btree order.
        "point" => ("point", 16, false, 0, 0, 0, 8),
        "box" => ("box", 32, false, 0, 0, 0, 8),
        "lseg" => ("lseg", 32, false, 0, 0, 0, 8),
        "line" => ("line", 24, false, 0, 0, 0, 8),
        "circle" => ("circle", 24, false, 0, 0, 0, 8),
        "path" => ("path", -1, false, 0, 0, 0, 8),
        "polygon" => ("polygon", -1, false, 0, 0, 0, 8),
        // Types without a default btree operator class: PostgreSQL
        // records only the null fraction, the width and a zero distinct
        // count (its trivial statistics).
        _ => ("other", -1, false, 0, 0, 0, 4),
    };
    TypeFacts {
        typlen,
        ordered,
        eq_opr,
        lt_opr,
        collation,
        name,
        align,
    }
}

/// Splits a data type into its base name and whether it is an array.
fn split_array_type(data_type: &str) -> (String, bool) {
    let normalized = data_type
        .trim()
        .trim_matches('"')
        .to_ascii_uppercase()
        .replace("CHARACTER VARYING", "VARCHAR")
        .replace("DOUBLE PRECISION", "DOUBLE")
        .replace("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ")
        .replace("TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP")
        .replace("TIME WITH TIME ZONE", "TIMETZ")
        .replace("TIME WITHOUT TIME ZONE", "TIME");
    let normalized = match normalized.strip_prefix('_') {
        Some(base) if !normalized.ends_with("[]") => format!("{base}[]"),
        _ => normalized,
    };
    let is_array = normalized.ends_with("[]");
    let base = normalized
        .trim_end_matches("[]")
        .split('(')
        .next()
        .unwrap_or("TEXT")
        .trim()
        .to_ascii_lowercase();
    (base, is_array)
}

/// The bytes a value occupies in a PostgreSQL row, for the average width:
/// the datum as it sits in a heap tuple, which packs small variable-width
/// values with a one-byte header.
fn value_width(
    value: &Value,
    facts: &TypeFacts,
    element: &TypeFacts,
    is_array: bool,
    range: Option<crate::ranges::Kind>,
) -> f64 {
    if !is_array && facts.typlen > 0 {
        return facts.typlen as f64;
    }
    if is_array {
        // A PostgreSQL array datum: `ArrayType`'s header, the dimension
        // header, an optional null bitmap, and the elements, each on its own
        // alignment.
        let Value::Array(items) = value else {
            return 0.0;
        };
        let bitmap = ((items.len() + 7) / 8) as i32;
        let has_null = items.iter().any(|item| matches!(item, Value::Null));
        let mut size = if has_null {
            maxalign(16 + 8 + bitmap)
        } else {
            maxalign(16 + 8)
        };
        for item in items {
            if matches!(item, Value::Null) {
                continue;
            }
            size = align_to(size, element.align) + packed_size(item, element);
        }
        return stored_size(size) as f64;
    }
    if let Some(kind) = range {
        // A range datum: `RangeType`'s aligned header, each present bound on
        // its subtype's alignment, and the flag byte.
        let Value::Text(text) = value else {
            return 0.0;
        };
        let mut size = 8i32;
        if let Ok(crate::ranges::Range::Bounds { lower, upper }) = crate::ranges::parse(text) {
            let subtype = type_facts(&kind.subtype().to_ascii_lowercase());
            for bound in [&lower, &upper] {
                let Some(bound_text) = &bound.value else {
                    continue;
                };
                size = align_to(size, subtype.align)
                    + packed_size(&Value::Text(bound_text.clone()), &subtype);
            }
        }
        size += 1;
        return stored_size(size) as f64;
    }
    if let Some(bits) = bit_width(&facts.name, value) {
        return stored_size(bits) as f64;
    }
    match value {
        // A cidr or inet value: a header and its address.
        Value::Text(text) if matches!(facts.name, "inet" | "cidr") => {
            stored_size(4 + 2 + if text.contains(':') { 16 } else { 4 }) as f64
        }
        // A tsvector's stored size, which its own module computes.
        Value::Text(text) if facts.name == "tsvector" => {
            match crate::textsearch::parse_tsvector(text) {
                Ok(vector) => stored_size(vector.stored_size() as i32) as f64,
                Err(_) => varlena_size(text.len() as i32),
            }
        }
        Value::Text(text) => varlena_size(text.len() as i32),
        Value::Json(text) => varlena_size(text.len() as i32),
        Value::Bytea(bytes) => varlena_size(bytes.len() as i32),
        // jsonb's binary form is its own container layout; the text's size
        // is the closest estimate without re-encoding it.
        Value::Jsonb(json) => 4.0 + json_text_size(json),
        Value::Numeric(decimal) => numeric_size(decimal) as f64,
        other => varlena_size(crate::render(other).len() as i32),
    }
}

/// A `bit(n)`/`varbit(n)` datum's stored size: a header and the bit count.
fn bit_width(name: &str, value: &Value) -> Option<i32> {
    if !matches!(name, "bit" | "varbit") {
        return None;
    }
    let Value::Text(bits) = value else {
        return None;
    };
    Some(4 + 4 + ((bits.chars().count() + 7) / 8) as i32)
}

/// A rough jsonb container size: the version header, the container's own
/// header and the keys and values as the text renders them.
fn json_text_size(json: &serde_json::Value) -> f64 {
    fn walk(value: &serde_json::Value) -> f64 {
        match value {
            serde_json::Value::Object(map) => {
                let mut size = 4.0;
                for (key, value) in map {
                    size += 4.0 + varlena_size(key.len() as i32) + walk(value);
                }
                size
            }
            serde_json::Value::Array(items) => {
                let mut size = 4.0;
                for item in items {
                    size += walk(item);
                }
                size
            }
            serde_json::Value::String(text) => 4.0 + varlena_size(text.len() as i32),
            serde_json::Value::Null | serde_json::Value::Bool(_) => 4.0,
            value => {
                4.0 + numeric_size(
                    &crate::value::parse_decimal(&value.to_string())
                        .unwrap_or_else(crate::numeric::Numeric::zero),
                ) as f64
            }
        }
    }
    4.0 + walk(json) - 3.0
}

/// `size` rounded up to 8 bytes, as PostgreSQL aligns headers.
fn maxalign(size: i32) -> i32 {
    align_to(size, 8)
}

/// `size` rounded up to `align` bytes.
fn align_to(size: i32, align: i32) -> i32 {
    if align <= 1 {
        return size;
    }
    (size + align - 1) / align * align
}

/// The bytes a value takes when packed into an array or a range: its stored
/// size, without a tuple's alignment padding.
fn packed_size(value: &Value, facts: &TypeFacts) -> i32 {
    if facts.typlen > 0 {
        return facts.typlen;
    }
    match value {
        Value::Text(text) => varlena_size(text.len() as i32) as i32,
        Value::Json(text) => varlena_size(text.len() as i32) as i32,
        Value::Bytea(bytes) => varlena_size(bytes.len() as i32) as i32,
        Value::Numeric(decimal) => numeric_size(decimal),
        other => varlena_size(crate::render(other).len() as i32) as i32,
    }
}

/// A variable-width datum's size as a tuple stores it: a short header saves
/// three bytes.
fn stored_size(size: i32) -> i32 {
    if size <= 126 { size - 3 } else { size }
}

/// The size of a varlena datum: a 1-byte header up to 126 bytes, 4 beyond.
fn varlena_size(len: i32) -> f64 {
    if len <= 126 {
        (len + 1) as f64
    } else {
        (len + 4) as f64
    }
}

/// The size of a `numeric` datum as PostgreSQL packs it: a short header and
/// two bytes per base-10000 digit group, leading and trailing zero groups
/// stripped.
fn numeric_size(decimal: &crate::numeric::Numeric) -> i32 {
    let text = decimal.to_string();
    let text = text.strip_prefix('-').unwrap_or(&text).to_string();
    let (int_part, frac_part) = match text.split_once('.') {
        Some((int_part, frac_part)) => (int_part.to_string(), frac_part.to_string()),
        None => (text, String::new()),
    };
    let digits = |part: &str| -> Vec<i64> {
        part.chars()
            .map(|c| c.to_digit(10).unwrap_or(0) as i64)
            .collect()
    };
    let int_digits = digits(&int_part);
    let frac_digits = digits(&frac_part);
    let mut groups: Vec<i64> = Vec::new();
    let int_groups = (int_digits.len() + 3) / 4;
    let mut padded = vec![0i64; int_groups * 4 - int_digits.len()];
    padded.extend(&int_digits);
    for chunk in padded.chunks(4) {
        groups.push(chunk.iter().fold(0i64, |acc, &d| acc * 10 + d));
    }
    let mut frac_padded = frac_digits;
    while frac_padded.len() % 4 != 0 {
        frac_padded.push(0);
    }
    for chunk in frac_padded.chunks(4) {
        groups.push(chunk.iter().fold(0i64, |acc, &d| acc * 10 + d));
    }
    while groups.first() == Some(&0) {
        groups.remove(0);
    }
    while groups.last() == Some(&0) {
        groups.pop();
    }
    // A short datum: a 1-byte varlena header, the sign/scale and weight
    // bytes, then the digit groups.
    if groups.is_empty() {
        2
    } else {
        2 * groups.len() as i32 + 3
    }
}

/// The average width PostgreSQL stores: a fixed type's width even when the
/// sample held no non-null value, the total truncated otherwise.
fn width_of(facts: &TypeFacts, total_width: f64, nonnull: usize) -> i32 {
    if facts.typlen > 0 {
        facts.typlen
    } else if nonnull == 0 {
        0
    } else {
        (total_width / nonnull as f64) as i32
    }
}
