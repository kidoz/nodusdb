//! Index keys. An index's key parts are table columns and, for an index on
//! expressions (`CREATE INDEX ON users (lower(email))`), expressions over
//! the row. As in PostgreSQL's `pg_index.indkey`, a key part that is an
//! expression has no column: its `column_id` is [`EXPRESSION_KEY`], and the
//! index's `expressions` hold those parts' SQL in order.

use std::cell::RefCell;
use std::collections::HashMap;

use nodus_catalog::{ColumnId, IndexColumn, IndexDescriptor, TableDescriptor};

use crate::{MemExecutor, ScalarExpr, Value};

/// The column of a key part that is an expression.
pub(crate) const EXPRESSION_KEY: ColumnId = ColumnId(uuid::Uuid::nil());

/// One part of an index's key.
pub(crate) enum KeyPart {
    /// The table column at this position.
    Column(usize),
    /// An expression, and its SQL.
    Expression(ScalarExpr, String),
}

/// Whether an index has expressions among its key parts.
pub(crate) fn has_expressions(index: &IndexDescriptor) -> bool {
    index.key_columns.iter().any(is_expression_key)
}

pub(crate) fn is_expression_key(key: &IndexColumn) -> bool {
    key.column_id == EXPRESSION_KEY
}

/// An expression's SQL as an expression over a row, parsed once per thread.
pub(crate) fn parse_expression(sql: &str) -> Option<ScalarExpr> {
    thread_local! {
        static PARSED: RefCell<HashMap<String, Option<ScalarExpr>>> = RefCell::new(HashMap::new());
    }
    if let Some(hit) = PARSED.with(|p| p.borrow().get(sql).cloned()) {
        return hit;
    }
    let parsed = sqlparser::parser::Parser::new(&sqlparser::dialect::PostgreSqlDialect {})
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_expr())
        .ok()
        .and_then(|e| crate::planner::lower_scalar(&e, &[]))
        .map(|e| crate::result_types::check_integer_ranges(&e, &|_: &str| None));
    PARSED.with(|p| {
        let mut p = p.borrow_mut();
        if p.len() >= 1024 {
            p.clear();
        }
        p.insert(sql.to_string(), parsed.clone());
    });
    parsed
}

/// An index's key parts, in order.
pub(crate) fn index_parts(table: &TableDescriptor, index: &IndexDescriptor) -> Vec<KeyPart> {
    let mut expressions = index.expressions.iter();
    index
        .key_columns
        .iter()
        .filter_map(|key| {
            if is_expression_key(key) {
                let sql = expressions.next()?.sql.clone();
                let expr = parse_expression(&sql).unwrap_or(ScalarExpr::Literal(Value::Null));
                Some(KeyPart::Expression(expr, sql))
            } else {
                table
                    .columns
                    .iter()
                    .position(|c| c.id == key.column_id)
                    .map(KeyPart::Column)
            }
        })
        .collect()
}

/// The key parts' values for a row of the table.
pub(crate) fn key_values(table: &TableDescriptor, parts: &[KeyPart], row: &[Value]) -> Vec<Value> {
    let names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
    parts
        .iter()
        .map(|part| match part {
            KeyPart::Column(p) => row.get(*p).cloned().unwrap_or(Value::Null),
            KeyPart::Expression(expr, _) => crate::eval_scalar_expr(expr, row, &names),
        })
        .collect()
}

/// The key parts as a unique violation's `DETAIL` names them.
pub(crate) fn key_names(table: &TableDescriptor, parts: &[KeyPart]) -> Vec<String> {
    parts
        .iter()
        .map(|part| match part {
            KeyPart::Column(p) => table.columns[*p].name.clone(),
            KeyPart::Expression(_, sql) => deparse_sql(sql),
        })
        .collect()
}

/// A key's parts written as SQL (`lower(email), id`), each as the parser
/// prints it back, in order of that text: what an `ON CONFLICT` target is
/// matched on.
pub(crate) fn key_list(sql: &str) -> Vec<String> {
    let parsed = sqlparser::parser::Parser::new(&sqlparser::dialect::PostgreSqlDialect {})
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_comma_separated(sqlparser::parser::Parser::parse_expr));
    let mut list: Vec<String> = match parsed {
        Ok(exprs) => exprs.iter().map(canonical_text).collect(),
        Err(_) => vec![sql.to_string()],
    };
    list.sort();
    list
}

/// An index's key parts as [`key_list`] gives a target's.
pub(crate) fn index_key_list(table: &TableDescriptor, index: &IndexDescriptor) -> Vec<String> {
    let mut list: Vec<String> = index_parts(table, index)
        .iter()
        .map(|part| match part {
            KeyPart::Column(p) => table.columns[*p].name.clone(),
            KeyPart::Expression(_, sql) => {
                parse_sql_expr(sql).map_or_else(|| sql.clone(), |e| canonical_text(&e))
            }
        })
        .collect();
    list.sort();
    list
}

fn canonical_text(expr: &sqlparser::ast::Expr) -> String {
    let mut expr = expr;
    while let sqlparser::ast::Expr::Nested(inner) = expr {
        expr = inner;
    }
    match expr {
        sqlparser::ast::Expr::Identifier(id) => id.value.clone(),
        other => other.to_string(),
    }
}

/// What an index on an expression is named after, as PostgreSQL names it:
/// a function call's function, else `expr`.
pub(crate) fn expression_label(expr: &sqlparser::ast::Expr) -> String {
    let mut expr = expr;
    while let sqlparser::ast::Expr::Nested(inner) = expr {
        expr = inner;
    }
    match expr {
        sqlparser::ast::Expr::Function(f) => f
            .name
            .0
            .last()
            .and_then(|p| p.as_ident())
            .map_or_else(|| "expr".to_string(), |i| i.value.clone()),
        _ => "expr".to_string(),
    }
}

/// An index key part's expression as `pg_get_indexdef` shows it: a
/// function call as it is, anything else in parentheses.
pub(crate) fn index_expression_text(sql: &str) -> String {
    let Some(expr) = parse_sql_expr(sql) else {
        return sql.to_string();
    };
    let mut bare = &expr;
    while let sqlparser::ast::Expr::Nested(inner) = bare {
        bare = inner;
    }
    let text = deparse(bare, false);
    match bare {
        sqlparser::ast::Expr::Function(_) => text,
        _ => format!("({text})"),
    }
}

fn parse_sql_expr(sql: &str) -> Option<sqlparser::ast::Expr> {
    sqlparser::parser::Parser::new(&sqlparser::dialect::PostgreSqlDialect {})
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_expr())
        .ok()
}

/// A domain constraint's SQL as PostgreSQL prints it back, its value as
/// `VALUE`.
pub(crate) fn deparse_domain_sql(sql: &str) -> String {
    parse_sql_expr(sql).map_or_else(|| sql.to_string(), |e| deparse(&e, true))
}

/// An expression's SQL as PostgreSQL prints it back (`pg_get_indexdef`,
/// `pg_get_constraintdef`): each operation in parentheses, a string
/// literal with its type.
pub(crate) fn deparse_sql(sql: &str) -> String {
    parse_sql_expr(sql).map_or_else(|| sql.to_string(), |e| deparse(&e, false))
}

fn deparse(expr: &sqlparser::ast::Expr, domain: bool) -> String {
    use sqlparser::ast::{BinaryOperator as B, Expr, Value as V};
    match expr {
        // A domain's constraint names its value `VALUE`.
        Expr::Identifier(id) if domain && id.quote_style.is_none() && id.value == "value" => {
            "VALUE".to_string()
        }
        Expr::Identifier(id) => quote_ident(&id.value),
        Expr::CompoundIdentifier(ids) => ids
            .iter()
            .map(|i| quote_ident(&i.value))
            .collect::<Vec<_>>()
            .join("."),
        Expr::Value(v) => match &v.value {
            V::SingleQuotedString(s) => format!("'{}'::text", s.replace('\'', "''")),
            other => other.to_string(),
        },
        Expr::Nested(inner) => deparse(inner, domain),
        Expr::BinaryOp { left, op, right } => {
            let op = match op {
                B::NotEq => "<>".to_string(),
                other => other.to_string(),
            };
            format!(
                "({} {op} {})",
                deparse(left, domain),
                deparse(right, domain)
            )
        }
        Expr::Like {
            negated,
            expr,
            pattern,
            escape_char: None,
            any: false,
        } => format!(
            "({} {} {})",
            deparse(expr, domain),
            if *negated { "!~~" } else { "~~" },
            deparse(pattern, domain)
        ),
        Expr::ILike {
            negated,
            expr,
            pattern,
            escape_char: None,
            any: false,
        } => format!(
            "({} {} {})",
            deparse(expr, domain),
            if *negated { "!~~*" } else { "~~*" },
            deparse(pattern, domain)
        ),
        Expr::IsNull(inner) => format!("({} IS NULL)", deparse(inner, domain)),
        Expr::IsNotNull(inner) => format!("({} IS NOT NULL)", deparse(inner, domain)),
        Expr::Function(f) => {
            let args = match &f.args {
                sqlparser::ast::FunctionArguments::List(list) => list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => deparse(e, domain),
                        other => other.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
                sqlparser::ast::FunctionArguments::None => return f.to_string(),
                other => other.to_string(),
            };
            format!("{}({args})", f.name.to_string().to_ascii_lowercase())
        }
        other => other.to_string(),
    }
}

/// An identifier as PostgreSQL prints it: quoted unless it is a plain
/// lower-case name.
fn quote_ident(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// Whether `idx` covers `needed` (column names, unqualified): every one is
/// a key column or an `INCLUDE` column, so its rows can come from the index
/// alone.
pub(crate) fn index_covers(
    tbl: &TableDescriptor,
    idx: &IndexDescriptor,
    needed: &[String],
) -> bool {
    let covered: Vec<nodus_catalog::ColumnId> = idx
        .key_columns
        .iter()
        .map(|key| key.column_id)
        .chain(idx.include_columns.iter().copied())
        .collect();
    needed.iter().all(|name| {
        tbl.columns
            .iter()
            .find(|c| &c.name == name)
            .is_some_and(|c| covered.contains(&c.id))
    })
}

/// Whether a type's values are encoded order-preservingly (see
/// [`key_bytes`]): plain numbers, booleans, text, and the temporal kinds.
/// Types whose PostgreSQL order their text does not follow (numeric,
/// interval, arrays, ranges, network, geometric, JSON, search vectors)
/// keep the legacy encoding and stay equality-only.
pub(crate) fn order_encoded_type(data_type: &str) -> bool {
    let t = data_type.trim().to_ascii_uppercase();
    if t.contains("INTERVAL")
        || crate::ranges::is_range_type(data_type)
        || crate::multiranges::is_multirange_type(data_type)
        || crate::net::is_net_type(data_type)
        || crate::geometric::is_geometric_type(data_type)
        || crate::textsearch::is_tsvector_type(data_type)
        || crate::textsearch::is_tsquery_type(data_type)
    {
        return false;
    }
    t.contains("INT")
        || t.contains("SERIAL")
        || t.contains("FLOAT")
        || t.contains("DOUBLE")
        || t.contains("REAL")
        || t.contains("BOOL")
        || t.contains("TEXT")
        || t.contains("VARCHAR")
        || t.contains("CHAR")
        || t.contains("DATE")
        || t.contains("TIME")
        || t.trim() == "NAME"
        || t.trim() == "UUID"
        || t.trim() == "OID"
        || t.trim() == "XID"
}

/// The order-preserving bytes of a key value, or `None` for a type without
/// one (whose entries keep the legacy text encoding and are equality-only).
/// The encoding is `marker + payload + 0x00`, so a value's bytes are never a
/// prefix of another's: NULL (after every value), then ints (8 bytes,
/// sign-flipped), floats (IEEE bits with the sign trick), booleans, text
/// (UTF-8, as our byte-wise order compares it), and the temporal kinds
/// (days or microseconds since their epoch).
pub(crate) fn key_bytes(value: &Value, data_type: &str) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    match value {
        Value::Null => {
            out.push(0x02);
            return Some(out);
        }
        Value::Int(n) => {
            out.push(0x01);
            out.extend(((*n as u64) ^ (1 << 63)).to_be_bytes());
        }
        Value::Float(f) => {
            out.push(0x01);
            let bits = f.to_bits();
            let flipped = if bits & (1 << 63) != 0 {
                !bits
            } else {
                bits ^ (1 << 63)
            };
            out.extend(flipped.to_be_bytes());
        }
        Value::Bool(b) => {
            out.push(0x01);
            out.push(u8::from(*b));
        }
        Value::Text(text) => {
            // A temporal column's text is an instant; range scans over
            // dates and timestamps compare instants, not letters.
            let temporal = match crate::datetime::Kind::of_type(data_type) {
                Some(crate::datetime::Kind::Date) => {
                    crate::datetime::Temporal::parse_as(text, crate::datetime::Kind::Date)
                }
                Some(
                    kind @ (crate::datetime::Kind::Timestamp | crate::datetime::Kind::TimestampTz),
                ) => crate::datetime::Temporal::parse_as(text, kind),
                _ => None,
            };
            match temporal {
                Some(crate::datetime::Temporal::Date(date)) => {
                    out.push(0x01);
                    let days = date
                        .signed_duration_since(chrono::NaiveDate::default())
                        .num_days();
                    out.extend(((days as u64) ^ (1 << 63)).to_be_bytes());
                }
                Some(
                    crate::datetime::Temporal::Timestamp(ts)
                    | crate::datetime::Temporal::TimestampTz(ts),
                ) => {
                    out.push(0x01);
                    let micros = ts.and_utc().timestamp_micros();
                    out.extend(((micros as u64) ^ (1 << 63)).to_be_bytes());
                }
                _ => {
                    out.push(0x01);
                    out.extend(text.as_bytes());
                }
            }
        }
        // Numeric, arrays, and the types our order does not encode keep the
        // legacy encoding.
        _ => return None,
    }
    out.push(0x00);
    Some(out)
}

/// The index a value's entries are keyed by, order-preserving: lowercase
/// hex is not used (it would not sort); uppercase `[0-9A-F]` sorts as the
/// bytes do.
pub(crate) fn key_component(value: &Value, data_type: &str) -> Option<String> {
    let bytes = key_bytes(value, data_type)?;
    Some(
        bytes
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<String>(),
    )
}

/// The bounds a filter puts on `column`: `(value, inclusive)` lower and
/// upper ends, from `<`, `<=`, `>`, `>=` predicates (a `BETWEEN` is their
/// conjunction). `None` when it bounds nothing.
pub(crate) fn range_bounds(
    filter: &crate::plan_types::FilterExpr,
    column: &str,
) -> Option<(Option<(Value, bool)>, Option<(Value, bool)>)> {
    use crate::plan_types::FilterExpr;
    /// Tightens the bounds with one predicate, when it bounds the column.
    fn tighten(
        filter: &FilterExpr,
        column: &str,
        lower: &mut Option<(Value, bool)>,
        upper: &mut Option<(Value, bool)>,
    ) {
        use crate::plan_types::{CompareOp, Operand};
        let FilterExpr::Predicate(predicate) = filter else {
            return;
        };
        let left = predicate.left.rsplit('.').next().unwrap_or(&predicate.left);
        if left != column {
            return;
        }
        let Operand::Literal(value) = &predicate.right else {
            return;
        };
        let stricter = |kept: Option<(Value, bool)>,
                        candidate: (Value, bool),
                        lower_end: bool|
         -> Option<(Value, bool)> {
            match kept {
                None => Some(candidate),
                Some(kept) => {
                    let ord = crate::value::compare(&candidate.0, &kept.0);
                    // An exclusive bound is the stricter one at a tie.
                    let take = if lower_end {
                        ord == std::cmp::Ordering::Greater
                            || (ord == std::cmp::Ordering::Equal && !candidate.1 && kept.1)
                    } else {
                        ord == std::cmp::Ordering::Less
                            || (ord == std::cmp::Ordering::Equal && !candidate.1 && kept.1)
                    };
                    Some(if take { candidate } else { kept })
                }
            }
        };
        match predicate.op {
            CompareOp::Gt => *lower = stricter(lower.take(), (value.clone(), false), true),
            CompareOp::Ge => *lower = stricter(lower.take(), (value.clone(), true), true),
            CompareOp::Lt => *upper = stricter(upper.take(), (value.clone(), false), false),
            CompareOp::Le => *upper = stricter(upper.take(), (value.clone(), true), false),
            _ => {}
        }
    }
    let mut lower: Option<(Value, bool)> = None;
    let mut upper: Option<(Value, bool)> = None;
    // A conjunction may bound both ends (`BETWEEN`); an `OR` bounds nothing.
    let mut current = Some(filter);
    while let Some(filter) = current {
        match filter {
            FilterExpr::And(left, right) => {
                tighten(left, column, &mut lower, &mut upper);
                current = Some(right);
            }
            other => {
                tighten(other, column, &mut lower, &mut upper);
                current = None;
            }
        }
    }
    (lower.is_some() || upper.is_some()).then_some((lower, upper))
}

/// The columns a sort's keys read, when they are plain columns (`None`
/// otherwise, so nothing index-only reads an unknown column).
pub(crate) fn sort_columns(
    sort: &[crate::plan_types::SortKey],
    projection: &[crate::plan_types::ProjectionItem],
) -> Option<Vec<String>> {
    use crate::plan_types::{ProjectionItem, SortTarget};
    let bare = |name: &str| name.rsplit('.').next().unwrap_or(name).to_string();
    let mut out = Vec::new();
    for key in sort {
        match &key.target {
            SortTarget::Name(name) => out.push(bare(name)),
            SortTarget::Output(at) => match projection.get(*at)? {
                ProjectionItem::Column(name) | ProjectionItem::AliasedColumn(name, _) => {
                    out.push(bare(name))
                }
                _ => return None,
            },
            SortTarget::Expr(_) => return None,
        }
    }
    Some(out)
}

/// The conjuncts of a filter: its `AND` chain, an `OR` kept whole.
pub(crate) fn conjuncts(
    filter: &crate::plan_types::FilterExpr,
) -> Vec<&crate::plan_types::FilterExpr> {
    use crate::plan_types::FilterExpr;
    fn walk<'a>(filter: &'a FilterExpr, out: &mut Vec<&'a FilterExpr>) {
        match filter {
            FilterExpr::And(left, right) => {
                walk(left, out);
                walk(right, out);
            }
            other => out.push(other),
        }
    }
    let mut out = Vec::new();
    walk(filter, &mut out);
    out
}

/// The literal an equality predicate on `column` compares it with, found in
/// a conjunction (`a = 5 AND b > 3` bounds `a`).
pub(crate) fn equality_operand<'a>(
    filter: &'a crate::plan_types::FilterExpr,
    column: &str,
) -> Option<&'a crate::plan_types::Operand> {
    use crate::plan_types::{CompareOp, FilterExpr, Operand};
    let mut current = Some(filter);
    while let Some(filter) = current {
        match filter {
            FilterExpr::And(left, right) => {
                if let FilterExpr::Predicate(predicate) = &**left
                    && predicate.op == CompareOp::Eq
                    && predicate.left.rsplit('.').next().unwrap_or(&predicate.left) == column
                    && matches!(predicate.right, Operand::Literal(_))
                {
                    return Some(&predicate.right);
                }
                current = Some(right);
            }
            other => {
                if let FilterExpr::Predicate(predicate) = other
                    && predicate.op == CompareOp::Eq
                    && predicate.left.rsplit('.').next().unwrap_or(&predicate.left) == column
                    && matches!(predicate.right, Operand::Literal(_))
                {
                    return Some(&predicate.right);
                }
                current = None;
            }
        }
    }
    None
}

/// The index an equality lookup on `leading` uses: the first covering every
/// column the statement reads, when it reads only those, else the first over
/// the leading column.
pub(crate) fn equality_index<'a>(
    tbl: &TableDescriptor,
    indexes: &'a [IndexDescriptor],
    leading: nodus_catalog::ColumnId,
    needed: Option<&[String]>,
) -> Option<&'a IndexDescriptor> {
    let matches = |idx: &IndexDescriptor| {
        idx.key_columns
            .first()
            .is_some_and(|key| key.column_id == leading)
    };
    if let Some(needed) = needed
        && let Some(covering) = indexes
            .iter()
            .find(|idx| matches(idx) && index_covers(tbl, idx, needed))
    {
        return Some(covering);
    }
    indexes.iter().find(|idx| matches(idx))
}

/// The columns a scan reads when it reads only named columns with no sort,
/// grouping, or join: `None` when the statement is more than that, or reads
/// a column that is not named (so no index-only scan).
pub(crate) fn index_only_needed(
    projection: &[crate::plan_types::ProjectionItem],
    filter: Option<&crate::plan_types::FilterExpr>,
) -> Option<Vec<String>> {
    use crate::plan_types::ProjectionItem;
    // No projection is every column.
    if projection.is_empty() {
        return None;
    }
    let mut needed = Vec::new();
    for item in projection {
        match item {
            ProjectionItem::Column(name) | ProjectionItem::AliasedColumn(name, _) => {
                needed.push(name.rsplit('.').next().unwrap_or(name).to_string())
            }
            _ => return None,
        }
    }
    if let Some(filter) = filter {
        let mut refs = Vec::new();
        crate::filter_eval::filter_column_refs(filter, &mut refs);
        needed.extend(
            refs.into_iter()
                .map(|name| name.rsplit('.').next().unwrap_or(name.as_str()).to_string()),
        );
    }
    Some(needed)
}

impl MemExecutor {
    /// The payload an index entry carries: the `INCLUDE` columns' values, in
    /// their order, so an index-only scan can read them; empty for an index
    /// without any.
    pub(crate) fn index_entry_payload(
        tbl: &TableDescriptor,
        idx: &IndexDescriptor,
        row: &[Value],
    ) -> String {
        if idx.include_columns.is_empty() {
            return String::new();
        }
        let values: Vec<Value> = idx
            .include_columns
            .iter()
            .map(|id| {
                tbl.columns
                    .iter()
                    .position(|c| c.id == *id)
                    .and_then(|at| row.get(at))
                    .cloned()
                    .unwrap_or(Value::Null)
            })
            .collect();
        crate::value::encode_row(&values).unwrap_or_default()
    }

    /// The value an index's entry for a row is kept under: its leading key
    /// part's (`None` for an index without parts).
    pub(crate) fn index_leading_value(
        tbl: &TableDescriptor,
        idx: &IndexDescriptor,
        row: &[Value],
    ) -> Option<Value> {
        let first = idx.key_columns.first()?;
        if is_expression_key(first) {
            let parts = index_parts(tbl, idx);
            return key_values(tbl, &parts[..1.min(parts.len())], row)
                .into_iter()
                .next();
        }
        let pos = tbl.columns.iter().position(|c| c.id == first.column_id)?;
        Some(row.get(pos).cloned().unwrap_or(Value::Null))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_encodings_order_as_their_values_do() {
        // The order-preserving encoding sorts as the values do, including
        // negatives, NULLs (last), and prefixes of text.
        let encoded = |value: &Value, data_type: &str| key_component(value, data_type).unwrap();
        assert!(encoded(&Value::Int(-5), "int4") < encoded(&Value::Int(0), "int4"));
        assert!(encoded(&Value::Int(9), "int4") < encoded(&Value::Int(10), "int4"));
        assert!(encoded(&Value::Int(-10), "int4") < encoded(&Value::Int(-5), "int4"));
        assert!(encoded(&Value::Float(-1.5), "float8") < encoded(&Value::Float(0.5), "float8"));
        assert!(encoded(&Value::Bool(false), "bool") < encoded(&Value::Bool(true), "bool"));
        let text = |s: &str| Value::Text(s.to_string());
        assert!(encoded(&text("ab"), "text") < encoded(&text("b"), "text"));
        assert!(encoded(&text("a"), "text") < encoded(&text("ab"), "text"));
        assert!(encoded(&text("z"), "text") < encoded(&Value::Null, "text"));
        // A date's text sorts as the instant, not the letters.
        assert!(encoded(&text("2020-02-01"), "date") < encoded(&text("2020-10-01"), "date"));
        // A type without the encoding stays equality-only.
        assert!(!order_encoded_type("numeric"));
        assert!(order_encoded_type("text"));
        assert!(!order_encoded_type("interval"));
    }

    #[test]
    fn range_bounds_come_from_conjuncts() {
        use crate::plan_types::LogicalPlan;
        let bounds = |statement: &str| {
            let mut statements = nodus_sql::parse_sql(statement).unwrap();
            let plan = crate::planner::plan_statement(&statements.remove(0), &[]).unwrap();
            let LogicalPlan::Select { filter, .. } = plan else {
                panic!("a select");
            };
            crate::index_keys::range_bounds(filter.as_ref().unwrap(), "a")
        };
        let (lower, upper) = bounds("select * from z where a > 5").unwrap();
        assert_eq!(lower, Some((Value::Int(5), false)));
        assert_eq!(upper, None);
        let (lower, upper) = bounds("select * from z where a between 5 and 9").unwrap();
        assert_eq!(lower, Some((Value::Int(5), true)));
        assert_eq!(upper, Some((Value::Int(9), true)));
        // The tighter bound of a conjunction wins.
        let (lower, _) = bounds("select * from z where a >= 5 and a > 7").unwrap();
        assert_eq!(lower, Some((Value::Int(7), false)));
        // An `OR` bounds nothing.
        assert!(bounds("select * from z where a > 5 or a < 1").is_none());
        assert!(bounds("select * from z where b > 5").is_none());
        // The equality of a conjunction is found too.
        let mut statements = nodus_sql::parse_sql("select * from z where a = 5 and b > 3").unwrap();
        let plan = crate::planner::plan_statement(&statements.remove(0), &[]).unwrap();
        let LogicalPlan::Select { filter, .. } = plan else {
            panic!("a select");
        };
        assert!(crate::index_keys::equality_operand(filter.as_ref().unwrap(), "a").is_some());
    }

    #[test]
    fn expressions_print_as_postgresql_prints_them() {
        assert_eq!(index_expression_text("lower(email)"), "lower(email)");
        assert_eq!(
            index_expression_text("(first || ' ' || last)"),
            "(((first || ' '::text) || last))"
        );
        assert_eq!(deparse_domain_sql("value > 0"), "(VALUE > 0)");
        assert_eq!(
            deparse_domain_sql("value LIKE '%@%'"),
            "(VALUE ~~ '%@%'::text)"
        );
        assert_eq!(deparse_sql("lower(value)"), "lower(value)");
        assert_eq!(
            expression_label(&parse_sql_expr("(lower(e))").unwrap()),
            "lower"
        );
        assert_eq!(expression_label(&parse_sql_expr("a + b").unwrap()), "expr");
    }
}
